/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_attribute.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pathnodes.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>
#include <planner.h>
#include <planner/planner.h>
#include <utils/palloc.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "columnar_scan.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/arrow_tts.h"
#include "compression/compression.h"
#include "compression/compressionam_handler.h"
#include "import/ts_explain.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/vector_predicates.h"

typedef enum VectorizedQualResult
{
	VQualSegmentFiltered,
	VQualRowFiltered,
	VQualRowMatch,
} VectorizedQualResult;

typedef struct VectorizedExprState
{
	List *vectorized_quals_constified;
	uint16 num_results;
	int bitmap_bytes_alloced;
	int bitmap_bytes;
	uint64 *vector_qual_result;
} VectorizedExprState;

typedef struct ColumnarScanState
{
	CustomScanState css;
	VectorizedExprState vexprstate;
	ScanKey scankeys;
	int nscankeys;
	List *scankey_quals;
	List *vectorized_quals_orig;
	bool have_constant_false_vectorized_qual;
} ColumnarScanState;

static bool
match_relvar(Expr *expr, Index relid)
{
	if (IsA(expr, Var))
	{
		Var *v = castNode(Var, expr);

		if ((Index) v->varno == relid)
			return true;
	}
	return false;
}

static ScanKey
build_scan_keys(const CompressionAmInfo *caminfo, Index relid, List *clauses, int *num_keys,
				List **scankey_quals)
{
	ListCell *lc;
	ScanKey scankeys = palloc0(sizeof(ScanKeyData) * list_length(clauses));
	int nkeys = 0;

	foreach (lc, clauses)
	{
		Expr *qual = lfirst(lc);

		/* ignore volatile expressions */
		if (contain_volatile_functions((Node *) qual))
			continue;

		switch (nodeTag(qual))
		{
			case T_OpExpr:
			{
				OpExpr *opexpr = castNode(OpExpr, qual);
				Oid opno = opexpr->opno;
				Expr *leftop, *rightop, *expr = NULL;
				Var *relvar = NULL;
				Datum scanvalue = 0;
				bool argfound = false;

				if (list_length(opexpr->args) != 2)
					break;

				leftop = linitial(opexpr->args);
				rightop = lsecond(opexpr->args);

				/* Strip any relabeling */
				if (IsA(leftop, RelabelType))
					leftop = ((RelabelType *) leftop)->arg;
				if (IsA(rightop, RelabelType))
					rightop = ((RelabelType *) rightop)->arg;

				if (match_relvar(leftop, relid))
				{
					relvar = castNode(Var, leftop);
					expr = rightop;
				}
				else if (match_relvar(rightop, relid))
				{
					relvar = castNode(Var, rightop);
					expr = leftop;
					opno = get_commutator(opno);
				}
				else
					break;

				if (!OidIsValid(opno) || !op_strict(opno))
					break;

				Assert(expr != NULL);

				if (IsA(expr, Const))
				{
					Const *c = castNode(Const, expr);
					scanvalue = c->constvalue;
					argfound = true;
				}

				bool is_segmentby =
					caminfo->columns[AttrNumberGetAttrOffset(relvar->varattno)].is_segmentby;
				if (argfound && is_segmentby)
				{
					TypeCacheEntry *tce =
						lookup_type_cache(relvar->vartype, TYPECACHE_BTREE_OPFAMILY);
					int op_strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

					if (op_strategy != InvalidStrategy)
					{
						Oid op_lefttype;
						Oid op_righttype;

						get_op_opfamily_properties(opno,
												   tce->btree_opf,
												   false,
												   &op_strategy,
												   &op_lefttype,
												   &op_righttype);

						Assert(relvar != NULL);

						ScanKeyEntryInitialize(&scankeys[nkeys++],
											   0 /* flags */,
											   relvar->varattno,	/* attribute number to scan */
											   op_strategy,			/* op's strategy */
											   op_righttype,		/* strategy subtype */
											   opexpr->inputcollid, /* collation */
											   opexpr->opfuncid,	/* reg proc to use */
											   scanvalue);			/* constant */

						/* Append to quals list for explain */
						*scankey_quals = lappend(*scankey_quals, qual);
					}
				}
				break;
			}

			default:
				break;
		}
	}

	if (nkeys > 0)
	{
		*num_keys = nkeys;
		return scankeys;
	}

	*num_keys = 0;
	pfree(scankeys);

	return NULL;
}

static bool
compute_vector_quals(VectorizedExprState *vexprstate, ExprContext *econtext)
{
	TupleTableSlot *slot = econtext->ecxt_scantuple;

	if (vexprstate->vectorized_quals_constified == NIL || !TTS_IS_ARROWTUPLE(slot))
		return true;

	Assert(!TTS_EMPTY(slot));
	Assert(arrow_slot_row_index(slot) == 1 || arrow_slot_row_index(slot) == InvalidTupleIndex);

	/*
	 * Allocate the bitmap that will hold the vectorized qual results. We will
	 * initialize it to all ones and AND the individual quals to it.
	 */
	vexprstate->num_results = arrow_slot_total_row_count(slot);
	Assert(vexprstate->num_results > 0);
	const int bitmap_bytes = sizeof(uint64) * ((vexprstate->num_results + 63) / 64);

	MemoryContext oldmcxt = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

	if (vexprstate->vector_qual_result == NULL)
	{
		vexprstate->vector_qual_result = palloc(bitmap_bytes);
		vexprstate->bitmap_bytes_alloced = bitmap_bytes;
	}
	else if (vexprstate->bitmap_bytes_alloced < bitmap_bytes)
	{
		vexprstate->vector_qual_result = repalloc(vexprstate->vector_qual_result, bitmap_bytes);
		vexprstate->bitmap_bytes_alloced = bitmap_bytes;
	}

	MemoryContextSwitchTo(oldmcxt);
	vexprstate->bitmap_bytes = bitmap_bytes;
	memset(vexprstate->vector_qual_result, 0xFF, vexprstate->bitmap_bytes_alloced);

	if (vexprstate->num_results % 64 != 0)
	{
		/*
		 * We have to zero out the bits for past-the-end elements in the last
		 * bitmap word. Since all predicates are ANDed to the result bitmap,
		 * we can do it here once instead of doing it in each predicate.
		 */
		const uint64 mask = ((uint64) -1) >> (64 - vexprstate->num_results % 64);
		vexprstate->vector_qual_result[vexprstate->num_results / 64] = mask;
	}

	/*
	 * Compute the quals.
	 */
	ListCell *lc;
	foreach (lc, vexprstate->vectorized_quals_constified)
	{
		/*
		 * For now we support "Var ? Const" predicates and
		 * ScalarArrayOperations.
		 */
		List *args = NULL;
		RegProcedure vector_const_opcode = InvalidOid;
		ScalarArrayOpExpr *saop = NULL;
		OpExpr *opexpr = NULL;
		if (IsA(lfirst(lc), ScalarArrayOpExpr))
		{
			saop = castNode(ScalarArrayOpExpr, lfirst(lc));
			args = saop->args;
			vector_const_opcode = get_opcode(saop->opno);
		}
		else
		{
			opexpr = castNode(OpExpr, lfirst(lc));
			args = opexpr->args;
			vector_const_opcode = get_opcode(opexpr->opno);
		}

		/*
		 * Find the vector_const predicate.
		 */
		VectorPredicate *vector_const_predicate = get_vector_const_predicate(vector_const_opcode);
		Assert(vector_const_predicate != NULL);

		/*
		 * Find the compressed column referred to by the Var.
		 */
		Var *var = castNode(Var, linitial(args));

		/*
		 * Prepare to compute the vector predicate. We have to handle the
		 * default values in a special way because they don't produce the usual
		 * decompressed ArrowArrays.
		 */
		uint64 default_value_predicate_result;
		uint64 *predicate_result = vexprstate->vector_qual_result;
		int attoff = AttrNumberGetAttrOffset(var->varattno);
		const ArrowArray *values = arrow_slot_get_array(slot, var->varattno);
		const ArrowArray *vector = values;

		if (vector == NULL)
		{
			Form_pg_attribute attr = &slot->tts_tupleDescriptor->attrs[attoff];
			/*
			 * A regular (non-compressed) value or a compressed column with a
			 * default value. We can't fall back to the non-vectorized quals
			 * now, so build a single-value ArrowArray with this (default)
			 * value, check if it passes the predicate, and apply it to the
			 * entire batch.
			 */
			vector = make_single_value_arrow(attr->atttypid,
											 slot->tts_values[attoff],
											 slot->tts_isnull[attoff]);

			/*
			 * We start from an all-valid bitmap, because the predicate is
			 * AND-ed to it.
			 */
			default_value_predicate_result = 1;
			predicate_result = &default_value_predicate_result;
		}

		/*
		 * The vectorizable predicates should be STRICT, so we shouldn't see null
		 * constants here.
		 */
		Const *constnode = castNode(Const, lsecond(args));
		Ensure(!constnode->constisnull, "vectorized predicate called for a null value");

		/*
		 * At last, compute the predicate.
		 */
		if (saop)
		{
			vector_array_predicate(vector_const_predicate,
								   saop->useOr,
								   vector,
								   constnode->constvalue,
								   predicate_result);
		}
		else
		{
			vector_const_predicate(vector, constnode->constvalue, predicate_result);
		}

		/* Account for nulls which shouldn't pass the predicate. */
		const size_t n = vector->length;
		const size_t n_words = (n + 63) / 64;
		const uint64 *restrict validity = (uint64 *restrict) vector->buffers[0];

		if (validity != NULL)
		{
			for (size_t i = 0; i < n_words; i++)
			{
				predicate_result[i] &= validity[i];
			}
		}
		else
		{
			Assert(vector->null_count == 0);
		}

		/* Process the result */
		if (values == NULL)
		{
			/* The column had a default value. */

			if (!(default_value_predicate_result & 1))
			{
				/*
				 * We had a default value for the compressed column, and it
				 * didn't pass the predicate, so the entire batch didn't pass.
				 */
				for (int i = 0; i < bitmap_bytes / 8; i++)
				{
					vexprstate->vector_qual_result[i] = 0;
				}
			}
		}

		/*
		 * Have to return whether we have any passing rows.
		 */
		bool have_passing_rows = false;

		for (int i = 0; i < bitmap_bytes / 8; i++)
		{
			if (vexprstate->vector_qual_result[i])
			{
				/* At least one row passed the qual, so no need to check rest
				 * of rows/bitmap */
				have_passing_rows = true;
				break;
			}
		}

		/* No rows passed the qual so no need to check other quals */
		if (!have_passing_rows)
			return false;
	}

	return true;
}

static VectorizedQualResult
ExecVectorizedQual(VectorizedExprState *vexprstate, ExprContext *econtext)
{
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	uint16 rowindex = arrow_slot_row_index(slot);

	/* Compute the vector quals over both compressed and non-compressed
	 * tuples. In case a non-compressed tuple is filtered, return
	 * VQualRowFiltered since it is only one row */
	if (rowindex <= 1 && !compute_vector_quals(vexprstate, econtext))
		return (rowindex == 1) ? VQualSegmentFiltered : VQualRowFiltered;

	if (vexprstate->vector_qual_result != NULL &&
		!arrow_row_is_valid(vexprstate->vector_qual_result, arrow_slot_arrow_offset(slot)))
		return VQualRowFiltered;

	return VQualRowMatch;
}

static TupleTableSlot *
columnar_next(ColumnarScanState *state)
{
	TableScanDesc scandesc;
	EState *estate;
	ExprContext *econtext;
	ScanDirection direction;
	TupleTableSlot *slot;

	scandesc = state->css.ss.ss_currentScanDesc;
	estate = state->css.ss.ps.state;
	econtext = state->css.ss.ps.ps_ExprContext;
	direction = estate->es_direction;
	slot = state->css.ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		ColumnarScanState *cstate = (ColumnarScanState *) state;
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(state->css.ss.ss_currentRelation,
								   estate->es_snapshot,
								   cstate->nscankeys,
								   cstate->scankeys);
		state->css.ss.ss_currentScanDesc = scandesc;

		/* Remove filter execution if all quals are used as scankeys on the
		 * compressed relation */
		if (scandesc->rs_nkeys == list_length(state->css.ss.ps.plan->qual))
		{
			/* Reset expression state for executing quals */
			state->css.ss.ps.qual = NULL;
			/* Need to reset qual expression too to remove "Filter:" from EXPLAIN */
			state->css.ss.ps.plan->qual = NIL;
		}
	}

	/*
	 * Scan tuples and apply vectorized filters
	 */
	while (table_scan_getnextslot(scandesc, direction, slot))
	{
		if (!likely(TTS_IS_ARROWTUPLE(slot)))
			return slot;

		econtext->ecxt_scantuple = slot;

		switch (ExecVectorizedQual(&state->vexprstate, econtext))
		{
			case VQualSegmentFiltered:
				arrow_slot_mark_consumed(slot);
				InstrCountTuples2(state, 1);
				InstrCountFiltered1(state, arrow_slot_total_row_count(slot));
				break;
			case VQualRowFiltered:
				InstrCountFiltered1(state, 1);
				break;
			case VQualRowMatch:
				return slot;
		}
	}

	return NULL;
}

/*
 * recheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
columnar_recheck(SeqScanState *state, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	elog(ERROR, "%s not implemented", __FUNCTION__);
	return true;
}

static void
columnar_scan_begin(CustomScanState *state, EState *estate, int eflags)
{
	ColumnarScanState *cstate = (ColumnarScanState *) state;
	Scan *scan = (Scan *) state->ss.ps.plan;
	Relation rel = state->ss.ss_currentRelation;
	const CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);

	/* The CustomScan state always creates a scan slot of type TTSOpsVirtual,
	 * even if one sets scan->scanrelid to a valid index to indicate scan of a
	 * base relation. This might be a bug in the custom scan state
	 * implementation. To ensure the base relation's scan slot type is used,
	 * we recreate the scan slot here with the slot type used by the
	 * underlying base relation. It is not necessary (or possible) to drop the
	 * existing slot since it is registered in the tuple table and will be
	 * released when the executor finishes. */
	ExecInitScanTupleSlot(estate,
						  &state->ss,
						  RelationGetDescr(rel),
						  table_slot_callbacks(state->ss.ss_currentRelation));

	/* Must reinitialize projection for the new slot type as well, including
	 * ExecQual state for the new slot */
	ExecInitResultTypeTL(&state->ss.ps);
	ExecAssignScanProjectionInfo(&state->ss);
	state->ss.ps.qual = ExecInitQual(scan->plan.qual, (PlanState *) state);
	cstate->scankeys = build_scan_keys(caminfo,
									   scan->scanrelid,
									   scan->plan.qual,
									   &cstate->nscankeys,
									   &cstate->scankey_quals);

	/* Constify stable expressions in vectorized predicates. */
	cstate->have_constant_false_vectorized_qual = false;

	PlannerGlobal glob = {
		.boundParams = state->ss.ps.state->es_param_list_info,
	};
	PlannerInfo root = {
		.glob = &glob,
	};
	ListCell *lc;
	foreach (lc, cstate->vectorized_quals_orig)
	{
		Node *constified = estimate_expression_value(&root, (Node *) lfirst(lc));

		/*
		 * Note that some expressions are evaluated to a null Const, like a
		 * strict comparison with stable expression that evaluates to null. If
		 * we have such filter, no rows can pass, so we set a special flag to
		 * return early.
		 */
		if (IsA(constified, Const))
		{
			Const *c = castNode(Const, constified);
			if (c->constisnull || !DatumGetBool(c->constvalue))
			{
				cstate->have_constant_false_vectorized_qual = true;
				break;
			}
			else
			{
				/*
				 * This is a constant true qual, every row passes and we can
				 * just ignore it. No idea how it can happen though.
				 */
				Assert(false);
				continue;
			}
		}

		List *args;
		if (IsA(constified, OpExpr))
		{
			args = castNode(OpExpr, constified)->args;
		}
		else
		{
			args = castNode(ScalarArrayOpExpr, constified)->args;
		}
		Ensure(IsA(lsecond(args), Const),
			   "failed to evaluate runtime constant in vectorized filter");
		cstate->vexprstate.vectorized_quals_constified =
			lappend(cstate->vexprstate.vectorized_quals_constified, constified);
	}
}

static TupleTableSlot *
columnar_scan_exec(CustomScanState *state)
{
	return ExecScan(&state->ss,
					(ExecScanAccessMtd) columnar_next,
					(ExecScanRecheckMtd) columnar_recheck);
}

static void
columnar_scan_end(CustomScanState *state)
{
	TableScanDesc scandesc = state->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&state->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (state->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(state->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(state->ss.ss_ScanTupleSlot);

	/*
	 * close the scan
	 */
	if (scandesc)
		table_endscan(scandesc);
}

static void
columnar_scan_rescan(CustomScanState *state)
{
	TableScanDesc scandesc = state->ss.ss_currentScanDesc;

	if (NULL != scandesc)
		table_rescan(scandesc, /* scan desc */
					 NULL);	   /* new scan keys */

	ExecScanReScan((ScanState *) state);
}

static void
columnar_scan_explain(CustomScanState *state, List *ancestors, ExplainState *es)
{
	ColumnarScanState *cstate = (ColumnarScanState *) state;

	if (cstate->scankey_quals != NIL)
		ts_show_scan_qual(cstate->scankey_quals, "Scankey", &state->ss.ps, ancestors, es);

	ts_show_scan_qual(cstate->vectorized_quals_orig,
					  "Vectorized Filter",
					  &state->ss.ps,
					  ancestors,
					  es);

	if (!state->ss.ps.plan->qual && cstate->vectorized_quals_orig)
	{
		/*
		 * The normal explain won't show this if there are no normal quals but
		 * only the vectorized ones.
		 */
		ts_show_instrumentation_count("Rows Removed by Filter", 1, &state->ss.ps, es);
	}

	if (es->analyze && es->verbose &&
		(state->ss.ps.instrument->ntuples2 > 0 || es->format != EXPLAIN_FORMAT_TEXT))
	{
		ExplainPropertyFloat("Batches Removed by Filter",
							 NULL,
							 state->ss.ps.instrument->ntuples2,
							 0,
							 es);
	}
}

static CustomExecMethods columnar_scan_state_methods = {
	.BeginCustomScan = columnar_scan_begin,
	.ExecCustomScan = columnar_scan_exec, /* To be determined later. */
	.EndCustomScan = columnar_scan_end,
	.ReScanCustomScan = columnar_scan_rescan,
	.ExplainCustomScan = columnar_scan_explain,
};

static Node *
columnar_scan_state_create(CustomScan *cscan)
{
	ColumnarScanState *cstate;

	cstate = (ColumnarScanState *) newNode(sizeof(ColumnarScanState), T_CustomScanState);
	cstate->css.methods = &columnar_scan_state_methods;
	cstate->vectorized_quals_orig = linitial(cscan->custom_exprs);

	return (Node *) cstate;
}

static CustomScanMethods columnar_scan_plan_methods = {
	.CustomName = "ColumnarScan",
	.CreateCustomScanState = columnar_scan_state_create,
};

static bool
contains_volatile_functions_checker(Oid func_id, void *context)
{
	return (func_volatile(func_id) == PROVOLATILE_VOLATILE);
}

static bool
is_not_runtime_constant_walker(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	switch (nodeTag(node))
	{
		case T_Var:
		case T_PlaceHolderVar:
		case T_Param:
			/*
			 * We might want to support these nodes to have vectorizable
			 * join clauses (T_Var), join clauses referencing a variable that is
			 * above outer join (T_PlaceHolderVar) or initplan parameters and
			 * prepared statement parameters (T_Param). We don't support them at
			 * the moment.
			 */
			return true;
		default:
			if (check_functions_in_node(node,
										contains_volatile_functions_checker,
										/* context = */ NULL))
			{
				return true;
			}
			return expression_tree_walker(node,
										  is_not_runtime_constant_walker,
										  /* context = */ NULL);
	}
}

/*
 * Check if the given node is a run-time constant, i.e. it doesn't contain
 * volatile functions or variables or parameters. This means we can evaluate
 * it at run time, allowing us to apply the vectorized comparison operators
 * that have the form "Var op Const". This applies for example to filter
 * expressions like `time > now() - interval '1 hour'`.
 * Note that we do the same evaluation when doing run time chunk exclusion, but
 * there is no good way to pass the evaluated clauses to the underlying nodes
 * like this DecompressChunk node.
 */
static bool
is_not_runtime_constant(Node *node)
{
	bool result = is_not_runtime_constant_walker(node, /* context = */ NULL);
	return result;
}

/*
 * Try to check if the current qual is vectorizable, and if needed make a
 * commuted copy. If not, return NULL.
 */
static Node *
make_vectorized_qual(Node *qual, const CompressionAmInfo *caminfo)
{
	/*
	 * Currently we vectorize some "Var op Const" binary predicates,
	 * and scalar array operations with these predicates.
	 */
	if (!IsA(qual, OpExpr) && !IsA(qual, ScalarArrayOpExpr))
	{
		return NULL;
	}

	List *args = NIL;
	OpExpr *opexpr = NULL;
	Oid opno = InvalidOid;
	ScalarArrayOpExpr *saop = NULL;
	if (IsA(qual, OpExpr))
	{
		opexpr = castNode(OpExpr, qual);
		args = opexpr->args;
		opno = opexpr->opno;
	}
	else
	{
		saop = castNode(ScalarArrayOpExpr, qual);
		args = saop->args;
		opno = saop->opno;
	}

	if (list_length(args) != 2)
	{
		return NULL;
	}

	if (opexpr && IsA(lsecond(args), Var))
	{
		/*
		 * Try to commute the operator if we have Var on the right.
		 */
		opno = get_commutator(opno);
		if (!OidIsValid(opno))
		{
			return NULL;
		}

		opexpr = (OpExpr *) copyObject(opexpr);
		opexpr->opno = opno;
		/*
		 * opfuncid is a cache, we can set it to InvalidOid like the
		 * CommuteOpExpr() does.
		 */
		opexpr->opfuncid = InvalidOid;
		args = list_make2(lsecond(args), linitial(args));
		opexpr->args = args;
	}

	/*
	 * We can vectorize the operation where the left side is a Var and the right
	 * side is a constant or can be evaluated to a constant at run time (e.g.
	 * contains stable functions).
	 */
	if (!IsA(linitial(args), Var) || is_not_runtime_constant(lsecond(args)))
	{
		return NULL;
	}

	Var *var = castNode(Var, linitial(args));

	/*
	 * ExecQual is performed before ExecProject and operates on the decompressed
	 * scan slot, so the qual attnos are the uncompressed chunk attnos.
	 */

	const ColumnCompressionSettings *column =
		&caminfo->columns[AttrNumberGetAttrOffset(var->varattno)];

	Assert(column->attnum == var->varattno);

	bool is_arrow_column =
		(!column->is_segmentby && column->attnum != InvalidAttrNumber &&
		 tsl_get_decompress_all_function(compression_get_default_algorithm(column->typid),
										 column->typid) != NULL);

	if (!is_arrow_column)
	{
		/* This column doesn't support bulk decompression. */
		return NULL;
	}

	Oid opcode = get_opcode(opno);
	if (!get_vector_const_predicate(opcode))
	{
		return NULL;
	}

#if PG14_GE
	if (saop)
	{
		if (saop->hashfuncid)
		{
			/*
			 * Don't vectorize if the planner decided to build a hash table.
			 */
			return NULL;
		}
	}
#endif

	return opexpr ? (Node *) opexpr : (Node *) saop;
}

static Plan *
columnar_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
						  List *scan_clauses, List *custom_plans)
{
	CustomScan *columnar_scan_plan = makeNode(CustomScan);
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	columnar_scan_plan->flags = best_path->flags;
	columnar_scan_plan->methods = &columnar_scan_plan_methods;
	columnar_scan_plan->scan.scanrelid = rel->relid;

	/* output target list */
	columnar_scan_plan->scan.plan.targetlist = tlist;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	Relation relation = RelationIdGetRelation(rte->relid);
	const CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(relation);
	List *vectorized_quals = NIL;
	List *nonvectorized_quals = NIL;
	ListCell *lc;

	foreach (lc, scan_clauses)
	{
		Node *source_qual = lfirst(lc);
		Node *vectorized_qual = make_vectorized_qual(source_qual, caminfo);

		if (vectorized_qual)
			vectorized_quals = lappend(vectorized_quals, vectorized_qual);
		else
			nonvectorized_quals = lappend(nonvectorized_quals, source_qual);
	}

	RelationClose(relation);

	columnar_scan_plan->scan.plan.qual = nonvectorized_quals;
	columnar_scan_plan->custom_exprs = list_make1(vectorized_quals);

	return &columnar_scan_plan->scan.plan;
}

static CustomPathMethods columnar_scan_path_methods = {
	.CustomName = "ColumnarScan",
	.PlanCustomPath = columnar_scan_plan_create,
};

static void
cost_columnar_scan(Path *path, PlannerInfo *root, RelOptInfo *rel)
{
	cost_seqscan(path, root, rel, path->param_info);

	/* Just make it a bit cheaper than seqscan for now */
	path->startup_cost *= 0.9;
	path->total_cost *= 0.9;
}

ColumnarScanPath *
columnar_scan_path_create(PlannerInfo *root, RelOptInfo *rel, Relids required_outer,
						  int parallel_workers)
{
	ColumnarScanPath *cspath;
	Path *path;

	cspath = (ColumnarScanPath *) newNode(sizeof(ColumnarScanPath), T_CustomPath);
	path = &cspath->custom_path.path;
	path->pathtype = T_CustomScan;
	path->parent = rel;
	path->pathtarget = rel->reltarget;
	path->param_info = get_baserel_parampathinfo(root, rel, required_outer);
	path->parallel_aware = false; // TODO: Implement parallel support
								  // (parallel_workers > 0);
	path->parallel_safe = rel->consider_parallel;
	path->parallel_workers = parallel_workers;
	path->pathkeys = NIL; /* currently has unordered result, but if pathkeys
						   * match the orderby,segmentby settings we could do
						   * ordering */

	cspath->custom_path.flags = 0;
	cspath->custom_path.methods = &columnar_scan_path_methods;

	cost_columnar_scan(path, root, rel);

	return cspath;
}

void
columnar_scan_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht)
{
	ColumnarScanPath *cspath;
	Relids required_outer;

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;
	cspath = columnar_scan_path_create(root, rel, required_outer, 0);
	add_path(rel, &cspath->custom_path.path);
}

void
_columnar_scan_init(void)
{
	TryRegisterCustomScanMethods(&columnar_scan_plan_methods);
}
