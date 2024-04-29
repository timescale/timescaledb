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
#include "compression/compression.h"
#include "hyperstore/arrow_tts.h"
#include "hyperstore/hyperstore_handler.h"
#include "import/ts_explain.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/vector_predicates.h"

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

/*
 * Extract clauses that can be used as scan keys from the scan qualifiers.
 *
 * Remaining qualifiers will be used as filters for the ColumnarScan.
 */
static ScanKey
extract_scan_keys(const HyperstoreInfo *caminfo, Scan *scan, int *num_keys, List **scankey_quals)
{
	ListCell *lc;
	ScanKey scankeys = palloc0(sizeof(ScanKeyData) * list_length(scan->plan.qual));
	int nkeys = 0;
	const Index relid = scan->scanrelid;

	foreach (lc, scan->plan.qual)
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

						/* Append to quals list for explain and delete it from
						 * the list of qualifiers. */
						*scankey_quals = lappend(*scankey_quals, qual);
						scan->plan.qual = foreach_delete_current(scan->plan.qual, lc);
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

static void
compute_plain_qual(VectorizedExprState *vexprstate, ExprContext *econtext, Node *qual,
				   uint64 *restrict result)
{
	/*
	 * Some predicates can be evaluated to a Const at run time.
	 */
	if (IsA(qual, Const))
	{
		Const *c = castNode(Const, qual);
		if (c->constisnull || !DatumGetBool(c->constvalue))
		{
			/*
			 * Some predicates are evaluated to a null Const, like a
			 * strict comparison with stable expression that evaluates to null.
			 * No rows pass.
			 */
			const size_t n_batch_result_words = (vexprstate->num_results + 63) / 64;
			for (size_t i = 0; i < n_batch_result_words; i++)
			{
				result[i] = 0;
			}
		}
		else
		{
			/*
			 * This is a constant true qual, every row passes and we can
			 * just ignore it. No idea how it can happen though.
			 */
			Assert(false);
		}
		return;
	}

	/*
	 * For now, we support NullTest, "Var ? Const" predicates and
	 * ScalarArrayOperations.
	 */
	List *args = NULL;
	RegProcedure vector_const_opcode = InvalidOid;
	ScalarArrayOpExpr *saop = NULL;
	OpExpr *opexpr = NULL;
	NullTest *nulltest = NULL;
	if (IsA(qual, NullTest))
	{
		nulltest = castNode(NullTest, qual);
		args = list_make1(nulltest->arg);
	}
	else if (IsA(qual, ScalarArrayOpExpr))
	{
		saop = castNode(ScalarArrayOpExpr, qual);
		args = saop->args;
		vector_const_opcode = get_opcode(saop->opno);
	}
	else
	{
		Ensure(IsA(qual, OpExpr), "expected OpExpr");
		opexpr = castNode(OpExpr, qual);
		args = opexpr->args;
		vector_const_opcode = get_opcode(opexpr->opno);
	}

	/*
	 * Find the compressed column referred to by the Var.
	 */
	Var *var = castNode(Var, linitial(args));
	TupleTableSlot *slot = econtext->ecxt_scantuple;

	/*
	 * Prepare to compute the vector predicate. We have to handle the
	 * default values in a special way because they don't produce the usual
	 * decompressed ArrowArrays.
	 */
	uint64 default_value_predicate_result[1];
	uint64 *predicate_result = result;
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
		default_value_predicate_result[0] = 1;
		predicate_result = default_value_predicate_result;
	}

	if (nulltest)
	{
		vector_nulltest(vector, nulltest->nulltesttype, predicate_result);
	}
	else
	{
		/*
		 * Find the vector_const predicate.
		 */
		VectorPredicate *vector_const_predicate = get_vector_const_predicate(vector_const_opcode);
		Assert(vector_const_predicate != NULL);

		Ensure(IsA(lsecond(args), Const),
			   "failed to evaluate runtime constant in vectorized filter");

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

		/*
		 * Account for nulls which shouldn't pass the predicate. Note that the
		 * vector here might have only one row, in contrast with the number of
		 * rows in the batch, if the column has a default value in this batch.
		 */
		const size_t n_vector_result_words = (vector->length + 63) / 64;
		Assert((predicate_result != default_value_predicate_result) ||
			   n_vector_result_words == 1); /* to placate Coverity. */
		const uint64 *restrict validity = (uint64 *restrict) vector->buffers[0];

		if (validity != NULL)
		{
			for (size_t i = 0; i < n_vector_result_words; i++)
			{
				predicate_result[i] &= validity[i];
			}
		}
		else
		{
			Assert(vector->null_count == 0);
		}
	}

	/* Translate the result if the column had a default value. */
	if (values == NULL)
	{
		// Assert(column_values->decompression_type == DT_Default);
		if (!(default_value_predicate_result[0] & 1))
		{
			/*
			 * We had a default value for the compressed column, and it
			 * didn't pass the predicate, so the entire batch didn't pass.
			 */
			const size_t n_batch_result_words = (vexprstate->num_results + 63) / 64;
			for (size_t i = 0; i < n_batch_result_words; i++)
			{
				result[i] = 0;
			}
		}
	}
}

static void compute_one_qual(VectorizedExprState *vexprstate, ExprContext *econtext, Node *qual,
							 uint64 *restrict result);

static void
compute_qual_conjunction(VectorizedExprState *vexprstate, ExprContext *econtext, List *quals,
						 uint64 *restrict result)
{
	ListCell *lc;
	foreach (lc, quals)
	{
		compute_one_qual(vexprstate, econtext, lfirst(lc), result);
		if (get_vector_qual_summary(result, vexprstate->num_results) == NoRowsPass)
		{
			/*
			 * Exit early if no rows pass already. This might allow us to avoid
			 * reading the columns required for the subsequent quals.
			 */
			return;
		}
	}
}

static void
compute_qual_disjunction(VectorizedExprState *vexprstate, ExprContext *econtext, List *quals,
						 uint64 *restrict result)
{
	const size_t n_rows = vexprstate->num_results;
	const size_t n_result_words = (n_rows + 63) / 64;
	uint64 *or_result = palloc(sizeof(uint64) * n_result_words);
	for (size_t i = 0; i < n_result_words; i++)
	{
		or_result[i] = 0;
	}

	uint64 *one_qual_result = palloc(sizeof(uint64) * n_result_words);

	ListCell *lc;
	foreach (lc, quals)
	{
		for (size_t i = 0; i < n_result_words; i++)
		{
			one_qual_result[i] = (uint64) -1;
		}
		compute_one_qual(vexprstate, econtext, lfirst(lc), one_qual_result);
		for (size_t i = 0; i < n_result_words; i++)
		{
			or_result[i] |= one_qual_result[i];
		}

		if (get_vector_qual_summary(or_result, n_rows) == AllRowsPass)
		{
			/*
			 * We can sometimes avoing reading the columns required for the
			 * rest of conditions if we break out early here.
			 */
			return;
		}
	}

	for (size_t i = 0; i < n_result_words; i++)
	{
		result[i] &= or_result[i];
	}
}

static void
compute_one_qual(VectorizedExprState *vexprstate, ExprContext *econtext, Node *qual,
				 uint64 *restrict result)
{
	if (!IsA(qual, BoolExpr))
	{
		compute_plain_qual(vexprstate, econtext, qual, result);
		return;
	}

	BoolExpr *boolexpr = castNode(BoolExpr, qual);
	if (boolexpr->boolop == AND_EXPR)
	{
		compute_qual_conjunction(vexprstate, econtext, boolexpr->args, result);
		return;
	}

	/*
	 * Postgres removes NOT for operators we can vectorize, so we don't support
	 * NOT and consider it non-vectorizable at planning time. So only OR is left.
	 */
	Ensure(boolexpr->boolop == OR_EXPR, "expected OR");
	compute_qual_disjunction(vexprstate, econtext, boolexpr->args, result);
}

/*
 * Compute the vectorized filters. Returns true if we have any passing rows. If not,
 * it means the entire batch is filtered out, and we use this for further
 * optimizations.
 */
static VectorQualSummary
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
	compute_qual_conjunction(vexprstate,
							 econtext,
							 vexprstate->vectorized_quals_constified,
							 vexprstate->vector_qual_result);

	return get_vector_qual_summary(vexprstate->vector_qual_result, vexprstate->num_results);
}

static inline uint16
ExecVectorizedQual(VectorizedExprState *vexprstate, ExprContext *econtext)
{
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	const uint16 rowindex = arrow_slot_row_index(slot);

	/* Compute the vector quals over both compressed and non-compressed
	 * tuples. In case a non-compressed tuple is filtered, return
	 * VQualRowFiltered since it is only one row */
	if (rowindex <= 1)
	{
		VectorQualSummary vector_qual_summary = vexprstate->vectorized_quals_constified != NIL ?
													compute_vector_quals(vexprstate, econtext) :
													AllRowsPass;

		switch (vector_qual_summary)
		{
			case NoRowsPass:
				return arrow_slot_total_row_count(slot);
			case AllRowsPass:
				/*
				 * If all rows pass, no need to test the vector qual for each row. This
				 * is a common case for time range conditions.
				 */
				vexprstate->vector_qual_result = NULL;
				return 0;
			case SomeRowsPass:
				break;
		}
	}

	if (vexprstate->vector_qual_result == NULL)
		return 0;

	const uint16 nrows = arrow_slot_total_row_count(slot);
	const uint16 off = arrow_slot_arrow_offset(slot);
	uint16 nfiltered = 0;

	for (uint16 i = off; i < nrows; i++)
	{
		if (arrow_row_is_valid(vexprstate->vector_qual_result, i))
			break;
		nfiltered++;
	}

	return nfiltered;
}

static TupleTableSlot *
columnar_scan_exec(CustomScanState *state)
{
	ColumnarScanState *cstate = (ColumnarScanState *) state;
	TableScanDesc scandesc;
	EState *estate;
	ExprContext *econtext;
	ExprState *qual;
	ProjectionInfo *projinfo;
	ScanDirection direction;
	TupleTableSlot *slot;
	bool has_vecquals = cstate->vexprstate.vectorized_quals_constified != NIL;

	scandesc = state->ss.ss_currentScanDesc;
	estate = state->ss.ps.state;
	econtext = state->ss.ps.ps_ExprContext;
	qual = state->ss.ps.qual;
	projinfo = state->ss.ps.ps_ProjInfo;
	direction = estate->es_direction;
	slot = state->ss.ss_ScanTupleSlot;

	TS_DEBUG_LOG("relation: %s", RelationGetRelationName(state->ss.ss_currentRelation));

	if (scandesc == NULL)
	{
		/*
		 * We reach here if the scan is not parallel, or if we're serially
		 * executing a scan that was planned to be parallel.
		 */
		scandesc = table_beginscan(state->ss.ss_currentRelation,
								   estate->es_snapshot,
								   cstate->nscankeys,
								   cstate->scankeys);
		state->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * If we have neither a qual to check nor a projection, do the fast path
	 * and just return the raw scan tuple.
	 */
	if (!qual && !projinfo && !has_vecquals)
	{
		CHECK_FOR_INTERRUPTS();
		ResetExprContext(econtext);

		if (table_scan_getnextslot(scandesc, direction, slot))
			return slot;
		return NULL;
	}

	ResetExprContext(econtext);

	/*
	 * Scan tuples and apply vectorized filters, followed by any remaining
	 * (non-vectorized) qual filters and projection.
	 */
	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		slot = state->ss.ss_ScanTupleSlot;

		if (!table_scan_getnextslot(scandesc, direction, slot))
		{
			/* Nothing to return, but be careful to use the projection result
			 * slot so it has correct tupleDesc. */
			if (projinfo)
				return ExecClearTuple(projinfo->pi_state.resultslot);
			else
				return NULL;
		}

		econtext->ecxt_scantuple = slot;

		if (likely(TTS_IS_ARROWTUPLE(slot)))
		{
			const uint16 nfiltered = ExecVectorizedQual(&cstate->vexprstate, econtext);

			if (nfiltered > 0)
			{
				const uint16 total_nrows = arrow_slot_total_row_count(slot);

				TS_DEBUG_LOG("vectorized filtering of %d rows", nfiltered);

				/* Skip ahead with the amount filtered */
				ExecIncrArrowTuple(slot, nfiltered);
				InstrCountFiltered1(state, nfiltered);

				if (nfiltered == total_nrows && total_nrows > 1)
				{
					/* A complete segment was filtered */
					Assert(arrow_slot_is_consumed(slot));
					InstrCountTuples2(state, 1);
				}

				/* If the whole segment was consumed, read next segment */
				if (arrow_slot_is_consumed(slot))
					continue;
			}
		}

		/* A row passed vectorized filters. Check remaining non-vectorized
		 * quals, if any, and do projection. */
		if (qual == NULL || ExecQual(qual, econtext))
		{
			if (projinfo)
				return ExecProject(projinfo);
			return slot;
		}

		/* Row was filtered by non-vectorized qual */
		ResetExprContext(econtext);
		InstrCountFiltered1(state, 1);
	}
}

static List *
get_args_from(Node *node)
{
	if (IsA(node, OpExpr))
		return castNode(OpExpr, node)->args;
	if (IsA(node, ScalarArrayOpExpr))
		return castNode(ScalarArrayOpExpr, node)->args;
	return NIL;
}

static void
columnar_scan_begin(CustomScanState *state, EState *estate, int eflags)
{
	ColumnarScanState *cstate = (ColumnarScanState *) state;
	Scan *scan = (Scan *) state->ss.ps.plan;
	Relation rel = state->ss.ss_currentRelation;
	const HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(rel);

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
	cstate->scankeys = extract_scan_keys(caminfo, scan, &cstate->nscankeys, &cstate->scankey_quals);

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

		List *args = get_args_from(constified);
		Ensure(args && IsA(lsecond(args), Const),
			   "failed to evaluate runtime constant in vectorized filter");
		cstate->vexprstate.vectorized_quals_constified =
			lappend(cstate->vexprstate.vectorized_quals_constified, constified);
	}
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

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */
static Size
columnar_scan_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	EState *estate = node->ss.ps.state;
	return table_parallelscan_estimate(node->ss.ss_currentRelation, estate->es_snapshot);
}

static void
columnar_scan_initialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *arg)
{
	EState *estate = node->ss.ps.state;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) arg;

	table_parallelscan_initialize(node->ss.ss_currentRelation, pscan, estate->es_snapshot);
	node->ss.ss_currentScanDesc = table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}

static void
columnar_scan_reinitialize_dsm(CustomScanState *node, ParallelContext *pcxt, void *arg)
{
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) arg;

	table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

static void
columnar_scan_initialize_worker(CustomScanState *node, shm_toc *toc, void *arg)
{
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) arg;

	node->ss.ss_currentScanDesc = table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
}

static CustomExecMethods columnar_scan_state_methods = {
	.BeginCustomScan = columnar_scan_begin,
	.ExecCustomScan = columnar_scan_exec,
	.EndCustomScan = columnar_scan_end,
	.ReScanCustomScan = columnar_scan_rescan,
	.ExplainCustomScan = columnar_scan_explain,
	.EstimateDSMCustomScan = columnar_scan_estimate_dsm,
	.InitializeDSMCustomScan = columnar_scan_initialize_dsm,
	.ReInitializeDSMCustomScan = columnar_scan_reinitialize_dsm,
	.InitializeWorkerCustomScan = columnar_scan_initialize_worker,
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
make_vectorized_qual(Node *qual, const HyperstoreInfo *caminfo)
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
	const HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(relation);
	List *vectorized_quals = NIL;
	List *nonvectorized_quals = NIL;
	ListCell *lc;

	foreach (lc, scan_clauses)
	{
		Node *source_qual = lfirst(lc);
		Node *vectorized_qual = make_vectorized_qual(source_qual, caminfo);

		if (vectorized_qual)
		{
			TS_DEBUG_LOG("qual identified as vectorizable: %s", nodeToString(vectorized_qual));
			vectorized_quals = lappend(vectorized_quals, vectorized_qual);
		}
		else
		{
			TS_DEBUG_LOG("qual identified as non-vectorized qual: %s", nodeToString(source_qual));
			nonvectorized_quals = lappend(nonvectorized_quals, source_qual);
		}
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
	path->parallel_aware = (parallel_workers > 0);
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

	if (rel->consider_parallel && required_outer == NULL)
	{
		int parallel_workers;

		parallel_workers =
			compute_parallel_worker(rel, rel->pages, -1, max_parallel_workers_per_gather);

		/* If any limit was set to zero, the user doesn't want a parallel scan. */
		if (parallel_workers > 0)
		{
			/* Add an unordered partial path based on a parallel sequential scan. */
			cspath = columnar_scan_path_create(root, rel, required_outer, parallel_workers);
			add_partial_path(rel, &cspath->custom_path.path);
		}
	}
}

void
_columnar_scan_init(void)
{
	TryRegisterCustomScanMethods(&columnar_scan_plan_methods);
}
