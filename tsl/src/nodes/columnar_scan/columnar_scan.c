/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/skey.h>
#include <catalog/pg_attribute.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pathnodes.h>
#include <nodes/pg_list.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>
#include <planner.h>
#include <planner/planner.h>
#include <utils/palloc.h>
#include <utils/typcache.h>

#include "columnar_scan.h"
#include "compression/arrow_c_data_interface.h"
#include "compression/compression.h"
#include "hyperstore/arrow_tts.h"
#include "hyperstore/hyperstore_handler.h"
#include "import/ts_explain.h"
#include "nodes/decompress_chunk/vector_quals.h"

typedef struct ColumnarScanState
{
	CustomScanState css;
	VectorQualState vqstate;
	ScanKey scankeys;
	int nscankeys;
	List *scankey_quals;
	List *vectorized_quals_orig;
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
 * ColumnarScan implementation of VectorQualState->get_arrow_array().
 *
 * Given a VectorQualState return the ArrowArray in the contained slot.
 */
static const ArrowArray *
vector_qual_state_get_arrow_array(VectorQualState *vqstate, Expr *expr, bool *is_default_value)
{
	TupleTableSlot *slot = vqstate->slot;
	const Var *var = castNode(Var, expr);
	const int attoff = AttrNumberGetAttrOffset(var->varattno);
	const ArrowArray *array = arrow_slot_get_array(slot, var->varattno);

	if (array == NULL)
	{
		Form_pg_attribute attr = &slot->tts_tupleDescriptor->attrs[attoff];
		/*
		 * If getting here, this is a non-compressed value or a compressed
		 * column with a default value. We can treat non-compressed values the
		 * same as default ones. It is not possible to fall back to the
		 * non-vectorized quals now, so build a single-value ArrowArray with
		 * this (default) value, check if it passes the predicate, and apply
		 * it to the entire batch.
		 */
		array = make_single_value_arrow(attr->atttypid,
										slot->tts_values[attoff],
										slot->tts_isnull[attoff]);
		*is_default_value = true;
	}
	else
		*is_default_value = false;

	return array;
}

static void
vector_qual_state_reset(VectorQualState *vqstate, ExprContext *econtext)
{
	MemoryContextReset(vqstate->per_vector_mcxt);
	vqstate->vector_qual_result = NULL;
	vqstate->slot = econtext->ecxt_scantuple;
	vqstate->num_results = arrow_slot_total_row_count(vqstate->slot);
}

static void
vector_qual_state_init(VectorQualState *vqstate, ExprContext *econtext)
{
	vqstate->per_vector_mcxt = GenerationContextCreateCompat(econtext->ecxt_per_query_memory,
															 "Per-vector memory context",
															 64 * 1024);
	vqstate->get_arrow_array = vector_qual_state_get_arrow_array;
	vqstate->slot = econtext->ecxt_scantuple;
}

/*
 * Utility function to extract quals that can be used as scankeys. The
 * remaining "normal" quals are optionally collected in the corresponding
 * argument.
 *
 * Returns:
 *
 * 1. A list of scankey quals if scankeys is NULL, or
 *
 * 2. NIL if scankeys is non-NULL.
 *
 * Thus, this function is designed to be called over two passes: one at plan
 * time to split the quals into scankey quals and remaining quals, and one at
 * execution time to populate a scankey array with the scankey quals found in
 * the first pass.
 *
 * The scankey quals returned in pass 1 is used for EXPLAIN.
 */
static List *
process_scan_key_quals(const HyperstoreInfo *hsinfo, Index relid, const List *quals,
					   List **remaining_quals, ScanKey scankeys, unsigned scankeys_capacity)
{
	List *scankey_quals = NIL;
	unsigned nkeys = 0;
	ListCell *lc;

	Assert(scankeys == NULL || (scankeys_capacity >= (unsigned) list_length(quals)));

	foreach (lc, quals)
	{
		Expr *qual = lfirst(lc);
		bool scankey_found = false;

		/* ignore volatile expressions */
		if (contain_volatile_functions((Node *) qual))
		{
			if (remaining_quals != NULL)
				*remaining_quals = lappend(*remaining_quals, qual);
			continue;
		}

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
				{
					/* If neither right nor left argument is a variable, we
					 * don't use it as scan key */
					break;
				}

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
					hsinfo->columns[AttrNumberGetAttrOffset(relvar->varattno)].is_segmentby;
				if (argfound && is_segmentby)
				{
					TypeCacheEntry *tce =
						lookup_type_cache(relvar->vartype, TYPECACHE_BTREE_OPFAMILY);
					int op_strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

					if (op_strategy != InvalidStrategy)
					{
						Oid op_lefttype;
						Oid op_righttype;

						scankey_found = true;
						get_op_opfamily_properties(opno,
												   tce->btree_opf,
												   false,
												   &op_strategy,
												   &op_lefttype,
												   &op_righttype);

						Assert(relvar != NULL);

						if (scankeys != NULL)
						{
							ScanKeyEntryInitialize(&scankeys[nkeys++],
												   0 /* flags */,
												   relvar->varattno, /* attribute number to scan */
												   op_strategy,		 /* op's strategy */
												   op_righttype,	 /* strategy subtype */
												   opexpr->inputcollid, /* collation */
												   opexpr->opfuncid,	/* reg proc to use */
												   scanvalue);			/* constant */
						}
						else
						{
							scankey_quals = lappend(scankey_quals, qual);
						}
					}
				}
				break;
			}
			default:
				break;
		}

		if (!scankey_found && remaining_quals != NULL)
			*remaining_quals = lappend(*remaining_quals, qual);
	}

	return scankey_quals;
}

static List *
extract_scankey_quals(const HyperstoreInfo *hsinfo, Index relid, const List *quals,
					  List **remaining_quals)
{
	return process_scan_key_quals(hsinfo, relid, quals, remaining_quals, NULL, 0);
}

static ScanKey
create_scankeys_from_quals(const HyperstoreInfo *hsinfo, Index relid, const List *quals)
{
	unsigned capacity = list_length(quals);
	ScanKey scankeys = palloc0(sizeof(ScanKeyData) * capacity);
	process_scan_key_quals(hsinfo, relid, quals, NULL, scankeys, capacity);
	return scankeys;
}

/*
 * Execute vectorized filter over a vector/array of values.
 *
 * Returns the number of values filtered until the first valid value.
 */
static inline uint16
ExecVectorQual(VectorQualState *vqstate, ExprContext *econtext)
{
	TupleTableSlot *slot = econtext->ecxt_scantuple;
	const uint16 rowindex = arrow_slot_row_index(slot);

	/* Compute the vector quals over both compressed and non-compressed
	 * tuples. In case a non-compressed tuple is filtered, return SomeRowsPass
	 * although only one row will pass. */
	if (rowindex <= 1)
	{
		vector_qual_state_reset(vqstate, econtext);
		VectorQualSummary vector_qual_summary = vqstate->vectorized_quals_constified != NIL ?
													vector_qual_compute(vqstate) :
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
				vector_qual_state_reset(vqstate, econtext);
				return 0;
			case SomeRowsPass:
				break;
		}
	}

	/* Fast path when all rows have passed (i.e., no rows filtered). No need
	 * to check qual result and it should be NULL. */
	if (vqstate->vector_qual_result == NULL)
		return 0;

	const uint16 nrows = arrow_slot_total_row_count(slot);
	const uint16 off = arrow_slot_arrow_offset(slot);
	uint16 nfiltered = 0;

	for (uint16 i = off; i < nrows; i++)
	{
		if (arrow_row_is_valid(vqstate->vector_qual_result, i))
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
	bool has_vecquals = cstate->vqstate.vectorized_quals_constified != NIL;

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
			const uint16 nfiltered = ExecVectorQual(&cstate->vqstate, econtext);

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

static void
columnar_scan_begin(CustomScanState *state, EState *estate, int eflags)
{
	ColumnarScanState *cstate = (ColumnarScanState *) state;

#if PG16_LT
	/* Since PG16, one can specify state->slotOps to initialize a CustomScan
	 * with a custom scan slot. But pre-PG16, the CustomScan state always
	 * created a scan slot of type TTSOpsVirtual, even if one sets
	 * scan->scanrelid to a valid index to indicate scan of a base relation.
	 * To ensure the base relation's scan slot type is used, we recreate the
	 * scan slot here with the slot type used by the underlying base
	 * relation. It is not necessary (or possible) to drop the existing slot
	 * since it is registered in the tuple table and will be released when the
	 * executor finishes. */
	Relation rel = state->ss.ss_currentRelation;
	ExecInitScanTupleSlot(estate,
						  &state->ss,
						  RelationGetDescr(rel),
						  table_slot_callbacks(state->ss.ss_currentRelation));

	/* Must reinitialize projection for the new slot type as well, including
	 * ExecQual state for the new slot. */
	ExecInitResultTypeTL(&state->ss.ps);
	ExecAssignScanProjectionInfo(&state->ss);
	state->ss.ps.qual = ExecInitQual(state->ss.ps.plan->qual, (PlanState *) state);
#endif
	vector_qual_state_init(&cstate->vqstate, state->ss.ps.ps_ExprContext);

	if (cstate->nscankeys > 0)
	{
		const HyperstoreInfo *hsinfo = RelationGetHyperstoreInfo(state->ss.ss_currentRelation);
		Scan *scan = (Scan *) state->ss.ps.plan;
		cstate->scankeys =
			create_scankeys_from_quals(hsinfo, scan->scanrelid, cstate->scankey_quals);
	}

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
		cstate->vqstate.vectorized_quals_constified =
			lappend(cstate->vqstate.vectorized_quals_constified, constified);
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
	cstate->scankey_quals = lsecond(cscan->custom_exprs);
	cstate->nscankeys = list_length(cstate->scankey_quals);
	cstate->scankeys = NULL;
#if PG16_GE
	cstate->css.slotOps = &TTSOpsArrowTuple;
#endif

	return (Node *) cstate;
}

static CustomScanMethods columnar_scan_plan_methods = {
	.CustomName = "ColumnarScan",
	.CreateCustomScanState = columnar_scan_state_create,
};

typedef struct VectorQualInfoHyperstore
{
	VectorQualInfo vqinfo;
	const HyperstoreInfo *hsinfo;
} VectorQualInfoHyperstore;

static bool *
columnar_scan_build_vector_attrs(const ColumnCompressionSettings *columns, int numcolumns)
{
	bool *vector_attrs = palloc0(sizeof(bool) * (numcolumns + 1));

	for (int i = 0; i < numcolumns; i++)
	{
		const ColumnCompressionSettings *column = &columns[i];
		AttrNumber attnum = AttrOffsetGetAttrNumber(i);

		Assert(column->attnum == attnum || column->attnum == InvalidAttrNumber);
		vector_attrs[attnum] =
			(!column->is_segmentby && column->attnum != InvalidAttrNumber &&
			 tsl_get_decompress_all_function(compression_get_default_algorithm(column->typid),
											 column->typid) != NULL);
	}
	return vector_attrs;
}

static Plan *
columnar_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
						  List *scan_clauses, List *custom_plans)
{
	CustomScan *columnar_scan_plan = makeNode(CustomScan);
	RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	Relation relation = RelationIdGetRelation(rte->relid);
	HyperstoreInfo *hsinfo = RelationGetHyperstoreInfo(relation);
	List *vectorized_quals = NIL;
	List *nonvectorized_quals = NIL;
	List *scankey_quals = NIL;
	List *remaining_quals = NIL;
	ListCell *lc;

	VectorQualInfoHyperstore vqih = {
		.vqinfo = {
			.rti = rel->relid,
			.vector_attrs = columnar_scan_build_vector_attrs(hsinfo->columns, hsinfo->num_columns),
		},
		.hsinfo = hsinfo,
	};
	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	columnar_scan_plan->flags = best_path->flags;
	columnar_scan_plan->methods = &columnar_scan_plan_methods;
	columnar_scan_plan->scan.scanrelid = rel->relid;

	/* output target list */
	columnar_scan_plan->scan.plan.targetlist = tlist;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	foreach (lc, scan_clauses)
	{
		Node *source_qual = lfirst(lc);
		Node *vectorized_qual = vector_qual_make(source_qual, &vqih.vqinfo);

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

	/* Need to split the nonvectorized quals into scankey quals and remaining
	 * quals before ExecInitQual() in CustomScanState::begin() since the qual
	 * execution state is created from the remaining quals. Note that it is
	 * not possible to create the scankeys themselves here because the only
	 * way to pass those on to the scan state is via custom_private. And
	 * anything that goes into custom_private needs to be a Node that is
	 * printable with nodeToString(). */
	scankey_quals =
		extract_scankey_quals(vqih.hsinfo, rel->relid, nonvectorized_quals, &remaining_quals);

	columnar_scan_plan->scan.plan.qual = remaining_quals;
	columnar_scan_plan->custom_exprs = list_make2(vectorized_quals, scankey_quals);

	RelationClose(relation);

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

	cspath->custom_path.flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;
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
