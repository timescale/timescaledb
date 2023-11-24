/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <executor/tuptable.h>
#include <nodes/pathnodes.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>
#include <planner.h>
#include <planner/planner.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "columnar_scan.h"
#include "import/ts_explain.h"

typedef struct ColumnarScanState
{
	CustomScanState css;
	ScanKey scankeys;
	int nscankeys;
	List *scankey_quals;
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
build_scan_keys(Index relid, List *clauses, int *num_keys, List **scankey_quals)
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

				if (argfound)
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

	pfree(scankeys);

	return NULL;
}

static TupleTableSlot *
columnar_next(ColumnarScanState *state)
{
	TableScanDesc scandesc;
	EState *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	scandesc = state->css.ss.ss_currentScanDesc;
	estate = state->css.ss.ps.state;
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
	 * get the next tuple from the table
	 */
	if (table_scan_getnextslot(scandesc, direction, slot))
		return slot;

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

	cstate->scankeys = build_scan_keys(scan->scanrelid,
									   scan->plan.qual,
									   &cstate->nscankeys,
									   &cstate->scankey_quals);
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

	return (Node *) cstate;
}

static CustomScanMethods columnar_scan_plan_methods = {
	.CustomName = "ColumnarScan",
	.CreateCustomScanState = columnar_scan_state_create,
};

static Plan *
columnar_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
						  List *scan_clauses, List *custom_plans)
{
	CustomScan *columnar_scan_plan = makeNode(CustomScan);

	Assert(best_path->path.parent->rtekind == RTE_RELATION);

	columnar_scan_plan->flags = best_path->flags;
	columnar_scan_plan->methods = &columnar_scan_plan_methods;
	columnar_scan_plan->scan.scanrelid = rel->relid;

	/* output target list */
	columnar_scan_plan->scan.plan.targetlist = tlist;

	/* Reduce RestrictInfo list to bare expressions; ignore pseudoconstants */
	scan_clauses = extract_actual_clauses(scan_clauses, false);
	columnar_scan_plan->scan.plan.qual = scan_clauses;

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
