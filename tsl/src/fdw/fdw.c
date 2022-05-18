/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <parser/parsetree.h>
#include <optimizer/appendinfo.h>
#include <optimizer/planmain.h>
#include <optimizer/pathnode.h>
#include <nodes/plannodes.h>
#include <nodes/makefuncs.h>
#include <nodes/execnodes.h>
#include <access/reloptions.h>
#include <access/sysattr.h>
#include <commands/defrem.h>
#include <foreign/fdwapi.h>
#include <foreign/foreign.h>
#include <utils/rel.h>
#include <fmgr.h>

#include <compat/compat.h>
#include <guc.h>

#include "data_node_scan_plan.h"
#include "debug_guc.h"
#include "debug.h"
#include "fdw.h"
#include "fdw_utils.h"
#include "modify_exec.h"
#include "modify_plan.h"
#include "option.h"
#include "relinfo.h"
#include "scan_exec.h"
#include "scan_plan.h"

/*
 * Parse options from foreign table and apply them to fpinfo.
 *
 * New options might also require tweaking merge_fdw_options().
 */
static void
apply_table_options(ForeignTable *table, TsFdwRelInfo *fpinfo)
{
	ListCell *lc;

	foreach (lc, table->options)
	{
		DefElem *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "fetch_size") == 0)
			fpinfo->fetch_size = strtol(defGetString(def), NULL, 10);
	}
}

/* This creates the fdw_relation_info object for hypertables and foreign table
 * type objects. */
static void
get_foreign_rel_size(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

	/* A base hypertable is a regular table and not a foreign table. It is the only
	 * kind of regular table that will ever have this callback called on it. */
	if (RELKIND_RELATION == rte->relkind)
	{
		fdw_relinfo_create(root, baserel, InvalidOid, foreigntableid, TS_FDW_RELINFO_HYPERTABLE);
	}
	else
	{
		ForeignTable *table = GetForeignTable(foreigntableid);

		fdw_relinfo_create(root,
						   baserel,
						   table->serverid,
						   foreigntableid,
						   TS_FDW_RELINFO_FOREIGN_TABLE);

		apply_table_options(table, fdw_relinfo_get(baserel));
	}
}

static void
get_foreign_paths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(baserel);
	Path *path;

	Assert(fpinfo->type != TS_FDW_RELINFO_HYPERTABLE_DATA_NODE);

	if (fpinfo->type == TS_FDW_RELINFO_HYPERTABLE)
	{
		if (ts_guc_enable_per_data_node_queries)
			data_node_scan_add_node_paths(root, baserel);
		return;
	}

	if (baserel->reloptkind == RELOPT_JOINREL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign joins are not supported")));

	/*
	 * Create simplest ForeignScan path node and add it to baserel.  This path
	 * corresponds to SeqScan path of regular tables (though depending on what
	 * baserestrict conditions we were able to send to remote, there might
	 * actually be an indexscan happening there).  We already did all the work
	 * to estimate cost and size of this path.
	 */
	path = (Path *) create_foreignscan_path(root,
											baserel,
											NULL, /* default pathtarget */
											fpinfo->rows,
											fpinfo->startup_cost,
											fpinfo->total_cost,
											NIL,  /* no pathkeys */
											NULL, /* no outer rel either */
											NULL, /* no extra plan */
											NIL); /* no fdw_private list */
	fdw_utils_add_path(baserel, path);

	/* Add paths with pathkeys */
	fdw_add_paths_with_pathkeys_for_rel(root,
										baserel,
										NULL,
										(CreatePathFunc) create_foreignscan_path);
#ifdef TS_DEBUG
	if (ts_debug_optimizer_flags.show_rel)
		tsl_debug_log_rel_with_paths(root, baserel, NULL);
#endif
}

static ForeignScan *
get_foreign_plan(PlannerInfo *root, RelOptInfo *foreignrel, Oid foreigntableid,
				 ForeignPath *best_path, List *tlist, List *scan_clauses, Plan *outer_plan)
{
	ScanInfo info;

	memset(&info, 0, sizeof(ScanInfo));

	fdw_scan_info_init(&info, root, foreignrel, &best_path->path, scan_clauses);

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist,
							info.local_exprs,
							info.scan_relid,
							info.params_list,
							info.fdw_private,
							info.fdw_scan_tlist,
							info.fdw_recheck_quals,
							outer_plan);
}

static void
begin_foreign_scan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;

	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) && !ts_guc_enable_remote_explain)
		return;

	node->fdw_state = (TsFdwScanState *) palloc0(sizeof(TsFdwScanState));

	fdw_scan_init(&node->ss,
				  node->fdw_state,
				  fsplan->fs_relids,
				  fsplan->fdw_private,
				  fsplan->fdw_exprs,
				  eflags);
}

static TupleTableSlot *
iterate_foreign_scan(ForeignScanState *node)
{
	TsFdwScanState *fsstate = (TsFdwScanState *) node->fdw_state;

	return fdw_scan_iterate(&node->ss, fsstate);
}

static void
end_foreign_scan(ForeignScanState *node)
{
	fdw_scan_end((TsFdwScanState *) node->fdw_state);
}

static void
rescan_foreign_scan(ForeignScanState *node)
{
	fdw_scan_rescan(&node->ss, (TsFdwScanState *) node->fdw_state);
}

static TupleTableSlot *
exec_foreign_update(EState *estate, ResultRelInfo *rri, TupleTableSlot *slot,
					TupleTableSlot *plan_slot)
{
	TsFdwModifyState *fmstate = (TsFdwModifyState *) rri->ri_FdwState;

	return fdw_exec_foreign_update_or_delete(fmstate, estate, slot, plan_slot, UPDATE_CMD);
}

static TupleTableSlot *
exec_foreign_delete(EState *estate, ResultRelInfo *rri, TupleTableSlot *slot,
					TupleTableSlot *plan_slot)
{
	TsFdwModifyState *fmstate = (TsFdwModifyState *) rri->ri_FdwState;

	return fdw_exec_foreign_update_or_delete(fmstate, estate, slot, plan_slot, DELETE_CMD);
}

static void
end_foreign_modify(EState *estate, ResultRelInfo *rri)
{
	TsFdwModifyState *fmstate = (TsFdwModifyState *) rri->ri_FdwState;

	/* If fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate == NULL)
		return;

	/* Destroy the execution state */
	fdw_finish_foreign_modify(fmstate);
}

/*
 * Add resjunk column(s) needed for update/delete on a foreign table
 */
#if PG14_LT
static void
add_foreign_update_targets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation)
{
	Var *var;
	const char *attrname;
	TargetEntry *tle;

	/*
	 * In timescaledb_fdw, what we need is the ctid, same as for a regular
	 * table.
	 */

	/* Make a Var representing the desired value */
	var = makeVar(parsetree->resultRelation,
				  SelfItemPointerAttributeNumber,
				  TIDOID,
				  -1,
				  InvalidOid,
				  0);

	/* Wrap it in a resjunk TLE with the right name ... */
	attrname = "ctid";

	tle = makeTargetEntry((Expr *) var,
						  list_length(parsetree->targetList) + 1,
						  pstrdup(attrname),
						  true);

	/* ... and add it to the query's targetlist */
	parsetree->targetList = lappend(parsetree->targetList, tle);
}
#else
static void
add_foreign_update_targets(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte,
						   Relation target_relation)
{
	/*
	 * In timescaledb_fdw, what we need is the ctid, same as for a regular
	 * table.
	 */

	/* Make a Var representing the desired value */
	Var *var = makeVar(rtindex, SelfItemPointerAttributeNumber, TIDOID, -1, InvalidOid, 0);

	/* Register it as a row-identity column needed by this target rel */
	add_row_identity_var(root, var, rtindex, "ctid");
}
#endif

static TupleTableSlot *
exec_foreign_insert(EState *estate, ResultRelInfo *rri, TupleTableSlot *slot,
					TupleTableSlot *planslot)
{
	TsFdwModifyState *fmstate = (TsFdwModifyState *) rri->ri_FdwState;

	return fdw_exec_foreign_insert(fmstate, estate, slot, planslot);
}

static int
is_foreign_rel_updatable(Relation rel)
{
	return (1 << CMD_INSERT) | (1 << CMD_DELETE) | (1 << CMD_UPDATE);
}

static void
explain_foreign_scan(ForeignScanState *node, struct ExplainState *es)
{
	List *fdw_private = ((ForeignScan *) node->ss.ps.plan)->fdw_private;

	fdw_scan_explain(&node->ss, fdw_private, es, (TsFdwScanState *) node->fdw_state);
}

static void
begin_foreign_modify(ModifyTableState *mtstate, ResultRelInfo *rri, List *fdw_private,
					 int subplan_index, int eflags)
{
#if PG14_LT
	Plan *subplan = mtstate->mt_plans[subplan_index]->plan;
#else
	Plan *subplan = outerPlanState(mtstate)->plan;
#endif

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  rri->ri_FdwState stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	fdw_begin_foreign_modify(&mtstate->ps, rri, mtstate->operation, fdw_private, subplan);
}

static void
explain_foreign_modify(ModifyTableState *mtstate, ResultRelInfo *rri, List *fdw_private,
					   int subplan_index, struct ExplainState *es)
{
	fdw_explain_modify(&mtstate->ps, rri, fdw_private, subplan_index, es);
}

static List *
plan_foreign_modify(PlannerInfo *root, ModifyTable *plan, Index result_relation, int subplan_index)
{
	return fdw_plan_foreign_modify(root, plan, result_relation, subplan_index);
}

/*
 * get_foreign_upper_paths
 *		Add paths for post-join operations like aggregation, grouping etc. if
 *		corresponding operations are safe to push down.
 *
 * Right now, we only support aggregate, grouping and having clause pushdown.
 */
static void
get_foreign_upper_paths(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
						RelOptInfo *output_rel, void *extra)
{
	TsFdwRelInfo *fpinfo = input_rel->fdw_private ? fdw_relinfo_get(input_rel) : NULL;

	if (fpinfo == NULL)
		return;

	/* We abuse the FDW API's GetForeignUpperPaths callback because, for some
	 * reason, the regular create_upper_paths_hook is never called for
	 * partially grouped rels, so we cannot use if for server rels. See end of
	 * PostgreSQL planner.c:create_partial_grouping_paths(). */
	if (fpinfo->type == TS_FDW_RELINFO_HYPERTABLE_DATA_NODE)
		data_node_scan_create_upper_paths(root, stage, input_rel, output_rel, extra);
	else
		fdw_create_upper_paths(fpinfo,
							   root,
							   stage,
							   input_rel,
							   output_rel,
							   extra,
							   (CreateUpperPathFunc) create_foreign_upper_path);

#ifdef TS_DEBUG
	if (ts_debug_optimizer_flags.show_upper & (1 << stage))
		tsl_debug_log_rel_with_paths(root, output_rel, &stage);
#endif
}

static FdwRoutine timescaledb_fdw_routine = {
	.type = T_FdwRoutine,
	/* scan (mandatory) */
	.GetForeignRelSize = get_foreign_rel_size,
	.GetForeignPaths = get_foreign_paths,
	.GetForeignPlan = get_foreign_plan,
	.BeginForeignScan = begin_foreign_scan,
	.IterateForeignScan = iterate_foreign_scan,
	.EndForeignScan = end_foreign_scan,
	.ReScanForeignScan = rescan_foreign_scan,
	.GetForeignUpperPaths = get_foreign_upper_paths,
	/* update */
	.IsForeignRelUpdatable = is_foreign_rel_updatable,
	.PlanForeignModify = plan_foreign_modify,
	.BeginForeignModify = begin_foreign_modify,
	.ExecForeignInsert = exec_foreign_insert,
	.ExecForeignDelete = exec_foreign_delete,
	.ExecForeignUpdate = exec_foreign_update,
	.EndForeignModify = end_foreign_modify,
	.AddForeignUpdateTargets = add_foreign_update_targets,
	/* explain/analyze */
	.ExplainForeignScan = explain_foreign_scan,
	.ExplainForeignModify = explain_foreign_modify,
	.AnalyzeForeignTable = NULL,
};

Datum
timescaledb_fdw_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&timescaledb_fdw_routine);
}

Datum
timescaledb_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);

	option_validate(options_list, catalog);

	PG_RETURN_VOID();
}
