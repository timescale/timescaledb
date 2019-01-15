/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/*
 * No op Foreign Data Wrapper implementation to be used as a mock object
 * when testing or a starting point for implementing/testing real implementation.
 *
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/rel.h>
#include <foreign/fdwapi.h>
#include <foreign/foreign.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/planmain.h>
#include <access/htup_details.h>
#include <executor/executor.h>
#include <utils/builtins.h>

#include <export.h>

#include "utils.h"
#include "timescaledb_fdw.h"

TS_FUNCTION_INFO_V1(timescaledb_fdw_handler);
TS_FUNCTION_INFO_V1(timescaledb_fdw_validator);

typedef struct TsScanState
{
	int row_counter;
} TsScanState;

static void
log_func_name(const char *name)
{
	elog(INFO, "executing function %s", name);
}

Datum
timescaledb_fdw_validator(PG_FUNCTION_ARGS)
{
	/*
	 * no logging b/c it seems that validator func is called even when not
	 * using foreign table
	 */
	PG_RETURN_VOID();
}

static void
get_foreign_rel_size(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	log_func_name(__func__);
	baserel->rows = 0;
}

static void
get_foreign_paths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	Cost startup_cost, total_cost;

	log_func_name(__func__);

	startup_cost = 0;
	total_cost = startup_cost + baserel->rows;

	add_path(baserel,
			 (Path *) create_foreignscan_path(root,
											  baserel,
											  NULL,
											  baserel->rows,
											  startup_cost,
											  total_cost,
											  NIL,
											  NULL,
											  NULL,
											  NIL));
}

static ForeignScan *
get_foreign_plan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path,
				 List *tlist, List *scan_clauses, Plan *outer_plan)
{
	Index scan_relid = baserel->relid;

	log_func_name(__func__);

	scan_clauses = extract_actual_clauses(scan_clauses, false);
	return make_foreignscan(tlist, scan_clauses, scan_relid, NIL, NIL, NIL, NIL, outer_plan);
}

static void
begin_foreign_scan(ForeignScanState *node, int eflags)
{
	TsScanState *state;

	log_func_name(__func__);
	state = palloc0(sizeof(TsScanState));
	state->row_counter = 0;
	node->fdw_state = state;
}

static TupleTableSlot *
iterate_foreign_scan(ForeignScanState *node)
{
	TupleTableSlot *slot;
	TupleDesc tuple_desc;
	TsScanState *state;
	HeapTuple tuple;

	log_func_name(__func__);
	slot = node->ss.ss_ScanTupleSlot;
	tuple_desc = node->ss.ss_currentRelation->rd_att;
	state = (TsScanState *) node->fdw_state;
	if (state->row_counter == 0)
	{
		/* Dummy implementation to return one tuple */
		MemoryContext oldcontext = MemoryContextSwitchTo(node->ss.ps.state->es_query_cxt);
		Datum values[2];
		bool nulls[2];

		values[0] = Int8GetDatum(1);
		values[1] = CStringGetTextDatum("test");
		nulls[0] = false;
		nulls[1] = false;
		tuple = heap_form_tuple(tuple_desc, values, nulls);
		MemoryContextSwitchTo(oldcontext);
		ExecStoreTuple(tuple, slot, InvalidBuffer, true);
		state->row_counter += 1;
	}
	else
		ExecClearTuple(slot);
	return slot;
}

static void
rescan_foreign_scan(ForeignScanState *node)
{
	log_func_name(__func__);
}

static void
end_foreign_scan(ForeignScanState *node)
{
	log_func_name(__func__);
}

static void
add_foreign_update_targets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation)
{
	log_func_name(__func__);
}

static List *
plan_foreign_modify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{
	log_func_name(__func__);
	return NULL;
}

static void
begin_foreign_modify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private,
					 int subplan_index, int eflags)
{
	log_func_name(__func__);
}

static TupleTableSlot *
exec_foreign_insert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot,
					TupleTableSlot *planSlot)
{
	log_func_name(__func__);
	return slot;
}

static TupleTableSlot *
exec_foreign_update(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot,
					TupleTableSlot *planSlot)
{
	log_func_name(__func__);
	return slot;
}

static TupleTableSlot *
exec_foreign_delete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot,
					TupleTableSlot *planSlot)
{
	log_func_name(__func__);
	return slot;
}

static void
end_foreign_modify(EState *estate, ResultRelInfo *rinfo)
{
	log_func_name(__func__);
}

static int
is_foreign_rel_updatable(Relation rel)
{
	log_func_name(__func__);
	return (1 << CMD_INSERT) | (1 << CMD_DELETE) | (1 << CMD_UPDATE);
}

static void
explain_foreign_scan(ForeignScanState *node, struct ExplainState *es)
{
	log_func_name(__func__);
}

static void
explain_foreign_modify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private,
					   int subplan_index, struct ExplainState *es)
{
	log_func_name(__func__);
}

static bool
analyze_foreign_table(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	log_func_name(__func__);
	return false;
}

static FdwRoutine timescaledb_fdw_routine = {
	.type = T_FdwRoutine,
	/* scan (mandatory) */
	.GetForeignPaths = get_foreign_paths,
	.GetForeignRelSize = get_foreign_rel_size,
	.GetForeignPlan = get_foreign_plan,
	.BeginForeignScan = begin_foreign_scan,
	.IterateForeignScan = iterate_foreign_scan,
	.EndForeignScan = end_foreign_scan,
	.ReScanForeignScan = rescan_foreign_scan,
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
	.AnalyzeForeignTable = analyze_foreign_table,
};

Datum
timescaledb_fdw_handler(PG_FUNCTION_ARGS)
{
	log_func_name(__func__);
	PG_RETURN_POINTER(&timescaledb_fdw_routine);
}
