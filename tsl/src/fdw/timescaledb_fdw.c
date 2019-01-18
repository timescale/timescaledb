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
#include <nodes/relation.h>
#include <parser/parsetree.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/planmain.h>
#include <access/htup_details.h>
#include <access/sysattr.h>
#include <executor/executor.h>
#include <utils/builtins.h>

#include <export.h>

#include "utils.h"
#include "timescaledb_fdw.h"
#include "deparse.h"

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
	CmdType operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation rel;
	StringInfoData sql;
	List *targetAttrs = NIL;
	List *returningList = NIL;
	List *retrieved_attrs = NIL;
	bool doNothing = false;

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, we transmit only columns that were explicitly
	 * targets of the UPDATE, so as to avoid unnecessary data transmission.
	 * (We can't do that for INSERT since we would miss sending default values
	 * for columns not listed in the source statement.)
	 */
	if (operation == CMD_INSERT)
	{
		TupleDesc tupdesc = RelationGetDescr(rel);
		int attnum;

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = tupdesc->attrs[attnum - 1];

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}
	}
	else if (operation == CMD_UPDATE)
	{
		int col;

		col = -1;
		while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
		{
			/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
			AttrNumber attno = col + FirstLowInvalidHeapAttributeNumber;

			if (attno <= InvalidAttrNumber) /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, attno);
		}
	}

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
		returningList = (List *) list_nth(plan->returningLists, subplan_index);

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (plan->onConflictAction == ONCONFLICT_NOTHING)
		doNothing = true;
	else if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "unexpected ON CONFLICT specification: %d", (int) plan->onConflictAction);

	/*
	 * Construct the SQL command string.
	 */
	switch (operation)
	{
		case CMD_INSERT:
			deparseInsertSql(&sql,
							 root,
							 resultRelation,
							 rel,
							 targetAttrs,
							 doNothing,
							 returningList,
							 &retrieved_attrs);
			break;
		case CMD_UPDATE:
			deparseUpdateSql(&sql,
							 root,
							 resultRelation,
							 rel,
							 targetAttrs,
							 returningList,
							 &retrieved_attrs);
			break;
		case CMD_DELETE:
			deparseDeleteSql(&sql, root, resultRelation, rel, returningList, &retrieved_attrs);
			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
			break;
	}

	heap_close(rel, NoLock);

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match enum FdwModifyPrivateIndex, above.
	 */
	return list_make4(makeString(sql.data),
					  targetAttrs,
					  makeInteger((retrieved_attrs != NIL)),
					  retrieved_attrs);
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
