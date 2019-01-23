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
#include <catalog/pg_type.h>
#include <foreign/fdwapi.h>
#include <foreign/foreign.h>
#include <nodes/relation.h>
#include <nodes/makefuncs.h>
#include <parser/parsetree.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/planmain.h>
#include <access/htup_details.h>
#include <access/sysattr.h>
#include <executor/executor.h>
#include <commands/explain.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <miscadmin.h>

#include <export.h>
#include <hypertable_server.h>
#include <hypertable_cache.h>
#include <chunk_server.h>
#include <chunk_insert_state.h>
#include <remote/dist_txn.h>
#include <remote/async.h>
#include <compat.h>

#include "utils.h"
#include "timescaledb_fdw.h"
#include "deparse.h"

TS_FUNCTION_INFO_V1(timescaledb_fdw_handler);
TS_FUNCTION_INFO_V1(timescaledb_fdw_validator);

typedef struct TsScanState
{
	int row_counter;
} TsScanState;

/*
 * This enum describes what's kept in the fdw_private list for a ModifyTable
 * node referencing a timescaledb_fdw foreign table.  We store:
 *
 * 1) INSERT/UPDATE/DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT/UPDATE
 *	  (NIL for a DELETE)
 * 3) Boolean flag showing if the remote query has a RETURNING clause
 * 4) Integer list of attribute numbers retrieved by RETURNING, if any
 */
enum FdwModifyPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwModifyPrivateUpdateSql,
	/* Integer list of target attribute numbers for INSERT/UPDATE */
	FdwModifyPrivateTargetAttnums,
	/* has-returning flag (as an integer Value node) */
	FdwModifyPrivateHasReturning,
	/* Integer list of attribute numbers retrieved by RETURNING */
	FdwModifyPrivateRetrievedAttrs,
	/* Insert state for the current chunk */
	FdwModifyPrivateChunkInsertState,
};

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct TsFdwModifyState
{
	Relation rel;			  /* relcache entry for the foreign table */
	AttInMetadata *attinmeta; /* attribute datatype conversion metadata */

	/* for remote query execution */
	PGconn *conn; /* connection for the scan */
	PreparedStmt *p_stmt;

	/* extracted fdw_private data */
	char *query;		   /* text of INSERT/UPDATE/DELETE command */
	List *target_attrs;	/* list of target attribute numbers */
	bool has_returning;	/* is there a RETURNING clause? */
	List *retrieved_attrs; /* attr numbers retrieved by RETURNING */

	/* info about parameters for prepared statement */
	AttrNumber ctidAttno; /* attnum of input resjunk ctid column */
	int p_nums;			  /* number of parameters to transmit */
	FmgrInfo *p_flinfo;   /* output conversion functions for them */

	/* working memory context */
	MemoryContext temp_cxt; /* context for per-tuple temporary data */
} TsFdwModifyState;

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
	baserel->rows = 0;
}

static void
get_foreign_paths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	Cost startup_cost, total_cost;

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

	scan_clauses = extract_actual_clauses(scan_clauses, false);
	return make_foreignscan(tlist, scan_clauses, scan_relid, NIL, NIL, NIL, NIL, outer_plan);
}

/*
 * create_foreign_modify
 *		Construct an execution state of a foreign insert/update/delete
 *		operation
 */
static TsFdwModifyState *
create_foreign_modify(EState *estate, RangeTblEntry *rte, ResultRelInfo *rri, CmdType operation,
					  Plan *subplan, char *query, List *target_attrs, bool has_returning,
					  List *retrieved_attrs)
{
	TsFdwModifyState *fmstate;
	Relation rel = rri->ri_RelationDesc;
	TupleDesc tupdesc = RelationGetDescr(rel);
	Oid userid;
	ForeignTable *table;
	UserMapping *user;
	AttrNumber n_params;
	Oid typefnoid;
	bool isvarlena;
	ListCell *lc;

	/* Begin constructing TsFdwModifyState. */
	fmstate = (TsFdwModifyState *) palloc0(sizeof(TsFdwModifyState));
	fmstate->rel = rel;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign table. */
	table = GetForeignTable(RelationGetRelid(rel));
	user = GetUserMapping(userid, table->serverid);

	/* Open connection; report that we'll create a prepared statement. */
	fmstate->conn = remote_dist_txn_get_connection(user, REMOTE_TXN_USE_PREP_STMT);
	fmstate->p_stmt = NULL;

	/* Set up remote query information. */
	fmstate->query = query;
	fmstate->target_attrs = target_attrs;
	fmstate->has_returning = has_returning;
	fmstate->retrieved_attrs = retrieved_attrs;

	/* Create context for per-tuple temp workspace. */
	fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "timescaledb_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/* Prepare for input conversion of RETURNING results. */
	if (fmstate->has_returning)
		fmstate->attinmeta = TupleDescGetAttInMetadata(tupdesc);

	/* Prepare for output conversion of parameters used in prepared stmt. */
	n_params = list_length(fmstate->target_attrs) + 1;
	fmstate->p_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * n_params);
	fmstate->p_nums = 0;

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		Assert(subplan != NULL);

		/* Find the ctid resjunk column in the subplan's result */
		fmstate->ctidAttno = ExecFindJunkAttributeInTlist(subplan->targetlist, "ctid");
		if (!AttributeNumberIsValid(fmstate->ctidAttno))
			elog(ERROR, "could not find junk ctid column");

		/* First transmittable parameter will be ctid */
		getTypeOutputInfo(TIDOID, &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
		fmstate->p_nums++;
	}

	if (operation == CMD_INSERT || operation == CMD_UPDATE)
	{
		/* Set up for remaining transmittable parameters */
		foreach (lc, fmstate->target_attrs)
		{
			int attnum = lfirst_int(lc);
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			Assert(!attr->attisdropped);

			getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
			fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
			fmstate->p_nums++;
		}
	}

	Assert(fmstate->p_nums <= n_params);

	return fmstate;
}

static void
begin_foreign_scan(ForeignScanState *node, int eflags)
{
	TsScanState *state;

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
}

static void
end_foreign_scan(ForeignScanState *node)
{
}

/*
 * Add resjunk column(s) needed for update/delete on a foreign table
 */
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

static List *
get_insert_attrs(Relation rel)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	List *attrs = NIL;
	int i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (!attr->attisdropped)
			attrs = lappend_int(attrs, AttrOffsetGetAttrNumber(i));
	}

	return attrs;
}

static List *
get_update_attrs(RangeTblEntry *rte)
{
	List *attrs = NIL;
	int col = -1;

	while ((col = bms_next_member(rte->updatedCols, col)) >= 0)
	{
		/* bit numbers are offset by FirstLowInvalidHeapAttributeNumber */
		AttrNumber attno = col + FirstLowInvalidHeapAttributeNumber;

		if (attno <= InvalidAttrNumber) /* shouldn't happen */
			elog(ERROR, "system-column update is not supported");

		attrs = lappend_int(attrs, attno);
	}

	return attrs;
}

/*
 * Plan INSERT, UPDATE, and DELETE.
 *
 * The main task of this function is to generate (deparse) the SQL statement
 * to send to remote tables. For the TimescaleDB insert path, we actually call
 * this function only once on the hypertable's root table instead of once per
 * chunk. This is because we want to send INSERT statements to each remote
 * hypertable rather than each remote chunk.
 *
 * UPDATEs and DELETEs work slightly different since we have no "optimized"
 * path for such operations. Instead, they happen once per chunk.
 */
static List *
plan_foreign_modify(PlannerInfo *root, ModifyTable *plan, Index result_relation, int subplan_index)
{
	CmdType operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(result_relation, root);
	Relation rel;
	StringInfoData sql;
	List *returning_list = NIL;
	List *retrieved_attrs = NIL;
	List *target_attrs = NIL;
	bool do_nothing = false;

	initStringInfo(&sql);

	/*
	 * Extract the relevant RETURNING list if any.
	 */
	if (plan->returningLists)
		returning_list = (List *) list_nth(plan->returningLists, subplan_index);

	/*
	 * ON CONFLICT DO UPDATE and DO NOTHING case with inference specification
	 * should have already been rejected in the optimizer, as presently there
	 * is no way to recognize an arbiter index on a foreign table.  Only DO
	 * NOTHING is supported without an inference specification.
	 */
	if (plan->onConflictAction == ONCONFLICT_NOTHING)
		do_nothing = true;
	else if (plan->onConflictAction != ONCONFLICT_NONE)
		elog(ERROR, "unexpected ON CONFLICT specification: %d", (int) plan->onConflictAction);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/*
	 * Construct the SQL command string
	 *
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, we transmit only columns that were explicitly
	 * targets of the UPDATE, so as to avoid unnecessary data transmission.
	 * (We can't do that for INSERT since we would miss sending default values
	 * for columns not listed in the source statement.)
	 */
	switch (operation)
	{
		case CMD_INSERT:
			target_attrs = get_insert_attrs(rel);
			deparseInsertSql(&sql,
							 rte,
							 result_relation,
							 rel,
							 target_attrs,
							 do_nothing,
							 returning_list,
							 &retrieved_attrs);
			break;
		case CMD_UPDATE:
			target_attrs = get_update_attrs(rte);
			deparseUpdateSql(&sql,
							 rte,
							 result_relation,
							 rel,
							 target_attrs,
							 returning_list,
							 &retrieved_attrs);
			break;
		case CMD_DELETE:
			deparseDeleteSql(&sql, rte, result_relation, rel, returning_list, &retrieved_attrs);
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
					  target_attrs,
					  makeInteger((retrieved_attrs != NIL)),
					  retrieved_attrs);
}

/*
 * Convert a relation's attribute numbers to the corresponding numbers for
 * another relation.
 *
 * Conversions are necessary when, e.g., a (new) chunk's attribute numbers do
 * not match the root table's numbers after a column has been removed.
 */
static List *
convert_attrs(TupleConversionMap *map, List *attrs)
{
	List *new_attrs = NIL;
	ListCell *lc;

	foreach (lc, attrs)
	{
		AttrNumber attnum = lfirst_int(lc);
		int i;

		for (i = 0; i < map->outdesc->natts; i++)
		{
			if (map->attrMap[i] == attnum)
			{
				new_attrs = lappend_int(new_attrs, AttrOffsetGetAttrNumber(i));
				break;
			}
		}

		/* Assert that we found the attribute */
		Assert(i != map->outdesc->natts);
	}

	Assert(list_length(attrs) == list_length(new_attrs));

	return new_attrs;
}

static void
begin_foreign_modify(ModifyTableState *mtstate, ResultRelInfo *rri, List *fdw_private,
					 int subplan_index, int eflags)
{
	TsFdwModifyState *fmstate;
	char *query;
	List *target_attrs;
	bool has_returning;
	List *retrieved_attrs;
	ChunkInsertState *cis;
	RangeTblEntry *rte;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  rri->ri_FdwState stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/* Deconstruct fdw_private data. */
	query = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));
	target_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateTargetAttnums);
	has_returning = intVal(list_nth(fdw_private, FdwModifyPrivateHasReturning));
	retrieved_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateRetrievedAttrs);

	if (list_length(fdw_private) > FdwModifyPrivateChunkInsertState)
	{
		cis = (ChunkInsertState *) list_nth(fdw_private, FdwModifyPrivateChunkInsertState);

		/*
		 * A chunk may have different attribute numbers than the root relation
		 * that we planned the attribute lists for
		 */
		if (NULL != cis->tup_conv_map)
		{
			/*
			 * Convert the target attributes (the inserted or updated
			 * attributes)
			 */
			target_attrs = convert_attrs(cis->tup_conv_map, target_attrs);

			/*
			 * Convert the retrieved attributes, if there is a RETURNING
			 * statement
			 */
			if (NIL != retrieved_attrs)
				retrieved_attrs = convert_attrs(cis->tup_conv_map, retrieved_attrs);
		}
	}

	/* Find RTE. */
	rte = rt_fetch(rri->ri_RangeTableIndex, mtstate->ps.state->es_range_table);

	/* Construct an execution state. */
	fmstate = create_foreign_modify(mtstate->ps.state,
									rte,
									rri,
									mtstate->operation,
									mtstate->mt_plans[subplan_index]->plan,
									query,
									target_attrs,
									has_returning,
									retrieved_attrs);

	rri->ri_FdwState = fmstate;
}

/*
 * convert_prep_stmt_params
 *		Create array of text strings representing parameter values
 *
 * tupleid is ctid to send, or NULL if none
 * slot is slot to get remaining parameters from, or NULL if none
 *
 * Data is constructed in temp_cxt; caller should reset that after use.
 */
static const char **
convert_prep_stmt_params(TsFdwModifyState *fmstate, ItemPointer tupleid, TupleTableSlot *slot)
{
	const char **p_values;
	int pindex = 0;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

	p_values = (const char **) palloc(sizeof(char *) * fmstate->p_nums);

	/* 1st parameter should be ctid, if it's in use */
	if (tupleid != NULL)
	{
		/* don't need set_transmission_modes for TID output */
		p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex], PointerGetDatum(tupleid));
		pindex++;
	}

	/* get following parameters from slot */
	if (slot != NULL && fmstate->target_attrs != NIL)
	{
		int nestlevel;
		ListCell *lc;

		nestlevel = set_transmission_modes();

		foreach (lc, fmstate->target_attrs)
		{
			int attnum = lfirst_int(lc);
			Datum value;
			bool isnull;

			value = slot_getattr(slot, attnum, &isnull);
			if (isnull)
				p_values[pindex] = NULL;
			else
				p_values[pindex] = OutputFunctionCall(&fmstate->p_flinfo[pindex], value);
			pindex++;
		}

		reset_transmission_modes(nestlevel);
	}

	Assert(pindex == fmstate->p_nums);

	MemoryContextSwitchTo(oldcontext);

	return p_values;
}

/*
 * store_returning_result
 *		Store the result of a RETURNING clause
 *
 * On error, be sure to release the PGresult on the way out.  Callers do not
 * have PG_TRY blocks to ensure this happens.
 */
static void
store_returning_result(TsFdwModifyState *fmstate, TupleTableSlot *slot, PGresult *res)
{
	PG_TRY();
	{
		HeapTuple newtup;

		newtup = make_tuple_from_result_row(res,
											0,
											fmstate->rel,
											fmstate->attinmeta,
											fmstate->retrieved_attrs,
											NULL,
											fmstate->temp_cxt);
		/* tuple will be deleted when it is cleared from the slot */
		ExecStoreTuple(newtup, slot, InvalidBuffer, true);
	}
	PG_CATCH();
	{
		if (res)
			PQclear(res);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * prepare_foreign_modify
 *		Establish a prepared statement for execution of INSERT/UPDATE/DELETE
 */
static void
prepare_foreign_modify(TsFdwModifyState *fmstate)
{
	AsyncRequest *req;

	req = async_request_send_prepare(fmstate->conn, fmstate->query, fmstate->p_nums);

	Assert(NULL != req);

	fmstate->p_stmt = async_request_wait_prepared_statement(req);
}

static TupleTableSlot *
exec_foreign_insert(EState *estate, ResultRelInfo *rri, TupleTableSlot *slot,
					TupleTableSlot *planSlot)
{
	TsFdwModifyState *fmstate = (TsFdwModifyState *) rri->ri_FdwState;
	AsyncRequest *req;
	AsyncResponseResult *rsp;
	const char **p_values;
	PGresult *res;
	int n_rows;

	/* Set up the prepared statement on the remote server, if we didn't yet */
	if (!fmstate->p_stmt)
		prepare_foreign_modify(fmstate);

	/* Convert parameters needed by prepared statement to text form */
	p_values = convert_prep_stmt_params(fmstate, NULL, slot);

	req = async_request_send_prepared_stmt(fmstate->p_stmt, p_values);

	Assert(NULL != req);

	rsp = async_request_wait_any_result(req);

	Assert(NULL != rsp);

	res = async_response_result_get_pg_result(rsp);

	if (PQresultStatus(res) != (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
		async_response_report_error((AsyncResponse *) rsp, ERROR);

	/* Check number of rows affected, and fetch RETURNING tuple if any */
	if (fmstate->has_returning)
	{
		n_rows = PQntuples(res);
		if (n_rows > 0)
			store_returning_result(fmstate, slot, res);
	}
	else
		n_rows = atoi(PQcmdTuples(res));

	/* And clean up */
	async_response_result_close(rsp);

	MemoryContextReset(fmstate->temp_cxt);

	/* Return NULL if nothing was inserted on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

static TupleTableSlot *
exec_foreign_update(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot,
					TupleTableSlot *planSlot)
{
	return slot;
}

static TupleTableSlot *
exec_foreign_delete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot,
					TupleTableSlot *planSlot)
{
	return slot;
}

/*
 * finish_foreign_modify
 *		Release resources for a foreign insert/update/delete operation
 */
static void
finish_foreign_modify(TsFdwModifyState *fmstate)
{
	Assert(fmstate != NULL);

	/* If we created a prepared statement, destroy it */
	if (NULL != fmstate->p_stmt)
		prepared_stmt_close(fmstate->p_stmt);

	fmstate->conn = NULL;
}

static void
end_foreign_modify(EState *estate, ResultRelInfo *rri)
{
	TsFdwModifyState *fmstate = (TsFdwModifyState *) rri->ri_FdwState;

	/* If fmstate is NULL, we are in EXPLAIN; nothing to do */
	if (fmstate == NULL)
		return;

	/* Destroy the execution state */
	finish_foreign_modify(fmstate);
}

static int
is_foreign_rel_updatable(Relation rel)
{
	return (1 << CMD_INSERT) | (1 << CMD_DELETE) | (1 << CMD_UPDATE);
}

static void
explain_foreign_scan(ForeignScanState *node, struct ExplainState *es)
{
}

static void
explain_foreign_modify(ModifyTableState *mtstate, ResultRelInfo *rri, List *fdw_private,
					   int subplan_index, struct ExplainState *es)
{
	if (es->verbose)
	{
		char *sql = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));

		ExplainPropertyText("Remote SQL", sql, es);
	}
}

static bool
analyze_foreign_table(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
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
	PG_RETURN_POINTER(&timescaledb_fdw_routine);
}
