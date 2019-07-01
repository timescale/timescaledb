/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <executor/executor.h>
#include <parser/parsetree.h>
#include <nodes/plannodes.h>
#include <nodes/relation.h>
#include <commands/explain.h>
#include <foreign/fdwapi.h>
#include <utils/rel.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <guc.h>

#include <remote/async.h>
#include <remote/stmt_params.h>
#include <remote/connection.h>
#include <remote/dist_txn.h>
#include <remote/utils.h>
#include <remote/tuplefactory.h>
#include <chunk_insert_state.h>

#include "scan_plan.h"
#include "modify_exec.h"

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
	/* The servers for the current chunk */
	FdwModifyPrivateServers,
	/* Insert state for the current chunk */
	FdwModifyPrivateChunkInsertState,
};

typedef struct TsFdwServerState
{
	Oid serverid;
	UserMapping *user;
	/* for remote query execution */
	TSConnection *conn;   /* connection for the scan */
	PreparedStmt *p_stmt; /* prepared statement handle, if created */
} TsFdwServerState;

/*
 * Execution state of a foreign insert/update/delete operation.
 */
typedef struct TsFdwModifyState
{
	Relation rel;						  /* relcache entry for the foreign table */
	AttConvInMetadata *att_conv_metadata; /* attribute datatype conversion metadata for converting
											 result to tuples */

	/* extracted fdw_private data */
	char *query;		/* text of INSERT/UPDATE/DELETE command */
	List *target_attrs; /* list of target attribute numbers */
	bool has_returning; /* is there a RETURNING clause? */
	TupleFactory *tupfactory;

	AttrNumber ctid_attno; /* attnum of input resjunk ctid column */

	bool prepared;
	int num_servers;
	StmtParams *stmt_params; /* prepared statement paremeters */
	TsFdwServerState servers[FLEXIBLE_ARRAY_MEMBER];
} TsFdwModifyState;

#define TS_FDW_MODIFY_STATE_SIZE(num_servers)                                                      \
	(sizeof(TsFdwModifyState) + (sizeof(TsFdwServerState) * num_servers))

static void
initialize_fdw_server_state(TsFdwServerState *fdw_server, UserMapping *um)
{
	fdw_server->user = um;
	fdw_server->serverid = um->serverid;
	fdw_server->conn = remote_dist_txn_get_connection(fdw_server->user, REMOTE_TXN_USE_PREP_STMT);
	fdw_server->p_stmt = NULL;
}

/*
 * create_foreign_modify
 *		Construct an execution state of a foreign insert/update/delete
 *		operation
 */
static TsFdwModifyState *
create_foreign_modify(EState *estate, Relation rel, CmdType operation, Oid check_as_user,
					  Plan *subplan, char *query, List *target_attrs, bool has_returning,
					  List *retrieved_attrs, List *usermappings)
{
	TsFdwModifyState *fmstate;
	TupleDesc tupdesc = RelationGetDescr(rel);
	ListCell *lc;
	int i = 0;
	int num_servers = usermappings == NIL ? 1 : list_length(usermappings);

	/* Begin constructing TsFdwModifyState. */
	fmstate = (TsFdwModifyState *) palloc0(TS_FDW_MODIFY_STATE_SIZE(num_servers));
	fmstate->rel = rel;

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.
	 */

	if (NIL != usermappings)
	{
		/*
		 * This is either (1) an INSERT on a hypertable chunk, or (2) an
		 * UPDATE or DELETE on a chunk. In the former case (1), the servers
		 * were passed on from the INSERT path via the chunk insert state, and
		 * in the latter case (2), the servers were resolved at planning time
		 * in the FDW planning callback.
		 */

		foreach (lc, usermappings)
		{
			UserMapping *um = lfirst(lc);

			initialize_fdw_server_state(&fmstate->servers[i++], um);
		}
	}
	else
	{
		/*
		 * If there is no chunk insert state and no servers from planning,
		 * this is an INSERT, UPDATE, or DELETE on a standalone foreign table.
		 * We must get the server from the foreign table's metadata.
		 */
		ForeignTable *table = GetForeignTable(rel->rd_id);
		Oid userid = OidIsValid(check_as_user) ? check_as_user : GetUserId();
		UserMapping *um = GetUserMapping(userid, table->serverid);

		initialize_fdw_server_state(&fmstate->servers[0], um);
	}

	/* Set up remote query information. */
	fmstate->query = query;
	fmstate->target_attrs = target_attrs;
	fmstate->has_returning = has_returning;
	fmstate->prepared = false; /* PREPARE will happen later */
	fmstate->num_servers = num_servers;

	/* Prepare for input conversion of RETURNING results. */
	if (fmstate->has_returning)
		fmstate->att_conv_metadata = data_format_create_att_conv_in_metadata(tupdesc);

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		Assert(subplan != NULL);

		/* Find the ctid resjunk column in the subplan's result */
		fmstate->ctid_attno = ExecFindJunkAttributeInTlist(subplan->targetlist, "ctid");
		if (!AttributeNumberIsValid(fmstate->ctid_attno))
			elog(ERROR, "could not find junk ctid column");
	}

	fmstate->stmt_params = stmt_params_create(fmstate->target_attrs,
											  operation == CMD_UPDATE || operation == CMD_DELETE,
											  tupdesc,
											  1);

	fmstate->tupfactory = tuplefactory_create_for_rel(rel, retrieved_attrs);

	return fmstate;
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

void
fdw_begin_foreign_modify(PlanState *pstate, ResultRelInfo *rri, CmdType operation,
						 List *fdw_private, Plan *subplan)
{
	TsFdwModifyState *fmstate;
	EState *estate = pstate->state;
	char *query;
	List *target_attrs;
	bool has_returning;
	List *retrieved_attrs;
	List *usermappings = NIL;
	ChunkInsertState *cis = NULL;
	RangeTblEntry *rte;

	/* Deconstruct fdw_private data. */
	query = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));
	target_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateTargetAttnums);
	has_returning = intVal(list_nth(fdw_private, FdwModifyPrivateHasReturning));
	retrieved_attrs = (List *) list_nth(fdw_private, FdwModifyPrivateRetrievedAttrs);

	/* Find RTE. */
	rte = rt_fetch(rri->ri_RangeTableIndex, estate->es_range_table);

	Assert(NULL != rte);

	if (list_length(fdw_private) > FdwModifyPrivateServers)
	{
		List *servers = (List *) list_nth(fdw_private, FdwModifyPrivateServers);
		ListCell *lc;

		foreach (lc, servers)
		{
			Oid serverid = lfirst_oid(lc);
			Oid userid = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
			UserMapping *um = GetUserMapping(userid, serverid);

			usermappings = lappend(usermappings, um);
		}
	}

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

		/*
		 * If there's a chunk insert state, then it has the authoritative
		 * server list.
		 */
		usermappings = cis->usermappings;
	}

	/* Construct an execution state. */
	fmstate = create_foreign_modify(estate,
									rri->ri_RelationDesc,
									operation,
									rte->checkAsUser,
									subplan,
									query,
									target_attrs,
									has_returning,
									retrieved_attrs,
									usermappings);

	rri->ri_FdwState = fmstate;
}

static PreparedStmt *
prepare_foreign_modify_server(TsFdwModifyState *fmstate, TsFdwServerState *fdw_server)
{
	AsyncRequest *req;

	Assert(NULL == fdw_server->p_stmt);

	req = async_request_send_prepare(fdw_server->conn,
									 fmstate->query,
									 stmt_params_num_params(fmstate->stmt_params));

	Assert(NULL != req);

	/*
	 * Async request interface doesn't seem to allow waiting for multiple
	 * prepared statements in an AsyncRequestSet. Should fix async API
	 */
	return async_request_wait_prepared_statement(req);
}

/*
 * prepare_foreign_modify
 *		Establish a prepared statement for execution of INSERT/UPDATE/DELETE
 */
static void
prepare_foreign_modify(TsFdwModifyState *fmstate)
{
	int i;

	for (i = 0; i < fmstate->num_servers; i++)
	{
		TsFdwServerState *fdw_server = &fmstate->servers[i];

		fdw_server->p_stmt = prepare_foreign_modify_server(fmstate, fdw_server);
	}

	fmstate->prepared = true;
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
		HeapTuple newtup = tuplefactory_make_tuple(fmstate->tupfactory, res, 0);

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

static int
response_type(AttConvInMetadata *att_conv_metadata)
{
	if (!ts_guc_enable_connection_binary_data)
		return FORMAT_TEXT;
	return att_conv_metadata == NULL || att_conv_metadata->binary ? FORMAT_BINARY : FORMAT_TEXT;
}

TupleTableSlot *
fdw_exec_foreign_insert(TsFdwModifyState *fmstate, EState *estate, TupleTableSlot *slot,
						TupleTableSlot *planslot)
{
	StmtParams *params = fmstate->stmt_params;
	AsyncRequestSet *reqset;
	AsyncResponseResult *rsp;
	int n_rows = -1;
	int i;

	if (!fmstate->prepared)
		prepare_foreign_modify(fmstate);

	reqset = async_request_set_create();

	stmt_params_convert_values(params, slot, NULL);

	for (i = 0; i < fmstate->num_servers; i++)
	{
		TsFdwServerState *fdw_server = &fmstate->servers[i];
		AsyncRequest *req = NULL;
		int type = response_type(fmstate->att_conv_metadata);
		req = async_request_send_prepared_stmt_with_params(fdw_server->p_stmt, params, type);
		Assert(NULL != req);
		async_request_set_add(reqset, req);
	}

	while ((rsp = async_request_set_wait_any_result(reqset)))
	{
		PGresult *res = async_response_result_get_pg_result(rsp);

		if (PQresultStatus(res) != (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
			async_response_report_error((AsyncResponse *) rsp, ERROR);

		/*
		 * If we insert into multiple replica chunks, we should only return
		 * the results from the first one
		 */
		if (n_rows == -1)
		{
			/* Check number of rows affected, and fetch RETURNING tuple if any */
			if (fmstate->has_returning)
			{
				n_rows = PQntuples(res);

				if (n_rows > 0)
					store_returning_result(fmstate, slot, res);
			}
			else
				n_rows = atoi(PQcmdTuples(res));
		}

		/* And clean up */
		async_response_result_close(rsp);
		stmt_params_reset(params);
	}

	/*
	 * Currently no way to do a deep cleanup of all request in the request
	 * set. The worry here is that since this runs in a per-chunk insert state
	 * memory context, the async API will accumulate a lot of cruft during
	 * inserts
	 */
	pfree(reqset);

	/* Return NULL if nothing was inserted on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

/*
 * Execute either an UPDATE or DELETE.
 */
TupleTableSlot *
fdw_exec_foreign_update_or_delete(TsFdwModifyState *fmstate, EState *estate, TupleTableSlot *slot,
								  TupleTableSlot *planslot, ModifyCommand cmd)
{
	StmtParams *params = fmstate->stmt_params;
	AsyncRequestSet *reqset;
	AsyncResponseResult *rsp;
	Datum datum;
	bool is_null;
	int n_rows = -1;
	int i;

	/* Set up the prepared statement on the remote server, if we didn't yet */
	if (!fmstate->prepared)
		prepare_foreign_modify(fmstate);

	/* Get the ctid that was passed up as a resjunk column */
	datum = ExecGetJunkAttribute(planslot, fmstate->ctid_attno, &is_null);

	/* shouldn't ever get a null result... */
	if (is_null)
		elog(ERROR, "ctid is NULL");

	stmt_params_convert_values(params,
							   (cmd == UPDATE_CMD ? slot : NULL),
							   (ItemPointer) DatumGetPointer(datum));
	reqset = async_request_set_create();

	for (i = 0; i < fmstate->num_servers; i++)
	{
		AsyncRequest *req = NULL;
		TsFdwServerState *fdw_server = &fmstate->servers[i];
		int type = response_type(fmstate->att_conv_metadata);
		req = async_request_send_prepared_stmt_with_params(fdw_server->p_stmt, params, type);

		Assert(NULL != req);

		async_request_attach_user_data(req, fdw_server);
		async_request_set_add(reqset, req);
	}

	while ((rsp = async_request_set_wait_any_result(reqset)))
	{
		PGresult *res = async_response_result_get_pg_result(rsp);
		TsFdwServerState *fdw_server = async_response_result_get_user_data(rsp);

		if (PQresultStatus(res) != (fmstate->has_returning ? PGRES_TUPLES_OK : PGRES_COMMAND_OK))
			remote_connection_report_error(ERROR, res, fdw_server->conn, false, fmstate->query);

		/*
		 * If we update multiple replica chunks, we should only return the
		 * results from the first one.
		 */
		if (n_rows == -1)
		{
			/* Check number of rows affected, and fetch RETURNING tuple if any */
			if (fmstate->has_returning)
			{
				n_rows = PQntuples(res);

				if (n_rows > 0)
					store_returning_result(fmstate, slot, res);
			}
			else
				n_rows = atoi(PQcmdTuples(res));
		}

		/* And clean up */
		async_response_result_close(rsp);
	}

	/*
	 * Currently no way to do a deep cleanup of all request in the request
	 * set. The worry here is that since this runs in a per-chunk insert state
	 * memory context, the async API will accumulate a lot of cruft during
	 * inserts
	 */
	pfree(reqset);
	stmt_params_reset(params);

	/* Return NULL if nothing was updated on the remote end */
	return (n_rows > 0) ? slot : NULL;
}

/*
 * finish_foreign_modify
 *		Release resources for a foreign insert/update/delete operation
 */
void
fdw_finish_foreign_modify(TsFdwModifyState *fmstate)
{
	int i;

	Assert(fmstate != NULL);

	for (i = 0; i < fmstate->num_servers; i++)
	{
		TsFdwServerState *fdw_server = &fmstate->servers[i];

		/* If we created a prepared statement, destroy it */
		if (NULL != fdw_server->p_stmt)
		{
			prepared_stmt_close(fdw_server->p_stmt);
			fdw_server->p_stmt = NULL;
		}

		fdw_server->conn = NULL;
	}

	stmt_params_free(fmstate->stmt_params);
}

void
fdw_explain_modify(PlanState *ps, ResultRelInfo *rri, List *fdw_private, int subplan_index,
				   ExplainState *es)
{
	if (es->verbose)
	{
		const char *sql = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));

		ExplainPropertyText("Remote SQL", sql, es);
	}
}
