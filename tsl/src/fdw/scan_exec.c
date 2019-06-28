/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <executor/executor.h>
#include <commands/explain.h>
#include <parser/parsetree.h>
#include <nodes/relation.h>
#include <nodes/nodeFuncs.h>
#include <utils/lsyscache.h>
#include <miscadmin.h>

#include <remote/dist_txn.h>
#include <remote/async.h>
#include <remote/stmt_params.h>

#include "scan_exec.h"
#include "utils.h"

/*
 * Indexes of FDW-private information stored in fdw_private lists.
 *
 * These items are indexed with the enum FdwScanPrivateIndex, so an item
 * can be fetched with list_nth().  For example, to get the SELECT statement:
 *		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
 */
enum FdwScanPrivateIndex
{
	/* SQL statement to execute remotely (as a String node) */
	FdwScanPrivateSelectSql,
	/* Integer list of attribute numbers retrieved by the SELECT */
	FdwScanPrivateRetrievedAttrs,
	/* Integer representing the desired fetch_size */
	FdwScanPrivateFetchSize,

	/* Integer for the OID of the foreign server, used by EXPLAIN */
	FdwScanPrivateServerId,
	/* OID list of chunk oids, used by EXPLAIN */
	FdwScanPrivateChunkOids,
	/*
	 * String describing join i.e. names of relations being joined and types
	 * of join, added when the scan is join
	 */
	FdwScanPrivateRelations
};

/*
 * Fill an array with query parameter values in text format.
 */
static void
fill_query_params_array(ExprContext *econtext, FmgrInfo *param_flinfo, List *param_exprs,
						const char **param_values)
{
	int nestlevel;
	int i;
	ListCell *lc;

	nestlevel = set_transmission_modes();

	i = 0;
	foreach (lc, param_exprs)
	{
		ExprState *expr_state = (ExprState *) lfirst(lc);
		Datum expr_value;
		bool is_null;

		/* Evaluate the parameter expression */
		expr_value = ExecEvalExpr(expr_state, econtext, &is_null);

		/*
		 * Get string representation of each parameter value by invoking
		 * type-specific output function, unless the value is null.
		 */
		if (is_null)
			param_values[i] = NULL;
		else
			param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);

		i++;
	}

	reset_transmission_modes(nestlevel);
}

/*
 * Create cursor for node's query with current parameter values.
 */
static void
create_cursor(ScanState *ss, TsFdwScanState *fsstate)
{
	ExprContext *econtext = ss->ps.ps_ExprContext;
	int num_params = fsstate->num_params;
	const char **values = fsstate->param_values;
	TSConnection *conn = fsstate->conn;
	StringInfoData buf;
	AsyncRequest *req;

	/*
	 * Construct array of query parameter values in text format.  We do the
	 * conversions in the short-lived per-tuple context, so as not to cause a
	 * memory leak over repeated scans.
	 */
	if (num_params > 0)
	{
		MemoryContext oldcontext;
		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
		fill_query_params_array(econtext, fsstate->param_flinfo, fsstate->param_exprs, values);
		MemoryContextSwitchTo(oldcontext);
	}

	/* Assign a unique ID for my cursor */
	fsstate->cursor_number = remote_connection_get_cursor_number();

	/* Construct the DECLARE CURSOR command */
	initStringInfo(&buf);
	appendStringInfo(&buf, "DECLARE c%u CURSOR FOR\n%s", fsstate->cursor_number, fsstate->query);

	/*
	 * Notice that we pass NULL for paramTypes, thus forcing the remote server
	 * to infer types for all parameters.  Since we explicitly cast every
	 * parameter (see deparse.c), the "inference" is trivial and will produce
	 * the desired result.  This allows us to avoid assuming that the remote
	 * server has the same OIDs we do for the parameters' types.
	 */
	req = async_request_send_with_params(conn,
										 buf.data,
										 stmt_params_create_from_values(values, num_params),
										 FORMAT_TEXT);

	Assert(NULL != req);

	async_request_wait_ok_command(req);
	pfree(req);

	/* Mark the cursor as created, and show no tuples have been retrieved */
	fsstate->cursor_exists = true;
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->fetch_ct_2 = 0;
	fsstate->eof_reached = false;

	/* Clean up */
	pfree(buf.data);
}

/*
 * Prepare for processing of parameters used in remote query.
 */
static void
prepare_query_params(PlanState *node, List *fdw_exprs, int num_params, FmgrInfo **param_flinfo,
					 List **param_exprs, const char ***param_values)
{
	int i;
	ListCell *lc;

	Assert(num_params > 0);

	/* Prepare for output conversion of parameters used in remote query. */
	*param_flinfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo) * num_params);

	i = 0;
	foreach (lc, fdw_exprs)
	{
		Node *param_expr = (Node *) lfirst(lc);
		Oid typefnoid;
		bool isvarlena;

		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &(*param_flinfo)[i]);
		i++;
	}

	/*
	 * Prepare remote-parameter expressions for evaluation.  (Note: in
	 * practice, we expect that all these expressions will be just Params, so
	 * we could possibly do something more efficient than using the full
	 * expression-eval machinery for this.  But probably there would be little
	 * benefit, and it'd require the foreign data wrapper to know more than is
	 * desirable about Param evaluation.)
	 */
	*param_exprs = ExecInitExprList(fdw_exprs, node);

	/* Allocate buffer for text form of query parameters. */
	*param_values = (const char **) palloc0(num_params * sizeof(char *));
}

void
fdw_scan_init(ScanState *ss, TsFdwScanState *fsstate, Bitmapset *scanrelids, List *fdw_private,
			  List *fdw_exprs, int eflags)
{
	Scan *scan = (Scan *) ss->ps.plan;
	EState *estate = ss->ps.state;
	RangeTblEntry *rte;
	Oid userid;
	UserMapping *user;
	int rtindex;
	int num_params;
	int server_id;

	/*
	 * Do nothing in EXPLAIN (no ANALYZE) case.  fdw_state stays NULL.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	/*
	 * We'll save private state in node->fdw_state.
	 */

	/*
	 * Identify which user to do the remote access as.  This should match what
	 * ExecCheckRTEPerms() does.  In case of a join or aggregate, use the
	 * lowest-numbered member RTE as a representative; we would get the same
	 * result from any.
	 */
	if (scan->scanrelid > 0)
		rtindex = scan->scanrelid;
	else
		rtindex = bms_next_member(scanrelids, -1);

	rte = rt_fetch(rtindex, estate->es_range_table);
	userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

	/* Get info about foreign server. */
	server_id = intVal(list_nth(fdw_private, FdwScanPrivateServerId));
	user = GetUserMapping(userid, server_id);

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fsstate->conn =
		remote_dist_txn_get_connection(user,
									   list_length(fdw_exprs) > 0 ? REMOTE_TXN_USE_PREP_STMT :
																	REMOTE_TXN_NO_PREP_STMT);

	/* Get private info created by planner functions. */
	fsstate->query = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fdw_private, FdwScanPrivateRetrievedAttrs);
	fsstate->fetch_size = intVal(list_nth(fdw_private, FdwScanPrivateFetchSize));

	/* Create contexts for batches of tuples and per-tuple temp workspace. */
	fsstate->batch_cxt = AllocSetContextCreate(estate->es_query_cxt,
											   "timescaledb_fdw tuple data",
											   ALLOCSET_DEFAULT_SIZES);
	fsstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
											  "timescaledb_fdw temporary data",
											  ALLOCSET_SMALL_SIZES);

	/*
	 * Get info we'll need for converting data fetched from the foreign server
	 * into local representation and error reporting during that process.
	 */
	if (scan->scanrelid > 0)
	{
		fsstate->rel = ss->ss_currentRelation;
		fsstate->tupdesc = RelationGetDescr(fsstate->rel);
	}
	else
	{
		fsstate->rel = NULL;
		fsstate->tupdesc = ss->ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	fsstate->att_conv_metadata = data_format_create_att_conv_in_metadata(fsstate->tupdesc);

	/*
	 * Prepare for processing of parameters used in remote query, if any.
	 */
	num_params = list_length(fdw_exprs);
	fsstate->num_params = num_params;

	if (num_params > 0)
		prepare_query_params(&ss->ps,
							 fdw_exprs,
							 num_params,
							 &fsstate->param_flinfo,
							 &fsstate->param_exprs,
							 &fsstate->param_values);

	fsstate->cursor_exists = false;
}

/*
 * Fetch some more rows from the node's cursor.
 */
static void
fetch_more_data(ScanState *ss, TsFdwScanState *fsstate)
{
	AsyncRequest *volatile req = NULL;
	AsyncResponseResult *volatile rsp = NULL;
	MemoryContext oldcontext;

	/*
	 * We'll store the tuples in the batch_cxt.  First, flush the previous
	 * batch.
	 */
	fsstate->tuples = NULL;
	MemoryContextReset(fsstate->batch_cxt);
	oldcontext = MemoryContextSwitchTo(fsstate->batch_cxt);

	/* PGresult must be released before leaving this function. */
	PG_TRY();
	{
		TSConnection *conn = fsstate->conn;
		PGresult *res;
		char sql[64];
		int numrows;
		int i;

		snprintf(sql,
				 sizeof(sql),
				 "FETCH %d FROM c%u",
				 fsstate->fetch_size,
				 fsstate->cursor_number);

		if (fsstate->att_conv_metadata->binary)
			req = async_request_send_binary(conn, sql);
		else
			req = async_request_send(conn, sql);

		Assert(NULL != req);

		rsp = async_request_wait_any_result(req);

		Assert(NULL != rsp);

		res = async_response_result_get_pg_result(rsp);

		/* On error, report the original query, not the FETCH. */
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			remote_connection_report_error(ERROR, res, fsstate->conn, false, fsstate->query);

		/* Convert the data into HeapTuples */
		numrows = PQntuples(res);
		fsstate->tuples = (HeapTuple *) palloc0(numrows * sizeof(HeapTuple));
		fsstate->num_tuples = numrows;
		fsstate->next_tuple = 0;

		for (i = 0; i < numrows; i++)
		{
			Assert(IsA(ss->ps.plan, ForeignScan) || IsA(ss->ps.plan, CustomScan));

			fsstate->tuples[i] = make_tuple_from_result_row(res,
															i,
															fsstate->rel,
															fsstate->att_conv_metadata,
															fsstate->retrieved_attrs,
															ss,
															fsstate->temp_cxt);
		}

		/* Update fetch_ct_2 */
		if (fsstate->fetch_ct_2 < 2)
			fsstate->fetch_ct_2++;

		/* Must be EOF if we didn't get as many tuples as we asked for. */
		fsstate->eof_reached = (numrows < fsstate->fetch_size);

		pfree(req);
		async_response_result_close(rsp);
		req = NULL;
		rsp = NULL;
	}
	PG_CATCH();
	{
		if (NULL != req)
			pfree(req);

		if (NULL != rsp)
			async_response_result_close(rsp);

		PG_RE_THROW();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
}

TupleTableSlot *
fdw_scan_iterate(ScanState *ss, TsFdwScanState *fsstate)
{
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;

	if (!fsstate->cursor_exists)
		create_cursor(ss, fsstate);

	/*
	 * Get some more tuples, if we've run out.
	 */
	if (fsstate->next_tuple >= fsstate->num_tuples)
	{
		/* No point in another fetch if we already detected EOF, though. */
		if (!fsstate->eof_reached)
			fetch_more_data(ss, fsstate);
		/* If we didn't get any tuples, must be end of data. */
		if (fsstate->next_tuple >= fsstate->num_tuples)
			return ExecClearTuple(slot);
	}

	/*
	 * Return the next tuple.
	 */
	ExecStoreTuple(fsstate->tuples[fsstate->next_tuple++], slot, InvalidBuffer, false);

	return slot;
}

void
fdw_scan_rescan(ScanState *ss, TsFdwScanState *fsstate)
{
	char sql[64];
	AsyncRequest *req;

	/* If we haven't created the cursor yet, nothing to do. */
	if (!fsstate->cursor_exists)
		return;

	/*
	 * If any internal parameters affecting this node have changed, we'd
	 * better destroy and recreate the cursor.  Otherwise, rewinding it should
	 * be good enough.  If we've only fetched zero or one batch, we needn't
	 * even rewind the cursor, just rescan what we have.
	 */
	if (ss->ps.chgParam != NULL)
	{
		fsstate->cursor_exists = false;
		snprintf(sql, sizeof(sql), "CLOSE c%u", fsstate->cursor_number);
	}
	else if (fsstate->fetch_ct_2 > 1)
	{
		snprintf(sql, sizeof(sql), "MOVE BACKWARD ALL IN c%u", fsstate->cursor_number);
	}
	else
	{
		/* Easy: just rescan what we already have in memory, if anything */
		fsstate->next_tuple = 0;
		return;
	}

	/*
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */
	req = async_request_send(fsstate->conn, sql);

	Assert(NULL != req);

	async_request_wait_ok_command(req);

	pfree(req);

	/* Now force a fresh FETCH. */
	fsstate->tuples = NULL;
	fsstate->num_tuples = 0;
	fsstate->next_tuple = 0;
	fsstate->fetch_ct_2 = 0;
	fsstate->eof_reached = false;
}

/*
 * Utility routine to close a cursor.
 */
static void
close_cursor(TSConnection *conn, unsigned int cursor_number)
{
	char sql[64];
	AsyncRequest *req;

	snprintf(sql, sizeof(sql), "CLOSE c%u", cursor_number);

	/*
	 * We don't use a PG_TRY block here, so be careful not to throw error
	 * without releasing the PGresult.
	 */

	req = async_request_send(conn, sql);

	Assert(NULL != req);

	async_request_wait_ok_command(req);

	pfree(req);
}

void
fdw_scan_end(TsFdwScanState *fsstate)
{
	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	/* Close the cursor if open, to prevent accumulation of cursors */
	if (fsstate->cursor_exists)
		close_cursor(fsstate->conn, fsstate->cursor_number);

	/* Release remote connection */
	fsstate->conn = NULL;

	/* MemoryContexts will be deleted automatically. */
}

void
fdw_scan_explain(ScanState *ss, List *fdw_private, ExplainState *es)
{
	const char *sql;
	const char *relations;

	/*
	 * Add names of relation handled by the foreign scan when the scan is an
	 * upper rel.
	 */
	if (list_length(fdw_private) > FdwScanPrivateRelations)
	{
		relations = strVal(list_nth(fdw_private, FdwScanPrivateRelations));
		ExplainPropertyText("Relations", relations, es);
	}

	/*
	 * Add remote query, server name, and chunks when VERBOSE option is specified.
	 */
	if (es->verbose)
	{
		Oid server_id = intVal(list_nth(fdw_private, FdwScanPrivateServerId));
		ForeignServer *server = GetForeignServer(server_id);
		List *chunk_oids = (List *) list_nth(fdw_private, FdwScanPrivateChunkOids);

		ExplainPropertyText("Server", server->servername, es);

		if (chunk_oids != NIL)
		{
			StringInfoData chunk_names;
			ListCell *lc;
			bool first = true;

			initStringInfo(&chunk_names);
			foreach (lc, chunk_oids)
			{
				if (!first)
					appendStringInfoString(&chunk_names, ", ");
				else
					first = false;
				appendStringInfoString(&chunk_names, get_rel_name(lfirst_oid(lc)));
			}
			ExplainPropertyText("Chunks", chunk_names.data, es);
		}

		sql = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
		ExplainPropertyText("Remote SQL", sql, es);
	}
}
