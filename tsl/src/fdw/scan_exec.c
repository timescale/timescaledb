/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <executor/executor.h>
#include <commands/explain.h>
#include <parser/parse_relation.h>
#include <parser/parsetree.h>
#include <nodes/nodeFuncs.h>
#include <utils/lsyscache.h>
#include <miscadmin.h>

#include <remote/dist_txn.h>
#include <remote/async.h>
#include <remote/stmt_params.h>
#include <remote/utils.h>

#include "scan_exec.h"
#include "utils.h"
#include "remote/data_fetcher.h"
#include "remote/copy_fetcher.h"
#include "remote/prepared_statement_fetcher.h"
#include "remote/cursor_fetcher.h"
#include "guc.h"
#include "planner.h"

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
 * Create data fetcher for node's query with current parameter values.
 */
DataFetcher *
create_data_fetcher(ScanState *ss, TsFdwScanState *fsstate)
{
	ExprContext *econtext = ss->ps.ps_ExprContext;
	int num_params = fsstate->num_params;
	const char **values = fsstate->param_values;
	MemoryContext oldcontext;
	StmtParams *params = NULL;
	DataFetcher *fetcher = NULL;

	if (NULL != fsstate->fetcher)
		return fsstate->fetcher;

	/*
	 * Construct array of query parameter values in text format.  We do the
	 * conversions in the short-lived per-tuple context, so as not to cause a
	 * memory leak over repeated scans.
	 */
	if (num_params > 0)
	{
		oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
		fill_query_params_array(econtext, fsstate->param_flinfo, fsstate->param_exprs, values);
		MemoryContextSwitchTo(oldcontext);

		/*
		 * Notice that we do not specify param types, thus forcing the data
		 * node to infer types for all parameters.  Since we explicitly cast
		 * every parameter (see deparse.c), the "inference" is trivial and
		 * will produce the desired result.  This allows us to avoid assuming
		 * that the data node has the same OIDs we do for the parameters'
		 * types.
		 */
		params = stmt_params_create_from_values(values, num_params);
	}

	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_query_memory);

	if (fsstate->planned_fetcher_type == CursorFetcherType)
	{
		fetcher =
			cursor_fetcher_create_for_scan(fsstate->conn, fsstate->query, params, fsstate->tf);
	}
	else if (fsstate->planned_fetcher_type == PreparedStatementFetcherType)
	{
		fetcher = prepared_statement_fetcher_create_for_scan(fsstate->conn,
															 fsstate->query,
															 params,
															 fsstate->tf);
	}
	else
	{
		/*
		 * The fetcher type must have been determined by the planner at this
		 * point, so we shouldn't see 'auto' here.
		 */
		Assert(fsstate->planned_fetcher_type == CopyFetcherType);
		fetcher = copy_fetcher_create_for_scan(fsstate->conn, fsstate->query, params, fsstate->tf);
	}

	fsstate->fetcher = fetcher;
	MemoryContextSwitchTo(oldcontext);

	fetcher->funcs->set_fetch_size(fetcher, fsstate->fetch_size);

	return fetcher;
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

#ifdef TS_DEBUG
/* Allow tests to specify the time to push down in place of now() */
TimestampTz ts_current_timestamp_override_value = -1;

extern void
fdw_scan_debug_override_current_timestamp(TimestampTz time)
{
	ts_current_timestamp_override_value = time;
}
#endif

static TSConnection *
get_connection(ScanState *ss, Oid const server_id, Bitmapset *scanrelids, List *exprs)
{
	Scan *scan = (Scan *) ss->ps.plan;
	EState *estate = ss->ps.state;
	RangeTblEntry *rte;
	TSConnectionId id;
	int rtindex;
	Oid user_oid;

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

#if PG16_LT
	user_oid = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
#else
	RTEPermissionInfo *perminfo = getRTEPermissionInfo(estate->es_rteperminfos, rte);
	user_oid = OidIsValid(perminfo->checkAsUser) ? perminfo->checkAsUser : GetUserId();
#endif

	remote_connection_id_set(&id, server_id, user_oid);

	return remote_dist_txn_get_connection(id,
										  list_length(exprs) ? REMOTE_TXN_USE_PREP_STMT :
															   REMOTE_TXN_NO_PREP_STMT);
}

void
fdw_scan_init(ScanState *ss, TsFdwScanState *fsstate, Bitmapset *scanrelids, List *fdw_private,
			  List *fdw_exprs, int eflags)
{
	int num_params;
	Oid server_oid;
	ForeignServer *server;

	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) && !ts_guc_enable_remote_explain)
		return;

	/* Check if the server is "available" for use before setting up a connection to it */
	server_oid = intVal(list_nth(fdw_private, FdwScanPrivateServerId));
	server = GetForeignServer(server_oid);
	if (!ts_data_node_is_available_by_server(server))
		ereport(ERROR, (errmsg("data node \"%s\" is not available", server->servername)));

	/*
	 * Get connection to the foreign server.  Connection manager will
	 * establish new connection if necessary.
	 */
	fsstate->conn = get_connection(ss, server_oid, scanrelids, fdw_exprs);

	/* Get private info created by planner functions. */
	fsstate->query = strVal(list_nth(fdw_private, FdwScanPrivateSelectSql));
	fsstate->retrieved_attrs = (List *) list_nth(fdw_private, FdwScanPrivateRetrievedAttrs);
	fsstate->fetch_size = intVal(list_nth(fdw_private, FdwScanPrivateFetchSize));

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

	fsstate->fetcher = NULL;

	fsstate->tf = tuplefactory_create_for_scan(ss, fsstate->retrieved_attrs);

	Assert(fsstate->planned_fetcher_type != AutoFetcherType);

	/*
	 * If the planner tells us to use the cursor fetcher because there are
	 * multiple distributed hypertables per query, we have no other option.
	 */
	if (fsstate->planned_fetcher_type == CursorFetcherType)
	{
		return;
	}

	if (!tuplefactory_is_binary(fsstate->tf) && fsstate->planned_fetcher_type == CopyFetcherType)
	{
		if (ts_guc_remote_data_fetcher == AutoFetcherType)
		{
			/*
			 * The user-set fetcher type was auto, and the planner decided to
			 * use COPY fetcher, but at execution time (now) we found out
			 * there is no binary serialization for some data types. In this
			 * case we can revert to cursor fetcher which supports text
			 * serialization.
			 */
			fsstate->planned_fetcher_type = CursorFetcherType;
		}
		else
		{
			ereport(ERROR,
					(errmsg("cannot use COPY fetcher because some of the column types do not "
							"have binary serialization")));
		}
	}

	/*
	 * COPY fetcher uses COPY statement that don't work with prepared
	 * statements. We only end up here in case the COPY fetcher was chosen by
	 * the user, so error out.
	 * Note that this can be optimized for parameters coming from initplans,
	 * where the parameter takes only one value and technically we could deparse
	 * it into the query string and use a non-parameterized COPY statement.
	 */
	if (num_params > 0 && fsstate->planned_fetcher_type == CopyFetcherType)
	{
		Assert(ts_guc_remote_data_fetcher == CopyFetcherType);
		ereport(ERROR,
				(errmsg("cannot use COPY fetcher because the plan is parameterized"),
				 errhint("Set \"timescaledb.remote_data_fetcher\" to \"cursor\" to explicitly "
						 "set the fetcher type or use \"auto\" to select the fetcher type "
						 "automatically.")));
	}
}

TupleTableSlot *
fdw_scan_iterate(ScanState *ss, TsFdwScanState *fsstate)
{
	TupleTableSlot *slot = ss->ss_ScanTupleSlot;
	DataFetcher *fetcher = fsstate->fetcher;

	if (NULL == fetcher)
		fetcher = create_data_fetcher(ss, fsstate);

	fetcher->funcs->store_next_tuple(fetcher, slot);

	return slot;
}

void
fdw_scan_rescan(ScanState *ss, TsFdwScanState *fsstate)
{
	DataFetcher *fetcher = fsstate->fetcher;

	/* If we haven't created the cursor yet, nothing to do. */
	if (NULL == fsstate->fetcher)
		return;

	/*
	 * If any internal parameters affecting this node have changed, we'd
	 * better destroy and recreate the cursor.  Otherwise, rewinding it should
	 * be good enough.  If we've only fetched zero or one batch, we needn't
	 * even rewind the cursor, just rescan what we have.
	 */
	if (ss->ps.chgParam != NULL)
	{
		int num_params = fsstate->num_params;
		Assert(num_params > 0);

		ExprContext *econtext = ss->ps.ps_ExprContext;

		/*
		 * Construct array of query parameter values in text format.
		 */
		const char **values = fsstate->param_values;
		fill_query_params_array(econtext, fsstate->param_flinfo, fsstate->param_exprs, values);

		/*
		 * Notice that we do not specify param types, thus forcing the data
		 * node to infer types for all parameters.  Since we explicitly cast
		 * every parameter (see deparse.c), the "inference" is trivial and
		 * will produce the desired result.  This allows us to avoid assuming
		 * that the data node has the same OIDs we do for the parameters'
		 * types.
		 */
		StmtParams *params = stmt_params_create_from_values(values, num_params);

		fetcher->funcs->rescan(fsstate->fetcher, params);
	}
	else
	{
		fetcher->funcs->rewind(fsstate->fetcher);
	}
}

void
fdw_scan_end(TsFdwScanState *fsstate)
{
	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (fsstate == NULL)
		return;

	/* Close the cursor if open, to prevent accumulation of cursors */
	if (NULL != fsstate->fetcher)
	{
		data_fetcher_free(fsstate->fetcher);
		fsstate->fetcher = NULL;
	}

	/* Release remote connection */
	fsstate->conn = NULL;

	/* MemoryContexts will be deleted automatically. */
}

static char *
get_data_node_explain(const char *sql, TSConnection *conn, ExplainState *es)
{
	AsyncRequest *volatile req = NULL;
	AsyncResponseResult *volatile res = NULL;
	StringInfo explain_sql = makeStringInfo();
	StringInfo buf = makeStringInfo();

	appendStringInfo(explain_sql, "%s", "EXPLAIN (VERBOSE ");
	if (es->analyze)
		appendStringInfo(explain_sql, "%s", ", ANALYZE");
	if (!es->costs)
		appendStringInfo(explain_sql, "%s", ", COSTS OFF");
	if (es->buffers)
		appendStringInfo(explain_sql, "%s", ", BUFFERS ON");
	if (!es->timing)
		appendStringInfo(explain_sql, "%s", ", TIMING OFF");
	if (es->summary)
		appendStringInfo(explain_sql, "%s", ", SUMMARY ON");
	else
		appendStringInfo(explain_sql, "%s", ", SUMMARY OFF");

	appendStringInfoChar(explain_sql, ')');

	appendStringInfo(explain_sql, " %s", sql);

	PG_TRY();
	{
		PGresult *pg_res;
		int i;

		req = async_request_send(conn, explain_sql->data);
		res = async_request_wait_ok_result(req);
		pg_res = async_response_result_get_pg_result(res);
		appendStringInfoChar(buf, '\n');

		for (i = 0; i < PQntuples(pg_res); i++)
		{
			appendStringInfoSpaces(buf, (es->indent + 1) * 2);
			appendStringInfo(buf, "%s\n", PQgetvalue(pg_res, i, 0));
		}

		pfree(req);
		async_response_result_close(res);
	}
	PG_CATCH();
	{
		if (req != NULL)
			pfree(req);
		if (res != NULL)
			async_response_result_close(res);

		PG_RE_THROW();
	}
	PG_END_TRY();

	return buf->data;
}

static char *
explain_fetcher_type(DataFetcherType type)
{
	switch (type)
	{
		case AutoFetcherType:
			return "Auto";
		case CopyFetcherType:
			return "COPY";
		case CursorFetcherType:
			return "Cursor";
		case PreparedStatementFetcherType:
			return "Prepared statement";
		default:
			Assert(false);
			return "";
	}
}

void
fdw_scan_explain(ScanState *ss, List *fdw_private, ExplainState *es, TsFdwScanState *fsstate)
{
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
	 * Add remote query, data node name, and chunks when VERBOSE option is specified.
	 */
	if (es->verbose)
	{
		Oid server_id = intVal(list_nth(fdw_private, FdwScanPrivateServerId));
		ForeignServer *server = GetForeignServer(server_id);
		List *chunk_oids = (List *) list_nth(fdw_private, FdwScanPrivateChunkOids);
		char *sql;

		ExplainPropertyText("Data node", server->servername, es);

		/* fsstate or fetcher can be NULL, so check that first */
		if (fsstate && fsstate->fetcher)
			ExplainPropertyText("Fetcher Type", explain_fetcher_type(fsstate->fetcher->type), es);

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

		/* fsstate should be set up but better check again to avoid crashes */
		if (ts_guc_enable_remote_explain && fsstate)
		{
			char *data_node_explain;

			/* EXPLAIN barfs on parameterized queries, so check that first */
			if (fsstate->num_params >= 1)
				data_node_explain = "Unavailable due to parameterized query";
			else
				data_node_explain = get_data_node_explain(fsstate->query, fsstate->conn, es);
			ExplainPropertyText("Remote EXPLAIN", data_node_explain, es);
		}
	}
}
