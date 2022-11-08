/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <catalog/namespace.h>
#include <funcapi.h>
#include <libpq-fe.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include <utils.h>

#include "dist_commands.h"
#include "dist_txn.h"
#include "connection_cache.h"
#include "data_node.h"
#include "dist_util.h"
#include "miscadmin.h"
#include "errors.h"
#include "deparse.h"
#include "debug_point.h"

#define CONNECTION_CLEANUP_TIME 30000

typedef struct DistPreparedStmt
{
	const char *data_node_name;
	PreparedStmt *prepared_stmt;
} DistPreparedStmt;

typedef struct DistCmdResponse
{
	const char *data_node;
	AsyncResponseResult *result;
	PGresult *pg_result;
	const char *errorMessage;
} DistCmdResponse;

typedef struct DistCmdResult
{
	Size num_responses;
	TypeFuncClass funcclass; /* Function class of invoked function, if any */
	Oid typeid;				 /* Expected result type, or InvalidOid */
	TupleDesc tupdesc;		 /* Tuple descriptor of invoked function
							  * result. Set if typeid is valid and has a
							  * composite return value */
	DistCmdResponse responses[FLEXIBLE_ARRAY_MEMBER];
} DistCmdResult;

typedef struct DistCmdResetCallback
{
	TSConnection *conn;
	PGresult *result;
} DistCmdResetCallback;

static DistCmdResult *
ts_dist_cmd_collect_responses(List *requests)
{
	AsyncRequestSet *rs;
	AsyncResponseResult *ar;
	ListCell *lc;
	DistCmdResult *results;
	int i = 0;

	rs = async_request_set_create();
	results = palloc0(sizeof(DistCmdResult) + list_length(requests) * sizeof(DistCmdResponse));

	foreach (lc, requests)
		async_request_set_add(rs, lfirst(lc));

	while ((ar = async_request_set_wait_ok_result(rs)))
	{
		DistCmdResponse *response = &results->responses[i];

		response->result = ar;
		response->data_node = pstrdup(async_response_result_get_user_data(ar));
		++i;
	}

	Assert(i == list_length(requests));
	results->num_responses = i;
	return results;
}

static List *
ts_dist_cmd_sanitize_data_node_list(List *data_nodes)
{
	switch (nodeTag(data_nodes))
	{
		case T_OidList:
			data_nodes = data_node_oids_to_node_name_list(data_nodes, ACL_NO_CHECK);
			break;
		case T_List:
			/* Already in the format we want */
			data_node_name_list_check_acl(data_nodes, ACL_NO_CHECK);
			break;
		default:
			elog(ERROR, "invalid list type %u", nodeTag(data_nodes));
			break;
	}
	return data_nodes;
}

/*
 * Callback to manage memory allocated by libpq via malloc
 *
 * If a PGresult does not belong to a connection or a connection isn't stored
 * in the connection cache, all memory associated needs to be handled when the
 * Memory Context is deleted/reset.
 */
static void
ts_dist_cmd_mctx_reset_callback(void *arg)
{
	Assert(arg != NULL);
	ListCell *lc;
	List *callback_list = *(List **) arg;

	foreach (lc, callback_list)
	{
		DistCmdResetCallback *cb = lfirst(lc);
		if (cb->result != NULL)
			PQclear(cb->result);
		if (cb->conn != NULL)
			remote_connection_close(cb->conn);
		pfree(cb);
	}
	list_free(callback_list);

	pfree(arg);
}

/*
 * Helper function that errors in case of OOM.
 *
 */
static PGresult *
ts_dist_cmd_make_pg_result(ExecStatusType status)
{
	PGresult *pg_result = PQmakeEmptyPGresult(NULL, status);
	if (pg_result == NULL)
	{
		/* If PQmakeEmptyPGresult returns NULL, we are OOM
		 * There's nothing left to do but to exit now */
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed to allocate pg_result")));
	}

	Assert(pg_result != NULL);
	return pg_result;
}

static void
ts_dist_cmd_create_error_response(DistCmdResponse *rsp, const char *err_msg, const char *data_node,
								  DistCmdResetCallback *cb)
{
	Assert(rsp != NULL);

	if (rsp->pg_result == NULL)
	{
		Assert(cb != NULL);
		PGresult *res = ts_dist_cmd_make_pg_result(PGRES_FATAL_ERROR);
		cb->result = res;
		rsp->pg_result = res;
	}

	/* Todo: There is no function to set errorMessage in a pg_result.
	 * For now, copy errorMessage around in DistCmdResponse */
	rsp->errorMessage = pstrdup(err_msg);
	rsp->data_node = pstrdup(data_node);
}

/*
 * Invoke SQL statement on the given data nodes without throwing.
 *
 * For each broken connection, a PGresult with PGRES_FATAL_ERROR is created.
 */
DistCmdResult *
ts_dist_cmd_invoke_on_data_nodes_no_throw(const char *sql, const char *search_path,
										  List *data_nodes)
{
	ListCell *lc;
	List *requests = NIL;
	List **callback_data;
	DistCmdResult *results;
	AsyncRequestSet *rs;
	AsyncResponse *ar;
	MemoryContextCallback *mcb;
	bool set_search_path = search_path != NULL;

	if (data_nodes == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no data nodes to execute command on"),
				 errhint("Add data nodes before executing a distributed command.")));

	data_nodes = ts_dist_cmd_sanitize_data_node_list(data_nodes);

	/* Since libpq uses malloc, register a callback that frees results
	 * on Memory Context Reset/Delete */
	callback_data = palloc(sizeof(List **));
	*callback_data = NIL;
	mcb = palloc0(sizeof(MemoryContextCallback));
	mcb->func = ts_dist_cmd_mctx_reset_callback;
	mcb->arg = callback_data;
	mcb->next = NULL;
	MemoryContextRegisterResetCallback(CurrentMemoryContext, mcb);

	int i = 0;
	results = palloc0(sizeof(DistCmdResult) + list_length(data_nodes) * sizeof(DistCmdResponse));
	rs = async_request_set_create();

	/* Connection, Search Path */
	foreach (lc, data_nodes)
	{
		const char *node_name = lfirst(lc);
		char *errmsg;
		AsyncRequest *req;
		bool error = false;
		DistCmdResetCallback *cb = palloc0(sizeof(DistCmdResetCallback));
		DistCmdResponse *response = &results->responses[i];
		TSConnection *connection = data_node_get_connection_nothrow(node_name, &errmsg);

		if (connection == NULL)
		{
			error = true;
			ts_dist_cmd_create_error_response(response, errmsg, node_name, cb);
		}
		else
			cb->conn = connection;
		*callback_data = lappend(*callback_data, cb);

		if (set_search_path)
		{
			char *set_search_path = psprintf("SET search_path = %s, pg_catalog", search_path);
			TimestampTz end_time =
				TimestampTzPlusMilliseconds(GetCurrentTimestamp(), CONNECTION_CLEANUP_TIME);

			ereport(DEBUG2,
					(errmsg_internal("sending \"%s\" to data node \"%s\"",
									 search_path,
									 node_name)));

			req = async_request_send(connection, set_search_path);
			ar = async_request_wait_any_response(req);

			/* This can not happen, so abort if it does */
			Assert(ar != NULL);

			/* A node can go down between establishing connection and setting search path */
			switch (async_response_get_type(ar))
			{
				case RESPONSE_RESULT:
				{
					PGresult *res = async_response_result_get_pg_result((AsyncResponseResult *) ar);
					ExecStatusType status = PQresultStatus(res);

					/* Need to set the remote connection to IDLE after a succesful
					 * search path set. Otherwise no further commands can execute.
					 * There is a chance that the connection died after a result
					 * but before closing */
					async_request_cleanup_result(req, end_time);
					remote_connection_set_status(connection, CONN_IDLE);

					switch (status)
					{
						/* Set search path is succesful */
						case PGRES_COMMAND_OK:
							break;
						/* Unexpected result, propage error and skip data node */
						default:
							ts_dist_cmd_create_error_response(response,
															  PQresultErrorMessage(res),
															  node_name,
															  cb);
							error = true;

							break;
					}

					break;
				}

				default:
					/* We did get an unexpected AsyncResponseResult. */
					error = true;
					char *message = async_response_get_error_message(ar, NULL);
					Assert(message != NULL);
					ts_dist_cmd_create_error_response(response, message, pstrdup(node_name), cb);

					async_request_cleanup_result(req, end_time);
					break;
			}

			async_response_close(ar);
		}

		if (error)
		{
			/* We have encountered an error: skip this data node */
			++i;
			continue;
		}
		ereport(DEBUG2, (errmsg_internal("sending \"%s\" to data node \"%s\"", sql, node_name)));

		req = async_request_send(connection, sql);
		async_request_attach_user_data(req, (char *) node_name);
		async_request_set_add(rs, req);
	}

	while ((ar = async_request_set_wait_any_response(rs)))
	{
		DistCmdResponse *response = &results->responses[i];
		const char *node_name = NULL;
		switch (async_response_get_type(ar))
		{
			case RESPONSE_RESULT:
			case RESPONSE_ROW:
			{
				node_name = async_response_result_get_user_data((AsyncResponseResult *) ar);

				/* Whatever result we have, simply return it. The caller has
				 * to handle PGresult errors */
				PGresult *res = async_response_result_get_pg_result((AsyncResponseResult *) ar);
				response->result = (AsyncResponseResult *) ar;
				response->errorMessage = pstrdup(PQresultErrorMessage(res));
				response->data_node = pstrdup(node_name);

				break;
			}
			case RESPONSE_TIMEOUT:
				/* async_request_set_wait_any_response() does not time out. If we end up
				 * here, something went very wrong. If a timeout occures, it happened for all
				 * remaining nodes, so we do not have a specific node to return. */
				elog(ERROR, "unexpected timeout in async request without timeout");

				break;
			default:
			{
				/* We do not have a PGresult. Make one */
				DistCmdResetCallback *cb = palloc0(sizeof(DistCmdResetCallback));
				char *message = async_response_get_error_message(ar, &node_name);
				ts_dist_cmd_create_error_response(response, message, pstrdup(node_name), cb);
				*callback_data = lappend(*callback_data, cb);
			}
		}

		++i;
	}

	Assert(i == list_length(data_nodes));
	results->num_responses = i;
	list_free(requests);

	return results;
}

DistCmdResult *
ts_dist_cmd_invoke_on_all_data_nodes_no_throw(const char *sql, const char *search_path)
{
	List *data_nodes = data_node_get_node_name_list();

	return ts_dist_cmd_invoke_on_data_nodes_no_throw(sql, search_path, data_nodes);
}

/*
 * Invoke multiple SQL statements (commands) on the given data nodes.
 *
 * The list of data nodes can either be a list of data node names, or foreign
 * server OIDs.
 *
 * If "transactional" is false then it means that the SQL should be executed
 * in autocommit (implicit statement level commit) mode without the need for
 * an explicit 2PC from the access node.
 */
DistCmdResult *
ts_dist_multi_cmds_params_invoke_on_data_nodes(List *cmd_descriptors, List *data_nodes,
											   bool transactional)
{
	ListCell *lc_data_node, *lc_cmd_descr;
	List *requests = NIL;
	DistCmdResult *results;

	if (data_nodes == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("no data nodes to execute command on"),
				 errhint("Add data nodes before executing a distributed command.")));

	Assert(list_length(data_nodes) == list_length(cmd_descriptors));
	data_nodes = ts_dist_cmd_sanitize_data_node_list(data_nodes);

	forboth (lc_data_node, data_nodes, lc_cmd_descr, cmd_descriptors)
	{
		const char *node_name = lfirst(lc_data_node);
		AsyncRequest *req;
		TSConnection *connection =
			data_node_get_connection(node_name, REMOTE_TXN_NO_PREP_STMT, transactional);
		DistCmdDescr *cmd_descr = lfirst(lc_cmd_descr);
		const char *sql = cmd_descr->sql;
		StmtParams *params = cmd_descr->params;

		ereport(DEBUG2, (errmsg_internal("sending \"%s\" to data node \"%s\"", sql, node_name)));

		if (params == NULL)
			req = async_request_send(connection, sql);
		else
			req = async_request_send_with_params(connection, sql, params, FORMAT_TEXT);

		async_request_attach_user_data(req, (char *) node_name);
		requests = lappend(requests, req);
	}

	results = ts_dist_cmd_collect_responses(requests);
	list_free(requests);
	Assert(ts_dist_cmd_response_count(results) == (Size) list_length(data_nodes));

	return results;
}

DistCmdResult *
ts_dist_cmd_params_invoke_on_data_nodes(const char *sql, StmtParams *params, List *data_nodes,
										bool transactional)
{
	List *cmd_descriptors = NIL;
	DistCmdDescr cmd_descr = { .sql = sql, .params = params };
	DistCmdResult *results;

	for (int i = 0; i < list_length(data_nodes); ++i)
	{
		cmd_descriptors = lappend(cmd_descriptors, &cmd_descr);
	}
	results =
		ts_dist_multi_cmds_params_invoke_on_data_nodes(cmd_descriptors, data_nodes, transactional);
	list_free(cmd_descriptors);
	return results;
}

DistCmdResult *
ts_dist_cmd_invoke_on_data_nodes(const char *sql, List *data_nodes, bool transactional)
{
	return ts_dist_cmd_params_invoke_on_data_nodes(sql, NULL, data_nodes, transactional);
}

DistCmdResult *
ts_dist_cmd_invoke_on_data_nodes_using_search_path(const char *sql, const char *search_path,
												   List *node_names, bool transactional)
{
	DistCmdResult *set_result;
	DistCmdResult *results;
	bool set_search_path = search_path != NULL;

	DEBUG_WAITPOINT("dist_cmd_using_search_path_1");

	/*
	 * As a workaround for non-transactional execution, we expect the same connection
	 * to be present after we set search_path.
	 */
	remote_connection_cache_invalidation_ignore(true);

	if (set_search_path)
	{
		char *set_request = psprintf("SET search_path = %s, pg_catalog", search_path);

		set_result = ts_dist_cmd_invoke_on_data_nodes(set_request, node_names, transactional);
		if (set_result)
			ts_dist_cmd_close_response(set_result);

		pfree(set_request);
	}

	DEBUG_WAITPOINT("dist_cmd_using_search_path_2");

	results = ts_dist_cmd_invoke_on_data_nodes(sql, node_names, transactional);

	if (set_search_path)
	{
		set_result = ts_dist_cmd_invoke_on_data_nodes("SET search_path = pg_catalog",
													  node_names,
													  transactional);
		if (set_result)
			ts_dist_cmd_close_response(set_result);
	}

	remote_connection_cache_invalidation_ignore(false);
	return results;
}

DistCmdResult *
ts_dist_multi_cmds_invoke_on_data_nodes_using_search_path(List *cmd_descriptors,
														  const char *search_path, List *node_names,
														  bool transactional)
{
	DistCmdResult *set_result;
	DistCmdResult *results;
	bool set_search_path = search_path != NULL;

	remote_connection_cache_invalidation_ignore(true);

	if (set_search_path)
	{
		char *set_request = psprintf("SET search_path = %s, pg_catalog", search_path);

		set_result = ts_dist_cmd_invoke_on_data_nodes(set_request, node_names, transactional);
		if (set_result)
			ts_dist_cmd_close_response(set_result);

		pfree(set_request);
	}

	results =
		ts_dist_multi_cmds_params_invoke_on_data_nodes(cmd_descriptors, node_names, transactional);

	if (set_search_path)
	{
		set_result = ts_dist_cmd_invoke_on_data_nodes("SET search_path = pg_catalog",
													  node_names,
													  transactional);
		if (set_result)
			ts_dist_cmd_close_response(set_result);
	}

	remote_connection_cache_invalidation_ignore(false);
	return results;
}

DistCmdResult *
ts_dist_cmd_invoke_on_all_data_nodes(const char *sql)
{
	return ts_dist_cmd_invoke_on_data_nodes(sql, data_node_get_node_name_list(), true);
}

/*
 * Relay a function call to data nodes.
 *
 * A NIL list of data nodes means invoke on ALL data nodes.
 */
DistCmdResult *
ts_dist_cmd_invoke_func_call_on_data_nodes(FunctionCallInfo fcinfo, List *data_nodes)
{
	DistCmdResult *result;

	if (NIL == data_nodes)
		data_nodes = data_node_get_node_name_list();

	result = ts_dist_cmd_invoke_on_data_nodes(deparse_func_call(fcinfo), data_nodes, true);

	/* Initialize result conversion info in case caller wants to convert the
	 * result to datums. */
	result->funcclass = get_call_result_type(fcinfo, &result->typeid, &result->tupdesc);

	return result;
}

DistCmdResult *
ts_dist_cmd_invoke_func_call_on_all_data_nodes(FunctionCallInfo fcinfo)
{
	return ts_dist_cmd_invoke_on_data_nodes(deparse_func_call(fcinfo),
											data_node_get_node_name_list(),
											true);
}

/*
 * Relay a function call to data nodes.
 *
 * This version throws away the result.
 */
void
ts_dist_cmd_func_call_on_data_nodes(FunctionCallInfo fcinfo, List *data_nodes)
{
	DistCmdResult *result = ts_dist_cmd_invoke_func_call_on_data_nodes(fcinfo, data_nodes);

	ts_dist_cmd_close_response(result);
}

static PGresult *
ts_dist_cmd_get_response_result(DistCmdResponse *response)
{
	PGresult *res;

	if (response->result)
		res = async_response_result_get_pg_result(response->result);
	else
		res = response->pg_result;
	return res;
}

PGresult *
ts_dist_cmd_get_result_by_node_name(DistCmdResult *response, const char *node_name)
{
	for (size_t i = 0; i < response->num_responses; ++i)
	{
		DistCmdResponse *resp = &response->responses[i];

		if (strcmp(node_name, resp->data_node) == 0)
			return ts_dist_cmd_get_response_result(resp);
	}
	return NULL;
}

const char *
ts_dist_cmd_get_error_by_index(DistCmdResult *response, Size index)
{
	DistCmdResponse *rsp;

	if (index >= response->num_responses)
		return NULL;

	rsp = &response->responses[index];

	return rsp->errorMessage;
}

/*
 * Get the n:th command result.
 *
 * Returns the n:th command result as given by the index, or NULL if no such
 * result.
 *
 * Optionally get the name of the node that the result was from via the
 * node_name parameter.
 */
PGresult *
ts_dist_cmd_get_result_by_index(DistCmdResult *response, Size index, const char **node_name)
{
	DistCmdResponse *rsp;

	if (index >= response->num_responses)
		return NULL;

	rsp = &response->responses[index];

	if (NULL != node_name)
		*node_name = rsp->data_node;

	return ts_dist_cmd_get_response_result(rsp);
}

/*
 * Get the number of responses in a distributed command result.
 */
Size
ts_dist_cmd_response_count(DistCmdResult *result)
{
	return result->num_responses;
}

long
ts_dist_cmd_total_row_count(DistCmdResult *result)
{
	long num_rows = 0;

	for (size_t i = 0; i < result->num_responses; ++i)
	{
		DistCmdResponse *resp = &result->responses[i];

		num_rows += PQntuples(ts_dist_cmd_get_response_result(resp));
	}

	return num_rows;
}

/*
 * Convert an expected scalar return value.
 *
 * Convert the result of a remote function invokation returning a single
 * scalar value. For example, a function returning a bool.
 */
Datum
ts_dist_cmd_get_single_scalar_result_by_index(DistCmdResult *result, Size index, bool *isnull,
											  const char **node_name_out)
{
	PGresult *pgres;
	Oid typioparam;
	Oid typinfunc;
	const char *node_name;

	if (!OidIsValid(result->typeid))
		elog(ERROR, "invalid result type of distributed command");

	if (result->funcclass != TYPEFUNC_SCALAR)
		elog(ERROR, "distributed command result is not scalar");

	pgres = ts_dist_cmd_get_result_by_index(result, index, &node_name);

	if (NULL == pgres)
		elog(ERROR, "invalid index for distributed command result");

	if (node_name_out)
		*node_name_out = node_name;

	if (PQresultStatus(pgres) != PGRES_TUPLES_OK || PQntuples(pgres) != 1 || PQnfields(pgres) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_TS_UNEXPECTED),
				 errmsg("unexpected response from data node \"%s\"", node_name)));

	if (PQgetisnull(pgres, 0, 0))
	{
		if (isnull)
			*isnull = true;

		return (Datum) 0;
	}

	if (isnull)
		*isnull = false;

	getTypeInputInfo(result->typeid, &typinfunc, &typioparam);
	Assert(OidIsValid(typinfunc));

	return OidInputFunctionCall(typinfunc, PQgetvalue(pgres, 0, 0), typioparam, -1);
}

void
ts_dist_cmd_clear_result_by_index(DistCmdResult *response, Size index)
{
	DistCmdResponse *resp;

	if (index >= response->num_responses)
		elog(ERROR, "no response for index %zu", index);

	resp = &response->responses[index];

	if (resp->result != NULL)
	{
		async_response_result_close(resp->result);
		resp->result = NULL;
	}

	if (resp->data_node != NULL)
	{
		pfree((char *) resp->data_node);
		resp->data_node = NULL;
	}
	if (resp->errorMessage != NULL)
	{
		pfree((char *) resp->errorMessage);
		resp->errorMessage = NULL;
	}
	/* pg_results are managed by memory context callback */
	if (resp->pg_result != NULL)
		resp->pg_result = NULL;
}

void
ts_dist_cmd_close_response(DistCmdResult *response)
{
	Size i;

	for (i = 0; i < response->num_responses; ++i)
		ts_dist_cmd_clear_result_by_index(response, i);

	pfree(response);
}

extern PreparedDistCmd *
ts_dist_cmd_prepare_command(const char *sql, size_t n_params, List *node_names)
{
	List *result = NIL;
	ListCell *lc;
	AsyncRequestSet *prep_requests = async_request_set_create();
	AsyncResponseResult *async_resp;

	if (node_names == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid data nodes list"),
				 errdetail("Must specify a non-empty list of data nodes.")));

	foreach (lc, node_names)
	{
		const char *name = lfirst(lc);
		TSConnection *connection = data_node_get_connection(name, REMOTE_TXN_USE_PREP_STMT, true);
		DistPreparedStmt *cmd = palloc(sizeof(DistPreparedStmt));
		AsyncRequest *ar = async_request_send_prepare(connection, sql, n_params);

		cmd->data_node_name = pstrdup(name);
		async_request_attach_user_data(ar, &cmd->prepared_stmt);
		result = lappend(result, cmd);
		async_request_set_add(prep_requests, ar);
	}

	while ((async_resp = async_request_set_wait_ok_result(prep_requests)))
	{
		*(PreparedStmt **) async_response_result_get_user_data(async_resp) =
			async_response_result_generate_prepared_stmt(async_resp);
		async_response_result_close(async_resp);
	}

	return result;
}

PreparedDistCmd *
ts_dist_cmd_prepare_command_on_all_data_nodes(const char *sql, size_t n_params)
{
	return ts_dist_cmd_prepare_command(sql, n_params, data_node_get_node_name_list());
}

extern DistCmdResult *
ts_dist_cmd_invoke_prepared_command(PreparedDistCmd *command, const char *const *param_values)
{
	List *reqs = NIL;
	ListCell *lc;
	DistCmdResult *results;

	foreach (lc, command)
	{
		DistPreparedStmt *stmt = lfirst(lc);
		AsyncRequest *req = async_request_send_prepared_stmt(stmt->prepared_stmt, param_values);

		async_request_attach_user_data(req, (char *) stmt->data_node_name);
		reqs = lappend(reqs, req);
	}

	results = ts_dist_cmd_collect_responses(reqs);
	list_free(reqs);
	return results;
}

void
ts_dist_cmd_close_prepared_command(PreparedDistCmd *command)
{
	ListCell *lc;

	foreach (lc, command)
		prepared_stmt_close(((DistPreparedStmt *) lfirst(lc))->prepared_stmt);

	list_free_deep(command);
}

Datum
ts_dist_cmd_exec(PG_FUNCTION_ARGS)
{
	const char *query = PG_ARGISNULL(0) ? NULL : TextDatumGetCString(PG_GETARG_DATUM(0));
	ArrayType *data_nodes = PG_ARGISNULL(1) ? NULL : PG_GETARG_ARRAYTYPE_P(1);
	bool transactional = PG_ARGISNULL(2) ? true : PG_GETARG_BOOL(2);
	DistCmdResult *result;
	List *data_node_list;
	const char *search_path;

	if (!transactional)
		TS_PREVENT_IN_TRANSACTION_BLOCK(true);

	if (NULL == query)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("empty command string")));

	if (dist_util_membership() != DIST_MEMBER_ACCESS_NODE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function must be run on the access node only")));

	if (data_nodes == NULL)
		data_node_list = data_node_get_node_name_list();
	else
	{
		int ndatanodes;

		if (ARR_NDIM(data_nodes) > 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid data nodes list"),
					 errdetail("The array of data nodes cannot be multi-dimensional.")));

		if (ARR_HASNULL(data_nodes))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid data nodes list"),
					 errdetail("The array of data nodes cannot contain null values.")));

		ndatanodes = ArrayGetNItems(ARR_NDIM(data_nodes), ARR_DIMS(data_nodes));

		if (ndatanodes == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid data nodes list"),
					 errdetail("The array of data nodes cannot be empty.")));

		data_node_list = data_node_array_to_node_name_list(data_nodes);
	}

	/* Assert that the data node list is not empty. Since we checked that the
	 * function is run on an access node above, the list of data nodes must
	 * per definition be non-empty for the case when not specifying an
	 * explicit list of data nodes. For the case of explicitly specifying data
	 * nodes, we already checked for a non-empty array, and then validated all
	 * the specified data nodes. If there was a node in the list that is not a
	 * data node, we would already have thrown an error. */
	Assert(data_node_list != NIL);
	search_path = GetConfigOption("search_path", false, false);
	result = ts_dist_cmd_invoke_on_data_nodes_using_search_path(query,
																search_path,
																data_node_list,
																transactional);
	if (result)
		ts_dist_cmd_close_response(result);

	list_free(data_node_list);

	PG_RETURN_VOID();
}
