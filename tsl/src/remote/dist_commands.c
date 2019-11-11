/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <catalog/namespace.h>

#include <libpq-fe.h>

#include "remote/dist_commands.h"
#include "remote/dist_txn.h"
#include "remote/connection_cache.h"
#include "data_node.h"
#include "dist_util.h"
#include "miscadmin.h"
#include "errors.h"
#include "deparse.h"

typedef struct DistPreparedStmt
{
	const char *data_node_name;
	PreparedStmt *prepared_stmt;
} DistPreparedStmt;

typedef struct DistCmdResponse
{
	const char *data_node;
	AsyncResponseResult *result;
} DistCmdResponse;

typedef struct DistCmdResult
{
	Size num_responses;
	DistCmdResponse responses[FLEXIBLE_ARRAY_MEMBER];
} DistCmdResult;

static DistCmdResult *
ts_dist_cmd_collect_responses(List *requests)
{
	AsyncRequestSet *rs = async_request_set_create();
	AsyncResponseResult *ar;
	ListCell *lc;
	DistCmdResult *results =
		palloc(sizeof(DistCmdResult) + requests->length * sizeof(DistCmdResponse));
	int i = 0;

	foreach (lc, requests)
		async_request_set_add(rs, lfirst(lc));

	while ((ar = async_request_set_wait_ok_result(rs)))
	{
		DistCmdResponse *response = &results->responses[i];

		response->result = ar;
		response->data_node = pstrdup(async_response_result_get_user_data(ar));
		++i;
	}

	Assert(i == requests->length);
	results->num_responses = i;
	return results;
}

/*
 * Invoke a SQL statement (command) on the given data nodes.
 *
 * The list of data nodes can either be a list of data node names, or foreign
 * server OIDs.
 */
DistCmdResult *
ts_dist_cmd_invoke_on_data_nodes(const char *sql, List *data_nodes, bool transactional)
{
	ListCell *lc;
	List *requests = NIL;
	DistCmdResult *results;

	if (data_nodes == NIL)
		elog(ERROR, "target data nodes must be specified for ts_dist_cmd_invoke_on_data_nodes");

	switch (nodeTag(data_nodes))
	{
		case T_OidList:
			data_nodes = data_node_oids_to_node_name_list(data_nodes, ACL_USAGE);
			break;
		case T_List:
			/* Already in the format we want. Just check permissions. */
			data_node_name_list_check_acl(data_nodes, ACL_USAGE);
			break;
		default:
			elog(ERROR, "invalid list type %u", nodeTag(data_nodes));
			break;
	}

	foreach (lc, data_nodes)
	{
		const char *node_name = lfirst(lc);
		AsyncRequest *req;
		TSConnection *connection =
			data_node_get_connection(node_name, REMOTE_TXN_NO_PREP_STMT, transactional);

		ereport(DEBUG2, (errmsg_internal("sending \"%s\" to data node \"%s\"", sql, node_name)));

		req = async_request_send(connection, sql);
		async_request_attach_user_data(req, (char *) node_name);
		requests = lappend(requests, req);
	}

	results = ts_dist_cmd_collect_responses(requests);
	list_free(requests);

	return results;
}

DistCmdResult *
ts_dist_cmd_invoke_on_data_nodes_using_search_path(const char *sql, const char *search_path,
												   List *node_names, bool transactional)
{
	DistCmdResult *set_result;
	DistCmdResult *results;
	bool set_search_path = search_path != NULL;

	if (set_search_path)
	{
		char *set_request = psprintf("SET search_path = %s, pg_catalog", search_path);

		set_result = ts_dist_cmd_invoke_on_data_nodes(set_request, node_names, transactional);
		if (set_result)
			ts_dist_cmd_close_response(set_result);

		pfree(set_request);
	}

	results = ts_dist_cmd_invoke_on_data_nodes(sql, node_names, transactional);

	if (set_search_path)
	{
		set_result = ts_dist_cmd_invoke_on_data_nodes("SET search_path = pg_catalog",
													  node_names,
													  transactional);
		if (set_result)
			ts_dist_cmd_close_response(set_result);
	}

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
	if (NIL == data_nodes)
		data_nodes = data_node_get_node_name_list();

	return ts_dist_cmd_invoke_on_data_nodes(deparse_func_call(fcinfo), data_nodes, true);
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

PGresult *
ts_dist_cmd_get_result_by_node_name(DistCmdResult *response, const char *node_name)
{
	int i;

	for (i = 0; i < response->num_responses; ++i)
	{
		DistCmdResponse *resp = &response->responses[i];

		if (strcmp(node_name, resp->data_node) == 0)
			return async_response_result_get_pg_result(resp->result);
	}
	return NULL;
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

	return async_response_result_get_pg_result(rsp->result);
}

void
ts_dist_cmd_close_response(DistCmdResult *response)
{
	int i;

	for (i = 0; i < response->num_responses; ++i)
	{
		DistCmdResponse *resp = &response->responses[i];

		async_response_result_close(resp->result);
		pfree((char *) resp->data_node);
	}

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
		elog(ERROR, "target data nodes must be specified for ts_dist_cmd_prepare_command");

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
	const char *query = TextDatumGetCString(PG_GETARG_DATUM(0));
	ArrayType *data_nodes = PG_ARGISNULL(1) ? NULL : PG_GETARG_ARRAYTYPE_P(1);
	DistCmdResult *result;
	List *data_node_list;
	const char *search_path;

	if (dist_util_membership() != DIST_MEMBER_ACCESS_NODE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function must be run on the access node only")));

	if (data_nodes == NULL)
		data_node_list = data_node_get_node_name_list();
	else
		data_node_list = data_node_array_to_node_name_list(data_nodes);

	search_path = GetConfigOption("search_path", false, false);
	result = ts_dist_cmd_invoke_on_data_nodes_using_search_path(query,
																search_path,
																data_node_list,
																true);

	if (result)
		ts_dist_cmd_close_response(result);

	list_free(data_node_list);

	PG_RETURN_VOID();
}
