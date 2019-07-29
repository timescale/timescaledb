/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <libpq-fe.h>

#include "remote/dist_commands.h"
#include "remote/dist_txn.h"
#include "remote/connection_cache.h"
#include "data_node.h"
#include "miscadmin.h"

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

static TSConnection *
ts_dist_cmd_get_connection_for_data_node(const char *data_node, RemoteTxnPrepStmtOption ps_opt)
{
	TSConnectionId id =
		remote_connection_id(GetForeignServerByName(data_node, false)->serverid, GetUserId());

	return remote_dist_txn_get_connection(id, ps_opt);
}

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

DistCmdResult *
ts_dist_cmd_invoke_on_data_nodes(const char *sql, List *node_names)
{
	ListCell *lc;
	List *requests = NIL;
	DistCmdResult *results;

	if (node_names == NIL)
		elog(ERROR, "target data nodes must be specified for ts_dist_cmd_invoke_on_data_nodes");

	foreach (lc, node_names)
	{
		const char *node_name = lfirst(lc);
		TSConnection *connection =
			ts_dist_cmd_get_connection_for_data_node(node_name, REMOTE_TXN_NO_PREP_STMT);

		AsyncRequest *req = async_request_send(connection, sql);

		async_request_attach_user_data(req, (char *) node_name);
		requests = lappend(requests, req);
	}

	results = ts_dist_cmd_collect_responses(requests);
	list_free(requests);

	return results;
}

DistCmdResult *
ts_dist_cmd_invoke_on_data_nodes_using_search_path(const char *sql, const char *search_path,
												   List *node_names)
{
	DistCmdResult *set_result;
	DistCmdResult *results;
	bool set_search_path = search_path != NULL;

	if (set_search_path)
	{
		char *set_request = psprintf("SET search_path = %s, pg_catalog", search_path);

		set_result = ts_dist_cmd_invoke_on_data_nodes(set_request, node_names);
		if (set_result)
			ts_dist_cmd_close_response(set_result);

		pfree(set_request);
	}

	results = ts_dist_cmd_invoke_on_data_nodes(sql, node_names);

	if (set_search_path)
	{
		set_result = ts_dist_cmd_invoke_on_data_nodes("SET search_path = pg_catalog", node_names);
		if (set_result)
			ts_dist_cmd_close_response(set_result);
	}

	return results;
}

DistCmdResult *
ts_dist_cmd_invoke_on_all_data_nodes(const char *sql)
{
	return ts_dist_cmd_invoke_on_data_nodes(sql, data_node_get_node_name_list());
}

PGresult *
ts_dist_cmd_get_data_node_result(DistCmdResult *response, const char *node_name)
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
		TSConnection *connection =
			ts_dist_cmd_get_connection_for_data_node(name, REMOTE_TXN_USE_PREP_STMT);
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
