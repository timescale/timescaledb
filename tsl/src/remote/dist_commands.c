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
	TypeFuncClass funcclass; /* Function class of invoked function, if any */
	Oid typeid;				 /* Expected result type, or InvalidOid */
	TupleDesc tupdesc;		 /* Tuple descriptor of invoked function
							  * result. Set if typeid is valid and has a
							  * composite return value */
	DistCmdResponse responses[FLEXIBLE_ARRAY_MEMBER];
} DistCmdResult;

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
	Assert(ts_dist_cmd_response_count(results) == list_length(data_nodes));

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
ts_dist_multi_cmds_invoke_on_data_nodes_using_search_path(List *cmd_descriptors,
														  const char *search_path, List *node_names,
														  bool transactional)
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
	int i;
	long num_rows = 0;

	for (i = 0; i < result->num_responses; ++i)
	{
		DistCmdResponse *resp = &result->responses[i];

		num_rows += PQntuples(async_response_result_get_pg_result(resp->result));
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
