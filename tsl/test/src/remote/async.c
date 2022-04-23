/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/fmgrprotos.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <storage/procarray.h>
#include <foreign/foreign.h>
#include <foreign/fdwapi.h>
#include <miscadmin.h>
#include <access/reloptions.h>
#include <commands/dbcommands.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <utils/guc.h>

#include "export.h"
#include "connection.h"
#include "test_utils.h"
#include "node_killer.h"
#include "remote/dist_txn.h"

TS_FUNCTION_INFO_V1(ts_test_remote_async);

#define query_with_params_ok_result(conn, sql_statement, n_values, values)                         \
	async_response_result_get_pg_result(async_request_wait_ok_result(                              \
		async_request_send_with_params(conn,                                                       \
									   sql_statement,                                              \
									   stmt_params_create_from_values(values, n_values),           \
									   0)))

#define query_prepared_ok_result(prepared_stmt, values)                                            \
	async_response_result_get_pg_result(                                                           \
		async_request_wait_ok_result(async_request_send_prepared_stmt(prepared_stmt, values)))

#define prepare(conn, sql_statement, nvars)                                                        \
	async_request_wait_prepared_statement(async_request_send_prepare(conn, sql_statement, nvars))

static void
test_prepared_stmts()
{
	TSConnection *conn = get_connection();
	const char **params = (const char **) palloc(sizeof(char *) * 5);
	AsyncResponseResult *rsp;
	PreparedStmt *prep;
	PGresult *res;

	rsp = async_request_wait_ok_result(async_request_send_prepare(conn, "SELECT 3", 0));
	prep = async_response_result_generate_prepared_stmt(rsp);
	res = query_prepared_ok_result(prep, NULL);
	TestAssertTrue(PQresultStatus(res) == PGRES_TUPLES_OK);
	TestAssertTrue(strcmp(PQgetvalue(res, 0, 0), "3") == 0);
	remote_result_close(res);
	prepared_stmt_close(prep);

	rsp = async_request_wait_ok_result(async_request_send_prepare(conn, "SELECT $1, $3, $2", 3));
	prep = async_response_result_generate_prepared_stmt(rsp);
	params[0] = "2";
	params[1] = "4";
	params[2] = "8";
	res = query_prepared_ok_result(prep, params);
	TestAssertTrue(PQresultStatus(res) == PGRES_TUPLES_OK);
	TestAssertTrue(strcmp(PQgetvalue(res, 0, 0), "2") == 0);
	TestAssertTrue(strcmp(PQgetvalue(res, 0, 1), "8") == 0);
	TestAssertTrue(strcmp(PQgetvalue(res, 0, 2), "4") == 0);
	remote_result_close(res);
	prepared_stmt_close(prep);

	/* malformed sql (missing commas) */
	TestEnsureError(prep = prepare(conn, "SELECT $1 $3 $2", 3));
	remote_connection_close(conn);
}

static void
test_params()
{
	TSConnection *conn = get_connection();
	const char **params = (const char **) palloc(sizeof(char *) * 5);
	PGresult *res;

	params[0] = "2";
	res = query_with_params_ok_result(conn, "SELECT $1", 1, params);
	TestAssertTrue(PQresultStatus(res) == PGRES_TUPLES_OK);
	TestAssertTrue(strcmp(PQgetvalue(res, 0, 0), "2") == 0);
	remote_result_close(res);
	TestEnsureError(res = query_with_params_ok_result(conn, "SELECT 1 2 3", 1, params));
	remote_connection_close(conn);
}

static void
test_basic_request()
{
	TSConnection *conn;
	TSConnection *conn_disconnected = NULL;
	AsyncRequest *req;
	AsyncResponseResult *result;
	conn = get_connection();

	req = async_request_send_with_params_elevel(conn, "SELECT 1", NULL, ERROR);
	result = async_request_wait_any_result(req);
	TestAssertTrue(PQresultStatus(async_response_result_get_pg_result(result)) == PGRES_TUPLES_OK);
	async_response_result_close(result);

	req = async_request_send_with_params_elevel(conn, "SELECT jjj", NULL, ERROR);
	TestEnsureError(result = async_request_wait_ok_result(req));

	TestEnsureError(
		req = async_request_send_with_params_elevel(conn_disconnected, "SELECT 1", NULL, ERROR));

	remote_connection_close(conn);
}

static void
test_parameter_order()
{
	TSConnection *conn = get_connection();
	AsyncRequest *req;
	PGresult *pg_res;
	const char **params = (const char **) palloc(sizeof(char *) * 5);
	AsyncResponseResult *result;

	/* Parameters get ordered correctly */
	params[0] = "0";
	params[1] = "1";
	req = async_request_send_with_params_elevel(conn,
												"SELECT $2, $1",
												stmt_params_create_from_values(params, 2),
												ERROR);
	result = async_request_wait_any_result(req);
	pg_res = async_response_result_get_pg_result(result);
	TestAssertTrue(PQresultStatus(pg_res) == PGRES_TUPLES_OK);
	TestAssertTrue(strcmp(PQgetvalue(pg_res, 0, 0), "1") == 0);
	TestAssertTrue(strcmp(PQgetvalue(pg_res, 0, 1), "0") == 0);
	async_response_result_close(result);

	remote_connection_close(conn);
}

static void
test_request_set()
{
	TSConnection *conn = get_connection();
	TSConnection *conn_2 = get_connection();
	AsyncRequest *req_1;
	AsyncRequest *req_2;
	AsyncRequestSet *set;
	PGresult *pg_res;
	AsyncResponseResult *result;
	AsyncResponse *response;
	int var_1 = 1;
	int var_2 = 2;
	int count = 0;
	int *i;

	/* test the set stuff */
	set = async_request_set_create();
	req_1 = async_request_send(conn, "SELECT 1");
	async_request_attach_user_data(req_1, &var_1);
	async_request_set_add(set, req_1);
	req_2 = async_request_send(conn_2, "SELECT 2");
	async_request_attach_user_data(req_2, &var_2);
	async_request_set_add(set, req_2);

	while ((result = async_request_set_wait_ok_result(set)))
	{
		pg_res = async_response_result_get_pg_result(result);
		TestAssertTrue(PQresultStatus(pg_res) == PGRES_TUPLES_OK);
		if (strcmp(PQgetvalue(pg_res, 0, 0), "1") == 0)
		{
			i = async_response_result_get_user_data(result);
			TestAssertTrue(*i == 1);
			TestAssertTrue(async_response_result_get_request(result) == req_1);
		}
		if (strcmp(PQgetvalue(pg_res, 0, 0), "2") == 0)
		{
			i = async_response_result_get_user_data(result);
			TestAssertTrue(*i == 2);
			TestAssertTrue(async_response_result_get_request(result) == req_2);
		}
		async_response_result_close(result);
		count++;
	}
	TestAssertTrue(count == 2);

	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");
	response = async_request_set_wait_any_response_deadline(set, TS_NO_TIMEOUT);
	TestAssertTrue(async_response_get_type(response) == RESPONSE_RESULT);
	async_response_close(response);
	response = async_request_set_wait_any_response_deadline(set, TS_NO_TIMEOUT);
	TestAssertTrue(response == NULL);

	remote_connection_close(conn);
	remote_connection_close(conn_2);
}

static void
test_node_death()
{
	TSConnection *conn = get_connection();
	AsyncResponseResult *result;
	AsyncResponse *response;
	PGresult *pg_res;
	AsyncRequestSet *set;
	RemoteNodeKiller rnk;

	/* killed node causes an error response, then a communication error */
	remote_node_killer_init(&rnk, conn, DTXN_EVENT_ANY);
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");
	remote_node_killer_kill(&rnk);
	response = async_request_set_wait_any_response_deadline(set, TS_NO_TIMEOUT);
	TestAssertTrue(async_response_get_type(response) == RESPONSE_RESULT);
	result = (AsyncResponseResult *) response;
	pg_res = async_response_result_get_pg_result(result);
	TestAssertTrue(PQresultStatus(pg_res) != PGRES_TUPLES_OK);
	async_response_close(response);

	/* This will throw if elevel == ERROR so set to DEBUG1 */
	response = async_request_set_wait_any_response_deadline(set, TS_NO_TIMEOUT);
	TestAssertTrue(async_response_get_type(response) == RESPONSE_COMMUNICATION_ERROR);
	elog(WARNING, "Expect warning about communication error:");
	async_response_report_error(response, WARNING);
	response = async_request_set_wait_any_response_deadline(set, TS_NO_TIMEOUT);
	TestAssertTrue(response == NULL);

	/* test error throwing in async_request_set_wait_any_result */
	conn = get_connection();
	remote_node_killer_init(&rnk, conn, DTXN_EVENT_ANY);
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");
	remote_node_killer_kill(&rnk);

	/* first we get error result */
	TestAssertTrue(NULL != async_request_set_wait_any_result(set));
	TestEnsureError(async_request_set_wait_any_result(set));

	/* do cancel query before first response */
	conn = get_connection();
	remote_node_killer_init(&rnk, conn, DTXN_EVENT_ANY);
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");

	remote_node_killer_kill(&rnk);
	TestAssertTrue(false == remote_connection_cancel_query(conn));

	/* do cancel query after seeing error */
	conn = get_connection();
	remote_node_killer_init(&rnk, conn, DTXN_EVENT_ANY);
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");
	remote_node_killer_kill(&rnk);

	/* first we get error result */
	TestEnsureError(async_request_set_wait_ok_result(set));
	TestAssertTrue(false == remote_connection_cancel_query(conn));

	remote_connection_close(conn);
}

static void
test_timeout()
{
	TSConnection *conn = get_connection();
	TSConnection *conn_2 = get_connection();
	AsyncRequestSet *set;
	AsyncResponse *response;

	/*
	 * Test timeout by locking a table in conn_2 and timing out on locking
	 * conn
	 */
	async_request_wait_ok_command(async_request_send(conn_2, "BEGIN;"));
	async_request_wait_ok_command(async_request_send(conn_2, "LOCK \"S 1\".\"T 1\""));

	async_request_wait_ok_command(async_request_send(conn, "BEGIN;"));
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "LOCK \"S 1\".\"T 1\"");
	response = async_request_set_wait_any_response_deadline(
		set, TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 100));
	TestAssertTrue(async_response_get_type(response) == RESPONSE_TIMEOUT);

	/* cancel the locked query and do another query */
	TestAssertTrue(remote_connection_cancel_query(conn));
	/* the txn is aborted waiting for abort */
	TestEnsureError(async_request_wait_ok_result(async_request_send(conn, "SELECT 1;")));
	async_request_wait_ok_command(async_request_send(conn, "ABORT;"));
	async_request_wait_ok_result(async_request_send(conn, "SELECT 1;"));

	/* release conn_2 lock */
	async_request_wait_ok_command(async_request_send(conn_2, "COMMIT;"));

	remote_connection_close(conn);
	remote_connection_close(conn_2);
}

static void
test_multiple_reqests()
{
	TSConnection *conn = get_connection();
	AsyncRequest *req1;
	AsyncRequest *req2;

	req1 = async_request_send(conn, "SELECT 1");
	req2 = async_request_send(conn, "SELECT 1");

	async_request_wait_ok_result(req1);
	async_request_wait_ok_result(req2);

	req1 = async_request_send(conn, "SELECT 1");
	req2 = async_request_send(conn, "SELECT 1");
	TestEnsureError(async_request_wait_ok_result(req2));
	async_request_wait_ok_result(req1);
	async_request_wait_ok_result(req2);

	remote_connection_close(conn);
}

Datum
ts_test_remote_async(PG_FUNCTION_ARGS)
{
	test_prepared_stmts();
	test_params();
	test_basic_request();
	test_parameter_order();
	test_request_set();
	test_node_death();
	test_multiple_reqests();
	test_timeout();

	PG_RETURN_VOID();
}
