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
#include "remote/connection.h"
#include "test_utils.h"
#include "node_killer.h"

TS_FUNCTION_INFO_V1(tsl_test_remote_async);

static void
test_basic_request()
{
	PGconn *conn;
	PGconn *conn_disconnected = NULL;
	AsyncRequest *req;
	AsyncResponseResult *result;
	conn = get_connection();

	req = async_request_send_with_params_elevel(conn, "SELECT 1", 0, NULL, ERROR);
	result = async_request_wait_any_result(req);
	Assert(PQresultStatus(async_response_result_get_pg_result(result)) == PGRES_TUPLES_OK);
	async_response_result_close(result);

	req = async_request_send_with_params_elevel(conn, "SELECT jjj", 0, NULL, ERROR);
	EXPECT_ERROR_STMT(result = async_request_wait_ok_result(req));

	EXPECT_ERROR_STMT(
		req = async_request_send_with_params_elevel(conn_disconnected, "SELECT 1", 0, NULL, ERROR));

	elog(WARNING, "Expect warning about bad connection pointer:");
	req = async_request_send_with_params_elevel(conn_disconnected, "SELECT 1", 0, NULL, WARNING);
	Assert(NULL == req);

	remote_connection_close(conn);
}

static void
test_parameter_order()
{
	PGconn *conn = get_connection();
	AsyncRequest *req;
	PGresult *pg_res;
	const char **params = (const char **) palloc(sizeof(char *) * 5);
	AsyncResponseResult *result;

	/* Parameters get ordered correctly */
	params[0] = "0";
	params[1] = "1";
	req = async_request_send_with_params_elevel(conn, "SELECT $2, $1", 2, params, ERROR);
	result = async_request_wait_any_result(req);
	pg_res = async_response_result_get_pg_result(result);
	Assert(PQresultStatus(pg_res) == PGRES_TUPLES_OK);
	Assert(strcmp(PQgetvalue(pg_res, 0, 0), "1") == 0);
	Assert(strcmp(PQgetvalue(pg_res, 0, 1), "0") == 0);
	async_response_result_close(result);

	remote_connection_close(conn);
}

static void
test_request_set()
{
	PGconn *conn = get_connection();
	PGconn *conn_2 = get_connection();
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
	req_1 = async_request_send_with_params_elevel(conn, "SELECT 1", 0, NULL, ERROR);
	async_request_attach_user_data(req_1, &var_1);
	async_request_set_add(set, req_1);
	req_2 = async_request_send_with_params_elevel(conn_2, "SELECT 2", 0, NULL, ERROR);
	async_request_attach_user_data(req_2, &var_2);
	async_request_set_add(set, req_2);

	while ((result = async_request_set_wait_ok_result(set)))
	{
		pg_res = async_response_result_get_pg_result(result);
		Assert(PQresultStatus(pg_res) == PGRES_TUPLES_OK);
		if (strcmp(PQgetvalue(pg_res, 0, 0), "1") == 0)
		{
			i = async_response_result_get_user_data(result);
			Assert(*i == 1);
			Assert(async_response_result_get_request(result) == req_1);
		}
		if (strcmp(PQgetvalue(pg_res, 0, 0), "2") == 0)
		{
			i = async_response_result_get_user_data(result);
			Assert(*i == 2);
			Assert(async_response_result_get_request(result) == req_2);
		}
		async_response_result_close(result);
		count++;
	}
	Assert(count == 2);

	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");
	response = async_request_set_wait_any_response_deadline(set, ERROR, TS_NO_TIMEOUT);
	Assert(async_response_get_type(response) == RESPONSE_RESULT);
	async_response_close(response);
	response = async_request_set_wait_any_response_deadline(set, ERROR, TS_NO_TIMEOUT);
	Assert(response == NULL);

	remote_connection_close(conn);
	remote_connection_close(conn_2);
}

static void
test_node_death()
{
	PGconn *conn = get_connection();
	AsyncResponseResult *result;
	AsyncResponse *response;
	PGresult *pg_res;
	AsyncRequestSet *set;
	RemoteNodeKiller *rnk;

	/* killed node causes an error response, then a communication error */
	rnk = remote_node_killer_init(conn);
	remote_node_killer_kill(rnk);
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");
	response = async_request_set_wait_any_response_deadline(set, ERROR, TS_NO_TIMEOUT);
	Assert(async_response_get_type(response) == RESPONSE_RESULT);
	result = (AsyncResponseResult *) response;
	pg_res = async_response_result_get_pg_result(result);
	Assert(PQresultStatus(pg_res) != PGRES_TUPLES_OK);
	async_response_close(response);
	/* This will throw if elevel == ERROR so set to DEBUG1 */
	response = async_request_set_wait_any_response_deadline(set, DEBUG1, TS_NO_TIMEOUT);
	Assert(async_response_get_type(response) == RESPONSE_COMMUNICATION_ERROR);
	elog(WARNING, "Expect warning about communication error:");
	async_response_report_error(response, WARNING);
	response = async_request_set_wait_any_response_deadline(set, ERROR, TS_NO_TIMEOUT);
	Assert(response == NULL);
	/* No socket error */
	EXPECT_ERROR_STMT(remote_connection_cancel_query(conn));

	set = async_request_set_create();
	EXPECT_ERROR_STMT(async_request_set_add_sql(set, conn, "SELECT 1"));

	/* test error throwing in async_request_set_wait_any_result */
	conn = get_connection();
	rnk = remote_node_killer_init(conn);
	remote_node_killer_kill(rnk);
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");
	/* first we get error result */
	Assert(NULL != async_request_set_wait_any_result(set));
	EXPECT_ERROR_STMT(async_request_set_wait_any_result(set));

	/* do cancel query before first response */
	conn = get_connection();
	rnk = remote_node_killer_init(conn);
	remote_node_killer_kill(rnk);
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");
	Assert(false == remote_connection_cancel_query(conn));

	/* do cancel query after seeing error */
	conn = get_connection();
	rnk = remote_node_killer_init(conn);
	remote_node_killer_kill(rnk);
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "SELECT 1");
	/* first we get error result */
	EXPECT_ERROR_STMT(async_request_set_wait_ok_result(set));
	Assert(false == remote_connection_cancel_query(conn));

	remote_connection_close(conn);
}

static void
test_timeout()
{
	PGconn *conn = get_connection();
	PGconn *conn_2 = get_connection();
	AsyncRequestSet *set;
	AsyncResponse *response;

	/*
	 * Test timeout by locking a table in conn_2 and timing out on locking
	 * conn
	 */
	async_request_wait_ok_command(
		async_request_send_with_params_elevel(conn_2, "BEGIN;", 0, NULL, ERROR));
	async_request_wait_ok_command(
		async_request_send_with_params_elevel(conn_2, "LOCK \"S 1\".\"T 1\"", 0, NULL, ERROR));

	async_request_wait_ok_command(
		async_request_send_with_params_elevel(conn, "BEGIN;", 0, NULL, ERROR));
	set = async_request_set_create();
	async_request_set_add_sql(set, conn, "LOCK \"S 1\".\"T 1\"");
	response = async_request_set_wait_any_response_deadline(
		set, DEBUG1, TimestampTzPlusMilliseconds(GetCurrentTimestamp(), 100));
	Assert(async_response_get_type(response) == RESPONSE_TIMEOUT);

	/* try again, use timeout instead of deadline interface */
	response = async_request_set_wait_any_response_timeout(set, DEBUG1, 100);
	Assert(async_response_get_type(response) == RESPONSE_TIMEOUT);

	/* cancel the locked query and do another query */
	Assert(remote_connection_cancel_query(conn));
	/* the txn is aborted waiting for abort */
	EXPECT_ERROR_STMT(async_request_wait_ok_result(
		async_request_send_with_params_elevel(conn, "SELECT 1;", 0, NULL, ERROR)));
	async_request_wait_ok_command(
		async_request_send_with_params_elevel(conn, "ABORT;", 0, NULL, ERROR));
	async_request_wait_ok_result(
		async_request_send_with_params_elevel(conn, "SELECT 1;", 0, NULL, ERROR));

	/* release conn_2 lock */
	async_request_wait_ok_command(
		async_request_send_with_params_elevel(conn_2, "COMMIT;", 0, NULL, ERROR));

	remote_connection_close(conn);
	remote_connection_close(conn_2);
}

Datum
tsl_test_remote_async(PG_FUNCTION_ARGS)
{
	test_basic_request();
	test_parameter_order();
	test_request_set();
	test_node_death();
	test_timeout();

	PG_RETURN_VOID();
}
