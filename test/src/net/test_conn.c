/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>

#include "compat/compat.h"
#include "config.h"
#include "net/conn.h"

#define MAX_RESULT_SIZE 2048

TS_FUNCTION_INFO_V1(ts_test_conn);

Datum
ts_test_conn(PG_FUNCTION_ARGS)
{
	char response[MAX_RESULT_SIZE];
	Connection *conn;
	int ret;
	int port = 80;
#ifdef TS_USE_OPENSSL
	int ssl_port = 443;
#endif
	char *host = "postman-echo.com";

	/* Test connection_init/destroy */
	conn = ts_connection_create(CONNECTION_PLAIN);
	ts_connection_destroy(conn);

	/* Check pass NULL won't crash */
	ts_connection_destroy(NULL);

	/* Check that delays on the socket are properly handled */
	conn = ts_connection_create(CONNECTION_PLAIN);

	ts_connection_set_timeout_millis(conn, 200);

	/* This is a brittle assert function because we might not necessarily have */
	/* connectivity on the server running this test? */
	ret = ts_connection_connect(conn, host, NULL, port);

	if (ret < 0)
		elog(ERROR, "%s", ts_connection_get_and_clear_error(conn));

	/* should timeout */
	ret = ts_connection_read(conn, response, 1);

	if (ret == 0)
		elog(ERROR, "Expected timeout");

	ts_connection_close(conn);
	ts_connection_destroy(conn);

#ifdef TS_USE_OPENSSL
	/* Now test ssl_ops */
	conn = ts_connection_create(CONNECTION_SSL);

	ts_connection_set_timeout_millis(conn, 200);

	ret = ts_connection_connect(conn, host, NULL, ssl_port);

	if (ret < 0)
		elog(ERROR, "%s", ts_connection_get_and_clear_error(conn));

	ret = ts_connection_read(conn, response, 1);

	if (ret == 0)
		elog(ERROR, "Expected timeout");

	ts_connection_close(conn);
	ts_connection_destroy(conn);
#endif

	PG_RETURN_NULL();
}
