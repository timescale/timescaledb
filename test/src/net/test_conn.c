#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>

#include "compat.h"
#include "net/conn.h"

#define MAX_RESULT_SIZE	2048

TS_FUNCTION_INFO_V1(test_conn);

Datum
test_conn(PG_FUNCTION_ARGS)
{
	char		response[MAX_RESULT_SIZE];
	Connection *conn;
	bool		should_fail = false;
	int			port = 80;
#ifdef USE_OPENSSL
	int			ssl_port = 443;
#endif
	char	   *host = "postman-echo.com";

	/* Test connection_init/destroy */
	conn = connection_create(CONNECTION_PLAIN);
	connection_destroy(conn);

	/* Check pass NULL won't crash */
	connection_destroy(NULL);

	/* Check that delays on the socket are properly handled */
	conn = connection_create(CONNECTION_PLAIN);
	/* This is a brittle assert function because we might not necessarily have */
	/* connectivity on the server running this test? */
	Assert(connection_connect(conn, host, port) >= 0);

	/* should timeout */
	PG_TRY();
	{
		connection_read(conn, response, 1);
	}
	PG_CATCH();
	{
		should_fail = true;
	}
	PG_END_TRY();
	Assert(should_fail);

	connection_close(conn);
	connection_destroy(conn);

#ifdef USE_OPENSSL
	/* Now test ssl_ops */
	conn = connection_create(CONNECTION_SSL);
	Assert(connection_connect(conn, host, ssl_port) >= 0);

	should_fail = false;
	PG_TRY();
	{
		connection_read(conn, response, 1);
	}
	PG_CATCH();
	{
		should_fail = true;
	}
	PG_END_TRY();
	Assert(should_fail);

	connection_close(conn);
	connection_destroy(conn);
#endif

	PG_RETURN_NULL();
}
