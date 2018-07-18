#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>

#include "compat.h"
#include "conn.h"

/*int
mock_read(int offset, char *buffer, int bytes_to_read, const char *message_to_read_from)
{
	int			max = strlen(message_to_read_from) - offset + 1;

	if (max > bytes_to_read)
		max = bytes_to_read;

	int			num_bytes = rand() % max;

	strncpy(buffer, message_to_read_from + offset, num_bytes);
	return num_bytes;
}*/
TS_FUNCTION_INFO_V1(test_conn);

Datum
test_conn(PG_FUNCTION_ARGS)
{
	int			port = 80;
	int			ssl_port = 443;
	char	   *host = "postman-echo.com";
	char		response[MAX_RESULT_SIZE];
	Connection *conn;

	// Test connection_init/destroy
	conn = connection_init();
	connection_destroy(conn);

	// Check pass NULL won't crash
	connection_destroy(NULL);

	// Check that delays on the socket are properly handled
	conn = connection_init();
	// This is a brittle assert function because we might not necessarily have
	// connectivity on the server running this test?
	Assert(conn->ops->connect(conn, host, port, response) >=  0);

	conn->ops->read(conn, response, 1); // should timeout
	conn->ops->close(conn);

	// Now test ssl_ops!
	connection_set_ops(conn, &ssl_ops);
	Assert(conn->ops->connect(conn, host, ssl_port, response) >= 0);
	conn->ops->read(conn, response, 1); 
	conn->ops->close(conn);
	
	connection_destroy(conn);
	PG_RETURN_NULL();	
}
