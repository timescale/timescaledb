#include <string.h>
#include <unistd.h>

#include "telemetry/telemetry.h"
#include "net/utils.h"

#define TEST_ENDPOINT	"postman-echo.com"

TS_FUNCTION_INFO_V1(test_status);
TS_FUNCTION_INFO_V1(test_status_ssl);
TS_FUNCTION_INFO_V1(test_status_mock);
TS_FUNCTION_INFO_V1(test_telemetry);

#ifdef DEBUG
static char *test_string;
#endif

static
HttpRequest *build_request(int status) {
	char uri[20];
	snprintf(uri, 20, "/status/%d", status);

	HttpRequest *req = http_request_create(HTTP_GET);
	http_request_set_uri(req, uri);
	http_request_set_version(req, HTTP_10);
	http_request_set_header(req, HTTP_HOST, TEST_ENDPOINT);
	http_request_set_header(req, HTTP_CONTENT_LENGTH, "0");
	return req;
}

static Datum test_factory(ConnectionType type, int status, char *host, int port) {
	int ret;
	char *response = "Problem during test";
	Connection *conn = connection_create(type);

	if (conn == NULL)
		return CStringGetTextDatum("Could not initialize a connection");
	ret = connection_connect(conn, host, port);
	if (ret < 0)
		goto cleanup_conn;

#ifdef DEBUG
	if (type == CONNECTION_MOCK)
		connection_mock_set_recv_buf(conn, test_string, strlen(test_string));	
#endif
	
	response = send_and_recv_http(conn, build_request(status));
cleanup_conn:
	connection_close(conn);
	connection_destroy(conn);
	return CStringGetTextDatum(response);
}

// Test ssl_ops
Datum
test_status_ssl(PG_FUNCTION_ARGS)
{
	int status = PG_GETARG_INT32(0);
	return test_factory(CONNECTION_SSL, status, TEST_ENDPOINT, HTTPS_PORT);
}

// Test default_ops
Datum
test_status(PG_FUNCTION_ARGS)
{
	int			port = 80;
	int status = PG_GETARG_INT32(0);
	return test_factory(CONNECTION_PLAIN, status, TEST_ENDPOINT, port);
}

#ifdef DEBUG
// Test mock_ops
Datum
test_status_mock(PG_FUNCTION_ARGS)
{
	int			port = 80;
	text  *arg1 = PG_GETARG_TEXT_P(0);
	test_string = text_to_cstring(arg1);
	
	return test_factory(CONNECTION_MOCK, 123, TEST_ENDPOINT, port);
}
#endif

Datum
test_telemetry(PG_FUNCTION_ARGS)
{
	int ret;
	char *response;
	Connection *conn = connection_create(CONNECTION_SSL);
	if (conn == NULL)
		PG_RETURN_NULL();
	ret = connection_connect(conn, get_guc_endpoint_hostname(),	get_guc_endpoint_port());
	if (ret < 0)
		goto cleanup_conn;

	response = send_and_recv_http(conn, build_version_request());
	// Just verify that it's some sort of valid JSON
	Assert(strtok(response, ":") != NULL);
cleanup_conn:
	connection_close(conn);
	connection_destroy(conn);
	PG_RETURN_NULL();
}
