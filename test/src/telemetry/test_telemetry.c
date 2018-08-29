#include <postgres.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <funcapi.h>
#include <fmgr.h>

#include "telemetry/telemetry.h"
#include "net/utils.h"
#ifdef DEBUG
#include "net/conn_mock.h"
#endif

#define HTTPS_PORT	443
#define TEST_ENDPOINT	"postman-echo.com"

TS_FUNCTION_INFO_V1(test_status);
TS_FUNCTION_INFO_V1(test_status_ssl);
TS_FUNCTION_INFO_V1(test_status_mock);
TS_FUNCTION_INFO_V1(test_telemetry);

#ifdef DEBUG
static char *test_string;
#endif

static
HttpRequest *
build_request(int status)
{
	HttpRequest *req = http_request_create(HTTP_GET);
	char		uri[20];

	snprintf(uri, 20, "/status/%d", status);


	http_request_set_uri(req, uri);
	http_request_set_version(req, HTTP_VERSION_10);
	http_request_set_header(req, HTTP_HOST, TEST_ENDPOINT);
	http_request_set_header(req, HTTP_CONTENT_LENGTH, "0");
	return req;
}

static Datum
test_factory(ConnectionType type, int status, char *host, int port)
{
	int			ret;
	char	   *response = "Problem during test";
	Connection *conn = connection_create(type);

	if (conn == NULL)
		return CStringGetTextDatum("Could not initialize a connection");
	ret = connection_connect(conn, host, NULL, port);
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

/*  Test ssl_ops */
Datum
test_status_ssl(PG_FUNCTION_ARGS)
{
	int			status = PG_GETARG_INT32(0);

	return test_factory(CONNECTION_SSL, status, TEST_ENDPOINT, HTTPS_PORT);
}

/*  Test default_ops */
Datum
test_status(PG_FUNCTION_ARGS)
{
	int			port = 80;
	int			status = PG_GETARG_INT32(0);

	return test_factory(CONNECTION_PLAIN, status, TEST_ENDPOINT, port);
}

#ifdef DEBUG
/* Test mock_ops */
Datum
test_status_mock(PG_FUNCTION_ARGS)
{
	int			port = 80;
	text	   *arg1 = PG_GETARG_TEXT_P(0);

	test_string = text_to_cstring(arg1);

	return test_factory(CONNECTION_MOCK, 123, TEST_ENDPOINT, port);
}
#endif

TS_FUNCTION_INFO_V1(test_telemetry_parse_version);

Datum
test_telemetry_parse_version(PG_FUNCTION_ARGS)
{
	text	   *response = PG_GETARG_TEXT_P(0);
	long		installed_version[3] = {
		PG_GETARG_INT32(1),
		PG_GETARG_INT32(2),
		PG_GETARG_INT32(3),
	};
	VersionResult result = {0};
	TupleDesc	tupdesc;
	Datum		values[5];
	bool		nulls[5] = {false};
	HeapTuple	tuple;
	bool		success;

	if (PG_NARGS() != 4)
		PG_RETURN_NULL();

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	success = telemetry_parse_version(text_to_cstring(response), installed_version, &result);

	if (!success)
		elog(ERROR, "%s", result.errhint);

	values[0] = CStringGetTextDatum(result.versionstr);
	values[1] = Int32GetDatum(result.version[0]);
	values[2] = Int32GetDatum(result.version[1]);
	values[3] = Int32GetDatum(result.version[2]);
	values[4] = BoolGetDatum(result.is_up_to_date);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

Datum
test_telemetry(PG_FUNCTION_ARGS)
{
	char	   *response;
	Connection *conn;

	conn = telemetry_connect();

	response = send_and_recv_http(conn, build_version_request(TELEMETRY_HOST, TELEMETRY_PATH));

	/* Just verify that it's some sort of valid JSON */
	Assert(strtok(response, ":") != NULL);
	connection_close(conn);
	connection_destroy(conn);
	PG_RETURN_NULL();
}
