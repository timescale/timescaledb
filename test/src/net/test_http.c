#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <postgres.h>
#include <fmgr.h>

#include "export.h"
#include "net/http.h"

#define MAX_REQUEST_SIZE	4096

/*  Tests for auxiliary HttpResponseState functions in http_parsing.h */

const char *TEST_RESPONSES[] = {
	"HTTP/1.1 200 OK\r\n"
	"Content-Type: application/json; charset=utf-8\r\n"
	"Date: Thu, 12 Jul 2018 18:33:04 GMT\r\n"
	"ETag: W/\"e-upYEWCL+q6R/++2nWHz5b76hBgo\"\r\n"
	"Server: nginx "
	"Vary: Accept-Encoding\r\n"
	"Content-Length: 14\r\n"
	"Connection: Close\r\n\r\n"
	"{\"status\":200}",
	"HTTP/1.1 200 OK\r\n"
	"Content-Length: 14\r\n"
	"Content-Type: application/json; charset=utf-8\r\n"
	"Date: Thu, 12 Jul 2018 18:33:04 GMT\r\n"
	"ETag: W/\"e-upYEWCL+q6R/++2nWHz5b76hBgo\"\r\n"
	"Vary: Accept-Encoding\r\n\r\n"
	"{\"status\":200}",
	"HTTP/1.1 200 OK\r\n"
	"Content-Length: 14\r\n"
	"Connection: Close\r\n\r\n"
	"{\"status\":200}",
	"HTTP/1.1 201 OK\r\n"
	"Date: Thu, 12 Jul 2018 18:33:04 GMT\r\n"
	"Content-Length: 14\r\n"
	"ETag: W/\"e-upYEWCL+q6R/++2nWHz5b76hBgo\"\r\n"
	"Connection: Close\r\n\r\n"
	"{\"status\":201}",
};

const char *const BAD_RESPONSES[] = {
	"HTTP/1.1 200 OK\r\n"
	"Content-Type: application/json; charset=utf-8\r\n"
	"Date: Thu, 12 Jul 2018 18:33:04 GMT\r\n"
	"ETag: W/\"e-upYEWCL+q6R/++2nWHz5b76hBgo\"\r\n"
	"Connection: Close\r\n"
	"{\"status\":200}",
	"Content-Length: 14\r\n"
	"{\"status\":200}",
	"Content-Length: 14\r\n"
	"HTTP/1.1 404 Not Found\r\n"
	"Connection: Close\r\n\r\n"
	"{\"status\":404}",
	NULL
};

int			TEST_LENGTHS[] = {14, 14, 14, 14};
const char *MESSAGE_BODY[] = {"{\"status\":200}", "{\"status\":200}", "{\"status\":200}", "{\"status\":201}"};

TS_FUNCTION_INFO_V1(ts_test_http_parsing);
TS_FUNCTION_INFO_V1(ts_test_http_parsing_full);
TS_FUNCTION_INFO_V1(ts_test_http_request_build);

static int
num_test_strings()
{
	return sizeof(TEST_LENGTHS) / sizeof(int);
}

/*  Check we can succesfully parse partial by well-formed HTTP responses */
Datum
ts_test_http_parsing(PG_FUNCTION_ARGS)
{
	int			num_iterations = PG_GETARG_INT32(0);
	int			bytes,
				i,
				j;

	srand(time(0));

	for (j = 0; j < num_iterations; j++)
	{
		for (i = 0; i < num_test_strings(); i++)
		{
			HttpResponseState *state = http_response_state_create();
			bool		success;
			ssize_t		bufsize = 0;
			char	   *buf;

			bytes = rand() % (strlen(TEST_RESPONSES[i]) + 1);

			buf = http_response_state_next_buffer(state, &bufsize);

			Assert(bufsize >= bytes);

			/* Copy part of the message into the parsing state */
			memcpy(buf, TEST_RESPONSES[i], bytes);

			/* Now do the parse */
			success = http_response_state_parse(state, bytes);

			Assert(success);

			success = http_response_state_is_done(state);

			Assert(bytes < strlen(TEST_RESPONSES[i]) ? !success : success);

			http_response_state_destroy(state);
		}
	}
	PG_RETURN_NULL();
}

/*  Check we can successfully parse full, well-formed HTTP response AND
 *  successfully find error with full, poorly-formed HTTP responses
 */
Datum
ts_test_http_parsing_full(PG_FUNCTION_ARGS)
{
	int			bytes,
				i;

	srand(time(0));

	for (i = 0; i < num_test_strings(); i++)
	{
		HttpResponseState *state = http_response_state_create();
		ssize_t		bufsize = 0;
		char	   *buf;

		buf = http_response_state_next_buffer(state, &bufsize);

		bytes = strlen(TEST_RESPONSES[i]);

		Assert(bufsize >= bytes);

		/* Copy all of the message into the parsing state */
		memcpy(buf, TEST_RESPONSES[i], bytes);

		/* Now do the parse */
		Assert(http_response_state_parse(state, bytes));

		Assert(http_response_state_is_done(state));
		Assert(http_response_state_content_length(state) == TEST_LENGTHS[i]);
		/* Make sure we read the right message body */
		Assert(!strncmp(MESSAGE_BODY[i], http_response_state_body_start(state), http_response_state_content_length(state)));

		http_response_state_destroy(state);
	}

	/* Now do the bad responses */
	for (i = 0; i < 3; i++)
	{
		HttpResponseState *state = http_response_state_create();
		ssize_t		bufsize = 0;
		char	   *buf;

		buf = http_response_state_next_buffer(state, &bufsize);

		bytes = strlen(BAD_RESPONSES[i]);

		Assert(bufsize >= bytes);

		memcpy(buf, BAD_RESPONSES[i], bytes);

		Assert(!http_response_state_parse(state, bytes) ||
			   !http_response_state_valid_status(state));

		http_response_state_destroy(state);
	}
	PG_RETURN_NULL();
}

Datum
ts_test_http_request_build(PG_FUNCTION_ARGS)
{
	const char *serialized;
	size_t		request_len;
	const char *expected_response = "GET /v1/alerts HTTP/1.1\r\n"
	"Host: herp.com\r\nContent-Length: 0\r\n\r\n";
	char	   *host = "herp.com";
	HttpRequest *req = http_request_create(HTTP_GET);

	http_request_set_uri(req, "/v1/alerts");
	http_request_set_version(req, HTTP_VERSION_11);
	http_request_set_header(req, HTTP_CONTENT_LENGTH, "0");
	http_request_set_header(req, HTTP_HOST, host);

	serialized = http_request_build(req, &request_len);

	Assert(!strncmp(expected_response, serialized, request_len));
	http_request_destroy(req);

	expected_response = "GET /tmp/path/to/uri HTTP/1.0\r\n"
		"Content-Length: 0\r\nHost: herp.com\r\nContent-Type: application/json\r\n\r\n";

	req = http_request_create(HTTP_GET);
	http_request_set_uri(req, "/tmp/path/to/uri");
	http_request_set_version(req, HTTP_VERSION_10);
	http_request_set_header(req, HTTP_CONTENT_TYPE, "application/json");
	http_request_set_header(req, HTTP_HOST, host);
	http_request_set_header(req, HTTP_CONTENT_LENGTH, "0");

	serialized = http_request_build(req, &request_len);

	Assert(!strncmp(expected_response, serialized, request_len));
	http_request_destroy(req);

	expected_response = "POST /tmp/status/1234 HTTP/1.1\r\n"
		"Content-Length: 0\r\nHost: herp.com\r\n\r\n";

	req = http_request_create(HTTP_POST);
	http_request_set_uri(req, "/tmp/status/1234");
	http_request_set_version(req, HTTP_VERSION_11);
	http_request_set_header(req, HTTP_HOST, host);
	http_request_set_header(req, HTTP_CONTENT_LENGTH, "0");

	serialized = http_request_build(req, &request_len);

	Assert(!strncmp(expected_response, serialized, request_len));
	http_request_destroy(req);

	/* Check that content-length checking works */
	req = http_request_create(HTTP_POST);
	http_request_set_uri(req, "/tmp/status/1234");
	http_request_set_version(req, HTTP_VERSION_11);
	http_request_set_header(req, HTTP_HOST, host);
	http_request_set_header(req, HTTP_CONTENT_LENGTH, "9");

	Assert(!http_request_build(req, &request_len));
	http_request_destroy(req);

	PG_RETURN_NULL();
}
