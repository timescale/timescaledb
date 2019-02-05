/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include "http.h"
#include "conn.h"

static const char *http_error_strings[] = {
	[HTTP_ERROR_NONE] = "no HTTP error",
	[HTTP_ERROR_WRITE] = "HTTP connection write error",
	[HTTP_ERROR_READ] = "HTTP connection read error",
	[HTTP_ERROR_CONN_CLOSED] = "HTTP connection closed",
	[HTTP_ERROR_REQUEST_BUILD] = "could not build HTTP request",
	[HTTP_ERROR_RESPONSE_PARSE] = "could not parse HTTP response",
	[HTTP_ERROR_RESPONSE_INCOMPLETE] = "incomplete HTTP response",
	[HTTP_ERROR_INVALID_BUFFER_STATE] = "invalid HTTP buffer state",
	[HTTP_ERROR_UNKNOWN] = "unknown HTTP error",
};

static const char *http_version_strings[] = {
	[HTTP_VERSION_10] = "HTTP/1.0",
	[HTTP_VERSION_11] = "HTTP/1.1",
	[HTTP_VERSION_INVALID] = "invalid HTTP version",
};

const char *
ts_http_strerror(HttpError http_errno)
{
	return http_error_strings[http_errno];
}

HttpVersion
ts_http_version_from_string(const char *version)
{
	int i;

	for (i = 0; i < HTTP_VERSION_INVALID; i++)
		if (pg_strcasecmp(http_version_strings[i], version) == 0)
			return i;

	return HTTP_VERSION_INVALID;
}

const char *
ts_http_version_string(HttpVersion version)
{
	return http_version_strings[version];
}

/*
 * Send an HTTP request and receive the HTTP response on the given connection.
 *
 * Returns HTTP_ERROR_NONE (0) on success or a HTTP-specific error on failure.
 */
HttpError
ts_http_send_and_recv(Connection *conn, HttpRequest *req, HttpResponseState *state)
{
	const char *built_request;
	size_t request_len;
	off_t write_off = 0;
	HttpError err = HTTP_ERROR_NONE;
	int ret;

	built_request = ts_http_request_build(req, &request_len);

	if (NULL == built_request)
		return HTTP_ERROR_REQUEST_BUILD;

	while (request_len > 0)
	{
		ret = ts_connection_write(conn, built_request + write_off, request_len);

		if (ret < 0 || ret > request_len)
			return HTTP_ERROR_WRITE;

		if (ret == 0)
			return HTTP_ERROR_CONN_CLOSED;

		write_off += ret;
		request_len -= ret;
	}

	while (err == HTTP_ERROR_NONE && !ts_http_response_state_is_done(state))
	{
		ssize_t remaining = 0;
		char *buf = ts_http_response_state_next_buffer(state, &remaining);

		if (remaining < 0)
			err = HTTP_ERROR_INVALID_BUFFER_STATE;
		else if (remaining == 0)
			err = HTTP_ERROR_RESPONSE_INCOMPLETE;
		else
		{
			ssize_t bytes_read = ts_connection_read(conn, buf, remaining);

			if (bytes_read < 0)
				err = HTTP_ERROR_READ;
			/* Check for error or closed socket/EOF (ret == 0) */
			else if (bytes_read == 0)
				err = HTTP_ERROR_CONN_CLOSED;
			else if (!ts_http_response_state_parse(state, bytes_read))
				err = HTTP_ERROR_RESPONSE_PARSE;
		}
	}

	return err;
}
