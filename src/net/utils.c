#include <stdio.h>
#include <stdlib.h>
#include <postgres.h>
#include <pg_config.h>
#include <utils/memutils.h>
#include <utils/builtins.h>

#include "utils.h"

#define MAX_RESPONSE_BODY_SIZE	1024

static void
parse_http_from_conn(Connection *conn, HttpResponseState *state)
{
	int			ret;

	while (!http_response_state_is_done(state) ||
		   http_response_state_buffer_remaining(state) == 0)
	{
		ret = connection_read(conn, http_response_state_next_buffer(state),
							  http_response_state_buffer_remaining(state));
		if (ret < 0)
			elog(ERROR, "could not read via connection_read");

		/*
		 * Because we use a blocking socket, this means the server has closed
		 * the conn. Or the response parse has run out of buffer space.
		 */
		if (ret == 0)
			return;

		if (!http_response_state_parse(state, ret))
			elog(ERROR, "could not parse invalid HTTP message");
	}
}

/*  Send an HTTP request and receive the HTTP response on the given Connection.
 *  Note that we create a new memory context for this function, that is a child of the
 *  CurrentMemoryContext, as opposed to a child of the toplevel memory context.
 */
char *
send_and_recv_http(Connection *conn, HttpRequest *req)
{
	const char *built_request;
	size_t		request_len;
	HttpResponseState *state;
	int			ret;
	MemoryContext old;
	/* Allocate these before we make a temporary context, so that it won't get freed
	 * after the function returns.
	 */
	char		*response = palloc(MAX_RESPONSE_BODY_SIZE);

	MemoryContext send_and_recv_context = AllocSetContextCreate(CurrentMemoryContext, "send_and_recv",
																ALLOCSET_DEFAULT_SIZES);
	old = MemoryContextSwitchTo(send_and_recv_context);

	built_request = http_request_build(req, &request_len);

	ret = connection_write(conn, built_request, request_len);
	if (ret < 0)
		elog(ERROR, "could not write via connection_write");

	/* Now we have to parse the HttpResponse */
	state = http_response_state_create();
	parse_http_from_conn(conn, state);

	/* Now check status and done-ness */
	if (!http_response_state_is_done(state))
		elog(ERROR, "HTTP parsing could not finish");

	if (!http_response_state_valid_status(state))
		elog(ERROR, "endpoint sent back unexpected HTTP status: %d", http_response_state_status_code(state));

	StrNCpy(response, http_response_state_body_start(state), MAX_RESPONSE_BODY_SIZE);

	/* Free resources and return string in response buffer */
	http_request_destroy(req);
	MemoryContextSwitchTo(old);
	MemoryContextDelete(send_and_recv_context);
	return response;
}
