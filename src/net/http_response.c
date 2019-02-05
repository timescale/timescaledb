/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <lib/stringinfo.h>
#include <utils/memutils.h>

#include "http.h"

#define CARRIAGE_RETURN '\r'
#define NEW_LINE '\n'
#define SEP_CHAR ':'
#define HTTP_VERSION_BUFFER_SIZE 128

extern HttpHeader *ts_http_header_create(const char *name, size_t name_len, const char *value,
										 size_t value_len, HttpHeader *next);

typedef enum HttpParseState
{
	HTTP_STATE_STATUS,
	HTTP_STATE_INTERM,		/* received a single \r */
	HTTP_STATE_HEADER_NAME, /* received \r\n */
	HTTP_STATE_HEADER_VALUE,
	HTTP_STATE_ALMOST_DONE,
	HTTP_STATE_BODY,
	HTTP_STATE_ERROR,
	HTTP_STATE_DONE,
} HttpParseState;

typedef struct HttpResponseState
{
	MemoryContext context;
	char version[HTTP_VERSION_BUFFER_SIZE];
	char raw_buffer[MAX_RAW_BUFFER_SIZE];
	/* The next read should copy data into the buffer starting here */
	off_t offset;
	off_t parse_offset;
	size_t cur_header_name_len;
	size_t cur_header_value_len;
	char *cur_header_name;
	char *cur_header_value;
	HttpHeader *headers;
	int status_code;
	size_t content_length;
	char *body_start;
	HttpParseState state;
} HttpResponseState;

void
ts_http_response_state_init(HttpResponseState *state)
{
	state->status_code = -1;
	state->state = HTTP_STATE_STATUS;
}

HttpResponseState *
ts_http_response_state_create()
{
	MemoryContext context =
		AllocSetContextCreate(CurrentMemoryContext, "Http Response", ALLOCSET_DEFAULT_SIZES);
	MemoryContext old = MemoryContextSwitchTo(context);
	HttpResponseState *ret = palloc(sizeof(HttpResponseState));

	memset(ret, 0, sizeof(*ret));

	ret->context = context;
	ts_http_response_state_init(ret);
	MemoryContextSwitchTo(old);
	return ret;
}

void
ts_http_response_state_destroy(HttpResponseState *state)
{
	MemoryContextDelete(state->context);
}

bool
ts_http_response_state_valid_status(HttpResponseState *state)
{
	/* If the status code hasn't been parsed yet, return */
	if (state->status_code == -1)
		return true;
	/* If it's a bad status code, then bad! */
	if (state->status_code / 100 == 2)
		return true;
	return false;
}

bool
ts_http_response_state_is_done(HttpResponseState *state)
{
	return (state->state == HTTP_STATE_DONE);
}

/*
 * Return the remaining buffer space.
 *
 * Returns 0 or a positive number, or -1 for invalid state.
 *
 */
ssize_t
ts_http_response_state_buffer_remaining(HttpResponseState *state)
{
	Assert(state->offset <= MAX_RAW_BUFFER_SIZE);

	return MAX_RAW_BUFFER_SIZE - state->offset;
}

/*
 * Return a pointer to the next buffer to write to.
 *
 * Optionally, return the buffer size via the bufsize parameter.
 */
char *
ts_http_response_state_next_buffer(HttpResponseState *state, ssize_t *bufsize)
{
	Assert(state->offset <= MAX_RAW_BUFFER_SIZE);

	if (NULL != bufsize)
		*bufsize = ts_http_response_state_buffer_remaining(state);

	/*
	 * This should not happen, be we return NULL in this case and let caller
	 * deal with it
	 */
	if (state->offset > MAX_RAW_BUFFER_SIZE)
		return NULL;

	return state->raw_buffer + state->offset;
}

const char *
ts_http_response_state_body_start(HttpResponseState *state)
{
	return state->body_start;
}

int
ts_http_response_state_status_code(HttpResponseState *state)
{
	return state->status_code;
}

size_t
ts_http_response_state_content_length(HttpResponseState *state)
{
	return state->content_length;
}

HttpHeader *
ts_http_response_state_headers(HttpResponseState *state)
{
	return state->headers;
}

static bool
http_parse_version(HttpResponseState *state)
{
	return ts_http_version_from_string(state->version) != HTTP_VERSION_INVALID;
}

static void
http_parse_status(HttpResponseState *state, const char next)
{
	char *raw_buf = palloc(state->parse_offset + 1);

	switch (next)
	{
		case CARRIAGE_RETURN:

			/*
			 * Then we are at the end of status and can use sscanf
			 *
			 * Need a second %s inside the sscanf so that we make sure to get
			 * all of the digits of the status code
			 */
			memcpy(raw_buf, state->raw_buffer, state->parse_offset);
			raw_buf[state->parse_offset] = '\0';
			state->state = HTTP_STATE_ERROR;
			memset(state->version, '\0', sizeof(state->version));

			if (sscanf(raw_buf, "%127s%*[ ]%d%*[ ]%*s", state->version, &state->status_code) == 2)
			{
				if (http_parse_version(state))
					state->state = HTTP_STATE_INTERM;
				else
					state->state = HTTP_STATE_ERROR;
			}
			break;
		case NEW_LINE:
			state->state = HTTP_STATE_ERROR;
			break;
		default:
			/* Don't try to parse Status line until we see '\r' */
			break;
	}

	pfree(raw_buf);
}

static void
http_response_state_add_header(HttpResponseState *state, const char *name, size_t name_len,
							   const char *value, size_t value_len)
{
	MemoryContext old = MemoryContextSwitchTo(state->context);
	HttpHeader *new_header =
		ts_http_header_create(name, name_len, value, value_len, state->headers);

	state->headers = new_header;
	MemoryContextSwitchTo(old);
}

static void
http_parse_interm(HttpResponseState *state, const char next)
{
	int temp_length;

	switch (next)
	{
		case NEW_LINE:
			state->state = HTTP_STATE_HEADER_NAME;
			/* Store another header */
			http_response_state_add_header(state,
										   state->cur_header_name,
										   state->cur_header_name_len,
										   state->cur_header_value,
										   state->cur_header_value_len);

			/* Check if the line we just read is Content-Length */
			if (state->cur_header_name != NULL &&
				strncmp(HTTP_CONTENT_LENGTH, state->cur_header_name, state->cur_header_name_len) ==
					0)
			{
				if (sscanf(state->cur_header_value, "%d", &temp_length) == 1)
				{
					state->content_length = temp_length;
				}
				else
				{
					state->state = HTTP_STATE_ERROR;
					break;
				}
			}
			state->cur_header_name_len = 0;
			state->cur_header_value_len = 0;
			break;
		default:
			state->state = HTTP_STATE_ERROR;
			break;
	}
}

static void
http_parse_header_name(HttpResponseState *state, const char next)
{
	switch (next)
	{
		case SEP_CHAR:
			state->state = HTTP_STATE_HEADER_VALUE;
			state->cur_header_value = state->raw_buffer + state->parse_offset + 1;
			break;
		case CARRIAGE_RETURN:
			if (state->cur_header_name_len == 0)
			{
				state->state = HTTP_STATE_ALMOST_DONE;
				break;
			}
			else
			{
				/*
				 * I'm guessing getting a carriage return in the middle of
				 * field
				 */
				/* name is bad... */
				state->state = HTTP_STATE_ERROR;
				break;
			}
		default:
			/* Header names are only alphabetic chars */
			if (('a' <= next && next <= 'z') || ('A' <= next && next <= 'Z') || next == '-')
			{
				/* Good, then the next call will save this char */
				state->cur_header_name_len++;
				break;
			}
			state->state = HTTP_STATE_ERROR;
			break;
	}
}

/*  We do not customize to header_name. Assume all non \r or \n chars are allowed. */
static void
http_parse_header_value(HttpResponseState *state, const char next)
{
	/* Allow everything except... \r, \n */
	switch (next)
	{
		case CARRIAGE_RETURN:
			state->state = HTTP_STATE_INTERM;
			break;
		case NEW_LINE:
			/* \n is not allowed */
			state->state = HTTP_STATE_ERROR;
			break;
		default:
			state->cur_header_value_len++;
			break;
	}
}

static void
http_parse_almost_done(HttpResponseState *state, const char next)
{
	/* Don't do anything, this is intermediate state */
	switch (next)
	{
		case NEW_LINE:
			state->state = HTTP_STATE_BODY;
			state->body_start = state->raw_buffer + state->parse_offset + 1;
			/* Special case if there is no body */
			if (state->content_length == 0)
				state->state = HTTP_STATE_DONE;
			break;
		default:
			state->state = HTTP_STATE_ERROR;
			break;
	}
}

bool
ts_http_response_state_parse(HttpResponseState *state, size_t bytes)
{
	state->offset += bytes;

	if (state->offset > MAX_RAW_BUFFER_SIZE)
		state->offset = MAX_RAW_BUFFER_SIZE;

	/* Each state function will do the state AND transition */
	while (state->parse_offset < state->offset)
	{
		char next = state->raw_buffer[state->parse_offset];

		switch (state->state)
		{
			case HTTP_STATE_STATUS:
				http_parse_status(state, next);
				state->parse_offset++;
				break;
			case HTTP_STATE_INTERM:
				http_parse_interm(state, next);
				state->parse_offset++;
				state->cur_header_name = state->raw_buffer + state->parse_offset;
				break;
			case HTTP_STATE_HEADER_NAME:
				http_parse_header_name(state, next);
				state->parse_offset++;
				break;
			case HTTP_STATE_HEADER_VALUE:
				http_parse_header_value(state, next);
				state->parse_offset++;
				break;
			case HTTP_STATE_ALMOST_DONE:
				http_parse_almost_done(state, next);
				state->parse_offset++;
				break;
			case HTTP_STATE_BODY:
				/* Stay here until we have read content_length */
				if ((state->body_start + state->content_length) <=
					(state->raw_buffer + state->offset))
				{
					/* Then we are done */
					state->state = HTTP_STATE_DONE;
					return true;
				}
				state->parse_offset++;
				break;
			case HTTP_STATE_ERROR:
				return false;
			case HTTP_STATE_DONE:
				return true;
		}
	}
	return true;
}
