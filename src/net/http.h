/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_NET_HTTP_H
#define TIMESCALEDB_NET_HTTP_H

#include <postgres.h>

#include <utils/jsonb.h>

#define HTTP_HOST "Host"
#define HTTP_CONTENT_LENGTH "Content-Length"
#define HTTP_CONTENT_TYPE "Content-Type"
#define MAX_RAW_BUFFER_SIZE 4096
#define MAX_REQUEST_DATA_SIZE 2048

typedef struct HttpHeader
{
	char *name;
	int name_len;
	char *value;
	int value_len;
	struct HttpHeader *next;
} HttpHeader;

/******* http_request.c *******/
/*  We can add more methods later, but for now we do not need others */
typedef enum HttpRequestMethod
{
	HTTP_GET,
	HTTP_POST,
} HttpRequestMethod;

typedef enum HttpVersion
{
	HTTP_VERSION_10,
	HTTP_VERSION_11,
	HTTP_VERSION_INVALID,
} HttpVersion;

typedef enum HttpError
{
	HTTP_ERROR_NONE = 0,
	HTTP_ERROR_WRITE, /* Connection write error, check errno */
	HTTP_ERROR_READ,  /* Connection read error, check errno */
	HTTP_ERROR_CONN_CLOSED,
	HTTP_ERROR_REQUEST_BUILD,
	HTTP_ERROR_RESPONSE_PARSE,
	HTTP_ERROR_RESPONSE_INCOMPLETE,
	HTTP_ERROR_INVALID_BUFFER_STATE,
	HTTP_ERROR_UNKNOWN, /* Should always be last */
} HttpError;

/*  NOTE: HttpRequest* structs are all responsible */
/*  for allocating and deallocating the char* */
typedef struct HttpRequest HttpRequest;
typedef struct Connection Connection;

extern HttpVersion ts_http_version_from_string(const char *version);
extern const char *ts_http_version_string(HttpVersion version);

extern void ts_http_request_init(HttpRequest *req, HttpRequestMethod method);
extern HttpRequest *ts_http_request_create(HttpRequestMethod method);
extern void ts_http_request_destroy(HttpRequest *req);

/* Assume that uri is null-terminated */
extern void ts_http_request_set_uri(HttpRequest *req, const char *uri);
extern void ts_http_request_set_version(HttpRequest *req, HttpVersion version);

/* Assume that name and value are null-terminated */
extern void ts_http_request_set_header(HttpRequest *req, const char *name, const char *value);
extern void ts_http_request_set_body_jsonb(HttpRequest *req, const Jsonb *json);

/*  Serialize the request into char *dst. Return the length of request in optional size pointer*/
extern const char *ts_http_request_build(HttpRequest *req, size_t *buf_size);

/******* http_response.c *******/

typedef struct HttpResponseState HttpResponseState;

extern void ts_http_response_state_init(HttpResponseState *state);
extern HttpResponseState *ts_http_response_state_create(void);
extern void ts_http_response_state_destroy(HttpResponseState *state);

/*  Accessor Functions */
extern bool ts_http_response_state_is_done(HttpResponseState *state);
extern bool ts_http_response_state_valid_status(HttpResponseState *state);
extern char *ts_http_response_state_next_buffer(HttpResponseState *state, ssize_t *bufsize);
extern ssize_t ts_http_response_state_buffer_remaining(HttpResponseState *state);
extern const char *ts_http_response_state_body_start(HttpResponseState *state);
extern size_t ts_http_response_state_content_length(HttpResponseState *state);
extern int ts_http_response_state_status_code(HttpResponseState *state);
extern HttpHeader *ts_http_response_state_headers(HttpResponseState *state);

/*  Returns false if encountered an error during parsing */
extern bool ts_http_response_state_parse(HttpResponseState *state, size_t bytes);

extern const char *ts_http_strerror(HttpError http_errno);
extern HttpError ts_http_send_and_recv(Connection *conn, HttpRequest *req,
									   HttpResponseState *state);

#endif /* TIMESCALEDB_HTTP_H */
