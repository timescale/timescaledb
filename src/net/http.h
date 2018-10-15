/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_NET_HTTP_H
#define TIMESCALEDB_NET_HTTP_H

#include <postgres.h>

#define HTTP_HOST	"Host"
#define HTTP_CONTENT_LENGTH	"Content-Length"
#define HTTP_CONTENT_TYPE	"Content-Type"
#define MAX_RAW_BUFFER_SIZE	4096
#define MAX_REQUEST_DATA_SIZE	2048

typedef struct HttpHeader
{
	char	   *name;
	int			name_len;
	char	   *value;
	int			value_len;
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
	HTTP_ERROR_WRITE,			/* Connection write error, check errno */
	HTTP_ERROR_READ,			/* Connection read error, check errno */
	HTTP_ERROR_CONN_CLOSED,
	HTTP_ERROR_REQUEST_BUILD,
	HTTP_ERROR_RESPONSE_PARSE,
	HTTP_ERROR_RESPONSE_INCOMPLETE,
	HTTP_ERROR_INVALID_BUFFER_STATE,
	HTTP_ERROR_UNKNOWN,			/* Should always be last */
} HttpError;

/*  NOTE: HttpRequest* structs are all responsible */
/*  for allocating and deallocating the char* */
typedef struct HttpRequest HttpRequest;
typedef struct Connection Connection;

HttpVersion http_version_from_string(const char *version);
const char *http_version_string(HttpVersion version);

void		http_request_init(HttpRequest *req, HttpRequestMethod method);
HttpRequest *http_request_create(HttpRequestMethod method);
void		http_request_destroy(HttpRequest *req);

/* Assume that uri is null-terminated */
void		http_request_set_uri(HttpRequest *req, const char *uri);
void		http_request_set_version(HttpRequest *req, HttpVersion version);

/* Assume that name and value are null-terminated */
void		http_request_set_header(HttpRequest *req, const char *name, const char *valuue);
void		http_request_set_body(HttpRequest *req, const char *body, size_t body_len);

/*  Serialize the request into char *dst. Return the length of request in optional size pointer*/
const char *http_request_build(HttpRequest *req, size_t *buf_size);

/******* http_response.c *******/

typedef struct HttpResponseState HttpResponseState;

void		http_response_state_init(HttpResponseState *state);
HttpResponseState *http_response_state_create(void);
void		http_response_state_destroy(HttpResponseState *state);

/*  Accessor Functions */
bool		http_response_state_is_done(HttpResponseState *state);
bool		http_response_state_valid_status(HttpResponseState *state);
char	   *http_response_state_next_buffer(HttpResponseState *state, ssize_t *bufsize);
ssize_t		http_response_state_buffer_remaining(HttpResponseState *state);
const char *http_response_state_body_start(HttpResponseState *state);
size_t		http_response_state_content_length(HttpResponseState *state);
int			http_response_state_status_code(HttpResponseState *state);
HttpHeader *http_response_state_headers(HttpResponseState *state);

/*  Returns false if encountered an error during parsing */
bool		http_response_state_parse(HttpResponseState *state, size_t bytes);

const char *http_strerror(HttpError http_errno);
HttpError	http_send_and_recv(Connection *conn, HttpRequest *req, HttpResponseState *state);

#endif							/* TIMESCALEDB_HTTP_H */
