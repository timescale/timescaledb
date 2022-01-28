/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <lib/stringinfo.h>
#include <utils/memutils.h>

#include "http.h"

#define SPACE ' '
#define COLON ':'
#define CARRIAGE '\r'
#define NEW_LINE '\n'

/*  So that http_response.c can find this function */
HttpHeader *ts_http_header_create(const char *name, size_t name_len, const char *value,
								  size_t value_len, HttpHeader *next);

HttpHeader *
ts_http_header_create(const char *name, size_t name_len, const char *value, size_t value_len,
					  HttpHeader *next)
{
	HttpHeader *new_header = palloc(sizeof(HttpHeader));

	memset(new_header, 0, sizeof(*new_header));
	new_header->name = palloc(name_len + 1);
	if (name_len > 0)
		memcpy(new_header->name, name, name_len);
	new_header->name[name_len] = '\0';
	new_header->name_len = name_len;

	new_header->value = palloc(value_len + 1);
	if (value_len > 0)
		memcpy(new_header->value, value, value_len);
	new_header->value[value_len] = '\0';
	new_header->value_len = value_len;

	new_header->next = next;
	return new_header;
}

/*  NOTE: The setter functions for HttpRequest should all */
/*  ensure that every char * in this struct is null-terminated */
typedef struct HttpRequest
{
	HttpRequestMethod method;
	char *uri;
	size_t uri_len;
	HttpVersion version;
	HttpHeader *headers;
	char *body;
	size_t body_len;
	MemoryContext context;
} HttpRequest;

static const char *http_method_strings[] = { [HTTP_GET] = "GET", [HTTP_POST] = "POST" };

#define METHOD_STRING(x) http_method_strings[x]
#define VERSION_STRING(x) ts_http_version_string(x)

/* appendBinaryStringInfo is UB if data is NULL. This function wraps it in a check that datalen > 0
 */
static void
appendOptionalBinaryStringInfo(StringInfo str, const char *data, int datalen)
{
	if (datalen <= 0)
		return;

	Assert(data != NULL);
	appendBinaryStringInfo(str, data, datalen);
}

void
ts_http_request_init(HttpRequest *req, HttpRequestMethod method)
{
	req->method = method;
}

HttpRequest *
ts_http_request_create(HttpRequestMethod method)
{
	MemoryContext request_context =
		AllocSetContextCreate(CurrentMemoryContext, "Http Request", ALLOCSET_DEFAULT_SIZES);
	MemoryContext old = MemoryContextSwitchTo(request_context);
	HttpRequest *req = palloc0(sizeof(HttpRequest));

	req->context = request_context;
	ts_http_request_init(req, method);

	MemoryContextSwitchTo(old);
	return req;
}

void
ts_http_request_destroy(HttpRequest *req)
{
	MemoryContextDelete(req->context);
}

void
ts_http_request_set_uri(HttpRequest *req, const char *uri)
{
	MemoryContext old = MemoryContextSwitchTo(req->context);
	int uri_len = strlen(uri);

	req->uri = palloc(uri_len + 1);
	memcpy(req->uri, uri, uri_len);
	req->uri[uri_len] = '\0';
	req->uri_len = uri_len;
	MemoryContextSwitchTo(old);
}

void
ts_http_request_set_version(HttpRequest *req, HttpVersion version)
{
	req->version = version;
}

static void
set_header(HttpRequest *req, const char *name, const char *value)
{
	int name_len = strlen(name);
	int value_len = strlen(value);

	req->headers = ts_http_header_create(name, name_len, value, value_len, req->headers);
}

void
ts_http_request_set_header(HttpRequest *req, const char *name, const char *value)
{
	MemoryContext old = MemoryContextSwitchTo(req->context);
	set_header(req, name, value);
	MemoryContextSwitchTo(old);
}

void
ts_http_request_set_body_jsonb(HttpRequest *req, const Jsonb *json)
{
	MemoryContext old = MemoryContextSwitchTo(req->context);
	StringInfo jtext = makeStringInfo();
	char content_length[10];

	JsonbToCString(jtext, (JsonbContainer *) &json->root, VARSIZE(json));
	req->body = jtext->data;
	req->body_len = jtext->len;
	snprintf(content_length, sizeof(content_length), "%d", jtext->len);
	set_header(req, HTTP_CONTENT_TYPE, "application/json");
	set_header(req, HTTP_CONTENT_LENGTH, content_length);
	MemoryContextSwitchTo(old);
}

static void
http_request_serialize_method(HttpRequest *req, StringInfo buf)
{
	const char *method = METHOD_STRING(req->method);

	appendStringInfoString(buf, method);
}

static void
http_request_serialize_version(HttpRequest *req, StringInfo buf)
{
	const char *version = VERSION_STRING(req->version);

	appendStringInfoString(buf, version);
}

static void
http_request_serialize_uri(HttpRequest *req, StringInfo buf)
{
	appendOptionalBinaryStringInfo(buf, req->uri, req->uri_len);
}

static void
http_request_serialize_char(char to_serialize, StringInfo buf)
{
	appendStringInfoChar(buf, to_serialize);
}

static void
http_request_serialize_body(HttpRequest *req, StringInfo buf)
{
	appendOptionalBinaryStringInfo(buf, req->body, req->body_len);
}

static void
http_header_serialize(HttpHeader *header, StringInfo buf)
{
	appendOptionalBinaryStringInfo(buf, header->name, header->name_len);
	http_request_serialize_char(COLON, buf);
	http_request_serialize_char(SPACE, buf);
	appendOptionalBinaryStringInfo(buf, header->value, header->value_len);
}

static int
http_header_get_content_length(HttpHeader *header)
{
	int content_length = -1;

	if (!strncmp(header->name, HTTP_CONTENT_LENGTH, header->name_len))
		sscanf(header->value, "%d", &content_length);
	return content_length;
}

const char *
ts_http_request_build(HttpRequest *req, size_t *buf_size)
{
	/* serialize into this buf, which is allocated on caller's memory context */
	StringInfoData buf;
	HttpHeader *cur_header;
	int content_length = 0;
	bool verified_content_length = false;

	initStringInfo(&buf);

	http_request_serialize_method(req, &buf);
	http_request_serialize_char(SPACE, &buf);

	http_request_serialize_uri(req, &buf);
	http_request_serialize_char(SPACE, &buf);

	http_request_serialize_version(req, &buf);
	http_request_serialize_char(CARRIAGE, &buf);
	http_request_serialize_char(NEW_LINE, &buf);

	cur_header = req->headers;

	while (cur_header != NULL)
	{
		content_length = http_header_get_content_length(cur_header);
		if (content_length != -1)
		{
			/* make sure it's equal to body_len */
			if (content_length != req->body_len)
			{
				return NULL;
			}
			else
				verified_content_length = true;
		}
		http_header_serialize(cur_header, &buf);
		http_request_serialize_char(CARRIAGE, &buf);
		http_request_serialize_char(NEW_LINE, &buf);

		cur_header = cur_header->next;
	}
	http_request_serialize_char(CARRIAGE, &buf);
	http_request_serialize_char(NEW_LINE, &buf);

	if (!verified_content_length)
	{
		/* Then there was no header field for Content-Length */
		if (req->body_len != 0)
		{
			return NULL;
		}
	}

	http_request_serialize_body(req, &buf);
	/* Now everything lives in buf.data */
	if (buf_size != NULL)
		*buf_size = buf.len;
	return buf.data;
}
