/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <access/xact.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <commands/extension.h>
#include <utils/builtins.h>
#include <utils/json.h>
#include <utils/jsonb.h>

#include "config.h"
#include "version.h"
#include "guc.h"
#include "hypertable.h"
#include "compat.h"
#include "telemetry.h"
#include "metadata.h"
#include "extension.h"
#include "net/http.h"

#define TS_VERSION_JSON_FIELD "current_timescaledb_version"

/*  HTTP request details */
#define TIMESCALE_TYPE	"application/json"
#define MAX_REQUEST_SIZE	4096

#define REQ_DB_UUID					"db_uuid"
#define REQ_EXPORTED_DB_UUID		"exported_db_uuid"
#define REQ_INSTALL_TIME			"installed_time"
#define REQ_INSTALL_METHOD			"install_method"
#define REQ_OS						"os_name"
#define REQ_OS_VERSION				"os_version"
#define REQ_OS_RELEASE				"os_release"
#define REQ_PS_VERSION				"postgresql_version"
#define REQ_TS_VERSION				"timescaledb_version"
#define REQ_BUILD_OS				"build_os_name"
#define REQ_BUILD_OS_VERSION		"build_os_version"
#define REQ_DATA_VOLUME				"data_volume"
#define REQ_NUM_HYPERTABLES			"num_hypertables"
#define REQ_RELATED_EXTENSIONS		"related_extensions"

#define PG_PROMETHEUS	"pg_prometheus"
#define POSTGIS			"postgis"

static const char *related_extensions[] = {PG_PROMETHEUS, POSTGIS};

bool
telemetry_parse_version(const char *json, VersionInfo *installed_version, VersionResult *result)
{
	Datum		version = DirectFunctionCall2(json_object_field_text,
											  CStringGetTextDatum(json),
											  PointerGetDatum(cstring_to_text(TS_VERSION_JSON_FIELD)));

	memset(result, 0, sizeof(VersionResult));

	result->versionstr = text_to_cstring(DatumGetTextPP(version));

	result->is_up_to_date = false;

	if (result->versionstr == NULL)
	{
		result->errhint = "no version string in response";
		return false;
	}

	/*
	 * Now parse the version string. We expect format to be
	 * XX.XX.XX-<prerelease_tag>, and if not, we error out
	 */
	if (!version_parse(result->versionstr, &result->vinfo))
	{
		result->errhint = psprintf("parsing failed for version string \"%s\"", result->versionstr);
		return false;
	}

	if (version_cmp(installed_version, &result->vinfo) >= 0)
		result->is_up_to_date = true;

	return true;
}

/*
 * Parse the JSON response from the TS endpoint. There should be a field
 * called "current_timescaledb_version". Check this against the local
 * version, and notify the user if it is behind.
 */
static bool
process_response(const char *json)
{
	VersionInfo installed_version;
	VersionResult result;

	version_get_info(&installed_version);

	if (!telemetry_parse_version(json, &installed_version, &result))
	{
		elog(WARNING, "could not get TimescaleDB version from server response: %s", result.errhint);
		return false;
	}

	if (result.is_up_to_date)
		elog(NOTICE, "the \"%s\" extension is up-to-date", EXTENSION_NAME);
	else
		ereport(LOG,
				(errmsg("the \"%s\" extension is not up-to-date", EXTENSION_NAME),
				 errhint("The most up-to-date version is %s, the installed version is %s",
						 result.versionstr, TIMESCALEDB_VERSION_MOD)));

	return true;
}

static char *
get_num_hypertables()
{
	StringInfo	buf = makeStringInfo();

	appendStringInfo(buf, "%d", number_of_hypertables());
	return buf->data;
}

static char *
get_database_size()
{
	StringInfo	buf = makeStringInfo();
	int64		data_size = DatumGetInt64(DirectFunctionCall1(pg_database_size_oid,
															  ObjectIdGetDatum(MyDatabaseId)));

	appendStringInfo(buf, "" INT64_FORMAT "", data_size);
	return buf->data;
}

static void
jsonb_add_pair(JsonbParseState *state, const char *key, const char *value)
{
	JsonbValue	json_key;
	JsonbValue	json_value;

	/* If there is a null entry, don't add it to the JSON */
	if (value == NULL)
		return;

	json_key.type = jbvString;
	json_key.val.string.val = (char *) key;
	json_key.val.string.len = strlen(key);

	json_value.type = jbvString;
	json_value.val.string.val = (char *) value;
	json_value.val.string.len = strlen(value);

	pushJsonbValue(&state, WJB_KEY, &json_key);
	pushJsonbValue(&state, WJB_VALUE, &json_value);
}

static void
add_related_extensions(JsonbParseState *state)
{
	int			i;

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	for (i = 0; i < sizeof(related_extensions) / sizeof(char *); i++)
	{
		const char *ext = related_extensions[i];

		jsonb_add_pair(state, ext, OidIsValid(get_extension_oid(ext, true)) ? "true" : "false");
	}

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

static StringInfo
build_version_body(void)
{
	JsonbValue	ext_key;
	JsonbValue *result;
	Jsonb	   *jb;
	StringInfo	jtext;
	VersionOSInfo osinfo;
	JsonbParseState *parseState = NULL;

	pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, NULL);

	jsonb_add_pair(parseState, REQ_DB_UUID,
				   DatumGetCString(DirectFunctionCall1(uuid_out, metadata_get_uuid())));
	jsonb_add_pair(parseState, REQ_EXPORTED_DB_UUID,
				   DatumGetCString(DirectFunctionCall1(uuid_out, metadata_get_exported_uuid())));
	jsonb_add_pair(parseState, REQ_INSTALL_TIME,
				   DatumGetCString(DirectFunctionCall1(timestamptz_out, metadata_get_install_timestamp())));

	jsonb_add_pair(parseState, REQ_INSTALL_METHOD, TIMESCALEDB_INSTALL_METHOD);

	if (version_get_os_info(&osinfo))
	{
		jsonb_add_pair(parseState, REQ_OS, osinfo.sysname);
		jsonb_add_pair(parseState, REQ_OS_VERSION, osinfo.version);
		jsonb_add_pair(parseState, REQ_OS_RELEASE, osinfo.release);
	}
	else
		jsonb_add_pair(parseState, REQ_OS, "Unknown");

	/*
	 * PACKAGE_VERSION does not include extra details that some systems (e.g.,
	 * Ubuntu) sometimes include in PG_VERSION
	 */
	jsonb_add_pair(parseState, REQ_PS_VERSION, PACKAGE_VERSION);
	jsonb_add_pair(parseState, REQ_TS_VERSION, TIMESCALEDB_VERSION_MOD);
	jsonb_add_pair(parseState, REQ_BUILD_OS, BUILD_OS_NAME);
	jsonb_add_pair(parseState, REQ_BUILD_OS_VERSION, BUILD_OS_VERSION);
	jsonb_add_pair(parseState, REQ_DATA_VOLUME, get_database_size());
	jsonb_add_pair(parseState, REQ_NUM_HYPERTABLES, get_num_hypertables());

	/* Add related extensions, which is a nested JSON */
	ext_key.type = jbvString;
	ext_key.val.string.val = REQ_RELATED_EXTENSIONS;
	ext_key.val.string.len = strlen(REQ_RELATED_EXTENSIONS);
	pushJsonbValue(&parseState, WJB_KEY, &ext_key);
	add_related_extensions(parseState);

	result = pushJsonbValue(&parseState, WJB_END_OBJECT, NULL);

	jb = JsonbValueToJsonb(result);
	jtext = makeStringInfo();
	JsonbToCString(jtext, &jb->root, VARSIZE(jb));

	return jtext;
}

HttpRequest *
build_version_request(const char *host, const char *path)
{
	char		body_len_string[5];
	HttpRequest *req;
	StringInfo	jtext = build_version_body();

	snprintf(body_len_string, 5, "%d", jtext->len);

	/* Fill in HTTP request */
	req = http_request_create(HTTP_POST);

	http_request_set_uri(req, path);
	http_request_set_version(req, HTTP_VERSION_10);
	http_request_set_header(req, HTTP_CONTENT_TYPE, TIMESCALE_TYPE);
	http_request_set_header(req, HTTP_CONTENT_LENGTH, body_len_string);
	http_request_set_header(req, HTTP_HOST, host);
	http_request_set_body(req, jtext->data, jtext->len);

	return req;
}

Connection *
telemetry_connect(const char *host, const char *service)
{
	Connection *conn = NULL;
	int			ret;

	if (strcmp("http", service) == 0)
		conn = connection_create(CONNECTION_PLAIN);
	else if (strcmp("https", service) == 0)
		conn = connection_create(CONNECTION_SSL);
	else
		ereport(WARNING,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("scheme \"%s\" not supported for telemetry",
						service)));

	if (conn == NULL)
		return NULL;

	ret = connection_connect(conn, host, service, 0);

	if (ret < 0)
	{
		const char *errstr = connection_get_and_clear_error(conn);

		connection_destroy(conn);
		conn = NULL;

		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("telemetry could not connect to \"%s\"", host),
				 errdetail("%s", errstr)));
	}

	return conn;
}

bool
telemetry_main_wrapper()
{
	return telemetry_main(TELEMETRY_HOST, TELEMETRY_PATH, TELEMETRY_SCHEME);
}

bool
telemetry_main(const char *host, const char *path, const char *service)
{
	HttpError	err;
	Connection *conn;
	HttpRequest *req;
	HttpResponseState *rsp;
	bool		started = false;

	if (!telemetry_on())
		return true;

	if (!IsTransactionOrTransactionBlock())
	{
		started = true;
		StartTransactionCommand();
	}

	conn = telemetry_connect(host, service);

	if (conn == NULL)
		goto cleanup;

	req = build_version_request(host, path);

	rsp = http_response_state_create();

	err = http_send_and_recv(conn, req, rsp);

	http_request_destroy(req);
	connection_destroy(conn);

	if (err != HTTP_ERROR_NONE)
	{
		elog(WARNING, "telemetry error: %s", http_strerror(err));
		goto cleanup;
	}

	if (!http_response_state_valid_status(rsp))
	{
		elog(WARNING, "telemetry got unexpected HTTP response status: %d",
			 http_response_state_status_code(rsp));
		goto cleanup;
	}

	/*
	 * Do the version-check. Response is the body of a well-formed HTTP
	 * response, since otherwise the previous line will throw an error.
	 */
	process_response(http_response_state_body_start(rsp));

	http_response_state_destroy(rsp);

	if (started)
		CommitTransactionCommand();
	return true;

cleanup:
	if (started)
		AbortCurrentTransaction();
	return false;
}

TS_FUNCTION_INFO_V1(ts_get_telemetry_report);

Datum
ts_get_telemetry_report(PG_FUNCTION_ARGS)
{
	StringInfo	request = build_version_body();

	return CStringGetTextDatum(request->data);
}
