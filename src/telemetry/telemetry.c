/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <commands/extension.h>
#include <catalog/pg_collation.h>
#include <utils/builtins.h>
#include <utils/json.h>
#include <utils/jsonb.h>

#include "compat.h"
#include "config.h"
#include "version.h"
#include "guc.h"
#include "telemetry.h"
#include "telemetry_metadata.h"
#include "hypertable.h"
#include "extension.h"
#include "net/http.h"
#include "jsonb_utils.h"
#include "license_guc.h"
#include "bgw_policy/drop_chunks.h"
#include "bgw_policy/reorder.h"
#include "compression_chunk_size.h"

#include "cross_module_fn.h"

#define TS_VERSION_JSON_FIELD "current_timescaledb_version"
#define TS_IS_UPTODATE_JSON_FIELD "is_up_to_date"

/*  HTTP request details */
#define TIMESCALE_TYPE "application/json"
#define MAX_REQUEST_SIZE 4096

#define REQ_DB_UUID "db_uuid"
#define REQ_EXPORTED_DB_UUID "exported_db_uuid"
#define REQ_INSTALL_TIME "installed_time"
#define REQ_INSTALL_METHOD "install_method"
#define REQ_OS "os_name"
#define REQ_OS_VERSION "os_version"
#define REQ_OS_RELEASE "os_release"
#define REQ_OS_VERSION_PRETTY "os_name_pretty"
#define REQ_PS_VERSION "postgresql_version"
#define REQ_TS_VERSION "timescaledb_version"
#define REQ_BUILD_OS "build_os_name"
#define REQ_BUILD_OS_VERSION "build_os_version"
#define REQ_BUILD_ARCHITECTURE_BIT_SIZE "build_architecture_bit_size"
#define REQ_BUILD_ARCHITECTURE "build_architecture"
#define REQ_DATA_VOLUME "data_volume"
#define REQ_NUM_HYPERTABLES "num_hypertables"
#define REQ_NUM_COMPRESSED_HYPERTABLES "num_compressed_hypertables"
#define REQ_NUM_CONTINUOUS_AGGS "num_continuous_aggs"
#define REQ_NUM_REORDER_POLICIES "num_reorder_policies"
#define REQ_NUM_DROP_CHUNKS_POLICIES "num_drop_chunks_policies"
#define REQ_RELATED_EXTENSIONS "related_extensions"
#define REQ_METADATA "db_metadata"
#define REQ_LICENSE_INFO "license"
#define REQ_LICENSE_EDITION "edition"
#define REQ_LICENSE_EDITION_APACHE "apache_only"
#define REQ_TS_LAST_TUNE_TIME "last_tuned_time"
#define REQ_TS_LAST_TUNE_VERSION "last_tuned_version"
#define REQ_INSTANCE_METADATA "instance_metadata"
#define REQ_TS_TELEMETRY_CLOUD "cloud"

#define PG_PROMETHEUS "pg_prometheus"
#define POSTGIS "postgis"

#define REQ_COMPRESSED_HEAP_SIZE "compressed_heap_size"
#define REQ_COMPRESSED_INDEX_SIZE "compressed_index_size"
#define REQ_COMPRESSED_TOAST_SIZE "compressed_toast_size"
#define REQ_UNCOMPRESSED_HEAP_SIZE "uncompressed_heap_size"
#define REQ_UNCOMPRESSED_INDEX_SIZE "uncompressed_index_size"
#define REQ_UNCOMPRESSED_TOAST_SIZE "uncompressed_toast_size"

#define TS_TELEMETRY_REPORT_OVERRIDE_ARG "always_display_report := true"

static const char *related_extensions[] = { PG_PROMETHEUS, POSTGIS };

static bool
char_in_valid_version_digits(const char c)
{
	switch (c)
	{
		case '.':
		case '-':
			return true;
		default:
			return false;
	}
}

/*
 * Makes sure the server version string is less than MAX_VERSION_STR_LEN
 * chars, and all digits are "valid". Valid chars are either
 * alphanumeric or in the array valid_version_digits above.
 *
 * Returns false if either of these conditions are false.
 */
bool
ts_validate_server_version(const char *json, VersionResult *result)
{
	int i;
	Datum version = DirectFunctionCall2(json_object_field_text,
										CStringGetTextDatum(json),
										PointerGetDatum(cstring_to_text(TS_VERSION_JSON_FIELD)));

	memset(result, 0, sizeof(VersionResult));

	result->versionstr = text_to_cstring(DatumGetTextPP(version));

	if (result->versionstr == NULL)
	{
		result->errhint = "no version string in response";
		return false;
	}

	if (strlen(result->versionstr) > MAX_VERSION_STR_LEN)
	{
		result->errhint = "version string is too long";
		return false;
	}

	for (i = 0; i < strlen(result->versionstr); i++)
	{
		if (!isalpha(result->versionstr[i]) && !isdigit(result->versionstr[i]) &&
			!char_in_valid_version_digits(result->versionstr[i]))
		{
			result->errhint = "version string has invalid characters";
			return false;
		}
	}

	return true;
}

/*
 * Parse the JSON response from the TS endpoint. There should be a field
 * called "current_timescaledb_version". Check this against the local
 * version, and notify the user if it is behind.
 */
void
ts_check_version_response(const char *json)
{
	VersionResult result;
	bool is_uptodate = DatumGetBool(
		DirectFunctionCall2Coll(texteq,
								C_COLLATION_OID,
								DirectFunctionCall2Coll(json_object_field_text,
														C_COLLATION_OID,
														CStringGetTextDatum(json),
														PointerGetDatum(cstring_to_text(
															TS_IS_UPTODATE_JSON_FIELD))),
								PointerGetDatum(cstring_to_text("true"))));

	if (is_uptodate)
		elog(NOTICE, "the \"%s\" extension is up-to-date", EXTENSION_NAME);
	else
	{
		if (!ts_validate_server_version(json, &result))
		{
			elog(WARNING, "server did not return a valid TimescaleDB version: %s", result.errhint);
			return;
		}

		ereport(LOG,
				(errmsg("the \"%s\" extension is not up-to-date", EXTENSION_NAME),
				 errhint("The most up-to-date version is %s, the installed version is %s",
						 result.versionstr,
						 TIMESCALEDB_VERSION_MOD)));
	}
}

static char *
get_size(int64 size)
{
	StringInfo buf = makeStringInfo();

	appendStringInfo(buf, INT64_FORMAT, size);
	return buf->data;
}

static char *
get_num_hypertables()
{
	StringInfo buf = makeStringInfo();

	appendStringInfo(buf, "%d", ts_number_of_user_hypertables());
	return buf->data;
}

static char *
get_num_compressed_hypertables()
{
	StringInfo buf = makeStringInfo();

	appendStringInfo(buf, "%d", ts_number_compressed_hypertables());
	return buf->data;
}

static char *
get_architecture_bit_size()
{
	StringInfo buf = makeStringInfo();

	appendStringInfo(buf, "%d", BUILD_POINTER_BYTES * 8);
	return buf->data;
}

static char *
get_num_continuous_aggs()
{
	StringInfo buf = makeStringInfo();

	appendStringInfo(buf, "%d", ts_number_of_continuous_aggs());
	return buf->data;
}

static char *
get_num_drop_chunks_policies()
{
	StringInfo buf = makeStringInfo();

	appendStringInfo(buf, "%d", ts_bgw_policy_drop_chunks_count());
	return buf->data;
}

static char *
get_num_reorder_policies()
{
	StringInfo buf = makeStringInfo();

	appendStringInfo(buf, "%d", ts_bgw_policy_reorder_count());
	return buf->data;
}

static char *
get_database_size()
{
	StringInfo buf = makeStringInfo();
	int64 data_size =
		DatumGetInt64(DirectFunctionCall1(pg_database_size_oid, ObjectIdGetDatum(MyDatabaseId)));

	appendStringInfo(buf, "" INT64_FORMAT "", data_size);
	return buf->data;
}

static void
add_related_extensions(JsonbParseState *state)
{
	int i;

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	for (i = 0; i < sizeof(related_extensions) / sizeof(char *); i++)
	{
		const char *ext = related_extensions[i];

		ts_jsonb_add_str(state, ext, OidIsValid(get_extension_oid(ext, true)) ? "true" : "false");
	}

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

static void
add_license_info(JsonbParseState *state)
{
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	if (TS_CURRENT_LICENSE_IS_APACHE_ONLY())
		ts_jsonb_add_str(state, REQ_LICENSE_EDITION, REQ_LICENSE_EDITION_APACHE);
	else
		ts_cm_functions->add_tsl_license_info_telemetry(state);

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

static char *
get_pgversion_string()
{
	StringInfo buf = makeStringInfo();
	int major, minor, patch;

	/*
	 * We have to read the server version from GUC and not use any of
	 * the macros. By using any of the macros we would get the version
	 * the extension is compiled against instead of the version actually
	 * running.
	 */
	char *server_version_num_guc = GetConfigOptionByName("server_version_num", NULL, false);
	long server_version_num = strtol(server_version_num_guc, NULL, 10);

	major = server_version_num / 10000;
	minor = (server_version_num / 100) % 100;
	patch = server_version_num % 100;

	if (server_version_num < 100000)
	{
		Assert(major == 9 && minor == 6);
		appendStringInfo(buf, "%d.%d.%d", major, minor, patch);
	}
	else
	{
		Assert(major >= 10 && minor == 0);
		appendStringInfo(buf, "%d.%d", major, patch);
	}

	return buf->data;
}

static StringInfo
build_version_body(void)
{
	JsonbValue ext_key;
	JsonbValue license_info_key;
	JsonbValue *result;
	Jsonb *jb;
	StringInfo jtext;
	VersionOSInfo osinfo;
	JsonbParseState *parseState = NULL;
	TotalSizes sizes = ts_compression_chunk_size_totals();

	pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, NULL);

	ts_jsonb_add_str(parseState,
					 REQ_DB_UUID,
					 DatumGetCString(
						 DirectFunctionCall1(uuid_out, ts_telemetry_metadata_get_uuid())));
	ts_jsonb_add_str(parseState,
					 REQ_EXPORTED_DB_UUID,
					 DatumGetCString(
						 DirectFunctionCall1(uuid_out, ts_telemetry_metadata_get_exported_uuid())));
	ts_jsonb_add_str(parseState,
					 REQ_INSTALL_TIME,
					 DatumGetCString(
						 DirectFunctionCall1(timestamptz_out,
											 ts_telemetry_metadata_get_install_timestamp())));

	ts_jsonb_add_str(parseState, REQ_INSTALL_METHOD, TIMESCALEDB_INSTALL_METHOD);

	if (ts_version_get_os_info(&osinfo))
	{
		ts_jsonb_add_str(parseState, REQ_OS, osinfo.sysname);
		ts_jsonb_add_str(parseState, REQ_OS_VERSION, osinfo.version);
		ts_jsonb_add_str(parseState, REQ_OS_RELEASE, osinfo.release);
		if (osinfo.has_pretty_version)
			ts_jsonb_add_str(parseState, REQ_OS_VERSION_PRETTY, osinfo.pretty_version);
	}
	else
		ts_jsonb_add_str(parseState, REQ_OS, "Unknown");

	ts_jsonb_add_str(parseState, REQ_PS_VERSION, get_pgversion_string());
	ts_jsonb_add_str(parseState, REQ_TS_VERSION, TIMESCALEDB_VERSION_MOD);
	ts_jsonb_add_str(parseState, REQ_BUILD_OS, BUILD_OS_NAME);
	ts_jsonb_add_str(parseState, REQ_BUILD_OS_VERSION, BUILD_OS_VERSION);
	ts_jsonb_add_str(parseState, REQ_BUILD_ARCHITECTURE, BUILD_PROCESSOR);
	ts_jsonb_add_str(parseState, REQ_BUILD_ARCHITECTURE_BIT_SIZE, get_architecture_bit_size());
	ts_jsonb_add_str(parseState, REQ_DATA_VOLUME, get_database_size());
	ts_jsonb_add_str(parseState, REQ_NUM_HYPERTABLES, get_num_hypertables());
	ts_jsonb_add_str(parseState, REQ_NUM_COMPRESSED_HYPERTABLES, get_num_compressed_hypertables());
	ts_jsonb_add_str(parseState, REQ_NUM_CONTINUOUS_AGGS, get_num_continuous_aggs());
	ts_jsonb_add_str(parseState, REQ_NUM_REORDER_POLICIES, get_num_reorder_policies());
	ts_jsonb_add_str(parseState, REQ_NUM_DROP_CHUNKS_POLICIES, get_num_drop_chunks_policies());

	ts_jsonb_add_str(parseState, REQ_COMPRESSED_HEAP_SIZE, get_size(sizes.compressed_heap_size));
	ts_jsonb_add_str(parseState, REQ_COMPRESSED_INDEX_SIZE, get_size(sizes.compressed_index_size));
	ts_jsonb_add_str(parseState, REQ_COMPRESSED_TOAST_SIZE, get_size(sizes.compressed_toast_size));
	ts_jsonb_add_str(parseState,
					 REQ_UNCOMPRESSED_HEAP_SIZE,
					 get_size(sizes.uncompressed_heap_size));
	ts_jsonb_add_str(parseState,
					 REQ_UNCOMPRESSED_INDEX_SIZE,
					 get_size(sizes.uncompressed_index_size));
	ts_jsonb_add_str(parseState,
					 REQ_UNCOMPRESSED_TOAST_SIZE,
					 get_size(sizes.uncompressed_toast_size));

	/* Add related extensions, which is a nested JSON */
	ext_key.type = jbvString;
	ext_key.val.string.val = REQ_RELATED_EXTENSIONS;
	ext_key.val.string.len = strlen(REQ_RELATED_EXTENSIONS);
	pushJsonbValue(&parseState, WJB_KEY, &ext_key);
	add_related_extensions(parseState);

	/* add license info, which is a nested JSON */
	license_info_key.type = jbvString;
	license_info_key.val.string.val = REQ_LICENSE_INFO;
	license_info_key.val.string.len = strlen(REQ_LICENSE_INFO);
	pushJsonbValue(&parseState, WJB_KEY, &license_info_key);
	add_license_info(parseState);

	/* add tuned info, which is optional */
	if (ts_last_tune_time != NULL)
		ts_jsonb_add_str(parseState, REQ_TS_LAST_TUNE_TIME, ts_last_tune_time);

	if (ts_last_tune_version != NULL)
		ts_jsonb_add_str(parseState, REQ_TS_LAST_TUNE_VERSION, ts_last_tune_version);

	/* add cloud to telemetry when set */
	if (ts_telemetry_cloud != NULL)
	{
		ext_key.type = jbvString;
		ext_key.val.string.val = REQ_INSTANCE_METADATA;
		ext_key.val.string.len = strlen(REQ_INSTANCE_METADATA);
		pushJsonbValue(&parseState, WJB_KEY, &ext_key);

		pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, NULL);
		ts_jsonb_add_str(parseState, REQ_TS_TELEMETRY_CLOUD, ts_telemetry_cloud);
		pushJsonbValue(&parseState, WJB_END_OBJECT, NULL);
	}

	/* Add additional content from metadata */
	ext_key.type = jbvString;
	ext_key.val.string.val = REQ_METADATA;
	ext_key.val.string.len = strlen(REQ_METADATA);
	pushJsonbValue(&parseState, WJB_KEY, &ext_key);
	pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, NULL);
	ts_telemetry_metadata_add_values(parseState);
	pushJsonbValue(&parseState, WJB_END_OBJECT, NULL);

	/* end of telemetry object */
	result = pushJsonbValue(&parseState, WJB_END_OBJECT, NULL);
	jb = JsonbValueToJsonb(result);
	jtext = makeStringInfo();
	JsonbToCString(jtext, &jb->root, VARSIZE(jb));

	return jtext;
}

HttpRequest *
ts_build_version_request(const char *host, const char *path)
{
	char body_len_string[5];
	HttpRequest *req;
	StringInfo jtext = build_version_body();

	snprintf(body_len_string, 5, "%d", jtext->len);

	/* Fill in HTTP request */
	req = ts_http_request_create(HTTP_POST);

	ts_http_request_set_uri(req, path);
	ts_http_request_set_version(req, HTTP_VERSION_10);
	ts_http_request_set_header(req, HTTP_CONTENT_TYPE, TIMESCALE_TYPE);
	ts_http_request_set_header(req, HTTP_CONTENT_LENGTH, body_len_string);
	ts_http_request_set_header(req, HTTP_HOST, host);
	ts_http_request_set_body(req, jtext->data, jtext->len);

	return req;
}

Connection *
ts_telemetry_connect(const char *host, const char *service)
{
	Connection *conn = NULL;
	int ret;

	if (strcmp("http", service) == 0)
		conn = ts_connection_create(CONNECTION_PLAIN);
	else if (strcmp("https", service) == 0)
		conn = ts_connection_create(CONNECTION_SSL);
	else
		ereport(WARNING,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("scheme \"%s\" not supported for telemetry", service)));

	if (conn == NULL)
		return NULL;

	ret = ts_connection_connect(conn, host, service, 0);

	if (ret < 0)
	{
		const char *errstr = ts_connection_get_and_clear_error(conn);

		ts_connection_destroy(conn);
		conn = NULL;

		ereport(WARNING,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("telemetry could not connect to \"%s\"", host),
				 errdetail("%s", errstr)));
	}

	return conn;
}

bool
ts_telemetry_main_wrapper()
{
	return ts_telemetry_main(TELEMETRY_HOST, TELEMETRY_PATH, TELEMETRY_SCHEME);
}

bool
ts_telemetry_main(const char *host, const char *path, const char *service)
{
	HttpError err;
	Connection *conn;
	HttpRequest *req;
	HttpResponseState *rsp;
	bool started = false;
	const char *volatile json = NULL;

	if (!ts_telemetry_on())
		return true;

	if (!IsTransactionOrTransactionBlock())
	{
		started = true;
		StartTransactionCommand();
	}

	conn = ts_telemetry_connect(host, service);

	if (conn == NULL)
		goto cleanup;

	req = ts_build_version_request(host, path);

	rsp = ts_http_response_state_create();

	err = ts_http_send_and_recv(conn, req, rsp);

	ts_http_request_destroy(req);
	ts_connection_destroy(conn);

	if (err != HTTP_ERROR_NONE)
	{
		elog(WARNING, "telemetry error: %s", ts_http_strerror(err));
		goto cleanup;
	}

	if (!ts_http_response_state_valid_status(rsp))
	{
		elog(WARNING,
			 "telemetry got unexpected HTTP response status: %d",
			 ts_http_response_state_status_code(rsp));
		goto cleanup;
	}

	/*
	 * Do the version-check. Response is the body of a well-formed HTTP
	 * response, since otherwise the previous line will throw an error.
	 */
	PG_TRY();
	{
		json = ts_http_response_state_body_start(rsp);
		ts_check_version_response(json);
	}
	PG_CATCH();
	{
		/* If the response is malformed, ts_check_version_response() will
		 * throw an error, so we capture the error here and print debugging
		 * information before re-throwing the error. */
		ereport(NOTICE,
				(errmsg("malformed telemetry response body"),
				 errdetail("host=%s, service=%s, path=%s: %s",
						   host,
						   service,
						   path,
						   json ? json : "<EMPTY>")));
		PG_RE_THROW();
	}
	PG_END_TRY();

	ts_http_response_state_destroy(rsp);

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
	StringInfo request;

	/* Show error message if telemetry is disabled and no override argument is passed */
	if (!ts_telemetry_on())
	{
		if (PG_NARGS() == 1 && (PG_ARGISNULL(0) || PG_GETARG_BOOL(0) == false))
		{
			elog(INFO,
				 "Telemetry is disabled. Call get_telemetry_report(%s) to view the report locally.",
				 TS_TELEMETRY_REPORT_OVERRIDE_ARG);
			PG_RETURN_NULL();
		}
	}

	request = build_version_body();

	return CStringGetTextDatum(request->data);
}
