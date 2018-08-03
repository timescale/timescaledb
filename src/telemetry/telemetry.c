#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <postgres.h>
#include <access/xact.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <commands/extension.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <utils/snapmgr.h>
#include <utils/fmgrprotos.h>

#include "compat.h"
#include "guc.h"
#include "telemetry.h"
#include "uuid.h"
#include "hypertable.h"
#include "net/utils.h"

#ifndef WIN32
#include <sys/utsname.h>
#endif

#define TS_VERSION_JSON_FIELD "current_timescaledb_version"

/*  HTTP request details */
#define TIMESCALE_URI	"/v1/metrics"
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
static const char *related_extensions[] = { PG_PROMETHEUS, POSTGIS };

static const char *version_delimiter[3] = { ".", ".", ""};

char *get_guc_endpoint_hostname() {
	char *endpoint = palloc(strlen(guc_telemetry_endpoint) + 1);
	strncpy(endpoint, guc_telemetry_endpoint, strlen(guc_telemetry_endpoint));
	endpoint[strlen(guc_telemetry_endpoint)] = '\0';

	char *protocol = strtok(endpoint, "/");
	char *hostname = strtok(NULL, "/");

	if (protocol == NULL || hostname == NULL) 
		return NULL;
	return hostname;
}

int get_guc_endpoint_port() {
	char *endpoint = palloc(strlen(guc_telemetry_endpoint) + 1);
	strncpy(endpoint, guc_telemetry_endpoint, strlen(guc_telemetry_endpoint));
	endpoint[strlen(guc_telemetry_endpoint)] = '\0';

	char *protocol = strtok(endpoint, ":");
	if (protocol == NULL)
		return -1;
	if (strcmp(protocol, "http"))
		return 80;
	else if (strcmp(protocol, "https"))
		return 443;
	return -1;
}

/*
 * Parse the JSON response from the TS endpoint. There should be a field
 * called "current_timescaledb_version". Check this against the local
 * version, and notify the user if it is behind.
 */
static void
process_response(char *endpoint_response)
{
	int i;
	char *curr_sub_version;
	long curr_sub_version_long;
	long local_version[3] = {
		strtol(TIMESCALEDB_MAJOR_VERSION, NULL, 10),
		strtol(TIMESCALEDB_MINOR_VERSION, NULL, 10),
		strtol(TIMESCALEDB_PATCH_VERSION, NULL, 10) };

	char	   *version_string = text_to_cstring(DatumGetTextPP(
		DirectFunctionCall2(json_object_field_text,
		CStringGetTextDatum(endpoint_response),
		PointerGetDatum(cstring_to_text(TS_VERSION_JSON_FIELD)))));

	if (version_string == NULL)
		elog(ERROR, "could not get TimescaleDB version from server response");
	else
	{
		/*
		 * Now parse the version string. We expect format to be XX.XX.XX, and
		 * if not, we error out
		 */
		for (i = 0; i < 3; i++) {
			curr_sub_version = strtok(i == 0 ? version_string : NULL, version_delimiter[i]);
			if (curr_sub_version == NULL)
				elog(ERROR, "ill-formatted TimescaleDB version from server response");

			curr_sub_version_long = strtol(curr_sub_version, NULL, 10);

			if (local_version[i] < curr_sub_version_long)
			{
				ereport(LOG, (errmsg("you are not running the most up-to-date version of TimescaleDB."),
						errhint("The most up-to-date version is %s, your version is %s", version_string, TIMESCALEDB_VERSION_MOD)));
				return;
			}
			if (local_version[i] > curr_sub_version_long)
				break;
		}
		/* Put the successful version check in a lower logging level to avoid clogging logs. */
		elog(NOTICE, "you are running the most up-to-date version of TimescaleDB.");
	}
}

static char *
get_num_hypertables()
{
	StringInfo buf = makeStringInfo();
	
	appendStringInfo(buf, "%d", number_of_hypertables());
	return buf->data;
}

static char *
get_database_size()
{
	StringInfoData buf;
	int64		data_size = DatumGetInt64(DirectFunctionCall1(pg_database_size_oid, ObjectIdGetDatum(MyDatabaseId)));

	initStringInfo(&buf);
	appendStringInfo(&buf, "" INT64_FORMAT "", data_size);
	return buf.data;
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
	int i;
	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	for (i = 0; i < sizeof(related_extensions) / sizeof(char *); i++) {
		const char *ext = related_extensions[i];
		jsonb_add_pair(state, ext, OidIsValid(get_extension_oid(ext, true)) ? "true" : "false");
	}

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

static StringInfo
build_version_body(void)
{
#ifndef WIN32
	/* Get the OS name  */
	struct utsname os_info;

	uname(&os_info);
#endif
	JsonbValue	ext_key;
	JsonbValue *result;
	Jsonb	   *jb;
	StringInfo	jtext;
	JsonbParseState *parseState = NULL;
	
	pushJsonbValue(&parseState, WJB_BEGIN_OBJECT, NULL);
	jsonb_add_pair(parseState, REQ_DB_UUID, 
		DatumGetCString(DirectFunctionCall1(uuid_out, UUIDPGetDatum(get_uuid()))));
	jsonb_add_pair(parseState, REQ_EXPORTED_DB_UUID,
		DatumGetCString(DirectFunctionCall1(uuid_out, UUIDPGetDatum(get_exported_uuid()))));
	jsonb_add_pair(parseState, REQ_INSTALL_TIME, get_install_timestamp());
	jsonb_add_pair(parseState, REQ_INSTALL_METHOD, TIMESCALEDB_INSTALL_METHOD);

#ifndef WIN32
	jsonb_add_pair(parseState, REQ_OS, os_info.sysname);
	jsonb_add_pair(parseState, REQ_OS_VERSION, os_info.version);
	jsonb_add_pair(parseState, REQ_OS_RELEASE, os_info.release);
#elif WIN32
	jsonb_add_pair(parseState, REQ_OS, "Windows");
#endif
	jsonb_add_pair(parseState, REQ_PS_VERSION, PG_VERSION);
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
build_version_request(void)
{
	char		body_len_string[5];
	HttpRequest *req;
	StringInfo	jtext = build_version_body();

	snprintf(body_len_string, 5, "%d", jtext->len);

	/* Fill in HTTP request */
	req = http_request_create(HTTP_POST);

	http_request_set_uri(req, TIMESCALE_URI);
	http_request_set_version(req, HTTP_10);
	http_request_set_header(req, HTTP_CONTENT_TYPE, TIMESCALE_TYPE);
	http_request_set_header(req, HTTP_CONTENT_LENGTH, body_len_string);
	http_request_set_header(req, HTTP_HOST, get_guc_endpoint_hostname());
	http_request_set_body(req, jtext->data, jtext->len);

	return req;
}

void
telemetry_main()
{
	int			ret;
	char	   *response;
	Connection *conn;

	if (!telemetry_on())
		return;

	conn = connection_create(CONNECTION_SSL);
	if (conn == NULL)
		elog(ERROR, "could not create an SSL connection");
	ret = connection_connect(conn, get_guc_endpoint_hostname(), HTTPS_PORT);
	if (ret < 0)
		elog(ERROR, "could not make a connection to %s:%d", guc_telemetry_endpoint, HTTPS_PORT);
		
	response = send_and_recv_http(conn, build_version_request());
	/*
     * Do the version-check. Response is the body of a well-formed HTTP response, since
	 * otherwise the previous line will throw an error.
     */
	process_response(response);
	connection_close(conn);
	connection_destroy(conn);
	return;
}

TS_FUNCTION_INFO_V1(ts_get_telemetry_report);

Datum
ts_get_telemetry_report(PG_FUNCTION_ARGS)
{
	StringInfo	request = build_version_body();

	return CStringGetTextDatum(request->data);
}
