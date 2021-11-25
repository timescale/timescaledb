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

#include "compat/compat.h"
#include "config.h"
#include "version.h"
#include "guc.h"
#include "telemetry.h"
#include "ts_catalog/metadata.h"
#include "telemetry_metadata.h"
#include "hypertable.h"
#include "extension.h"
#include "net/http.h"
#include "jsonb_utils.h"
#include "license_guc.h"
#include "bgw_policy/policy.h"
#include "ts_catalog/compression_chunk_size.h"
#include "stats.h"

#include "cross_module_fn.h"

#define TS_TELEMETRY_VERSION 2
#define TS_VERSION_JSON_FIELD "current_timescaledb_version"
#define TS_IS_UPTODATE_JSON_FIELD "is_up_to_date"

/*  HTTP request details */
#define MAX_REQUEST_SIZE 4096

#define REQ_TELEMETRY_VERSION "telemetry_version"
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

#define REQ_NUM_POLICY_CAGG "num_continuous_aggs_policies"
#define REQ_NUM_POLICY_COMPRESSION "num_compression_policies"
#define REQ_NUM_POLICY_REORDER "num_reorder_policies"
#define REQ_NUM_POLICY_RETENTION "num_retention_policies"
#define REQ_NUM_USER_DEFINED_ACTIONS "num_user_defined_actions"
#define REQ_RELATED_EXTENSIONS "related_extensions"
#define REQ_METADATA "db_metadata"
#define REQ_LICENSE_EDITION_APACHE "apache_only"
#define REQ_LICENSE_EDITION_COMMUNITY "community"
#define REQ_TS_LAST_TUNE_TIME "last_tuned_time"
#define REQ_TS_LAST_TUNE_VERSION "last_tuned_version"
#define REQ_INSTANCE_METADATA "instance_metadata"
#define REQ_TS_TELEMETRY_CLOUD "cloud"

#define PG_PROMETHEUS "pg_prometheus"
#define PROMSCALE "promscale"
#define POSTGIS "postgis"
#define TIMESCALE_ANALYTICS "timescale_analytics"
#define TIMESCALEDB_TOOLKIT "timescaledb_toolkit"

static const char *related_extensions[] = {
	PG_PROMETHEUS, PROMSCALE, POSTGIS, TIMESCALE_ANALYTICS, TIMESCALEDB_TOOLKIT,
};

/* This function counts background worker jobs by type. */
static BgwJobTypeCount
bgw_job_type_counts()
{
	ListCell *lc;
	List *jobs = ts_bgw_job_get_all(sizeof(BgwJob), CurrentMemoryContext);
	BgwJobTypeCount counts = { 0 };

	foreach (lc, jobs)
	{
		BgwJob *job = lfirst(lc);

		if (namestrcmp(&job->fd.proc_schema, INTERNAL_SCHEMA_NAME) == 0)
		{
			if (namestrcmp(&job->fd.proc_name, "policy_refresh_continuous_aggregate") == 0)
				counts.policy_cagg++;
			else if (namestrcmp(&job->fd.proc_name, "policy_compression") == 0)
				counts.policy_compression++;
			else if (namestrcmp(&job->fd.proc_name, "policy_reorder") == 0)
				counts.policy_reorder++;
			else if (namestrcmp(&job->fd.proc_name, "policy_retention") == 0)
				counts.policy_retention++;
			else if (namestrcmp(&job->fd.proc_name, "policy_telemetry") == 0)
				counts.policy_telemetry++;
		}
		else
		{
			counts.user_defined_action++;
		}
	}

	return counts;
}

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
				 errhint("The most up-to-date version is %s, the installed version is %s.",
						 result.versionstr,
						 TIMESCALEDB_VERSION_MOD)));
	}
}

static int32
get_architecture_bit_size()
{
	return BUILD_POINTER_BYTES * 8;
}

static void
add_job_counts(JsonbParseState *state)
{
	BgwJobTypeCount counts = bgw_job_type_counts();

	ts_jsonb_add_int32(state, REQ_NUM_POLICY_CAGG, counts.policy_cagg);
	ts_jsonb_add_int32(state, REQ_NUM_POLICY_COMPRESSION, counts.policy_compression);
	ts_jsonb_add_int32(state, REQ_NUM_POLICY_REORDER, counts.policy_reorder);
	ts_jsonb_add_int32(state, REQ_NUM_POLICY_RETENTION, counts.policy_retention);
	ts_jsonb_add_int32(state, REQ_NUM_USER_DEFINED_ACTIONS, counts.user_defined_action);
}

static int64
get_database_size()
{
	return DatumGetInt64(DirectFunctionCall1(pg_database_size_oid, ObjectIdGetDatum(MyDatabaseId)));
}

static void
add_related_extensions(JsonbParseState *state)
{
	int i;

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	for (i = 0; i < sizeof(related_extensions) / sizeof(char *); i++)
	{
		const char *ext = related_extensions[i];

		ts_jsonb_add_bool(state, ext, OidIsValid(get_extension_oid(ext, true)));
	}

	pushJsonbValue(&state, WJB_END_OBJECT, NULL);
}

static char *
get_pgversion_string()
{
	StringInfo buf = makeStringInfo();
	int major, patch;

	/*
	 * We have to read the server version from GUC and not use any of
	 * the macros. By using any of the macros we would get the version
	 * the extension is compiled against instead of the version actually
	 * running.
	 */
	char *server_version_num_guc = GetConfigOptionByName("server_version_num", NULL, false);
	long server_version_num = strtol(server_version_num_guc, NULL, 10);

	major = server_version_num / 10000;
	patch = server_version_num % 100;

	Assert(major > 10);
	appendStringInfo(buf, "%d.%d", major, patch);

	return buf->data;
}

#define ISO8601_FORMAT "YYYY-MM-DD\"T\"HH24:MI:SSOF"

static char *
format_iso8601(Datum value)
{
	return TextDatumGetCString(
		DirectFunctionCall2(timestamptz_to_char, value, CStringGetTextDatum(ISO8601_FORMAT)));
}

#define REQ_RELKIND_COUNT "num_relations"
#define REQ_RELKIND_RELTUPLES "num_reltuples"

#define REQ_RELKIND_HEAP_SIZE "heap_size"
#define REQ_RELKIND_TOAST_SIZE "toast_size"
#define REQ_RELKIND_INDEXES_SIZE "indexes_size"

#define REQ_RELKIND_CHILDREN "num_children"
#define REQ_RELKIND_REPLICA_CHUNKS "num_replica_chunks"
#define REQ_RELKIND_COMPRESSED_CHUNKS "num_compressed_chunks"
#define REQ_RELKIND_COMPRESSED_HYPERTABLES "num_compressed_hypertables"
#define REQ_RELKIND_COMPRESSED_CAGGS "num_compressed_caggs"
#define REQ_RELKIND_REPLICATED_HYPERTABLES "num_replicated_distributed_hypertables"

#define REQ_RELKIND_UNCOMPRESSED_HEAP_SIZE "uncompressed_heap_size"
#define REQ_RELKIND_UNCOMPRESSED_TOAST_SIZE "uncompressed_toast_size"
#define REQ_RELKIND_UNCOMPRESSED_INDEXES_SIZE "uncompressed_indexes_size"
#define REQ_RELKIND_UNCOMPRESSED_ROWCOUNT "uncompressed_row_count"
#define REQ_RELKIND_COMPRESSED_HEAP_SIZE "compressed_heap_size"
#define REQ_RELKIND_COMPRESSED_TOAST_SIZE "compressed_toast_size"
#define REQ_RELKIND_COMPRESSED_INDEXES_SIZE "compressed_indexes_size"
#define REQ_RELKIND_COMPRESSED_ROWCOUNT "compressed_row_count"

#define REQ_RELKIND_CAGG_ON_DISTRIBUTED_HYPERTABLE_COUNT "num_caggs_on_distributed_hypertables"
#define REQ_RELKIND_CAGG_USES_REAL_TIME_AGGREGATION_COUNT "num_caggs_using_real_time_aggregation"

static JsonbValue *
add_compression_stats_object(JsonbParseState *parse_state, StatsRelType reltype,
							 const HyperStats *hs)
{
	JsonbValue name = {
		.type = jbvString,
		.val.string.val = pstrdup("compression"),
		.val.string.len = strlen("compression"),
	};
	pushJsonbValue(&parse_state, WJB_KEY, &name);
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	ts_jsonb_add_int64(parse_state, REQ_RELKIND_COMPRESSED_CHUNKS, hs->compressed_chunk_count);

	if (reltype == RELTYPE_CONTINUOUS_AGG)
		ts_jsonb_add_int64(parse_state,
						   REQ_RELKIND_COMPRESSED_CAGGS,
						   hs->compressed_hypertable_count);
	else
		ts_jsonb_add_int64(parse_state,
						   REQ_RELKIND_COMPRESSED_HYPERTABLES,
						   hs->compressed_hypertable_count);

	ts_jsonb_add_int64(parse_state, REQ_RELKIND_COMPRESSED_ROWCOUNT, hs->compressed_row_count);
	ts_jsonb_add_int64(parse_state, REQ_RELKIND_COMPRESSED_HEAP_SIZE, hs->compressed_heap_size);
	ts_jsonb_add_int64(parse_state, REQ_RELKIND_COMPRESSED_TOAST_SIZE, hs->compressed_toast_size);
	ts_jsonb_add_int64(parse_state,
					   REQ_RELKIND_COMPRESSED_INDEXES_SIZE,
					   hs->compressed_indexes_size);
	ts_jsonb_add_int64(parse_state, REQ_RELKIND_UNCOMPRESSED_ROWCOUNT, hs->uncompressed_row_count);
	ts_jsonb_add_int64(parse_state, REQ_RELKIND_UNCOMPRESSED_HEAP_SIZE, hs->uncompressed_heap_size);
	ts_jsonb_add_int64(parse_state,
					   REQ_RELKIND_UNCOMPRESSED_TOAST_SIZE,
					   hs->uncompressed_toast_size);
	ts_jsonb_add_int64(parse_state,
					   REQ_RELKIND_UNCOMPRESSED_INDEXES_SIZE,
					   hs->uncompressed_indexes_size);

	return pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
}

static JsonbValue *
add_relkind_stats_object(JsonbParseState *parse_state, const char *relkindname,
						 const BaseStats *stats, StatsRelType reltype, StatsType statstype)
{
	JsonbValue name = {
		.type = jbvString,
		.val.string.val = pstrdup(relkindname),
		.val.string.len = strlen(relkindname),
	};
	pushJsonbValue(&parse_state, WJB_KEY, &name);
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	ts_jsonb_add_int64(parse_state, REQ_RELKIND_COUNT, stats->relcount);

	if (statstype >= STATS_TYPE_STORAGE)
	{
		const StorageStats *ss = (const StorageStats *) stats;
		ts_jsonb_add_int64(parse_state, REQ_RELKIND_RELTUPLES, stats->reltuples);
		ts_jsonb_add_int64(parse_state, REQ_RELKIND_HEAP_SIZE, ss->relsize.heap_size);
		ts_jsonb_add_int64(parse_state, REQ_RELKIND_TOAST_SIZE, ss->relsize.toast_size);
		ts_jsonb_add_int64(parse_state, REQ_RELKIND_INDEXES_SIZE, ss->relsize.index_size);
	}

	if (statstype >= STATS_TYPE_HYPER)
	{
		const HyperStats *hs = (const HyperStats *) stats;
		ts_jsonb_add_int64(parse_state, REQ_RELKIND_CHILDREN, hs->child_count);

		if (reltype != RELTYPE_PARTITIONED_TABLE)
			add_compression_stats_object(parse_state, reltype, hs);

		if (reltype == RELTYPE_DISTRIBUTED_HYPERTABLE)
		{
			ts_jsonb_add_int64(parse_state,
							   REQ_RELKIND_REPLICATED_HYPERTABLES,
							   hs->replicated_hypertable_count);
			ts_jsonb_add_int64(parse_state, REQ_RELKIND_REPLICA_CHUNKS, hs->replica_chunk_count);
		}
	}

	if (statstype == STATS_TYPE_CAGG)
	{
		const CaggStats *cs = (const CaggStats *) stats;

		ts_jsonb_add_int64(parse_state,
						   REQ_RELKIND_CAGG_ON_DISTRIBUTED_HYPERTABLE_COUNT,
						   cs->on_distributed_hypertable_count);
		ts_jsonb_add_int64(parse_state,
						   REQ_RELKIND_CAGG_USES_REAL_TIME_AGGREGATION_COUNT,
						   cs->uses_real_time_aggregation_count);
	}

	return pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
}

#define REQ_RELS "relations"
#define REQ_RELS_TABLES "tables"
#define REQ_RELS_PARTITIONED_TABLES "partitioned_tables"
#define REQ_RELS_MATVIEWS "materialized_views"
#define REQ_RELS_VIEWS "views"
#define REQ_RELS_HYPERTABLES "hypertables"
#define REQ_RELS_DISTRIBUTED_HYPERTABLES_AN "distributed_hypertables_access_node"
#define REQ_RELS_DISTRIBUTED_HYPERTABLES_DN "distributed_hypertables_data_node"
#define REQ_RELS_CONTINUOUS_AGGS "continuous_aggregates"

static Jsonb *
build_telemetry_report()
{
	JsonbParseState *parse_state = NULL;
	JsonbValue key;
	JsonbValue *result;
	TelemetryStats relstats;
	VersionOSInfo osinfo;

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	ts_jsonb_add_int32(parse_state, REQ_TELEMETRY_VERSION, TS_TELEMETRY_VERSION);
	ts_jsonb_add_str(parse_state,
					 REQ_DB_UUID,
					 DatumGetCString(DirectFunctionCall1(uuid_out, ts_metadata_get_uuid())));
	ts_jsonb_add_str(parse_state,
					 REQ_EXPORTED_DB_UUID,
					 DatumGetCString(
						 DirectFunctionCall1(uuid_out, ts_metadata_get_exported_uuid())));
	ts_jsonb_add_str(parse_state,
					 REQ_INSTALL_TIME,
					 format_iso8601(ts_metadata_get_install_timestamp()));
	ts_jsonb_add_str(parse_state, REQ_INSTALL_METHOD, TIMESCALEDB_INSTALL_METHOD);

	if (ts_version_get_os_info(&osinfo))
	{
		ts_jsonb_add_str(parse_state, REQ_OS, osinfo.sysname);
		ts_jsonb_add_str(parse_state, REQ_OS_VERSION, osinfo.version);
		ts_jsonb_add_str(parse_state, REQ_OS_RELEASE, osinfo.release);
		if (osinfo.has_pretty_version)
			ts_jsonb_add_str(parse_state, REQ_OS_VERSION_PRETTY, osinfo.pretty_version);
	}
	else
		ts_jsonb_add_str(parse_state, REQ_OS, "Unknown");

	ts_jsonb_add_str(parse_state, REQ_PS_VERSION, get_pgversion_string());
	ts_jsonb_add_str(parse_state, REQ_TS_VERSION, TIMESCALEDB_VERSION_MOD);
	ts_jsonb_add_str(parse_state, REQ_BUILD_OS, BUILD_OS_NAME);
	ts_jsonb_add_str(parse_state, REQ_BUILD_OS_VERSION, BUILD_OS_VERSION);
	ts_jsonb_add_str(parse_state, REQ_BUILD_ARCHITECTURE, BUILD_PROCESSOR);
	ts_jsonb_add_int32(parse_state, REQ_BUILD_ARCHITECTURE_BIT_SIZE, get_architecture_bit_size());
	ts_jsonb_add_int64(parse_state, REQ_DATA_VOLUME, get_database_size());

	/* Add relation stats */
	ts_telemetry_stats_gather(&relstats);
	key.type = jbvString;
	key.val.string.val = REQ_RELS;
	key.val.string.len = strlen(REQ_RELS);
	pushJsonbValue(&parse_state, WJB_KEY, &key);
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	add_relkind_stats_object(parse_state,
							 REQ_RELS_TABLES,
							 &relstats.tables.base,
							 RELTYPE_TABLE,
							 STATS_TYPE_STORAGE);
	add_relkind_stats_object(parse_state,
							 REQ_RELS_PARTITIONED_TABLES,
							 &relstats.partitioned_tables.storage.base,
							 RELTYPE_PARTITIONED_TABLE,
							 STATS_TYPE_HYPER);
	add_relkind_stats_object(parse_state,
							 REQ_RELS_MATVIEWS,
							 &relstats.materialized_views.base,
							 RELTYPE_MATVIEW,
							 STATS_TYPE_STORAGE);
	add_relkind_stats_object(parse_state,
							 REQ_RELS_VIEWS,
							 &relstats.views,
							 RELTYPE_VIEW,
							 STATS_TYPE_BASE);
	add_relkind_stats_object(parse_state,
							 REQ_RELS_HYPERTABLES,
							 &relstats.hypertables.storage.base,
							 RELTYPE_HYPERTABLE,
							 STATS_TYPE_HYPER);

	/*
	 * Distinguish between distributed hypertables on access nodes and the
	 * "partial" distributed hypertables on data nodes.
	 *
	 * Access nodes currently don't store data (chunks), but could potentially
	 * do it in the future. We only report the data that is actually stored on
	 * an access node, which currently is zero. One could report the aggregate
	 * numbers across all data nodes, but that requires using, e.g., a
	 * function like hypertable_size() that calls out to each data node to get
	 * its size. However, telemetry probably shouldn't perform such
	 * distributed calls across data nodes, as it could, e.g., revent the
	 * access node from reporting telemetry if a data node is down.
	 *
	 * It is assumed that data nodes will report telemetry themselves, and the
	 * size of the data they store will be reported under
	 * "distributed_hypertables_data_node" to easily distinguish from an
	 * access node. The aggregate information for the whole distributed
	 * hypertable could be joined on the server side based on the dist_uuid.
	 */
	add_relkind_stats_object(parse_state,
							 REQ_RELS_DISTRIBUTED_HYPERTABLES_AN,
							 &relstats.distributed_hypertables.storage.base,
							 RELTYPE_DISTRIBUTED_HYPERTABLE,
							 STATS_TYPE_HYPER);
	add_relkind_stats_object(parse_state,
							 REQ_RELS_DISTRIBUTED_HYPERTABLES_DN,
							 &relstats.distributed_hypertable_members.storage.base,
							 RELTYPE_DISTRIBUTED_HYPERTABLE_MEMBER,
							 STATS_TYPE_HYPER);
	add_relkind_stats_object(parse_state,
							 REQ_RELS_CONTINUOUS_AGGS,
							 &relstats.continuous_aggs.hyp.storage.base,
							 RELTYPE_CONTINUOUS_AGG,
							 STATS_TYPE_CAGG);

	pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);

	add_job_counts(parse_state);

	/* Add related extensions, which is a nested JSON */
	key.type = jbvString;
	key.val.string.val = REQ_RELATED_EXTENSIONS;
	key.val.string.len = strlen(REQ_RELATED_EXTENSIONS);
	pushJsonbValue(&parse_state, WJB_KEY, &key);
	add_related_extensions(parse_state);

	/* license */
	key.type = jbvString;
	key.val.string.val = REQ_LICENSE_INFO;
	key.val.string.len = strlen(REQ_LICENSE_INFO);
	pushJsonbValue(&parse_state, WJB_KEY, &key);
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	if (ts_license_is_apache())
		ts_jsonb_add_str(parse_state, REQ_LICENSE_EDITION, REQ_LICENSE_EDITION_APACHE);
	else
		ts_jsonb_add_str(parse_state, REQ_LICENSE_EDITION, REQ_LICENSE_EDITION_COMMUNITY);
	pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);

	/* add distributed database fields */
	if (!ts_license_is_apache())
		ts_cm_functions->add_tsl_telemetry_info(&parse_state);

	/* add tuned info, which is optional */
	if (ts_last_tune_time != NULL)
		ts_jsonb_add_str(parse_state, REQ_TS_LAST_TUNE_TIME, ts_last_tune_time);

	if (ts_last_tune_version != NULL)
		ts_jsonb_add_str(parse_state, REQ_TS_LAST_TUNE_VERSION, ts_last_tune_version);

	/* add cloud to telemetry when set */
	if (ts_telemetry_cloud != NULL)
	{
		key.type = jbvString;
		key.val.string.val = REQ_INSTANCE_METADATA;
		key.val.string.len = strlen(REQ_INSTANCE_METADATA);
		pushJsonbValue(&parse_state, WJB_KEY, &key);

		pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
		ts_jsonb_add_str(parse_state, REQ_TS_TELEMETRY_CLOUD, ts_telemetry_cloud);
		pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	}

	/* Add additional content from metadata */
	key.type = jbvString;
	key.val.string.val = REQ_METADATA;
	key.val.string.len = strlen(REQ_METADATA);
	pushJsonbValue(&parse_state, WJB_KEY, &key);
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_telemetry_metadata_add_values(parse_state);
	pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);

	/* end of telemetry object */
	result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);

	return JsonbValueToJsonb(result);
}

HttpRequest *
ts_build_version_request(const char *host, const char *path)
{
	HttpRequest *req;
	Jsonb *json = build_telemetry_report();

	/* Fill in HTTP request */
	req = ts_http_request_create(HTTP_POST);
	ts_http_request_set_uri(req, path);
	ts_http_request_set_version(req, HTTP_VERSION_10);
	ts_http_request_set_header(req, HTTP_HOST, host);
	ts_http_request_set_body_jsonb(req, json);

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
		return false;

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

TS_FUNCTION_INFO_V1(ts_telemetry_get_report_jsonb);

Datum
ts_telemetry_get_report_jsonb(PG_FUNCTION_ARGS)
{
	Jsonb *jb = build_telemetry_report();

	PG_RETURN_JSONB_P(jb);
}
