/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/guc.h>
#include <miscadmin.h>

#include "guc.h"
#include "license_guc.h"
#include "config.h"
#include "hypertable_cache.h"
#ifdef USE_TELEMETRY
#include "telemetry/telemetry.h"
#endif

#ifdef USE_TELEMETRY
typedef enum TelemetryLevel
{
	TELEMETRY_OFF,
	TELEMETRY_BASIC,
} TelemetryLevel;

/* Define which level means on. We use this object to have at least one object
 * of type TelemetryLevel in the code, otherwise pgindent won't work for the
 * type */
static const TelemetryLevel on_level = TELEMETRY_BASIC;

bool
ts_telemetry_on()
{
	return ts_guc_telemetry_level == on_level;
}

static const struct config_enum_entry telemetry_level_options[] = {
	{ "off", TELEMETRY_OFF, false }, { "basic", TELEMETRY_BASIC, false }, { NULL, 0, false }
};
#endif

static const struct config_enum_entry remote_data_fetchers[] = {
	{ "rowbyrow", RowByRowFetcherType, false },
	{ "cursor", CursorFetcherType, false },
	{ "auto", AutoFetcherType, false },
	{ NULL, 0, false }
};

bool ts_guc_enable_optimizations = true;
bool ts_guc_restoring = false;
bool ts_guc_enable_constraint_aware_append = true;
bool ts_guc_enable_ordered_append = true;
bool ts_guc_enable_chunk_append = true;
bool ts_guc_enable_parallel_chunk_append = true;
bool ts_guc_enable_runtime_exclusion = true;
bool ts_guc_enable_constraint_exclusion = true;
bool ts_guc_enable_qual_propagation = true;
bool ts_guc_enable_cagg_reorder_groupby = true;
TSDLLEXPORT bool ts_guc_enable_transparent_decompression = true;
bool ts_guc_enable_per_data_node_queries = true;
bool ts_guc_enable_async_append = true;
TSDLLEXPORT bool ts_guc_enable_skip_scan = true;
int ts_guc_max_open_chunks_per_insert = 10;
int ts_guc_max_cached_chunks_per_hypertable = 10;
#ifdef USE_TELEMETRY
int ts_guc_telemetry_level = TELEMETRY_DEFAULT;
char *ts_telemetry_cloud = NULL;
#endif

TSDLLEXPORT char *ts_guc_license = TS_LICENSE_DEFAULT;
char *ts_last_tune_time = NULL;
char *ts_last_tune_version = NULL;
TSDLLEXPORT bool ts_guc_enable_2pc;
TSDLLEXPORT int ts_guc_max_insert_batch_size = 1000;
TSDLLEXPORT bool ts_guc_enable_connection_binary_data;
TSDLLEXPORT bool ts_guc_enable_client_ddl_on_data_nodes = false;
TSDLLEXPORT char *ts_guc_ssl_dir = NULL;
TSDLLEXPORT char *ts_guc_passfile = NULL;
TSDLLEXPORT bool ts_guc_enable_remote_explain = false;
TSDLLEXPORT DataFetcherType ts_guc_remote_data_fetcher = AutoFetcherType;

#ifdef TS_DEBUG
bool ts_shutdown_bgw = false;
char *ts_current_timestamp_mock = "";
#endif

static void
assign_max_cached_chunks_per_hypertable_hook(int newval, void *extra)
{
	/* invalidate the hypertable cache to reset */
	ts_hypertable_cache_invalidate_callback();
}

void
_guc_init(void)
{
	/* Main database to connect to. */
	DefineCustomBoolVariable("timescaledb.enable_optimizations",
							 "Enable TimescaleDB query optimizations",
							 NULL,
							 &ts_guc_enable_optimizations,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.restoring",
							 "Install timescale in restoring mode",
							 "Used for running pg_restore",
							 &ts_guc_restoring,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_constraint_aware_append",
							 "Enable constraint-aware append scans",
							 "Enable constraint exclusion at execution time",
							 &ts_guc_enable_constraint_aware_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_ordered_append",
							 "Enable ordered append scans",
							 "Enable ordered append optimization for queries that are ordered by "
							 "the time dimension",
							 &ts_guc_enable_ordered_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_chunk_append",
							 "Enable chunk append node",
							 "Enable using chunk append node",
							 &ts_guc_enable_chunk_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_parallel_chunk_append",
							 "Enable parallel chunk append node",
							 "Enable using parallel aware chunk append node",
							 &ts_guc_enable_parallel_chunk_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_runtime_exclusion",
							 "Enable runtime chunk exclusion",
							 "Enable runtime chunk exclusion in ChunkAppend node",
							 &ts_guc_enable_runtime_exclusion,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_constraint_exclusion",
							 "Enable constraint exclusion",
							 "Enable planner constraint exclusion",
							 &ts_guc_enable_constraint_exclusion,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_qual_propagation",
							 "Enable qualifier propagation",
							 "Enable propagation of qualifiers in JOINs",
							 &ts_guc_enable_qual_propagation,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_transparent_decompression",
							 "Enable transparent decompression",
							 "Enable transparent decompression when querying hypertable",
							 &ts_guc_enable_transparent_decompression,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_skipscan",
							 "Enable SkipScan",
							 "Enable SkipScan for DISTINCT queries",
							 &ts_guc_enable_skip_scan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_cagg_reorder_groupby",
							 "Enable group by reordering",
							 "Enable group by clause reordering for continuous aggregates",
							 &ts_guc_enable_cagg_reorder_groupby,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_2pc",
							 "Enable two-phase commit",
							 "Enable two-phase commit on distributed hypertables",
							 &ts_guc_enable_2pc,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_per_data_node_queries",
							 "Enable the per data node query optimization for hypertables",
							 "Enable the optimization that combines different chunks belonging to "
							 "the same hypertable into a single query per data_node",
							 &ts_guc_enable_per_data_node_queries,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("timescaledb.max_insert_batch_size",
							"The max number of tuples to batch before sending to a data node",
							"When acting as a access node, TimescaleDB splits batches of "
							"inserted tuples across multiple data nodes. It will batch up to the "
							"configured batch size tuples per data node before flushing. "
							"Setting this to 0 disables batching, reverting to tuple-by-tuple "
							"inserts",
							&ts_guc_max_insert_batch_size,
							1000,
							0,
							65536,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("timescaledb.enable_connection_binary_data",
							 "Enable binary format for connection",
							 "Enable binary format for data exchanged between nodes in the cluster",
							 &ts_guc_enable_connection_binary_data,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_client_ddl_on_data_nodes",
							 "Enable DDL operations on data nodes by a client",
							 "Do not restrict execution of DDL operations only by access node",
							 &ts_guc_enable_client_ddl_on_data_nodes,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_async_append",
							 "Enable async query execution on data nodes",
							 "Enable optimization that runs remote queries asynchronously"
							 "across data nodes",
							 &ts_guc_enable_async_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.enable_remote_explain",
							 "Show explain from remote nodes when using VERBOSE flag",
							 "Enable getting and showing EXPLAIN output from remote nodes",
							 &ts_guc_enable_remote_explain,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomEnumVariable("timescaledb.remote_data_fetcher",
							 "Set remote data fetcher type",
							 "Pick data fetcher type based on type of queries you plan to run "
							 "(rowbyrow or cursor)",
							 (int *) &ts_guc_remote_data_fetcher,
							 AutoFetcherType,
							 remote_data_fetchers,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomStringVariable("timescaledb.ssl_dir",
							   "TimescaleDB user certificate directory",
							   "Determines a path which is used to search user certificates and "
							   "private keys",
							   &ts_guc_ssl_dir,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomStringVariable("timescaledb.passfile",
							   "TimescaleDB password file path",
							   "Specifies the name of the file used to store passwords used for "
							   "data node connections",
							   &ts_guc_passfile,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL,
							   NULL,
							   NULL);

	DefineCustomIntVariable("timescaledb.max_open_chunks_per_insert",
							"Maximum open chunks per insert",
							"Maximum number of open chunk tables per insert",
							&ts_guc_max_open_chunks_per_insert,
							Min(work_mem * INT64CONST(1024) / INT64CONST(25000),
								PG_INT16_MAX), /* Measurements via
												* `MemoryContextStats(TopMemoryContext)`
												* show chunk insert
												* state memory context
												* takes up ~25K bytes
												* (work_mem is in
												* kbytes) */
							0,
							PG_INT16_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("timescaledb.max_cached_chunks_per_hypertable",
							"Maximum cached chunks",
							"Maximum number of chunks stored in the cache",
							&ts_guc_max_cached_chunks_per_hypertable,
							100,
							0,
							65536,
							PGC_USERSET,
							0,
							NULL,
							assign_max_cached_chunks_per_hypertable_hook,
							NULL);
#ifdef USE_TELEMETRY
	DefineCustomEnumVariable("timescaledb.telemetry_level",
							 "Telemetry settings level",
							 "Level used to determine which telemetry to send",
							 &ts_guc_telemetry_level,
							 TELEMETRY_DEFAULT,
							 telemetry_level_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
#endif

	DefineCustomStringVariable(/* name= */ "timescaledb.license",
							   /* short_dec= */ "TimescaleDB license type",
							   /* long_dec= */ "Determines which features are enabled",
							   /* valueAddr= */ &ts_guc_license,
							   /* bootValue= */ TS_LICENSE_DEFAULT,
							   /* context= */ PGC_SUSET,
							   /* flags= */ 0,
							   /* check_hook= */ ts_license_guc_check_hook,
							   /* assign_hook= */ ts_license_guc_assign_hook,
							   /* show_hook= */ NULL);

	DefineCustomStringVariable(/* name= */ "timescaledb.last_tuned",
							   /* short_dec= */ "last tune run",
							   /* long_dec= */ "records last time timescaledb-tune ran",
							   /* valueAddr= */ &ts_last_tune_time,
							   /* bootValue= */ NULL,
							   /* context= */ PGC_SIGHUP,
							   /* flags= */ 0,
							   /* check_hook= */ NULL,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);

	DefineCustomStringVariable(/* name= */ "timescaledb.last_tuned_version",
							   /* short_dec= */ "version of timescaledb-tune",
							   /* long_dec= */ "version of timescaledb-tune used to tune",
							   /* valueAddr= */ &ts_last_tune_version,
							   /* bootValue= */ NULL,
							   /* context= */ PGC_SIGHUP,
							   /* flags= */ 0,
							   /* check_hook= */ NULL,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);

#ifdef USE_TELEMETRY
	DefineCustomStringVariable(/* name= */ "timescaledb_telemetry.cloud",
							   /* short_dec= */ "cloud provider",
							   /* long_dec= */ "cloud provider used for this instance",
							   /* valueAddr= */ &ts_telemetry_cloud,
							   /* bootValue= */ NULL,
							   /* context= */ PGC_SIGHUP,
							   /* flags= */ 0,
							   /* check_hook= */ NULL,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);
#endif

#ifdef TS_DEBUG
	DefineCustomBoolVariable(/* name= */ "timescaledb.shutdown_bgw_scheduler",
							 /* short_dec= */ "immediately shutdown the bgw scheduler",
							 /* long_dec= */ "this is for debugging purposes",
							 /* valueAddr= */ &ts_shutdown_bgw,
							 /* bootValue= */ false,
							 /* context= */ PGC_SIGHUP,
							 /* flags= */ 0,
							 /* check_hook= */ NULL,
							 /* assign_hook= */ NULL,
							 /* show_hook= */ NULL);

	DefineCustomStringVariable(/* name= */ "timescaledb.current_timestamp_mock",
							   /* short_dec= */ "set the current timestamp",
							   /* long_dec= */ "this is for debugging purposes",
							   /* valueAddr= */ &ts_current_timestamp_mock,
							   /* bootValue= */ NULL,
							   /* context= */ PGC_USERSET,
							   /* flags= */ 0,
							   /* check_hook= */ NULL,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);
#endif
}

void
_guc_fini(void)
{
}
