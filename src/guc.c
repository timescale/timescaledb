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
#include "telemetry/telemetry.h"

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

bool ts_guc_disable_optimizations = false;
bool ts_guc_optimize_non_hypertables = false;
bool ts_guc_restoring = false;
bool ts_guc_constraint_aware_append = true;
bool ts_guc_enable_ordered_append = true;
bool ts_guc_enable_chunk_append = true;
bool ts_guc_enable_parallel_chunk_append = true;
bool ts_guc_enable_runtime_exclusion = true;
bool ts_guc_enable_constraint_exclusion = true;
bool ts_guc_enable_cagg_reorder_groupby = true;
TSDLLEXPORT bool ts_guc_enable_transparent_decompression = true;
int ts_guc_max_open_chunks_per_insert = 10;
int ts_guc_max_cached_chunks_per_hypertable = 10;
int ts_guc_telemetry_level = TELEMETRY_DEFAULT;

TSDLLEXPORT char *ts_guc_license_key = TS_DEFAULT_LICENSE;
char *ts_last_tune_time = NULL;
char *ts_last_tune_version = NULL;
char *ts_telemetry_cloud = NULL;

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
	DefineCustomBoolVariable("timescaledb.disable_optimizations",
							 "Disable all timescale query optimizations",
							 NULL,
							 &ts_guc_disable_optimizations,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("timescaledb.optimize_non_hypertables",
							 "Apply timescale query optimization to plain tables",
							 "Apply timescale query optimization to plain tables in addition to "
							 "hypertables",
							 &ts_guc_optimize_non_hypertables,
							 false,
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

	DefineCustomBoolVariable("timescaledb.constraint_aware_append",
							 "Enable constraint-aware append scans",
							 "Enable constraint exclusion at execution time",
							 &ts_guc_constraint_aware_append,
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
							 true, // FIXME currently broken on 12
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

	DefineCustomStringVariable(/* name= */ "timescaledb.license_key",
							   /* short_dec= */ "TimescaleDB license key",
							   /* long_dec= */ "Determines which features are enabled",
							   /* valueAddr= */ &ts_guc_license_key,
							   /* bootValue= */ TS_DEFAULT_LICENSE,
							   /* context= */ PGC_SUSET,
							   /* flags= */ GUC_SUPERUSER_ONLY,
							   /* check_hook= */ ts_license_update_check,
							   /* assign_hook= */ ts_license_on_assign,
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
