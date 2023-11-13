/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <miscadmin.h>
#include <parser/parse_func.h>
#include <utils/guc.h>
#include <utils/regproc.h>
#include <utils/varlena.h>

#include "config.h"
#include "extension.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "license_guc.h"
#ifdef USE_TELEMETRY
#include "telemetry/telemetry.h"
#endif

#ifdef USE_TELEMETRY
/* Define which level means on. We use this object to have at least one object
 * of type TelemetryLevel in the code, otherwise pgindent won't work for the
 * type */
static const TelemetryLevel on_level = TELEMETRY_NO_FUNCTIONS;

bool
ts_telemetry_on()
{
	return ts_guc_telemetry_level >= on_level;
}

bool
ts_function_telemetry_on()
{
	return ts_guc_telemetry_level > TELEMETRY_NO_FUNCTIONS;
}

static const struct config_enum_entry telemetry_level_options[] = {
	{ "off", TELEMETRY_OFF, false },
	{ "no_functions", TELEMETRY_NO_FUNCTIONS, false },
	{ "basic", TELEMETRY_BASIC, false },
	{ NULL, 0, false }
};
#endif

/* Copied from contrib/auto_explain/auto_explain.c */
static const struct config_enum_entry loglevel_options[] = {
	{ "debug5", DEBUG5, false }, { "debug4", DEBUG4, false }, { "debug3", DEBUG3, false },
	{ "debug2", DEBUG2, false }, { "debug1", DEBUG1, false }, { "debug", DEBUG2, true },
	{ "info", INFO, false },	 { "notice", NOTICE, false }, { "warning", WARNING, false },
	{ "log", LOG, false },		 { NULL, 0, false }
};

bool ts_guc_enable_deprecation_warnings = true;
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
bool ts_guc_enable_now_constify = true;
bool ts_guc_enable_foreign_key_propagation = true;
TSDLLEXPORT bool ts_guc_enable_cagg_watermark_constify = true;
TSDLLEXPORT int ts_guc_cagg_max_individual_materializations = 10;
bool ts_guc_enable_osm_reads = true;
TSDLLEXPORT bool ts_guc_enable_compressed_direct_batch_delete = true;
TSDLLEXPORT bool ts_guc_enable_dml_decompression = true;
TSDLLEXPORT bool ts_guc_enable_dml_decompression_tuple_filtering = true;
TSDLLEXPORT int ts_guc_max_tuples_decompressed_per_dml = 100000;
TSDLLEXPORT bool ts_guc_enable_transparent_decompression = true;
TSDLLEXPORT bool ts_guc_enable_compression_wal_markers = false;
TSDLLEXPORT bool ts_guc_enable_decompression_sorted_merge = true;
bool ts_guc_enable_chunkwise_aggregation = true;
bool ts_guc_enable_vectorized_aggregation = true;
TSDLLEXPORT bool ts_guc_enable_compression_indexscan = false;
TSDLLEXPORT bool ts_guc_enable_bulk_decompression = true;
TSDLLEXPORT bool ts_guc_auto_sparse_indexes = true;
TSDLLEXPORT bool ts_guc_enable_columnarscan = true;
TSDLLEXPORT int ts_guc_bgw_log_level = WARNING;
TSDLLEXPORT bool ts_guc_enable_skip_scan = true;
static char *ts_guc_default_segmentby_fn = NULL;
static char *ts_guc_default_orderby_fn = NULL;
TSDLLEXPORT bool ts_guc_enable_job_execution_logging = false;
bool ts_guc_enable_tss_callbacks = true;
TSDLLEXPORT bool ts_guc_enable_delete_after_compression = false;
TSDLLEXPORT bool ts_guc_enable_merge_on_cagg_refresh = false;

/* default value of ts_guc_max_open_chunks_per_insert and ts_guc_max_cached_chunks_per_hypertable
 * will be set as their respective boot-value when the GUC mechanism starts up */
int ts_guc_max_open_chunks_per_insert;
int ts_guc_max_cached_chunks_per_hypertable;
#ifdef USE_TELEMETRY
TelemetryLevel ts_guc_telemetry_level = TELEMETRY_DEFAULT;
char *ts_telemetry_cloud = NULL;
#endif

TSDLLEXPORT char *ts_guc_license = TS_LICENSE_DEFAULT;
char *ts_last_tune_time = NULL;
char *ts_last_tune_version = NULL;

bool ts_guc_debug_require_batch_sorted_merge = false;
bool ts_guc_debug_allow_cagg_with_deprecated_funcs = false;

#ifdef TS_DEBUG
bool ts_shutdown_bgw = false;
char *ts_current_timestamp_mock = NULL;
#endif

int ts_guc_debug_toast_tuple_target = 128;

#ifdef TS_DEBUG
static const struct config_enum_entry debug_require_options[] = { { "allow", DRO_Allow, false },
																  { "forbid", DRO_Forbid, false },
																  { "require", DRO_Require, false },
																  { NULL, 0, false } };

DebugRequireOption ts_guc_debug_require_vector_qual = DRO_Allow;

DebugRequireOption ts_guc_debug_require_vector_agg = DRO_Allow;
#endif

bool ts_guc_debug_compression_path_info = false;
bool ts_guc_enable_rowlevel_compression_locking = false;

static bool ts_guc_enable_hypertable_create = true;
static bool ts_guc_enable_hypertable_compression = true;
static bool ts_guc_enable_cagg_create = true;
static bool ts_guc_enable_policy_create = true;

typedef struct
{
	const char *name;
	const char *description;
	bool *enable;
} FeatureFlag;

static FeatureFlag ts_feature_flags[] = {
	[FEATURE_HYPERTABLE] = { MAKE_EXTOPTION("enable_hypertable_create"),
							 "Enable creation of hypertable",
							 &ts_guc_enable_hypertable_create },

	[FEATURE_HYPERTABLE_COMPRESSION] = { MAKE_EXTOPTION("enable_hypertable_compression"),
										 "Enable hypertable compression functions",
										 &ts_guc_enable_hypertable_compression },

	[FEATURE_CAGG] = { MAKE_EXTOPTION("enable_cagg_create"),
					   "Enable creation of continuous aggregate",
					   &ts_guc_enable_cagg_create },

	[FEATURE_POLICY] = { MAKE_EXTOPTION("enable_policy_create"),
						 "Enable creation of policies and user-defined actions",
						 &ts_guc_enable_policy_create }
};

static void
ts_feature_flag_add(FeatureFlagType type)
{
	FeatureFlag *flag = &ts_feature_flags[type];
	int flag_context = PGC_SIGHUP;
#ifdef TS_DEBUG
	flag_context = PGC_USERSET;
#endif
	DefineCustomBoolVariable(flag->name,
							 flag->description,
							 NULL,
							 flag->enable,
							 true,
							 flag_context,
							 GUC_SUPERUSER_ONLY,
							 NULL,
							 NULL,
							 NULL);
}

void
ts_feature_flag_check(FeatureFlagType type)
{
	FeatureFlag *flag = &ts_feature_flags[type];
	if (likely(*flag->enable))
		return;
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("You are using a Dynamic PostgreSQL service. This feature is only available on "
					"Time-series services. https://tsdb.co/dynamic-postgresql")));
}

/*
 * We have to understand if we have finished initializing the GUCs, so that we
 * know when it's OK to check their values for mutual consistency.
 */
static bool gucs_are_initialized = false;

/*
 * Warn about the mismatched cache sizes that can lead to cache thrashing.
 */
static void
validate_chunk_cache_sizes(int hypertable_chunks, int insert_chunks)
{
	/*
	 * Note that this callback is also called when the individual GUCs are
	 * initialized, so we are going to see temporary mismatched values here.
	 * That's why we also have to check that the GUC initialization have
	 * finished.
	 */
	if (gucs_are_initialized && insert_chunks > hypertable_chunks)
	{
		ereport(WARNING,
				(errmsg("insert cache size is larger than hypertable chunk cache size"),
				 errdetail("insert cache size is %d, hypertable chunk cache size is %d",
						   insert_chunks,
						   hypertable_chunks),
				 errhint("This is a configuration problem. Either increase "
						 "timescaledb.max_cached_chunks_per_hypertable (preferred) or decrease "
						 "timescaledb.max_open_chunks_per_insert.")));
	}
}

static void
assign_max_cached_chunks_per_hypertable_hook(int newval, void *extra)
{
	/* invalidate the hypertable cache to reset */
	ts_hypertable_cache_invalidate_callback();

	validate_chunk_cache_sizes(newval, ts_guc_max_open_chunks_per_insert);
}

static void
assign_max_open_chunks_per_insert_hook(int newval, void *extra)
{
	validate_chunk_cache_sizes(ts_guc_max_cached_chunks_per_hypertable, newval);
}

static Oid
get_segmentby_func(char *input_name)
{
	List *namelist = NIL;

	if (strlen(input_name) == 0)
	{
		return InvalidOid;
	}

#if PG16_LT
	namelist = stringToQualifiedNameList(input_name);
#else
	namelist = stringToQualifiedNameList(input_name, NULL);
#endif
	Oid argtyp[] = { REGCLASSOID };
	return LookupFuncName(namelist, lengthof(argtyp), argtyp, true);
}

static bool
check_segmentby_func(char **newval, void **extra, GucSource source)
{
	/* if the extension doesn't exist you can't check for the function, have to take it on faith */
	if (ts_extension_is_loaded_and_not_upgrading())
	{
		Oid segment_func_oid = get_segmentby_func(*newval);

		if (strlen(*newval) > 0 && !OidIsValid(segment_func_oid))
		{
			GUC_check_errdetail("Function \"%s\" does not exist.", *newval);
			return false;
		}
	}
	return true;
}

Oid
ts_guc_default_segmentby_fn_oid()
{
	return get_segmentby_func(ts_guc_default_segmentby_fn);
}

static Oid
get_orderby_func(char *input_name)
{
	List *namelist = NIL;

	if (strlen(input_name) == 0)
	{
		return InvalidOid;
	}

#if PG16_LT
	namelist = stringToQualifiedNameList(input_name);
#else
	namelist = stringToQualifiedNameList(input_name, NULL);
#endif
	Oid argtyp[] = { REGCLASSOID, TEXTARRAYOID };
	return LookupFuncName(namelist, lengthof(argtyp), argtyp, true);
}

static bool
check_orderby_func(char **newval, void **extra, GucSource source)
{
	/* if the extension doesn't exist you can't check for the function, have to take it on faith */
	if (ts_extension_is_loaded_and_not_upgrading())
	{
		Oid func_oid = get_orderby_func(*newval);

		if (strlen(*newval) > 0 && !OidIsValid(func_oid))
		{
			GUC_check_errdetail("Function \"%s\" does not exist.", *newval);
			return false;
		}
	}
	return true;
}

Oid
ts_guc_default_orderby_fn_oid()
{
	return get_orderby_func(ts_guc_default_orderby_fn);
}

void
_guc_init(void)
{
	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_deprecation_warnings"),
							 "Enable warnings when using deprecated functionality",
							 NULL,
							 &ts_guc_enable_deprecation_warnings,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_optimizations"),
							 "Enable TimescaleDB query optimizations",
							 NULL,
							 &ts_guc_enable_optimizations,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("restoring"),
							 "Install timescale in restoring mode",
							 "Used for running pg_restore",
							 &ts_guc_restoring,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_constraint_aware_append"),
							 "Enable constraint-aware append scans",
							 "Enable constraint exclusion at execution time",
							 &ts_guc_enable_constraint_aware_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_ordered_append"),
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

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_chunk_append"),
							 "Enable chunk append node",
							 "Enable using chunk append node",
							 &ts_guc_enable_chunk_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_parallel_chunk_append"),
							 "Enable parallel chunk append node",
							 "Enable using parallel aware chunk append node",
							 &ts_guc_enable_parallel_chunk_append,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_runtime_exclusion"),
							 "Enable runtime chunk exclusion",
							 "Enable runtime chunk exclusion in ChunkAppend node",
							 &ts_guc_enable_runtime_exclusion,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_constraint_exclusion"),
							 "Enable constraint exclusion",
							 "Enable planner constraint exclusion",
							 &ts_guc_enable_constraint_exclusion,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_foreign_key_propagation"),
							 "Enable foreign key propagation",
							 "Adjust foreign key lookup queries to target whole hypertable",
							 &ts_guc_enable_foreign_key_propagation,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_qual_propagation"),
							 "Enable qualifier propagation",
							 "Enable propagation of qualifiers in JOINs",
							 &ts_guc_enable_qual_propagation,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_dml_decompression"),
							 "Enable DML decompression",
							 "Enable DML decompression when modifying compressed hypertable",
							 &ts_guc_enable_dml_decompression,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_dml_decompression_tuple_filtering"),
							 "Enable DML decompression tuple filtering",
							 "Recheck tuples during DML decompression to only decompress batches "
							 "with matching tuples",
							 &ts_guc_enable_dml_decompression_tuple_filtering,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_compressed_direct_batch_delete"),
							 "Enable direct deletion of compressed batches",
							 "Enable direct batch deletion in compressed chunks",
							 &ts_guc_enable_compressed_direct_batch_delete,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable(MAKE_EXTOPTION("max_tuples_decompressed_per_dml_transaction"),
							"The max number of tuples that can be decompressed during an "
							"INSERT, UPDATE, or DELETE.",
							" If the number of tuples exceeds this value, an error will "
							"be thrown and transaction rolled back. "
							"Setting this to 0 sets this value to unlimited number of "
							"tuples decompressed.",
							&ts_guc_max_tuples_decompressed_per_dml,
							100000,
							0,
							2147483647,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_transparent_decompression"),
							 "Enable transparent decompression",
							 "Enable transparent decompression when querying hypertable",
							 &ts_guc_enable_transparent_decompression,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_skipscan"),
							 "Enable SkipScan",
							 "Enable SkipScan for DISTINCT queries",
							 &ts_guc_enable_skip_scan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_compression_wal_markers"),
							 "Enable WAL markers for compression ops",
							 "Enable the generation of markers in the WAL stream which mark the "
							 "start and end of compression operations",
							 &ts_guc_enable_compression_wal_markers,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_decompression_sorted_merge"),
							 "Enable compressed batches heap merge",
							 "Enable the merge of compressed batches to preserve the compression "
							 "order by",
							 &ts_guc_enable_decompression_sorted_merge,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_cagg_reorder_groupby"),
							 "Enable group by reordering",
							 "Enable group by clause reordering for continuous aggregates",
							 &ts_guc_enable_cagg_reorder_groupby,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_now_constify"),
							 "Enable now() constify",
							 "Enable constifying now() in query constraints",
							 &ts_guc_enable_now_constify,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_cagg_watermark_constify"),
							 "Enable cagg watermark constify",
							 "Enable constifying cagg watermark for real-time caggs",
							 &ts_guc_enable_cagg_watermark_constify,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_merge_on_cagg_refresh"),
							 "Enable MERGE statement on cagg refresh",
							 "Enable MERGE statement on cagg refresh",
							 &ts_guc_enable_merge_on_cagg_refresh,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * Define the limit on number of invalidation-based refreshes we allow per
	 * refresh call. If this limit is exceeded, fall back to a single refresh that
	 * covers the range decided by the min and max invalidated time.
	 */
	DefineCustomIntVariable(MAKE_EXTOPTION("materializations_per_refresh_window"),
							"Max number of materializations per cagg refresh window",
							"The maximal number of individual refreshes per cagg refresh. If more "
							"refreshes need to be performed, they are merged into a larger "
							"single refresh.",
							&ts_guc_cagg_max_individual_materializations,
							10,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_tiered_reads"),
							 "Enable tiered data reads",
							 "Enable reading of tiered data by including a foreign table "
							 "representing the data in the object storage into the query plan",
							 &ts_guc_enable_osm_reads,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_chunkwise_aggregation"),
							 "Enable chunk-wise aggregation",
							 "Enable the pushdown of aggregations to the"
							 " chunk level",
							 &ts_guc_enable_chunkwise_aggregation,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_vectorized_aggregation"),
							 "Enable vectorized aggregation",
							 "Enable vectorized aggregation for compressed data",
							 &ts_guc_enable_vectorized_aggregation,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_compression_indexscan"),
							 "Enable compression to take indexscan path",
							 "Enable indexscan during compression, if matching index is found",
							 &ts_guc_enable_compression_indexscan,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_bulk_decompression"),
							 "Enable decompression of the entire compressed batches",
							 "Increases throughput of decompression, but might increase query "
							 "memory usage",
							 &ts_guc_enable_bulk_decompression,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("auto_sparse_indexes"),
							 "Create sparse indexes on compressed chunks",
							 "The hypertable columns that are used as index keys will have "
							 "suitable sparse indexes when compressed. Must be set at the moment "
							 "of chunk compression, e.g. when the `compress_chunk()` is called.",
							 &ts_guc_auto_sparse_indexes,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_columnarscan"),
							 "Enable columnar-optimized scans for supported access methods",
							 "Use scan optimizations for columnar-oriented storage",
							 &ts_guc_enable_columnarscan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable(MAKE_EXTOPTION("max_open_chunks_per_insert"),
							"Maximum open chunks per insert",
							"Maximum number of open chunk tables per insert",
							&ts_guc_max_open_chunks_per_insert,
							1024,
							0,
							PG_INT16_MAX,
							PGC_USERSET,
							0,
							NULL,
							assign_max_open_chunks_per_insert_hook,
							NULL);

	DefineCustomIntVariable(MAKE_EXTOPTION("max_cached_chunks_per_hypertable"),
							"Maximum cached chunks",
							"Maximum number of chunks stored in the cache",
							&ts_guc_max_cached_chunks_per_hypertable,
							1024,
							0,
							65536,
							PGC_USERSET,
							0,
							NULL,
							assign_max_cached_chunks_per_hypertable_hook,
							NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_job_execution_logging"),
							 "Enable job execution logging",
							 "Retain job run status in logging table",
							 &ts_guc_enable_job_execution_logging,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_tss_callbacks"),
							 "Enable ts_stat_statements callbacks",
							 "Enable ts_stat_statements callbacks",
							 &ts_guc_enable_tss_callbacks,
							 true,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_delete_after_compression"),
							 "Delete all rows after compression instead of truncate",
							 "Delete all rows after compression instead of truncate",
							 &ts_guc_enable_delete_after_compression,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

#ifdef USE_TELEMETRY
	DefineCustomEnumVariable(MAKE_EXTOPTION("telemetry_level"),
							 "Telemetry settings level",
							 "Level used to determine which telemetry to send",
							 (int *) &ts_guc_telemetry_level,
							 TELEMETRY_DEFAULT,
							 telemetry_level_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
#endif

	DefineCustomStringVariable(/* name= */ MAKE_EXTOPTION("compression_segmentby_default_function"),
							   /* short_desc= */ "Function that sets default segment_by",
							   /* long_desc= */
							   "Function to use for calculating default segment_by setting for "
							   "compression",
							   /* valueAddr= */ &ts_guc_default_segmentby_fn,
							   /* Value= */ "_timescaledb_functions.get_segmentby_defaults",
							   /* context= */ PGC_USERSET,
							   /* flags= */ 0,
							   /* check_hook= */ check_segmentby_func,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);

	DefineCustomStringVariable(/* name= */ MAKE_EXTOPTION("compression_orderby_default_function"),
							   /* short_desc= */ "Function that sets default order_by",
							   /* long_desc= */
							   "Function to use for calculating default order_by setting for "
							   "compression",
							   /* valueAddr= */ &ts_guc_default_orderby_fn,
							   /* Value= */ "_timescaledb_functions.get_orderby_defaults",
							   /* context= */ PGC_USERSET,
							   /* flags= */ 0,
							   /* check_hook= */ check_orderby_func,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);

	DefineCustomStringVariable(/* name= */ MAKE_EXTOPTION("license"),
							   /* short_desc= */ "TimescaleDB license type",
							   /* long_desc= */ "Determines which features are enabled",
							   /* valueAddr= */ &ts_guc_license,
							   /* bootValue= */ TS_LICENSE_DEFAULT,
							   /* context= */ PGC_SUSET,
							   /* flags= */ 0,
							   /* check_hook= */ ts_license_guc_check_hook,
							   /* assign_hook= */ ts_license_guc_assign_hook,
							   /* show_hook= */ NULL);

	DefineCustomStringVariable(/* name= */ MAKE_EXTOPTION("last_tuned"),
							   /* short_desc= */ "last tune run",
							   /* long_desc= */ "records last time timescaledb-tune ran",
							   /* valueAddr= */ &ts_last_tune_time,
							   /* bootValue= */ NULL,
							   /* context= */ PGC_SIGHUP,
							   /* flags= */ 0,
							   /* check_hook= */ NULL,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);

	DefineCustomStringVariable(/* name= */ MAKE_EXTOPTION("last_tuned_version"),
							   /* short_desc= */ "version of timescaledb-tune",
							   /* long_desc= */ "version of timescaledb-tune used to tune",
							   /* valueAddr= */ &ts_last_tune_version,
							   /* bootValue= */ NULL,
							   /* context= */ PGC_SIGHUP,
							   /* flags= */ 0,
							   /* check_hook= */ NULL,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);

	DefineCustomEnumVariable(MAKE_EXTOPTION("bgw_log_level"),
							 "Log level for the background worker subsystem",
							 "Log level for the scheduler and workers of the background worker "
							 "subsystem. Requires configuration reload to change.",
							 /* valueAddr= */ &ts_guc_bgw_log_level,
							 /* bootValue= */ WARNING,
							 /* options= */ loglevel_options,
							 /* context= */ PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/* this information is useful in general on customer deployments */
	DefineCustomBoolVariable(/* name= */ MAKE_EXTOPTION("debug_compression_path_info"),
							 /* short_desc= */ "show various compression-related debug info",
							 /* long_desc= */ "this is for debugging/information purposes",
							 /* valueAddr= */ &ts_guc_debug_compression_path_info,
							 /* bootValue= */ false,
							 /* context= */ PGC_USERSET,
							 /* flags= */ 0,
							 /* check_hook= */ NULL,
							 /* assign_hook= */ NULL,
							 /* show_hook= */ NULL);

	DefineCustomBoolVariable(/* name= */ MAKE_EXTOPTION("enable_rowlevel_compression_locking"),
							 /* short_desc= */ "Use rowlevel locking during compression",
							 /* long_desc= */ "Use only if you know what you are doing",
							 /* valueAddr= */ &ts_guc_enable_rowlevel_compression_locking,
							 /* bootValue= */ false,
							 /* context= */ PGC_USERSET,
							 /* flags= */ 0,
							 /* check_hook= */ NULL,
							 /* assign_hook= */ NULL,
							 /* show_hook= */ NULL);

#ifdef USE_TELEMETRY
	DefineCustomStringVariable(/* name= */ "timescaledb_telemetry.cloud",
							   /* short_desc= */ "cloud provider",
							   /* long_desc= */ "cloud provider used for this instance",
							   /* valueAddr= */ &ts_telemetry_cloud,
							   /* bootValue= */ NULL,
							   /* context= */ PGC_SIGHUP,
							   /* flags= */ 0,
							   /* check_hook= */ NULL,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);
#endif

#ifdef TS_DEBUG
	DefineCustomBoolVariable(/* name= */ MAKE_EXTOPTION("shutdown_bgw_scheduler"),
							 /* short_desc= */ "immediately shutdown the bgw scheduler",
							 /* long_desc= */ "this is for debugging purposes",
							 /* valueAddr= */ &ts_shutdown_bgw,
							 /* bootValue= */ false,
							 /* context= */ PGC_SIGHUP,
							 /* flags= */ 0,
							 /* check_hook= */ NULL,
							 /* assign_hook= */ NULL,
							 /* show_hook= */ NULL);

	DefineCustomStringVariable(/* name= */ MAKE_EXTOPTION("current_timestamp_mock"),
							   /* short_desc= */ "set the current timestamp",
							   /* long_desc= */ "this is for debugging purposes",
							   /* valueAddr= */ &ts_current_timestamp_mock,
							   /* bootValue= */ NULL,
							   /* context= */ PGC_USERSET,
							   /* flags= */ 0,
							   /* check_hook= */ NULL,
							   /* assign_hook= */ NULL,
							   /* show_hook= */ NULL);

	DefineCustomIntVariable(/* name= */ MAKE_EXTOPTION("debug_toast_tuple_target"),
							/* short_desc= */ "set toast tuple target on compressed chunks",
							/* long_desc= */ "this is for debugging purposes",
							/* valueAddr= */ &ts_guc_debug_toast_tuple_target,
							/* bootValue = */ 128,
							/* minValue = */ 1,
							/* maxValue = */ 65535,
							/* context= */ PGC_USERSET,
							/* flags= */ 0,
							/* check_hook= */ NULL,
							/* assign_hook= */ NULL,
							/* show_hook= */ NULL);

	DefineCustomEnumVariable(/* name= */ MAKE_EXTOPTION("debug_require_vector_agg"),
							 /* short_desc= */
							 "ensure that vectorized aggregation is used or not",
							 /* long_desc= */ "this is for debugging purposes",
							 /* valueAddr= */ (int *) &ts_guc_debug_require_vector_agg,
							 /* bootValue= */ DRO_Allow,
							 /* options = */ debug_require_options,
							 /* context= */ PGC_USERSET,
							 /* flags= */ 0,
							 /* check_hook= */ NULL,
							 /* assign_hook= */ NULL,
							 /* show_hook= */ NULL);

	DefineCustomEnumVariable(/* name= */ MAKE_EXTOPTION("debug_require_vector_qual"),
							 /* short_desc= */
							 "ensure that non-vectorized or vectorized filters are used in "
							 "DecompressChunk node",
							 /* long_desc= */
							 "this is for debugging purposes, to let us check if the vectorized "
							 "quals are used or not. EXPLAIN differs after PG15 for custom nodes, "
							 "and "
							 "using the test templates is a pain",
							 /* valueAddr= */ (int *) &ts_guc_debug_require_vector_qual,
							 /* bootValue= */ DRO_Allow,
							 /* options = */ debug_require_options,
							 /* context= */ PGC_USERSET,
							 /* flags= */ 0,
							 /* check_hook= */ NULL,
							 /* assign_hook= */ NULL,
							 /* show_hook= */ NULL);

	DefineCustomBoolVariable(/* name= */ MAKE_EXTOPTION("debug_require_batch_sorted_merge"),
							 /* short_desc= */ "require batch sorted merge in DecompressChunk node",
							 /* long_desc= */ "this is for debugging purposes",
							 /* valueAddr= */ &ts_guc_debug_require_batch_sorted_merge,
							 /* bootValue= */ false,
							 /* context= */ PGC_USERSET,
							 /* flags= */ 0,
							 /* check_hook= */ NULL,
							 /* assign_hook= */ NULL,
							 /* show_hook= */ NULL);

	DefineCustomBoolVariable(/* name= */ MAKE_EXTOPTION("debug_allow_cagg_with_deprecated_funcs"),
							 /* short_desc= */ "allow new caggs using time_bucket_ng",
							 /* long_desc= */ "this is for debugging/testing purposes",
							 /* valueAddr= */ &ts_guc_debug_allow_cagg_with_deprecated_funcs,
							 /* bootValue= */ false,
							 /* context= */ PGC_USERSET,
							 /* flags= */ 0,
							 /* check_hook= */ NULL,
							 /* assign_hook= */ NULL,
							 /* show_hook= */ NULL);
#endif

	/* register feature flags */
	ts_feature_flag_add(FEATURE_HYPERTABLE);
	ts_feature_flag_add(FEATURE_HYPERTABLE_COMPRESSION);
	ts_feature_flag_add(FEATURE_CAGG);
	ts_feature_flag_add(FEATURE_POLICY);

	gucs_are_initialized = true;

	validate_chunk_cache_sizes(ts_guc_max_cached_chunks_per_hypertable,
							   ts_guc_max_open_chunks_per_insert);
}
