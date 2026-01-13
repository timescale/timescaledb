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

#include "compat/compat.h"
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
	{ "log", LOG, false },		 { "error", ERROR, false },	  { "fatal", FATAL, false },
	{ NULL, 0, false }
};

static const struct config_enum_entry compress_truncate_behaviour_options[] = {
	{ "truncate_only", COMPRESS_TRUNCATE_ONLY, false },
	{ "truncate_or_delete", COMPRESS_TRUNCATE_OR_DELETE, false },
	{ "truncate_disabled", COMPRESS_TRUNCATE_DISABLED, false },
	{ NULL, 0, false }
};

bool ts_guc_enable_direct_compress_copy = false;
bool ts_guc_enable_direct_compress_copy_sort_batches = true;
bool ts_guc_enable_direct_compress_copy_client_sorted = false;
int ts_guc_direct_compress_copy_tuple_sort_limit = 100000;
TSDLLEXPORT bool ts_guc_enable_direct_compress_insert = false;
bool ts_guc_enable_direct_compress_insert_sort_batches = true;
TSDLLEXPORT bool ts_guc_enable_direct_compress_insert_client_sorted = false;
TSDLLEXPORT bool ts_guc_enable_direct_compress_on_cagg_refresh = false;
int ts_guc_direct_compress_insert_tuple_sort_limit = 10000;
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
TSDLLEXPORT bool ts_guc_enable_cagg_window_functions = false;
bool ts_guc_enable_now_constify = true;
bool ts_guc_enable_foreign_key_propagation = true;
#if PG16_GE
TSDLLEXPORT bool ts_guc_enable_cagg_sort_pushdown = true;
#endif
TSDLLEXPORT bool ts_guc_enable_cagg_watermark_constify = true;
TSDLLEXPORT int ts_guc_cagg_max_individual_materializations = 10;
bool ts_guc_enable_osm_reads = true;
TSDLLEXPORT bool ts_guc_enable_compressed_direct_batch_delete = true;
TSDLLEXPORT bool ts_guc_enable_dml_decompression = true;
TSDLLEXPORT bool ts_guc_enable_dml_decompression_tuple_filtering = true;
TSDLLEXPORT int ts_guc_max_tuples_decompressed_per_dml = 100000;
TSDLLEXPORT bool ts_guc_enable_compression_wal_markers = false;
TSDLLEXPORT bool ts_guc_enable_decompression_sorted_merge = true;
bool ts_guc_enable_chunkwise_aggregation = true;
bool ts_guc_enable_vectorized_aggregation = true;
TSDLLEXPORT bool ts_guc_enable_compression_indexscan = false;
TSDLLEXPORT bool ts_guc_enable_bulk_decompression = true;
TSDLLEXPORT bool ts_guc_auto_sparse_indexes = true;
TSDLLEXPORT bool ts_guc_enable_sparse_index_bloom = true;

TSDLLEXPORT bool ts_guc_read_legacy_bloom1_v1 = false;

bool ts_guc_enable_chunk_skipping = false;
TSDLLEXPORT bool ts_guc_enable_segmentwise_recompression = true;
TSDLLEXPORT bool ts_guc_enable_in_memory_recompression = true;
TSDLLEXPORT bool ts_guc_enable_exclusive_locking_recompression = false;
TSDLLEXPORT bool ts_guc_enable_bool_compression = true;
TSDLLEXPORT bool ts_guc_enable_uuid_compression = true;
TSDLLEXPORT int ts_guc_compression_batch_size_limit = 1000;
TSDLLEXPORT bool ts_guc_compression_enable_compressor_batch_limit = false;
TSDLLEXPORT CompressTruncateBehaviour ts_guc_compress_truncate_behaviour = COMPRESS_TRUNCATE_ONLY;
bool ts_guc_enable_event_triggers = false;
bool ts_guc_debug_skip_scan_info = false;

/* Only settable in debug mode for testing */
TSDLLEXPORT bool ts_guc_enable_null_compression = true;
TSDLLEXPORT bool ts_guc_enable_compression_ratio_warnings = true;

/* Enable of disable columnar scans for columnar-oriented storage engines. If
 * disabled, regular sequence scans will be used instead. */
TSDLLEXPORT bool ts_guc_enable_columnarscan = true;
TSDLLEXPORT bool ts_guc_enable_columnarindexscan = true;
TSDLLEXPORT int ts_guc_bgw_log_level = WARNING;
TSDLLEXPORT bool ts_guc_enable_skip_scan = true;
#if PG16_GE
TSDLLEXPORT bool ts_guc_enable_skip_scan_for_distinct_aggregates = true;
#endif
TSDLLEXPORT bool ts_guc_enable_compressed_skip_scan = true;
TSDLLEXPORT bool ts_guc_enable_multikey_skip_scan = true;
TSDLLEXPORT double ts_guc_skip_scan_run_cost_multiplier = 1.0;
static char *ts_guc_default_segmentby_fn = NULL;
static char *ts_guc_default_orderby_fn = NULL;
TSDLLEXPORT bool ts_guc_enable_job_execution_logging = false;
bool ts_guc_enable_tss_callbacks = true;
TSDLLEXPORT bool ts_guc_enable_delete_after_compression = false;
TSDLLEXPORT bool ts_guc_enable_merge_on_cagg_refresh = false;

bool ts_guc_enable_partitioned_hypertables = false;

/* default value of ts_guc_max_open_chunks_per_insert and
 * ts_guc_max_cached_chunks_per_hypertable will be set as their respective boot-value when the
 * GUC mechanism starts up */
int ts_guc_max_open_chunks_per_insert;
int ts_guc_max_cached_chunks_per_hypertable;
#ifdef USE_TELEMETRY
TelemetryLevel ts_guc_telemetry_level = TELEMETRY_DEFAULT;
char *ts_telemetry_cloud = NULL;
#endif

TSDLLEXPORT char *ts_guc_license = TS_LICENSE_DEFAULT;

bool ts_guc_debug_allow_cagg_with_deprecated_funcs = false;

/*
 * Exit code for the scheduler.
 *
 * Normally it exits with a zero which means that it will not restart. If an
 * error is raised, it exits with error code 1, which will trigger a
 * restart.
 *
 * This variable exists to be able to trigger a restart for a normal exit,
 * which is useful when debugging.
 *
 * See backend/postmaster/bgworker.c
 */
int ts_debug_bgw_scheduler_exit_status = 0;

#ifdef TS_DEBUG
bool ts_shutdown_bgw = false;
char *ts_current_timestamp_mock = NULL;
#endif

int ts_guc_debug_toast_tuple_target = 128;

static const struct config_enum_entry debug_require_options[] = { { "allow", DRO_Allow, false },
																  { "forbid", DRO_Forbid, false },
																  { "require", DRO_Require, false },
																  { "force", DRO_Force, false },
																  { NULL, 0, false } };

#ifdef TS_DEBUG

bool ts_guc_debug_have_int128;

DebugRequireOption ts_guc_debug_require_vector_qual = DRO_Allow;

DebugRequireOption ts_guc_debug_require_vector_agg = DRO_Allow;
#endif

DebugRequireOption ts_guc_debug_require_batch_sorted_merge = false;

bool ts_guc_debug_compression_path_info = false;
bool ts_guc_enable_rowlevel_compression_locking = false;

static bool ts_guc_enable_hypertable_create = true;
static bool ts_guc_enable_hypertable_compression = true;
static bool ts_guc_enable_cagg_create = true;
static bool ts_guc_enable_policy_create = true;
bool ts_guc_enable_calendar_chunking = false;

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
			 errmsg("You are using a PostgreSQL service. This feature is only available on "
					"Time-series and analytics services. "
					"https://docs.timescale.com/use-timescale/latest/services/")));
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

/*
 * Assign hook for chunk skipping.
 *
 * When chunk skipping is enabled, we need to clear the hypertable cache.
 * Otherwise there might be cached entries without a valid range_space entry,
 * which could lead to column stats not being created.
 */
static void
chunk_skipping_assign_hook(bool newval, void *extra)
{
	if (newval)
		ts_hypertable_cache_invalidate_callback();
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

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_direct_compress_copy"),
							 "Enable direct compression during COPY",
							 "Enable experimental support for direct compression during COPY",
							 &ts_guc_enable_direct_compress_copy,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_direct_compress_copy_sort_batches"),
							 "Enable batch sorting during direct compress COPY",
							 NULL,
							 &ts_guc_enable_direct_compress_copy_sort_batches,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_direct_compress_copy_client_sorted"),
							 "Enable direct compress COPY with presorted data",
							 "Correct handling of data sorting by the user is required for this "
							 "option.",
							 &ts_guc_enable_direct_compress_copy_client_sorted,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable(MAKE_EXTOPTION("direct_compress_copy_tuple_sort_limit"),
							"Number of tuples that can be sorted at once in a COPY operation",
							"This is mainly used to keep the memory footprint down for "
							"operations like importing large amounts of data in "
							"single transaction. Setting this to 0 would make it unlimited.",
							&ts_guc_direct_compress_copy_tuple_sort_limit,
							100000,
							0,
							2147483647,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_direct_compress_insert"),
							 "Enable direct compression during INSERT",
							 "Enable experimental support for direct compression during INSERT",
							 &ts_guc_enable_direct_compress_insert,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_direct_compress_insert_sort_batches"),
							 "Enable batch sorting during direct compress INSERT",
							 NULL,
							 &ts_guc_enable_direct_compress_insert_sort_batches,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_direct_compress_insert_client_sorted"),
							 "Enable direct compress INSERT with presorted data",
							 "Correct handling of data sorting by the user is required for this "
							 "option.",
							 &ts_guc_enable_direct_compress_insert_client_sorted,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_direct_compress_on_cagg_refresh"),
							 "Enable direct compress on Continuous Aggregate refresh",
							 "Enable experimental support for direct compression during Continuous "
							 "Aggregate refresh",
							 &ts_guc_enable_direct_compress_on_cagg_refresh,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable(MAKE_EXTOPTION("direct_compress_insert_tuple_sort_limit"),
							"Number of tuples that can be sorted at once in an INSERT operation",
							"This is mainly used to keep the memory footprint down for "
							"operations like importing large amounts of data in "
							"single transaction. Setting this to 0 would make it unlimited.",
							&ts_guc_direct_compress_insert_tuple_sort_limit,
							10000,
							0,
							2147483647,
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
							 "Enable restoring mode for timescaledb",
							 "In restoring mode all timescaledb internal hooks are disabled. This "
							 "mode is required for restoring logical dumps of databases with "
							 "timescaledb.",
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
#if PG16_GE
	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_skipscan_for_distinct_aggregates"),
							 "Enable SkipScan for DISTINCT aggregates",
							 "Enable SkipScan for DISTINCT aggregates",
							 &ts_guc_enable_skip_scan_for_distinct_aggregates,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
#endif

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_compressed_skipscan"),
							 "Enable SkipScan for compressed chunks",
							 "Enable SkipScan for distinct inputs over compressed chunks",
							 &ts_guc_enable_compressed_skip_scan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_multikey_skipscan"),
							 "Enable SkipScan for multiple distinct keys",
							 "Enable SkipScan for multiple distinct inputs",
							 &ts_guc_enable_multikey_skip_scan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomRealVariable(MAKE_EXTOPTION("skip_scan_run_cost_multiplier"),
							 "Multiplier for SkipScan run cost as an option to make the cost "
							 "smaller so that SkipScan can be chosen",
							 "Default is 1.0 i.e. regularly estimated SkipScan run cost, 0.0 will "
							 "make SkipScan to have run cost = 0",
							 &ts_guc_skip_scan_run_cost_multiplier,
							 1.0,
							 0.0,
							 1.0,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("debug_skip_scan_info"),
							 "Print debug info about SkipScan",
							 "Print debug info about SkipScan distinct columns",
							 &ts_guc_debug_skip_scan_info,
							 false,
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

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_cagg_window_functions"),
							 "Enable window functions in continuous aggregates",
							 "Allow window functions in continuous aggregate views",
							 &ts_guc_enable_cagg_window_functions,
							 false,
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

#if PG16_GE
	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_cagg_sort_pushdown"),
							 "Enable sort pushdown for continuous aggregates",
							 "Enable pushdown of ORDER BY clause for continuous aggregates",
							 &ts_guc_enable_cagg_sort_pushdown,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
#endif

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

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_chunk_skipping"),
							 "Enable chunk skipping functionality",
							 "Enable using chunk column stats to filter chunks based on column "
							 "filters",
							 &ts_guc_enable_chunk_skipping,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 chunk_skipping_assign_hook,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_segmentwise_recompression"),
							 "Enable segmentwise recompression functionality",
							 "Enable segmentwise recompression",
							 &ts_guc_enable_segmentwise_recompression,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_in_memory_recompression"),
							 "Enable in-memory recompression functionality",
							 "Enable in-memory recompression",
							 &ts_guc_enable_in_memory_recompression,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_exclusive_locking_recompression"),
							 "Enable exclusive locking recompression",
							 "Enable getting exclusive lock on chunk during segmentwise "
							 "recompression",
							 &ts_guc_enable_exclusive_locking_recompression,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_bool_compression"),
							 "Enable bool compression functionality",
							 "Enable bool compression",
							 &ts_guc_enable_bool_compression,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_uuid_compression"),
							 "Enable uuid compression functionality",
							 "Enable uuid compression",
							 &ts_guc_enable_uuid_compression,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable(MAKE_EXTOPTION("compression_batch_size_limit"),
							"The max number of tuples that can be batched together during "
							"compression",
							"Setting this option to a number between 1 and 999 will force "
							"compression "
							"to limit the size of compressed batches to that amount of "
							"uncompressed tuples."
							"Setting this to 0 defaults to the max batch size of 1000.",
							&ts_guc_compression_batch_size_limit,
							1000,
							1,
							1000,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);
	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_compressor_batch_limit"),
							 "Enable compressor batch limit",
							 "Enable compressor batch limit for compressors which "
							 "can go over the allocation limit (1 GB). This feature will "
							 "limit those compressors by reducing the size of the batch and thus "
							 "avoid hitting the limit.",
							 &ts_guc_compression_enable_compressor_batch_limit,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_event_triggers"),
							 "Enable event triggers for chunks creation",
							 "Enable event triggers for chunks creation",
							 &ts_guc_enable_event_triggers,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

#ifdef TS_DEBUG
	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_null_compression"),
							 "Debug only flag to enable NULL compression",
							 "Enable null compression",
							 &ts_guc_enable_null_compression,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
#endif

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_compression_ratio_warnings"),
							 "Enable warnings for poor compression ratio",
							 "Enable warnings for poor compression ratio",
							 &ts_guc_enable_compression_ratio_warnings,
							 true,
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

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_sparse_index_bloom"),
							 "Enable creation of the bloom1 sparse index on compressed chunks",
							 "This sparse index speeds up the equality queries on compressed "
							 "columns, and can be disabled when not desired.",
							 &ts_guc_enable_sparse_index_bloom,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("read_legacy_bloom1_v1"),
							 "Enable reading the legacy bloom1 version 1 sparse indexes for SELECT "
							 "queries",
							 "These legacy indexes might give false negatives if they were built "
							 "by the TimescaleDB extension compiled with different build options.",
							 &ts_guc_read_legacy_bloom1_v1,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_columnarscan"),
							 "Enable ColumnarScan for columnar storage",
							 "Transparently decompress columnar data using ColumnarScan custom "
							 "node. Disabling columnar scan will ignore data stored in columnar "
							 "format in queries.",
							 &ts_guc_enable_columnarscan,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_columnarindexscan"),
							 "Enable metadata-only optimization for ColumnarScans",
							 "Enable returning results directly from compression "
							 "metadata without decompression",
							 &ts_guc_enable_columnarindexscan,
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

	DefineCustomEnumVariable(MAKE_EXTOPTION("compress_truncate_behaviour"),
							 "Define behaviour of truncate after compression",
							 "Defines how truncate behaves at the end of compression. "
							 "'truncate_only' forces truncation. 'truncate_disabled' deletes rows "
							 "instead of truncate. 'truncate_or_delete' allows falling back to "
							 "deletion.",
							 (int *) &ts_guc_compress_truncate_behaviour,
							 COMPRESS_TRUNCATE_ONLY,
							 compress_truncate_behaviour_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

#ifdef TS_DEBUG
	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_partitioned_hypertables"),
							 "Enable hypertables using declarative partitioning",
							 "Enable experimental support for creating hypertables using "
							 "PostgreSQL's native declarative partitioning",
							 &ts_guc_enable_partitioned_hypertables,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
#endif

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

	DefineCustomIntVariable(/* name= */ MAKE_EXTOPTION("debug_bgw_scheduler_exit_status"),
							/* short_desc= */ "exit status to use when shutting down the scheduler",
							/* long_desc= */ "this is for debugging purposes",
							/* valueAddr= */ &ts_debug_bgw_scheduler_exit_status,
							/* bootValue= */ 0,
							/* minValue= */ 0,
							/* maxValue= */ 255,
							/* context= */ PGC_SIGHUP,
							/* flags= */ 0,
							/* check_hook= */ NULL,
							/* assign_hook= */ NULL,
							/* show_hook= */ NULL);

	DefineCustomEnumVariable(/* name= */ MAKE_EXTOPTION("debug_require_batch_sorted_merge"),
							 /* short_desc= */ "require batch sorted merge in ColumnarScan node",
							 /* long_desc= */ "this is for debugging purposes",
							 /* valueAddr= */ (int *) &ts_guc_debug_require_batch_sorted_merge,
							 /* bootValue= */ DRO_Allow,
							 /* options = */ debug_require_options,
							 /* context= */ PGC_USERSET,
							 /* flags= */ 0,
							 /* check_hook= */ NULL,
							 /* assign_hook= */ NULL,
							 /* show_hook= */ NULL);

	DefineCustomBoolVariable(MAKE_EXTOPTION("enable_calendar_chunking"),
							 "Create chunks aligned with calendar.",
							 "When enabled, chunks are created so that they align "
							 "with the start and end of, e.g., days, months, and years. "
							 "This only applies to hypertables that use a primary partitioning "
							 "dimension that uses TIMESTAMPTZ, DATE, and UUID (version 7).",
							 &ts_guc_enable_calendar_chunking,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

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

	DefineCustomBoolVariable(/* name= */ MAKE_EXTOPTION("debug_have_int128"),
							 /* short_desc= */ "whether we have int128 support",
							 /* long_desc= */ "this is for debugging purposes",
							 /* valueAddr= */ &ts_guc_debug_have_int128,
#ifdef HAVE_INT128
							 /* bootValue= */ true,
#else
							 /* bootValue= */ false,
#endif
							 /* context= */ PGC_INTERNAL,
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
							 "ColumnarScan node",
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
