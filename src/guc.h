/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "compat/compat.h"
#include "config.h"
#include "export.h"

#ifdef USE_TELEMETRY
extern bool ts_telemetry_on(void);
extern bool ts_function_telemetry_on(void);
#endif

extern bool ts_guc_enable_deprecation_warnings;
extern bool ts_guc_enable_optimizations;
extern bool ts_guc_enable_constraint_aware_append;
extern bool ts_guc_enable_ordered_append;
extern bool ts_guc_enable_chunk_append;
extern bool ts_guc_enable_parallel_chunk_append;
extern bool ts_guc_enable_qual_propagation;
extern bool ts_guc_enable_runtime_exclusion;
extern bool ts_guc_enable_constraint_exclusion;
extern bool ts_guc_enable_cagg_reorder_groupby;
extern TSDLLEXPORT bool ts_guc_enable_cagg_window_functions;
extern TSDLLEXPORT int ts_guc_cagg_max_individual_materializations;
extern bool ts_guc_enable_now_constify;
extern bool ts_guc_enable_foreign_key_propagation;
extern TSDLLEXPORT bool ts_guc_enable_osm_reads;
#if PG16_GE
extern TSDLLEXPORT bool ts_guc_enable_cagg_sort_pushdown;
#endif
extern TSDLLEXPORT bool ts_guc_enable_cagg_watermark_constify;
extern TSDLLEXPORT bool ts_guc_enable_dml_decompression;
extern TSDLLEXPORT bool ts_guc_enable_dml_decompression_tuple_filtering;
extern bool ts_guc_enable_direct_compress_copy;
extern bool ts_guc_enable_direct_compress_copy_sort_batches;
extern bool ts_guc_enable_direct_compress_copy_client_sorted;
extern int ts_guc_direct_compress_copy_tuple_sort_limit;
extern TSDLLEXPORT bool ts_guc_enable_direct_compress_insert;
extern bool ts_guc_enable_direct_compress_insert_sort_batches;
extern TSDLLEXPORT bool ts_guc_enable_direct_compress_insert_client_sorted;
extern TSDLLEXPORT bool ts_guc_enable_direct_compress_on_cagg_refresh;
extern int ts_guc_direct_compress_insert_tuple_sort_limit;
extern TSDLLEXPORT bool ts_guc_enable_compressed_direct_batch_delete;
extern TSDLLEXPORT int ts_guc_max_tuples_decompressed_per_dml;
extern TSDLLEXPORT bool ts_guc_enable_compression_wal_markers;
extern TSDLLEXPORT bool ts_guc_enable_decompression_sorted_merge;
extern TSDLLEXPORT bool ts_guc_enable_skip_scan;
extern TSDLLEXPORT bool ts_guc_enable_chunkwise_aggregation;
extern TSDLLEXPORT bool ts_guc_enable_vectorized_aggregation;
extern bool ts_guc_restoring;
extern int ts_guc_max_open_chunks_per_insert;
extern int ts_guc_max_cached_chunks_per_hypertable;
extern TSDLLEXPORT bool ts_guc_enable_job_execution_logging;
extern bool ts_guc_enable_tss_callbacks;
extern TSDLLEXPORT bool ts_guc_enable_delete_after_compression;
extern TSDLLEXPORT bool ts_guc_enable_merge_on_cagg_refresh;
extern bool ts_guc_enable_chunk_skipping;
extern TSDLLEXPORT bool ts_guc_enable_segmentwise_recompression;
extern TSDLLEXPORT bool ts_guc_enable_in_memory_recompression;
extern TSDLLEXPORT bool ts_guc_enable_exclusive_locking_recompression;
extern TSDLLEXPORT bool ts_guc_enable_bool_compression;
extern TSDLLEXPORT bool ts_guc_enable_uuid_compression;
extern TSDLLEXPORT int ts_guc_compression_batch_size_limit;
extern TSDLLEXPORT bool ts_guc_compression_enable_compressor_batch_limit;
#if PG16_GE
extern TSDLLEXPORT bool ts_guc_enable_skip_scan_for_distinct_aggregates;
#endif
extern bool ts_guc_enable_event_triggers;
extern TSDLLEXPORT bool ts_guc_enable_compressed_skip_scan;
extern TSDLLEXPORT bool ts_guc_enable_multikey_skip_scan;
extern TSDLLEXPORT double ts_guc_skip_scan_run_cost_multiplier;
extern TSDLLEXPORT bool ts_guc_debug_skip_scan_info;

/* Only settable in debug mode for testing */
extern TSDLLEXPORT bool ts_guc_enable_null_compression;
extern TSDLLEXPORT bool ts_guc_enable_compression_ratio_warnings;
extern bool ts_guc_enable_calendar_chunking;

typedef enum CompressTruncateBehaviour
{
	COMPRESS_TRUNCATE_ONLY,
	COMPRESS_TRUNCATE_OR_DELETE,
	COMPRESS_TRUNCATE_DISABLED,
} CompressTruncateBehaviour;
extern TSDLLEXPORT CompressTruncateBehaviour ts_guc_compress_truncate_behaviour;

#ifdef USE_TELEMETRY
typedef enum TelemetryLevel
{
	TELEMETRY_OFF,
	TELEMETRY_NO_FUNCTIONS,
	TELEMETRY_BASIC,
} TelemetryLevel;

extern TelemetryLevel ts_guc_telemetry_level;
extern char *ts_telemetry_cloud;
#endif

extern TSDLLEXPORT char *ts_guc_license;
extern TSDLLEXPORT bool ts_guc_enable_compression_indexscan;
extern TSDLLEXPORT bool ts_guc_enable_bulk_decompression;
extern TSDLLEXPORT bool ts_guc_auto_sparse_indexes;
extern TSDLLEXPORT bool ts_guc_enable_sparse_index_bloom;
extern TSDLLEXPORT bool ts_guc_read_legacy_bloom1_v1;
extern TSDLLEXPORT bool ts_guc_enable_columnarscan;
extern TSDLLEXPORT bool ts_guc_enable_columnarindexscan;
extern TSDLLEXPORT int ts_guc_bgw_log_level;

/*
 * Exit code to use when scheduler exits.
 *
 * Used for debugging.
 */
extern TSDLLEXPORT int ts_debug_bgw_scheduler_exit_status;
#ifdef TS_DEBUG
extern bool ts_shutdown_bgw;
extern char *ts_current_timestamp_mock;
#else
#define ts_shutdown_bgw false
#endif

extern TSDLLEXPORT int ts_guc_debug_toast_tuple_target;

typedef enum DebugRequireOption
{
	DRO_Allow = 0,
	DRO_Forbid,
	DRO_Require,
	DRO_Force,
} DebugRequireOption;

#ifdef TS_DEBUG
extern TSDLLEXPORT DebugRequireOption ts_guc_debug_require_vector_qual;

extern TSDLLEXPORT DebugRequireOption ts_guc_debug_require_vector_agg;

#endif

extern TSDLLEXPORT bool ts_guc_debug_compression_path_info;
extern TSDLLEXPORT bool ts_guc_enable_rowlevel_compression_locking;

extern TSDLLEXPORT DebugRequireOption ts_guc_debug_require_batch_sorted_merge;

extern TSDLLEXPORT bool ts_guc_debug_allow_cagg_with_deprecated_funcs;

extern bool ts_guc_enable_partitioned_hypertables;

void _guc_init(void);

typedef enum
{
	FEATURE_HYPERTABLE,
	FEATURE_HYPERTABLE_COMPRESSION,
	FEATURE_CAGG,
	FEATURE_POLICY
} FeatureFlagType;

extern TSDLLEXPORT void ts_feature_flag_check(FeatureFlagType);
extern TSDLLEXPORT Oid ts_guc_default_segmentby_fn_oid(void);
extern TSDLLEXPORT Oid ts_guc_default_orderby_fn_oid(void);
