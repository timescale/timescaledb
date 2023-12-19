/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <storage/ipc.h>

#include "bgw_policy/compression_api.h"
#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/job.h"
#include "bgw_policy/job_api.h"
#include "bgw_policy/policies_v2.h"
#include "bgw_policy/reorder_api.h"
#include "bgw_policy/retention_api.h"
#include "chunk.h"
#include "chunk_api.h"
#include "compression/algorithms/array.h"
#include "compression/algorithms/deltadelta.h"
#include "compression/algorithms/dictionary.h"
#include "compression/algorithms/gorilla.h"
#include "compression/api.h"
#include "compression/arrow_cache_explain.h"
#include "compression/compression.h"
#include "compression/compressionam_handler.h"
#include "compression/create.h"
#include "compression/segment_meta.h"
#include "config.h"
#include "continuous_aggs/create.h"
#include "continuous_aggs/insert.h"
#include "continuous_aggs/invalidation.h"
#include "continuous_aggs/options.h"
#include "continuous_aggs/refresh.h"
#include "continuous_aggs/repair.h"
#include "continuous_aggs/utils.h"
#include "cross_module_fn.h"
#include "export.h"
#include "hypertable.h"
#include "license_guc.h"
#include "nodes/columnar_scan/columnar_scan.h"
#include "nodes/decompress_chunk/planner.h"
#include "nodes/gapfill/gapfill_functions.h"
#include "nodes/skip_scan/skip_scan.h"
#include "nodes/vector_agg/plan.h"
#include "partialize_finalize.h"
#include "planner.h"
#include "process_utility.h"
#include "reorder.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#ifdef APACHE_ONLY
#error "cannot compile the TSL for ApacheOnly mode"
#endif

#if PG16_LT
extern void PGDLLEXPORT _PG_init(void);
#endif

static void
tsl_xact_event(XactEvent event, void *arg)
{
	compressionam_xact_event(event, arg);
}

/*
 * Cross module function initialization.
 *
 * During module start we set ts_cm_functions to point at the tsl version of the
 * function registry.
 *
 * NOTE: To ensure that your cross-module function has a correct default, you
 * must also add it to ts_cm_functions_default in cross_module_fn.c in the
 * Apache codebase.
 */
CrossModuleFunctions tsl_cm_functions = {

	.create_upper_paths_hook = tsl_create_upper_paths_hook,
	.set_rel_pathlist_dml = tsl_set_rel_pathlist_dml,
	.set_rel_pathlist_query = tsl_set_rel_pathlist_query,
	.process_explain_def = tsl_process_explain_def,

	/* bgw policies */
	.policy_compression_add = policy_compression_add,
	.policy_compression_remove = policy_compression_remove,
	.policy_recompression_proc = policy_recompression_proc,
	.policy_compression_check = policy_compression_check,
	.policy_refresh_cagg_add = policy_refresh_cagg_add,
	.policy_refresh_cagg_proc = policy_refresh_cagg_proc,
	.policy_refresh_cagg_check = policy_refresh_cagg_check,
	.policy_refresh_cagg_remove = policy_refresh_cagg_remove,
	.policy_reorder_add = policy_reorder_add,
	.policy_reorder_proc = policy_reorder_proc,
	.policy_reorder_check = policy_reorder_check,
	.policy_reorder_remove = policy_reorder_remove,
	.policy_retention_add = policy_retention_add,
	.policy_retention_proc = policy_retention_proc,
	.policy_retention_check = policy_retention_check,
	.policy_retention_remove = policy_retention_remove,

	.job_add = job_add,
	.job_alter = job_alter,
	.job_alter_set_hypertable_id = job_alter_set_hypertable_id,
	.job_delete = job_delete,
	.job_run = job_run,
	.job_execute = job_execute,

	/* gapfill */
	.gapfill_marker = gapfill_marker,
	.gapfill_int16_time_bucket = gapfill_int16_time_bucket,
	.gapfill_int32_time_bucket = gapfill_int32_time_bucket,
	.gapfill_int64_time_bucket = gapfill_int64_time_bucket,
	.gapfill_date_time_bucket = gapfill_date_time_bucket,
	.gapfill_timestamp_time_bucket = gapfill_timestamp_time_bucket,
	.gapfill_timestamptz_time_bucket = gapfill_timestamptz_time_bucket,
	.gapfill_timestamptz_timezone_time_bucket = gapfill_timestamptz_timezone_time_bucket,

	.reorder_chunk = tsl_reorder_chunk,
	.move_chunk = tsl_move_chunk,

	.policies_add = policies_add,
	.policies_remove = policies_remove,
	.policies_remove_all = policies_remove_all,
	.policies_alter = policies_alter,
	.policies_show = policies_show,

	/* Vectorized queries */
	.tsl_postprocess_plan = tsl_postprocess_plan,

	/* Continuous Aggregates */
	.partialize_agg = tsl_partialize_agg,
	.finalize_agg_sfunc = tsl_finalize_agg_sfunc,
	.finalize_agg_ffunc = tsl_finalize_agg_ffunc,
	.process_cagg_viewstmt = tsl_process_continuous_agg_viewstmt,
	.continuous_agg_invalidation_trigger = continuous_agg_trigfn,
	.continuous_agg_call_invalidation_trigger = execute_cagg_trigger,
	.continuous_agg_refresh = continuous_agg_refresh,
	.continuous_agg_invalidate_raw_ht = continuous_agg_invalidate_raw_ht,
	.continuous_agg_invalidate_mat_ht = continuous_agg_invalidate_mat_ht,
	.continuous_agg_update_options = continuous_agg_update_options,
	.continuous_agg_validate_query = continuous_agg_validate_query,
	.continuous_agg_get_bucket_function = continuous_agg_get_bucket_function,
	.continuous_agg_get_bucket_function_info = continuous_agg_get_bucket_function_info,
	.continuous_agg_migrate_to_time_bucket = continuous_agg_migrate_to_time_bucket,
	.cagg_try_repair = tsl_cagg_try_repair,

	/* Compression */
	.compressed_data_decompress_forward = tsl_compressed_data_decompress_forward,
	.compressed_data_decompress_reverse = tsl_compressed_data_decompress_reverse,
	.compressed_data_send = tsl_compressed_data_send,
	.compressed_data_recv = tsl_compressed_data_recv,
	.compressed_data_in = tsl_compressed_data_in,
	.compressed_data_out = tsl_compressed_data_out,
	.compressed_data_info = tsl_compressed_data_info,
	.deltadelta_compressor_append = tsl_deltadelta_compressor_append,
	.deltadelta_compressor_finish = tsl_deltadelta_compressor_finish,
	.gorilla_compressor_append = tsl_gorilla_compressor_append,
	.gorilla_compressor_finish = tsl_gorilla_compressor_finish,
	.dictionary_compressor_append = tsl_dictionary_compressor_append,
	.dictionary_compressor_finish = tsl_dictionary_compressor_finish,
	.array_compressor_append = tsl_array_compressor_append,
	.array_compressor_finish = tsl_array_compressor_finish,
	.process_compress_table = tsl_process_compress_table,
	.process_altertable_cmd = tsl_process_altertable_cmd,
	.process_rename_cmd = tsl_process_rename_cmd,
	.compress_chunk = tsl_compress_chunk,
	.decompress_chunk = tsl_decompress_chunk,
	.decompress_batches_for_insert = decompress_batches_for_insert,
	.decompress_target_segments = decompress_target_segments,
	.compressionam_handler = compressionam_handler,
	.ddl_command_start = tsl_ddl_command_start,
	.ddl_command_end = tsl_ddl_command_end,
	.show_chunk = chunk_show,
	.create_compressed_chunk = tsl_create_compressed_chunk,
	.create_chunk = chunk_create,
	.chunk_freeze_chunk = chunk_freeze_chunk,
	.chunk_unfreeze_chunk = chunk_unfreeze_chunk,
	.set_rel_pathlist = tsl_set_rel_pathlist,
	.chunk_create_empty_table = chunk_create_empty_table,
	.recompress_chunk_segmentwise = tsl_recompress_chunk_segmentwise,
	.get_compressed_chunk_index_for_recompression =
		tsl_get_compressed_chunk_index_for_recompression,
	.preprocess_query_tsl = tsl_preprocess_query,
};

static void
ts_module_cleanup_on_pg_exit(int code, Datum arg)
{
	_continuous_aggs_cache_inval_fini();
	UnregisterXactCallback(tsl_xact_event, NULL);
}

TS_FUNCTION_INFO_V1(ts_module_init);
/*
 * Module init function, sets ts_cm_functions to point at tsl_cm_functions
 */
PGDLLEXPORT Datum
ts_module_init(PG_FUNCTION_ARGS)
{
	bool register_proc_exit = PG_GETARG_BOOL(0);
	ts_cm_functions = &tsl_cm_functions;

	_continuous_aggs_cache_inval_init();
	_decompress_chunk_init();
	_columnar_scan_init();
	_arrow_cache_explain_init();
	_skip_scan_init();
	_vector_agg_init();
	/* Register a cleanup function to be called when the backend exits */
	if (register_proc_exit)
		on_proc_exit(ts_module_cleanup_on_pg_exit, 0);

	RegisterXactCallback(tsl_xact_event, NULL);
	PG_RETURN_BOOL(true);
}

/* Informative functions */

PGDLLEXPORT void
_PG_init(void)
{
	/*
	 * In a normal backend, we disable loading the tsl until after the main
	 * timescale library is loaded, after which we enable it from the loader.
	 * In parallel workers the restore shared libraries function will load the
	 * libraries itself, and we bypass the loader, so we need to ensure that
	 * timescale is aware it can use the tsl if needed. It is always safe to
	 * do this here, because if we reach this point, we must have already
	 * loaded the tsl, so we no longer need to worry about its load order
	 * relative to the other libraries.
	 */
	ts_license_enable_module_loading();
}
