/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>

#include <export.h>
#include <cross_module_fn.h>
#include <license_guc.h>

#include "planner.h"
#include "nodes/gapfill/gapfill.h"
#include "partialize_finalize.h"

#include "license.h"
#include "reorder.h"
#include "telemetry.h"
#include "bgw_policy/job.h"
#include "bgw_policy/reorder_api.h"
#include "bgw_policy/drop_chunks_api.h"
#include "bgw_policy/compress_chunks_api.h"
#include "compression/compression.h"
#include "compression/dictionary.h"
#include "compression/gorilla.h"
#include "compression/array.h"
#include "compression/deltadelta.h"
#include "continuous_aggs/create.h"
#include "continuous_aggs/drop.h"
#include "continuous_aggs/insert.h"
#include "continuous_aggs/materialize.h"
#include "continuous_aggs/options.h"
#include "nodes/decompress_chunk/planner.h"
#include "process_utility.h"
#include "hypertable.h"
#include "compression/create.h"
#include "compression/compress_utils.h"
#include "compression/segment_meta.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#ifdef APACHE_ONLY
#error "cannot compile the TSL for ApacheOnly mode"
#endif

extern void PGDLLEXPORT _PG_init(void);

static void module_shutdown(void);
static bool enterprise_enabled_internal(void);
static bool check_tsl_loaded(void);

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
	.tsl_license_on_assign = tsl_license_on_assign,
	.enterprise_enabled_internal = enterprise_enabled_internal,
	.check_tsl_loaded = check_tsl_loaded,
	.license_end_time = license_end_time,
	.print_tsl_license_expiration_info_hook = license_print_expiration_info,
	.module_shutdown_hook = module_shutdown,
	.add_tsl_license_info_telemetry = tsl_telemetry_add_license_info,
	.bgw_policy_job_execute = tsl_bgw_policy_job_execute,
	.continuous_agg_materialize = continuous_agg_materialize,
	.add_drop_chunks_policy = drop_chunks_add_policy,
	.add_reorder_policy = reorder_add_policy,
	.add_compress_chunks_policy = compress_chunks_add_policy,
	.remove_drop_chunks_policy = drop_chunks_remove_policy,
	.remove_reorder_policy = reorder_remove_policy,
	.remove_compress_chunks_policy = compress_chunks_remove_policy,
	.create_upper_paths_hook = tsl_create_upper_paths_hook,
	.set_rel_pathlist_dml = tsl_set_rel_pathlist_dml,
	.set_rel_pathlist_query = tsl_set_rel_pathlist_query,
	.gapfill_marker = gapfill_marker,
	.gapfill_int16_time_bucket = gapfill_int16_time_bucket,
	.gapfill_int32_time_bucket = gapfill_int32_time_bucket,
	.gapfill_int64_time_bucket = gapfill_int64_time_bucket,
	.gapfill_date_time_bucket = gapfill_date_time_bucket,
	.gapfill_timestamp_time_bucket = gapfill_timestamp_time_bucket,
	.gapfill_timestamptz_time_bucket = gapfill_timestamptz_time_bucket,
	.alter_job_schedule = bgw_policy_alter_job_schedule,
	.reorder_chunk = tsl_reorder_chunk,
	.move_chunk = tsl_move_chunk,
	.partialize_agg = tsl_partialize_agg,
	.finalize_agg_sfunc = tsl_finalize_agg_sfunc,
	.finalize_agg_ffunc = tsl_finalize_agg_ffunc,
	.process_cagg_viewstmt = tsl_process_continuous_agg_viewstmt,
	.continuous_agg_drop_chunks_by_chunk_id = ts_continuous_agg_drop_chunks_by_chunk_id,
	.continuous_agg_trigfn = continuous_agg_trigfn,
	.continuous_agg_update_options = continuous_agg_update_options,
	.compressed_data_decompress_forward = tsl_compressed_data_decompress_forward,
	.compressed_data_decompress_reverse = tsl_compressed_data_decompress_reverse,
	.compressed_data_send = tsl_compressed_data_send,
	.compressed_data_recv = tsl_compressed_data_recv,
	.compressed_data_in = tsl_compressed_data_in,
	.compressed_data_out = tsl_compressed_data_out,
	.deltadelta_compressor_append = tsl_deltadelta_compressor_append,
	.deltadelta_compressor_finish = tsl_deltadelta_compressor_finish,
	.gorilla_compressor_append = tsl_gorilla_compressor_append,
	.gorilla_compressor_finish = tsl_gorilla_compressor_finish,
	.dictionary_compressor_append = tsl_dictionary_compressor_append,
	.dictionary_compressor_finish = tsl_dictionary_compressor_finish,
	.array_compressor_append = tsl_array_compressor_append,
	.array_compressor_finish = tsl_array_compressor_finish,
	.process_compress_table = tsl_process_compress_table,
	.compress_chunk = tsl_compress_chunk,
	.decompress_chunk = tsl_decompress_chunk,
};

TS_FUNCTION_INFO_V1(ts_module_init);
/*
 * Module init function, sets ts_cm_functions to point at tsl_cm_functions
 */
PGDLLEXPORT Datum
ts_module_init(PG_FUNCTION_ARGS)
{
	ts_cm_functions = &tsl_cm_functions;

	_continuous_aggs_cache_inval_init();
	_decompress_chunk_init();

	PG_RETURN_BOOL(true);
}

/*
 * Currently we disallow shutting down this submodule in a live session,
 * but if we did, this would be the function we'd use.
 */
static void
module_shutdown(void)
{
	_continuous_aggs_cache_inval_fini();
	ts_cm_functions = &ts_cm_functions_default;
}

/* Informative functions */

static bool
enterprise_enabled_internal(void)
{
	return license_enterprise_enabled();
}

static bool
check_tsl_loaded(void)
{
	return true;
}

PGDLLEXPORT void
_PG_init(void)
{
	/*
	 * In a normal backend, we disable loading the tsl until after the main
	 * timescale library is loaded, after which we enable it from the loader.
	 * In parallel workers the restore shared libraries function will load the
	 * libraries itself, and we bypass the loader, so we need to ensure that
	 * timescale is aware it can  use the tsl if needed. It is always safe to
	 * do this here, because if we reach this point, we must have already
	 * loaded the tsl, so we no longer need to worry about its load order
	 * relative to the other libraries.
	 */
	ts_license_enable_module_loading();
}
