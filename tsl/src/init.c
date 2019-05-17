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
#include "server.h"
#include "fdw/timescaledb_fdw.h"
#include "chunk_api.h"
#include "hypertable.h"
#include "compat.h"

#if PG_VERSION_SUPPORTS_MULTINODE
#include "remote/connection_cache.h"
#include "remote/dist_txn.h"
#include "remote/txn_id.h"
#include "remote/txn_resolve.h"
#include "server_dispatch.h"
#include "remote/dist_copy.h"
#include "process_utility.h"
#include "dist_util.h"
#endif

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

static void
cache_syscache_invalidate(Datum arg, int cacheid, uint32 hashvalue)
{
	/*
	 * Using hash_value it is possible to do more fine grained validation in
	 * the future see `postgres_fdw` connection management for an example. For
	 * now, invalidate the entire cache.
	 */
#if PG_VERSION_SUPPORTS_MULTINODE
	remote_connection_cache_invalidate_callback();
#endif
}

#if !PG_VERSION_SUPPORTS_MULTINODE

static Datum
empty_fn(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

static void
error_not_supported(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function is not supported under the current PostgreSQL version %s",
					PG_VERSION),
			 errhint("Upgrade PostgreSQL to version %s or greater.",
					 PG_VERSION_MINIMUM_FOR_MULTINODE)));
	pg_unreachable();
}

static Datum
error_not_supported_default_fn(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function \"%s\" is not supported under the current PostgreSQL version %s",
					get_func_name(fcinfo->flinfo->fn_oid),
					PG_VERSION),
			 errhint("Upgrade PostgreSQL to version %s or greater.",
					 PG_VERSION_MINIMUM_FOR_MULTINODE)));
	pg_unreachable();
}

static List *
error_get_serverlist_not_supported(void)
{
	error_not_supported();
	pg_unreachable();
}

static void
error_hypertable_make_distributed_not_supported(Hypertable *ht, ArrayType *servers)
{
	error_not_supported();
	pg_unreachable();
}

static void
error_create_chunk_on_servers_not_supported(Chunk *chunk, Hypertable *ht)
{
	error_not_supported();
	pg_unreachable();
}

static Path *
error_server_dispatch_path_create_not_supported(PlannerInfo *root, ModifyTablePath *mtpath,
												Index hypertable_rti, int subpath_index)
{
	error_not_supported();
	pg_unreachable();
}

static void
error_distributed_copy_not_supported(const CopyStmt *stmt, uint64 *processed,
									 CopyChunkState *ccstate, List *attnums)
{
	error_not_supported();
	pg_unreachable();
}

static Datum
error_server_set_block_new_chunks_not_supported(PG_FUNCTION_ARGS, bool block)
{
	error_not_supported();
	pg_unreachable();
}

#endif /* PG_VERSION_SUPPORTS_MULTINODE */

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
#if !PG_VERSION_SUPPORTS_MULTINODE
	.add_server = error_not_supported_default_fn,
	.delete_server = error_not_supported_default_fn,
	.attach_server = error_not_supported_default_fn,
	.detach_server = error_not_supported_default_fn,
	.server_set_block_new_chunks = error_server_set_block_new_chunks_not_supported,
	.set_chunk_default_server = error_not_supported_default_fn,
	.show_chunk = error_not_supported_default_fn,
	.create_chunk = error_not_supported_default_fn,
	.create_chunk_on_servers = error_create_chunk_on_servers_not_supported,
	.get_servername_list = error_get_serverlist_not_supported,
	.hypertable_make_distributed = error_hypertable_make_distributed_not_supported,
	.timescaledb_fdw_handler = error_not_supported_default_fn,
	.timescaledb_fdw_validator = empty_fn,
	.remote_txn_id_in = error_not_supported_default_fn,
	.remote_txn_id_out = error_not_supported_default_fn,
	.set_rel_pathlist = NULL,
	.server_dispatch_path_create = error_server_dispatch_path_create_not_supported,
	.distributed_copy = error_distributed_copy_not_supported,
	.ddl_command_start = NULL,
	.ddl_command_end = NULL,
	.sql_drop = NULL,
	.set_distributed_id = NULL,
	.set_distributed_peer_id = NULL,
	.is_frontend_session = NULL,
	.remove_from_distributed_db = NULL,
	.remote_hypertable_info = error_not_supported_default_fn,
#else
	.add_server = server_add,
	.delete_server = server_delete,
	.attach_server = server_attach,
	.server_ping = server_ping,
	.detach_server = server_detach,
	.server_set_block_new_chunks = server_set_block_new_chunks,
	.set_chunk_default_server = server_set_chunk_default_server,
	.show_chunk = chunk_show,
	.create_chunk = chunk_create,
	.create_chunk_on_servers = chunk_api_create_on_servers,
	.get_servername_list = server_get_servername_list,
	.hypertable_make_distributed = hypertable_make_distributed,
	.timescaledb_fdw_handler = timescaledb_fdw_handler,
	.timescaledb_fdw_validator = timescaledb_fdw_validator,
	.remote_txn_id_in = remote_txn_id_in_pg,
	.remote_txn_id_out = remote_txn_id_out_pg,
	.remote_txn_heal_server = remote_txn_heal_server,
	.set_rel_pathlist = tsl_set_rel_pathlist,
	.server_dispatch_path_create = server_dispatch_path_create,
	.distributed_copy = remote_distributed_copy,
	.ddl_command_start = tsl_ddl_command_start,
	.ddl_command_end = tsl_ddl_command_end,
	.sql_drop = tsl_sql_drop,
	.set_distributed_id = dist_util_set_id,
	.set_distributed_peer_id = dist_util_set_peer_id,
	.is_frontend_session = dist_util_is_frontend_session,
	.remove_from_distributed_db = dist_util_remove_from_db,
	.remote_hypertable_info = dist_util_remote_hypertable_info,
#endif
	.cache_syscache_invalidate = cache_syscache_invalidate,
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
#if PG_VERSION_SUPPORTS_MULTINODE
	_remote_connection_cache_init();
	_remote_dist_txn_init();
	_tsl_process_utility_init();
#endif

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

	/*
	 * Order of items should be strict reverse order of ts_module_init. Please
	 * document any exceptions.
	 */
#if PG_VERSION_SUPPORTS_MULTINODE
	_remote_dist_txn_fini();
	_remote_connection_cache_fini();
	_tsl_process_utility_fini();
#endif

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
