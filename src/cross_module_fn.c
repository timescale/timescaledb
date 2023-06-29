/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>

#include "export.h"
#include "cross_module_fn.h"
#include "guc.h"
#include "license_guc.h"
#include "bgw/job.h"
#ifdef USE_TELEMETRY
#include "telemetry/telemetry.h"
#endif

#define CROSSMODULE_WRAPPER(func)                                                                  \
	TS_FUNCTION_INFO_V1(ts_##func);                                                                \
	Datum ts_##func(PG_FUNCTION_ARGS)                                           \
	{                                                                                              \
		PG_RETURN_DATUM(ts_cm_functions->func(fcinfo));                                            \
	}

/* bgw policy functions */
CROSSMODULE_WRAPPER(policy_compression_add);
CROSSMODULE_WRAPPER(policy_compression_remove);
CROSSMODULE_WRAPPER(policy_recompression_proc);
CROSSMODULE_WRAPPER(policy_compression_check);
CROSSMODULE_WRAPPER(policy_refresh_cagg_add);
CROSSMODULE_WRAPPER(policy_refresh_cagg_proc);
CROSSMODULE_WRAPPER(policy_refresh_cagg_check);
CROSSMODULE_WRAPPER(policy_refresh_cagg_remove);
CROSSMODULE_WRAPPER(policy_reorder_add);
CROSSMODULE_WRAPPER(policy_reorder_proc);
CROSSMODULE_WRAPPER(policy_reorder_check);
CROSSMODULE_WRAPPER(policy_reorder_remove);
CROSSMODULE_WRAPPER(policy_retention_add);
CROSSMODULE_WRAPPER(policy_retention_proc);
CROSSMODULE_WRAPPER(policy_retention_check);
CROSSMODULE_WRAPPER(policy_retention_remove);

CROSSMODULE_WRAPPER(job_add);
CROSSMODULE_WRAPPER(job_delete);
CROSSMODULE_WRAPPER(job_run);
CROSSMODULE_WRAPPER(job_alter);
CROSSMODULE_WRAPPER(job_alter_set_hypertable_id);

CROSSMODULE_WRAPPER(reorder_chunk);
CROSSMODULE_WRAPPER(move_chunk);
CROSSMODULE_WRAPPER(move_chunk_proc);
CROSSMODULE_WRAPPER(copy_chunk_proc);
CROSSMODULE_WRAPPER(copy_chunk_cleanup_proc);
CROSSMODULE_WRAPPER(subscription_exec);

CROSSMODULE_WRAPPER(policies_add);
CROSSMODULE_WRAPPER(policies_remove);
CROSSMODULE_WRAPPER(policies_remove_all);
CROSSMODULE_WRAPPER(policies_alter);
CROSSMODULE_WRAPPER(policies_show);

/* partialize/finalize aggregate */
CROSSMODULE_WRAPPER(partialize_agg);
CROSSMODULE_WRAPPER(finalize_agg_sfunc);
CROSSMODULE_WRAPPER(finalize_agg_ffunc);

/* compression functions */
CROSSMODULE_WRAPPER(compressed_data_decompress_forward);
CROSSMODULE_WRAPPER(compressed_data_decompress_reverse);
CROSSMODULE_WRAPPER(compressed_data_send);
CROSSMODULE_WRAPPER(compressed_data_recv);
CROSSMODULE_WRAPPER(compressed_data_in);
CROSSMODULE_WRAPPER(compressed_data_out);
CROSSMODULE_WRAPPER(deltadelta_compressor_append);
CROSSMODULE_WRAPPER(deltadelta_compressor_finish);
CROSSMODULE_WRAPPER(gorilla_compressor_append);
CROSSMODULE_WRAPPER(gorilla_compressor_finish);
CROSSMODULE_WRAPPER(dictionary_compressor_append);
CROSSMODULE_WRAPPER(dictionary_compressor_finish);
CROSSMODULE_WRAPPER(array_compressor_append);
CROSSMODULE_WRAPPER(array_compressor_finish);
CROSSMODULE_WRAPPER(create_compressed_chunk);
CROSSMODULE_WRAPPER(create_compressed_chunks_for_hypertable);
CROSSMODULE_WRAPPER(compress_chunk);
CROSSMODULE_WRAPPER(decompress_chunk);

/* continuous aggregate */
CROSSMODULE_WRAPPER(continuous_agg_invalidation_trigger);
CROSSMODULE_WRAPPER(continuous_agg_refresh);
CROSSMODULE_WRAPPER(invalidation_cagg_log_add_entry);
CROSSMODULE_WRAPPER(invalidation_hyper_log_add_entry);
CROSSMODULE_WRAPPER(drop_dist_ht_invalidation_trigger);
CROSSMODULE_WRAPPER(invalidation_process_hypertable_log);
CROSSMODULE_WRAPPER(invalidation_process_cagg_log);
CROSSMODULE_WRAPPER(cagg_try_repair);

CROSSMODULE_WRAPPER(data_node_ping);
CROSSMODULE_WRAPPER(data_node_block_new_chunks);
CROSSMODULE_WRAPPER(data_node_allow_new_chunks);
CROSSMODULE_WRAPPER(data_node_add);
CROSSMODULE_WRAPPER(data_node_delete);
CROSSMODULE_WRAPPER(data_node_attach);
CROSSMODULE_WRAPPER(data_node_detach);
CROSSMODULE_WRAPPER(data_node_alter);
CROSSMODULE_WRAPPER(chunk_drop_replica);
CROSSMODULE_WRAPPER(chunk_freeze_chunk);
CROSSMODULE_WRAPPER(chunk_unfreeze_chunk);
CROSSMODULE_WRAPPER(chunks_drop_stale);

CROSSMODULE_WRAPPER(chunk_set_default_data_node);
CROSSMODULE_WRAPPER(chunk_get_relstats);
CROSSMODULE_WRAPPER(chunk_get_colstats);
CROSSMODULE_WRAPPER(chunk_create_empty_table);
CROSSMODULE_WRAPPER(chunk_create_replica_table);

CROSSMODULE_WRAPPER(timescaledb_fdw_handler);
CROSSMODULE_WRAPPER(timescaledb_fdw_validator);
CROSSMODULE_WRAPPER(remote_txn_id_in);
CROSSMODULE_WRAPPER(remote_txn_id_out);
CROSSMODULE_WRAPPER(remote_txn_heal_data_node);
CROSSMODULE_WRAPPER(remote_connection_cache_show);
CROSSMODULE_WRAPPER(dist_remote_hypertable_info);
CROSSMODULE_WRAPPER(dist_remote_chunk_info);
CROSSMODULE_WRAPPER(dist_remote_compressed_chunk_info);
CROSSMODULE_WRAPPER(dist_remote_hypertable_index_info);
CROSSMODULE_WRAPPER(distributed_exec);
CROSSMODULE_WRAPPER(create_distributed_restore_point);
CROSSMODULE_WRAPPER(hypertable_distributed_set_replication_factor);
CROSSMODULE_WRAPPER(health_check);
CROSSMODULE_WRAPPER(recompress_chunk_segmentwise);
CROSSMODULE_WRAPPER(get_compressed_chunk_index_for_recompression);

TS_FUNCTION_INFO_V1(ts_dist_set_id);
Datum
ts_dist_set_id(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(ts_cm_functions->set_distributed_id(PG_GETARG_DATUM(0)));
}

TS_FUNCTION_INFO_V1(ts_dist_set_peer_id);
Datum
ts_dist_set_peer_id(PG_FUNCTION_ARGS)
{
	ts_cm_functions->set_distributed_peer_id(PG_GETARG_DATUM(0));
	PG_RETURN_VOID();
}

TS_FUNCTION_INFO_V1(ts_dist_validate_as_data_node);
Datum
ts_dist_validate_as_data_node(PG_FUNCTION_ARGS)
{
	ts_cm_functions->validate_as_data_node();
	PG_RETURN_VOID();
}

/*
 * casting a function pointer to a pointer of another type is undefined
 * behavior, so we need one of these for every function type we have
 */

static void
error_no_default_fn_community(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("functionality not supported under the current \"%s\" license. Learn more at "
					"https://timescale.com/.",
					ts_guc_license),
			 errhint("To access all features and the best time-series experience, try out "
					 "Timescale Cloud.")));
}

static bool
error_no_default_fn_bool_void_community(void)
{
	error_no_default_fn_community();
	pg_unreachable();
}

#ifdef USE_TELEMETRY
static void
add_tsl_telemetry_info_default(JsonbParseState **parse_state)
{
	error_no_default_fn_community();
}
#endif

static bool
job_execute_default_fn(BgwJob *job)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static bool
process_compress_table_default(AlterTableCmd *cmd, Hypertable *ht,
							   WithClauseResult *with_clause_options)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static Datum
error_no_default_fn_pg_community(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("function \"%s\" is not supported under the current \"%s\" license",
					get_func_name(fcinfo->flinfo->fn_oid),
					ts_guc_license),
			 errhint("Upgrade your license to 'timescale' to use this free community feature.")));

	pg_unreachable();
}

/*
 * TSL library is not loaded by the replication worker for some reason,
 * so a call to `compressed_data_in` and `compressed_data_out` functions would
 * produce a misleading error saying that your license is "timescale" and you
 * should upgrade to "timescale" license, even if you have already upgraded.
 *
 * As a workaround, we try to load the TSL module it in this function.
 * It will still error out in the "apache" version
 */

static Datum
process_compressed_data_in(PG_FUNCTION_ARGS)
{
	ts_license_enable_module_loading();

	if (ts_cm_functions->compressed_data_in != process_compressed_data_in)
		return ts_cm_functions->compressed_data_in(fcinfo);

	error_no_default_fn_pg_community(fcinfo);
	pg_unreachable();
}

static Datum
process_compressed_data_out(PG_FUNCTION_ARGS)
{
	ts_license_enable_module_loading();

	if (ts_cm_functions->compressed_data_out != process_compressed_data_out)
		return ts_cm_functions->compressed_data_out(fcinfo);

	error_no_default_fn_pg_community(fcinfo);
	pg_unreachable();
}

/*
 * This function ensures that the TSL library is loaded and the call to
 * post_update_cagg_try_repair is dispatched to the correct
 * function.
 *
 * The TSL library might not be loaded when post_update_cagg_try_repair is
 * called during a database upgrade, resulting in an error message about
 * improper licensing:
 *
 * "[..] is not supported under the current "timescale" license
 *  INT:  Upgrade your license to 'timescale'""
 *
 * See also the comment about this problem in the function
 * process_compressed_data_in.
 */
static Datum
process_cagg_try_repair(PG_FUNCTION_ARGS)
{
	ts_license_enable_module_loading();

	if (ts_cm_functions->cagg_try_repair != process_cagg_try_repair)
		return ts_cm_functions->cagg_try_repair(fcinfo);

	error_no_default_fn_pg_community(fcinfo);
	pg_unreachable();
}

static void
hypertable_make_distributed_default_fn(Hypertable *ht, List *data_node_names)
{
	error_no_default_fn_community();
}

static List *
get_and_validate_data_node_list_default_fn(ArrayType *nodearr)
{
	error_no_default_fn_community();
	pg_unreachable();

	return NIL;
}

static void
cache_syscache_invalidate_default(Datum arg, int cacheid, uint32 hashvalue)
{
	/* The default is a no-op */
}

static DDLResult
process_cagg_viewstmt_default(Node *stmt, const char *query_string, void *pstmt,
							  WithClauseResult *with_clause_options)
{
	return error_no_default_fn_bool_void_community();
}

static void
continuous_agg_update_options_default(ContinuousAgg *cagg, WithClauseResult *with_clause_options)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static void
continuous_agg_invalidate_raw_ht_all_default(const Hypertable *raw_ht, int64 start, int64 end)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static void
continuous_agg_invalidate_mat_ht_all_default(const Hypertable *raw_ht, const Hypertable *mat_ht,
											 int64 start, int64 end)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static void
continuous_agg_call_invalidation_trigger_default(int32 hypertable_id, Relation chunk_rel,
												 HeapTuple chunk_tuple, HeapTuple chunk_newtuple,
												 bool update,
												 bool is_distributed_hypertable_trigger,
												 int32 parent_hypertable_id)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static Datum
empty_fn(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}

static void
create_chunk_on_data_nodes_default(const Chunk *chunk, const Hypertable *ht,
								   const char *remote_chunk_name, List *data_nodes)
{
	error_no_default_fn_community();
}

static Path *
distributed_insert_path_create_default(PlannerInfo *root, ModifyTablePath *mtpath,
									   Index hypertable_rti, int subpath_index)
{
	error_no_default_fn_community();
	pg_unreachable();

	return NULL;
}

static uint64
distributed_copy_default(const CopyStmt *stmt, CopyChunkState *ccstate, List *attnums)
{
	error_no_default_fn_community();

	return 0;
}

static bool
set_distributed_id_default(Datum d)
{
	return error_no_default_fn_bool_void_community();
}

static void
set_distributed_peer_id_default(Datum d)
{
	error_no_default_fn_community();
}

static void
func_call_on_data_nodes_default(FunctionCallInfo finfo, List *data_node_oids)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static void
dist_update_stale_chunk_metadata_default(Chunk *new_chunk, List *chunk_data_nodes)
{
	error_no_default_fn_community();
	pg_unreachable();
}

TS_FUNCTION_INFO_V1(ts_tsl_loaded);

PGDLLEXPORT Datum
ts_tsl_loaded(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(ts_cm_functions != &ts_cm_functions_default);
}

static void
mn_get_foreign_join_path_default_fn_pg_community(PlannerInfo *root, RelOptInfo *joinrel,
												 RelOptInfo *outerrel, RelOptInfo *innerrel,
												 JoinType jointype, JoinPathExtraData *extra)
{
}

/*
 * Define cross-module functions' default values:
 * If the submodule isn't activated, using one of the cm functions will throw an
 * exception.
 */
TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default = {
#ifdef USE_TELEMETRY
	.add_tsl_telemetry_info = add_tsl_telemetry_info_default,
#endif
	.create_upper_paths_hook = NULL,
	.set_rel_pathlist_dml = NULL,
	.set_rel_pathlist_query = NULL,
	.set_rel_pathlist = NULL,
	.ddl_command_start = NULL,
	.ddl_command_end = NULL,
	.sql_drop = NULL,
	.process_altertable_cmd = NULL,
	.process_rename_cmd = NULL,

	/* gapfill */
	.gapfill_marker = error_no_default_fn_pg_community,
	.gapfill_int16_time_bucket = error_no_default_fn_pg_community,
	.gapfill_int32_time_bucket = error_no_default_fn_pg_community,
	.gapfill_int64_time_bucket = error_no_default_fn_pg_community,
	.gapfill_date_time_bucket = error_no_default_fn_pg_community,
	.gapfill_timestamp_time_bucket = error_no_default_fn_pg_community,
	.gapfill_timestamptz_time_bucket = error_no_default_fn_pg_community,
	.gapfill_timestamptz_timezone_time_bucket = error_no_default_fn_pg_community,

	/* bgw policies */
	.policy_compression_add = error_no_default_fn_pg_community,
	.policy_compression_remove = error_no_default_fn_pg_community,
	.policy_recompression_proc = error_no_default_fn_pg_community,
	.policy_compression_check = error_no_default_fn_pg_community,
	.policy_refresh_cagg_add = error_no_default_fn_pg_community,
	.policy_refresh_cagg_proc = error_no_default_fn_pg_community,
	.policy_refresh_cagg_check = error_no_default_fn_pg_community,
	.policy_refresh_cagg_remove = error_no_default_fn_pg_community,
	.policy_reorder_add = error_no_default_fn_pg_community,
	.policy_reorder_proc = error_no_default_fn_pg_community,
	.policy_reorder_check = error_no_default_fn_pg_community,
	.policy_reorder_remove = error_no_default_fn_pg_community,
	.policy_retention_add = error_no_default_fn_pg_community,
	.policy_retention_proc = error_no_default_fn_pg_community,
	.policy_retention_check = error_no_default_fn_pg_community,
	.policy_retention_remove = error_no_default_fn_pg_community,

	.job_add = error_no_default_fn_pg_community,
	.job_alter = error_no_default_fn_pg_community,
	.job_alter_set_hypertable_id = error_no_default_fn_pg_community,
	.job_delete = error_no_default_fn_pg_community,
	.job_run = error_no_default_fn_pg_community,
	.job_execute = job_execute_default_fn,

	.move_chunk = error_no_default_fn_pg_community,
	.move_chunk_proc = error_no_default_fn_pg_community,
	.copy_chunk_proc = error_no_default_fn_pg_community,
	.copy_chunk_cleanup_proc = error_no_default_fn_pg_community,
	.subscription_exec = error_no_default_fn_pg_community,
	.reorder_chunk = error_no_default_fn_pg_community,

	.policies_add = error_no_default_fn_pg_community,
	.policies_remove = error_no_default_fn_pg_community,
	.policies_remove_all = error_no_default_fn_pg_community,
	.policies_alter = error_no_default_fn_pg_community,
	.policies_show = error_no_default_fn_pg_community,

	.partialize_agg = error_no_default_fn_pg_community,
	.finalize_agg_sfunc = error_no_default_fn_pg_community,
	.finalize_agg_ffunc = error_no_default_fn_pg_community,
	.process_cagg_viewstmt = process_cagg_viewstmt_default,
	.continuous_agg_invalidation_trigger = error_no_default_fn_pg_community,
	.continuous_agg_call_invalidation_trigger = continuous_agg_call_invalidation_trigger_default,
	.continuous_agg_refresh = error_no_default_fn_pg_community,
	.continuous_agg_invalidate_raw_ht = continuous_agg_invalidate_raw_ht_all_default,
	.continuous_agg_invalidate_mat_ht = continuous_agg_invalidate_mat_ht_all_default,
	.continuous_agg_update_options = continuous_agg_update_options_default,
	.invalidation_cagg_log_add_entry = error_no_default_fn_pg_community,
	.invalidation_hyper_log_add_entry = error_no_default_fn_pg_community,
	.remote_invalidation_log_delete = NULL,
	.drop_dist_ht_invalidation_trigger = error_no_default_fn_pg_community,
	.remote_drop_dist_ht_invalidation_trigger = NULL,
	.invalidation_process_hypertable_log = error_no_default_fn_pg_community,
	.invalidation_process_cagg_log = error_no_default_fn_pg_community,
	.cagg_try_repair = process_cagg_try_repair,

	/* compression */
	.compressed_data_send = error_no_default_fn_pg_community,
	.compressed_data_recv = error_no_default_fn_pg_community,
	.compressed_data_in = process_compressed_data_in,
	.compressed_data_out = process_compressed_data_out,
	.process_compress_table = process_compress_table_default,
	.create_compressed_chunk = error_no_default_fn_pg_community,
	.compress_chunk = error_no_default_fn_pg_community,
	.decompress_chunk = error_no_default_fn_pg_community,
	.compressed_data_decompress_forward = error_no_default_fn_pg_community,
	.compressed_data_decompress_reverse = error_no_default_fn_pg_community,
	.deltadelta_compressor_append = error_no_default_fn_pg_community,
	.deltadelta_compressor_finish = error_no_default_fn_pg_community,
	.gorilla_compressor_append = error_no_default_fn_pg_community,
	.gorilla_compressor_finish = error_no_default_fn_pg_community,
	.dictionary_compressor_append = error_no_default_fn_pg_community,
	.dictionary_compressor_finish = error_no_default_fn_pg_community,
	.array_compressor_append = error_no_default_fn_pg_community,
	.array_compressor_finish = error_no_default_fn_pg_community,

	.data_node_add = error_no_default_fn_pg_community,
	.data_node_delete = error_no_default_fn_pg_community,
	.data_node_attach = error_no_default_fn_pg_community,
	.data_node_ping = error_no_default_fn_pg_community,
	.data_node_detach = error_no_default_fn_pg_community,
	.data_node_alter = error_no_default_fn_pg_community,
	.data_node_allow_new_chunks = error_no_default_fn_pg_community,
	.data_node_block_new_chunks = error_no_default_fn_pg_community,
	.distributed_exec = error_no_default_fn_pg_community,
	.create_distributed_restore_point = error_no_default_fn_pg_community,
	.chunk_set_default_data_node = error_no_default_fn_pg_community,
	.show_chunk = error_no_default_fn_pg_community,
	.create_chunk = error_no_default_fn_pg_community,
	.create_chunk_on_data_nodes = create_chunk_on_data_nodes_default,
	.chunk_drop_replica = error_no_default_fn_pg_community,
	.chunk_freeze_chunk = error_no_default_fn_pg_community,
	.chunk_unfreeze_chunk = error_no_default_fn_pg_community,
	.chunks_drop_stale = error_no_default_fn_pg_community,
	.hypertable_make_distributed = hypertable_make_distributed_default_fn,
	.get_and_validate_data_node_list = get_and_validate_data_node_list_default_fn,
	.timescaledb_fdw_handler = error_no_default_fn_pg_community,
	.timescaledb_fdw_validator = empty_fn,
	.cache_syscache_invalidate = cache_syscache_invalidate_default,
	.remote_txn_id_in = error_no_default_fn_pg_community,
	.remote_txn_id_out = error_no_default_fn_pg_community,
	.remote_txn_heal_data_node = error_no_default_fn_pg_community,
	.remote_connection_cache_show = error_no_default_fn_pg_community,
	.distributed_insert_path_create = distributed_insert_path_create_default,
	.distributed_copy = distributed_copy_default,
	.set_distributed_id = set_distributed_id_default,
	.set_distributed_peer_id = set_distributed_peer_id_default,
	.is_access_node_session = error_no_default_fn_bool_void_community,
	.remove_from_distributed_db = error_no_default_fn_bool_void_community,
	.dist_remote_hypertable_info = error_no_default_fn_pg_community,
	.dist_remote_chunk_info = error_no_default_fn_pg_community,
	.dist_remote_compressed_chunk_info = error_no_default_fn_pg_community,
	.dist_remote_hypertable_index_info = error_no_default_fn_pg_community,
	.dist_update_stale_chunk_metadata = dist_update_stale_chunk_metadata_default,
	.validate_as_data_node = error_no_default_fn_community,
	.func_call_on_data_nodes = func_call_on_data_nodes_default,
	.chunk_get_relstats = error_no_default_fn_pg_community,
	.chunk_get_colstats = error_no_default_fn_pg_community,
	.chunk_create_empty_table = error_no_default_fn_pg_community,
	.chunk_create_replica_table = error_no_default_fn_pg_community,
	.hypertable_distributed_set_replication_factor = error_no_default_fn_pg_community,
	.health_check = error_no_default_fn_pg_community,
	.recompress_chunk_segmentwise = error_no_default_fn_pg_community,
	.get_compressed_chunk_index_for_recompression = error_no_default_fn_pg_community,
	.mn_get_foreign_join_paths = mn_get_foreign_join_path_default_fn_pg_community,
};

TSDLLEXPORT CrossModuleFunctions *ts_cm_functions = &ts_cm_functions_default;
