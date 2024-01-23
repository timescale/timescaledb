/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>

#include "bgw/job.h"
#include "cross_module_fn.h"
#include "export.h"
#include "guc.h"
#include "license_guc.h"

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
CROSSMODULE_WRAPPER(compressed_data_info);
CROSSMODULE_WRAPPER(deltadelta_compressor_append);
CROSSMODULE_WRAPPER(deltadelta_compressor_finish);
CROSSMODULE_WRAPPER(gorilla_compressor_append);
CROSSMODULE_WRAPPER(gorilla_compressor_finish);
CROSSMODULE_WRAPPER(dictionary_compressor_append);
CROSSMODULE_WRAPPER(dictionary_compressor_finish);
CROSSMODULE_WRAPPER(array_compressor_append);
CROSSMODULE_WRAPPER(array_compressor_finish);
CROSSMODULE_WRAPPER(create_compressed_chunk);
CROSSMODULE_WRAPPER(compress_chunk);
CROSSMODULE_WRAPPER(decompress_chunk);
CROSSMODULE_WRAPPER(compressionam_handler);

/* continuous aggregate */
CROSSMODULE_WRAPPER(continuous_agg_invalidation_trigger);
CROSSMODULE_WRAPPER(continuous_agg_refresh);
CROSSMODULE_WRAPPER(continuous_agg_validate_query);
CROSSMODULE_WRAPPER(continuous_agg_get_bucket_function);
CROSSMODULE_WRAPPER(continuous_agg_get_bucket_function_info);
CROSSMODULE_WRAPPER(continuous_agg_migrate_to_time_bucket);
CROSSMODULE_WRAPPER(cagg_try_repair);

CROSSMODULE_WRAPPER(chunk_freeze_chunk);
CROSSMODULE_WRAPPER(chunk_unfreeze_chunk);

CROSSMODULE_WRAPPER(chunk_create_empty_table);

CROSSMODULE_WRAPPER(recompress_chunk_segmentwise);
CROSSMODULE_WRAPPER(get_compressed_chunk_index_for_recompression);

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

static bool
job_execute_default_fn(BgwJob *job)
{
	error_no_default_fn_community();
	pg_unreachable();
}

static void
tsl_postprocess_plan_stub(PlannedStmt *stmt)
{
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
												 bool update)
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
preprocess_query_tsl_default_fn_community(Query *parse)
{
	/* No op in community licensed code */
}

/*
 * Define cross-module functions' default values:
 * If the submodule isn't activated, using one of the cm functions will throw an
 * exception.
 */
TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default = {
	.create_upper_paths_hook = NULL,
	.set_rel_pathlist_dml = NULL,
	.set_rel_pathlist_query = NULL,
	.set_rel_pathlist = NULL,
	.ddl_command_start = NULL,
	.ddl_command_end = NULL,
	.process_vacuum_cmd = NULL,
	.process_altertable_cmd = NULL,
	.process_rename_cmd = NULL,
	.process_explain_def = NULL,

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

	.reorder_chunk = error_no_default_fn_pg_community,
	.move_chunk = error_no_default_fn_pg_community,

	.policies_add = error_no_default_fn_pg_community,
	.policies_remove = error_no_default_fn_pg_community,
	.policies_remove_all = error_no_default_fn_pg_community,
	.policies_alter = error_no_default_fn_pg_community,
	.policies_show = error_no_default_fn_pg_community,

	.tsl_postprocess_plan = tsl_postprocess_plan_stub,

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
	.continuous_agg_validate_query = error_no_default_fn_pg_community,
	.continuous_agg_get_bucket_function = error_no_default_fn_pg_community,
	.continuous_agg_get_bucket_function_info = error_no_default_fn_pg_community,
	.continuous_agg_migrate_to_time_bucket = error_no_default_fn_pg_community,
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
	.compressionam_handler = error_no_default_fn_pg_community,

	.show_chunk = error_no_default_fn_pg_community,
	.create_chunk = error_no_default_fn_pg_community,
	.chunk_freeze_chunk = error_no_default_fn_pg_community,
	.chunk_unfreeze_chunk = error_no_default_fn_pg_community,
	.chunk_create_empty_table = error_no_default_fn_pg_community,
	.recompress_chunk_segmentwise = error_no_default_fn_pg_community,
	.get_compressed_chunk_index_for_recompression = error_no_default_fn_pg_community,
	.preprocess_query_tsl = preprocess_query_tsl_default_fn_community,
};

TSDLLEXPORT CrossModuleFunctions *ts_cm_functions = &ts_cm_functions_default;
