/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <commands/event_trigger.h>
#include <fmgr.h>
#include <optimizer/planner.h>
#include <utils/array.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>

#include "compat/compat.h"
#include "bgw/job.h"
#include "export.h"
#include "planner/planner.h"
#include "process_utility.h"
#include "ts_catalog/continuous_agg.h"
#include "with_clause_parser.h"

/*
 * To define a cross-module function add it to this struct, add a default
 * version in to ts_cm_functions_default cross_module_fn.c, and the overridden
 * version to tsl_cm_functions tsl/src/init.c.
 * This will allow the function to be called from this codebase as
 *     ts_cm_functions-><function name>
 */

typedef struct JsonbParseState JsonbParseState;
typedef struct Hypertable Hypertable;
typedef struct Chunk Chunk;
typedef struct ChunkInsertState ChunkInsertState;
typedef struct CopyChunkState CopyChunkState;
typedef struct HypertableModifyState HypertableModifyState;

typedef struct CrossModuleFunctions
{
	PGFunction policy_compression_add;
	PGFunction policy_compression_remove;
	PGFunction policy_recompression_proc;
	PGFunction policy_compression_check;
	PGFunction policy_refresh_cagg_add;
	PGFunction policy_refresh_cagg_proc;
	PGFunction policy_refresh_cagg_check;
	PGFunction policy_refresh_cagg_remove;
	PGFunction policy_reorder_add;
	PGFunction policy_reorder_proc;
	PGFunction policy_reorder_check;
	PGFunction policy_reorder_remove;
	PGFunction policy_retention_add;
	PGFunction policy_retention_proc;
	PGFunction policy_retention_check;
	PGFunction policy_retention_remove;

	PGFunction policies_add;
	PGFunction policies_remove;
	PGFunction policies_remove_all;
	PGFunction policies_alter;
	PGFunction policies_show;

	PGFunction job_add;
	PGFunction job_alter;
	PGFunction job_alter_set_hypertable_id;
	PGFunction job_delete;
	PGFunction job_run;

	bool (*job_execute)(BgwJob *job);

	void (*create_upper_paths_hook)(PlannerInfo *, UpperRelationKind, RelOptInfo *, RelOptInfo *,
									TsRelType input_reltype, Hypertable *ht, void *extra);
	void (*set_rel_pathlist_dml)(PlannerInfo *, RelOptInfo *, Index, RangeTblEntry *, Hypertable *);
	void (*set_rel_pathlist_query)(PlannerInfo *, RelOptInfo *, Index, RangeTblEntry *,
								   Hypertable *);
	void (*set_rel_pathlist)(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
	bool (*process_explain_def)(DefElem *def);

	/* gapfill */
	PGFunction gapfill_marker;
	PGFunction gapfill_int16_time_bucket;
	PGFunction gapfill_int32_time_bucket;
	PGFunction gapfill_int64_time_bucket;
	PGFunction gapfill_date_time_bucket;
	PGFunction gapfill_timestamp_time_bucket;
	PGFunction gapfill_timestamptz_time_bucket;
	PGFunction gapfill_timestamptz_timezone_time_bucket;

	PGFunction reorder_chunk;
	PGFunction move_chunk;

	void (*ddl_command_start)(ProcessUtilityArgs *args);
	void (*ddl_command_end)(EventTriggerData *trigdata);

	/* Vectorized queries */
	void (*tsl_postprocess_plan)(PlannedStmt *stmt);

	/* Continuous Aggregates */
	PGFunction partialize_agg;
	PGFunction finalize_agg_sfunc;
	PGFunction finalize_agg_ffunc;
	DDLResult (*process_cagg_viewstmt)(Node *stmt, const char *query_string, void *pstmt,
									   WithClauseResult *with_clause_options);
	PGFunction continuous_agg_invalidation_trigger;
	void (*continuous_agg_call_invalidation_trigger)(int32 hypertable_id, Relation chunk_rel,
													 HeapTuple chunk_tuple,
													 HeapTuple chunk_newtuple, bool update);
	PGFunction continuous_agg_refresh;
	void (*continuous_agg_invalidate_raw_ht)(const Hypertable *raw_ht, int64 start, int64 end);
	void (*continuous_agg_invalidate_mat_ht)(const Hypertable *raw_ht, const Hypertable *mat_ht,
											 int64 start, int64 end);
	void (*continuous_agg_update_options)(ContinuousAgg *cagg,
										  WithClauseResult *with_clause_options);
	PGFunction continuous_agg_validate_query;
	PGFunction continuous_agg_get_bucket_function;
	PGFunction continuous_agg_get_bucket_function_info;
	PGFunction continuous_agg_migrate_to_time_bucket;
	PGFunction cagg_try_repair;

	PGFunction compressed_data_send;
	PGFunction compressed_data_recv;
	PGFunction compressed_data_in;
	PGFunction compressed_data_out;
	PGFunction compressed_data_info;
	bool (*process_compress_table)(AlterTableCmd *cmd, Hypertable *ht,
								   WithClauseResult *with_clause_options);
	void (*process_altertable_cmd)(Hypertable *ht, const AlterTableCmd *cmd);
	void (*process_rename_cmd)(Oid relid, Cache *hcache, const RenameStmt *stmt);
	PGFunction create_compressed_chunk;
	PGFunction compress_chunk;
	PGFunction decompress_chunk;
	void (*decompress_batches_for_insert)(const ChunkInsertState *state, TupleTableSlot *slot);
	bool (*decompress_target_segments)(HypertableModifyState *ht_state);
	/* The compression functions below are not installed in SQL as part of create extension;
	 *  They are installed and tested during testing scripts. They are exposed in cross-module
	 *  functions because they may be very useful for debugging customer problems if the sql
	 *  stub is installed on the customer's machine.
	 */
	PGFunction compressed_data_decompress_forward;
	PGFunction compressed_data_decompress_reverse;
	PGFunction deltadelta_compressor_append;
	PGFunction deltadelta_compressor_finish;
	PGFunction gorilla_compressor_append;
	PGFunction gorilla_compressor_finish;
	PGFunction dictionary_compressor_append;
	PGFunction dictionary_compressor_finish;
	PGFunction array_compressor_append;
	PGFunction array_compressor_finish;
	PGFunction compressionam_handler;
	PGFunction is_compressed_tid;

	PGFunction create_chunk;
	PGFunction show_chunk;

	PGFunction chunk_create_empty_table;
	PGFunction chunk_freeze_chunk;
	PGFunction chunk_unfreeze_chunk;
	PGFunction recompress_chunk_segmentwise;
	PGFunction get_compressed_chunk_index_for_recompression;
	void (*preprocess_query_tsl)(Query *parse);
} CrossModuleFunctions;

extern TSDLLEXPORT CrossModuleFunctions *ts_cm_functions;
extern TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default;
