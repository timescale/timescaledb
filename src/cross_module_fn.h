/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CROSS_MODULE_FN_H
#define TIMESCALEDB_CROSS_MODULE_FN_H

#include <postgres.h>
#include <fmgr.h>
#include <commands/event_trigger.h>
#include <optimizer/planner.h>
#include <utils/timestamp.h>
#include <utils/jsonb.h>
#include <utils/array.h>

#include "export.h"
#include "compat/compat.h"
#include "bgw/job.h"
#include "process_utility.h"
#include "with_clause_parser.h"
#include "ts_catalog/continuous_agg.h"
#include "planner/planner.h"

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

typedef struct CrossModuleFunctions
{
	void (*add_tsl_telemetry_info)(JsonbParseState **parse_state);

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
	PGFunction move_chunk_proc;
	PGFunction copy_chunk_proc;
	PGFunction subscription_exec;
	PGFunction copy_chunk_cleanup_proc;
	void (*ddl_command_start)(ProcessUtilityArgs *args);
	void (*ddl_command_end)(EventTriggerData *command);
	void (*sql_drop)(List *dropped_objects);

	/* Continuous Aggregates */
	PGFunction partialize_agg;
	PGFunction finalize_agg_sfunc;
	PGFunction finalize_agg_ffunc;
	DDLResult (*process_cagg_viewstmt)(Node *stmt, const char *query_string, void *pstmt,
									   WithClauseResult *with_clause_options);
	PGFunction continuous_agg_invalidation_trigger;
	void (*continuous_agg_call_invalidation_trigger)(int32 hypertable_id, Relation chunk_rel,
													 HeapTuple chunk_tuple,
													 HeapTuple chunk_newtuple, bool update,
													 bool is_distributed_hypertable_trigger,
													 int32 parent_hypertable_id);
	PGFunction continuous_agg_refresh;
	void (*continuous_agg_invalidate_raw_ht)(const Hypertable *raw_ht, int64 start, int64 end);
	void (*continuous_agg_invalidate_mat_ht)(const Hypertable *raw_ht, const Hypertable *mat_ht,
											 int64 start, int64 end);
	void (*continuous_agg_update_options)(ContinuousAgg *cagg,
										  WithClauseResult *with_clause_options);
	PGFunction invalidation_cagg_log_add_entry;
	PGFunction invalidation_hyper_log_add_entry;
	void (*remote_invalidation_log_delete)(int32 raw_hypertable_id,
										   ContinuousAggHypertableStatus caggstatus);
	PGFunction drop_dist_ht_invalidation_trigger;
	void (*remote_drop_dist_ht_invalidation_trigger)(int32 raw_hypertable_id);
	PGFunction invalidation_process_hypertable_log;
	PGFunction invalidation_process_cagg_log;
	PGFunction cagg_try_repair;

	PGFunction compressed_data_send;
	PGFunction compressed_data_recv;
	PGFunction compressed_data_in;
	PGFunction compressed_data_out;
	bool (*process_compress_table)(AlterTableCmd *cmd, Hypertable *ht,
								   WithClauseResult *with_clause_options);
	void (*process_altertable_cmd)(Hypertable *ht, const AlterTableCmd *cmd);
	void (*process_rename_cmd)(Oid relid, Cache *hcache, const RenameStmt *stmt);
	PGFunction create_compressed_chunk;
	PGFunction create_compressed_chunks_for_hypertable;
	PGFunction compress_chunk;
	PGFunction decompress_chunk;
	void (*decompress_batches_for_insert)(ChunkInsertState *state, Chunk *chunk,
										  TupleTableSlot *slot);
	bool (*decompress_target_segments)(ModifyTableState *ps);
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

	PGFunction data_node_add;
	PGFunction data_node_delete;
	PGFunction data_node_attach;
	PGFunction data_node_ping;
	PGFunction data_node_detach;
	PGFunction data_node_alter;
	PGFunction data_node_allow_new_chunks;
	PGFunction data_node_block_new_chunks;

	PGFunction chunk_set_default_data_node;
	PGFunction create_chunk;
	PGFunction show_chunk;

	List *(*get_and_validate_data_node_list)(ArrayType *nodearr);
	void (*hypertable_make_distributed)(Hypertable *ht, List *data_node_names);
	PGFunction timescaledb_fdw_handler;
	PGFunction timescaledb_fdw_validator;
	void (*cache_syscache_invalidate)(Datum arg, int cacheid, uint32 hashvalue);
	PGFunction remote_txn_id_in;
	PGFunction remote_txn_id_out;
	PGFunction remote_txn_heal_data_node;
	PGFunction remote_connection_cache_show;
	void (*create_chunk_on_data_nodes)(const Chunk *chunk, const Hypertable *ht,
									   const char *remote_chunk_name, List *data_nodes);
	Path *(*distributed_insert_path_create)(PlannerInfo *root, ModifyTablePath *mtpath,
											Index hypertable_rti, int subpath_index);
	uint64 (*distributed_copy)(const CopyStmt *stmt, CopyChunkState *ccstate, List *attnums);
	bool (*set_distributed_id)(Datum id);
	void (*set_distributed_peer_id)(Datum id);
	bool (*is_access_node_session)(void);
	bool (*remove_from_distributed_db)(void);
	PGFunction dist_remote_hypertable_info;
	PGFunction dist_remote_chunk_info;
	PGFunction dist_remote_compressed_chunk_info;
	PGFunction dist_remote_hypertable_index_info;
	void (*dist_update_stale_chunk_metadata)(Chunk *new_chunk, List *chunk_data_nodes);
	void (*validate_as_data_node)(void);
	void (*func_call_on_data_nodes)(FunctionCallInfo fcinfo, List *data_node_oids);
	PGFunction distributed_exec;
	PGFunction create_distributed_restore_point;
	PGFunction chunk_get_relstats;
	PGFunction chunk_get_colstats;
	PGFunction hypertable_distributed_set_replication_factor;
	PGFunction chunk_create_empty_table;
	PGFunction chunk_create_replica_table;
	PGFunction chunk_drop_replica;
	PGFunction chunk_freeze_chunk;
	PGFunction chunk_unfreeze_chunk;
	PGFunction chunks_drop_stale;
	PGFunction health_check;
	PGFunction recompress_chunk_segmentwise;
	PGFunction get_compressed_chunk_index_for_recompression;
	void (*mn_get_foreign_join_paths)(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
									  RelOptInfo *innerrel, JoinType jointype,
									  JoinPathExtraData *extra);
} CrossModuleFunctions;

extern TSDLLEXPORT CrossModuleFunctions *ts_cm_functions;
extern TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default;

#endif /* TIMESCALEDB_CROSS_MODULE_FN_H */
