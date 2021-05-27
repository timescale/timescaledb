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
#include "compat.h"
#include "bgw/job.h"
#include "process_utility.h"
#include "with_clause_parser.h"
#include "continuous_agg.h"
#include "plan_expand_hypertable.h"
#include "planner.h"

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
typedef struct CopyChunkState CopyChunkState;

typedef struct CrossModuleFunctions
{
	void (*add_tsl_telemetry_info)(JsonbParseState **parse_state);

	PGFunction policy_compression_add;
	PGFunction policy_compression_proc;
	PGFunction policy_compression_remove;
	PGFunction policy_refresh_cagg_add;
	PGFunction policy_refresh_cagg_proc;
	PGFunction policy_refresh_cagg_remove;
	PGFunction policy_reorder_add;
	PGFunction policy_reorder_proc;
	PGFunction policy_reorder_remove;
	PGFunction policy_retention_add;
	PGFunction policy_retention_proc;
	PGFunction policy_retention_remove;

	PGFunction job_add;
	PGFunction job_alter;
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

	PGFunction reorder_chunk;
	PGFunction move_chunk;
	void (*ddl_command_start)(ProcessUtilityArgs *args);
	void (*ddl_command_end)(EventTriggerData *command);
	void (*sql_drop)(List *dropped_objects);
	PGFunction partialize_agg;
	PGFunction finalize_agg_sfunc;
	PGFunction finalize_agg_ffunc;
	DDLResult (*process_cagg_viewstmt)(Node *stmt, const char *query_string, void *pstmt,
									   WithClauseResult *with_clause_options);
	PGFunction continuous_agg_invalidation_trigger;
	PGFunction continuous_agg_refresh;
	PGFunction continuous_agg_refresh_chunk;
	void (*continuous_agg_invalidate)(const Hypertable *ht, int64 start, int64 end);
	void (*continuous_agg_update_options)(ContinuousAgg *cagg,
										  WithClauseResult *with_clause_options);

	PGFunction compressed_data_send;
	PGFunction compressed_data_recv;
	PGFunction compressed_data_in;
	PGFunction compressed_data_out;
	bool (*process_compress_table)(AlterTableCmd *cmd, Hypertable *ht,
								   WithClauseResult *with_clause_options);
	void (*process_altertable_cmd)(Hypertable *ht, const AlterTableCmd *cmd);
	void (*process_rename_cmd)(Hypertable *ht, const RenameStmt *stmt);
	PGFunction compress_chunk;
	PGFunction decompress_chunk;
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
	PGFunction data_node_allow_new_chunks;
	PGFunction data_node_block_new_chunks;

	PGFunction chunk_set_default_data_node;
	PGFunction create_chunk;
	PGFunction show_chunk;
	PGFunction copy_chunk_data;

	List *(*get_and_validate_data_node_list)(ArrayType *nodearr);
	void (*hypertable_make_distributed)(Hypertable *ht, List *data_node_names);
	PGFunction timescaledb_fdw_handler;
	PGFunction timescaledb_fdw_validator;
	void (*cache_syscache_invalidate)(Datum arg, int cacheid, uint32 hashvalue);
	PGFunction remote_txn_id_in;
	PGFunction remote_txn_id_out;
	PGFunction remote_txn_heal_data_node;
	PGFunction remote_connection_cache_show;
	void (*create_chunk_on_data_nodes)(Chunk *chunk, Hypertable *ht);
	Path *(*distributed_insert_path_create)(PlannerInfo *root, ModifyTablePath *mtpath,
											Index hypertable_rti, int subpath_index);
	uint64 (*distributed_copy)(const CopyStmt *stmt, CopyChunkState *ccstate, List *attnums);
	bool (*set_distributed_id)(Datum id);
	void (*set_distributed_peer_id)(Datum id);
	bool (*is_frontend_session)(void);
	bool (*remove_from_distributed_db)(void);
	PGFunction dist_remote_hypertable_info;
	PGFunction dist_remote_chunk_info;
	PGFunction dist_remote_compressed_chunk_info;
	PGFunction dist_remote_hypertable_index_info;
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
	void (*update_compressed_chunk_relstats)(Oid uncompressed_relid, Oid compressed_relid);
} CrossModuleFunctions;

extern TSDLLEXPORT CrossModuleFunctions *ts_cm_functions;
extern TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default;

#endif /* TIMESCALEDB_CROSS_MODULE_FN_H */
