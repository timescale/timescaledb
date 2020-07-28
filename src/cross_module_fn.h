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
	void (*tsl_license_on_assign)(const char *newval, const void *license);
	bool (*enterprise_enabled_internal)(void);
	bool (*check_tsl_loaded)(void);
	TimestampTz (*license_end_time)(void);
	void (*print_tsl_license_expiration_info_hook)(void);
	void (*module_shutdown_hook)(void);
	void (*add_tsl_telemetry_info)(JsonbParseState **parse_state);
	bool (*bgw_policy_job_execute)(BgwJob *job);
	bool (*continuous_agg_materialize)(int32 materialization_id, ContinuousAggMatOptions *options);

	PGFunction add_retention_policy;
	PGFunction remove_retention_policy;
	PGFunction policy_compression_add;
	PGFunction policy_compression_proc;
	PGFunction policy_compression_remove;
	PGFunction policy_reorder_add;
	PGFunction policy_reorder_proc;
	PGFunction policy_reorder_remove;

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

	PGFunction alter_job_schedule;
	PGFunction reorder_chunk;
	PGFunction move_chunk;
	void (*ddl_command_start)(ProcessUtilityArgs *args);
	void (*ddl_command_end)(EventTriggerData *command);
	void (*sql_drop)(List *dropped_objects);
	PGFunction partialize_agg;
	PGFunction finalize_agg_sfunc;
	PGFunction finalize_agg_ffunc;
	DDLResult (*process_cagg_viewstmt)(ViewStmt *stmt, const char *query_string, void *pstmt,
									   WithClauseResult *with_clause_options);
	void (*continuous_agg_drop_chunks_by_chunk_id)(int32 raw_hypertable_id, Chunk **chunks,
												   Size num_chunks, Datum older_than_datum,
												   Datum newer_than_datum, Oid older_than_type,
												   Oid newer_than_type, int32 log_level);
	PGFunction continuous_agg_invalidation_trigger;
	PGFunction continuous_agg_refresh;
	void (*continuous_agg_update_options)(ContinuousAgg *cagg,
										  WithClauseResult *with_clause_options);

	PGFunction compressed_data_send;
	PGFunction compressed_data_recv;
	PGFunction compressed_data_in;
	PGFunction compressed_data_out;
	bool (*process_compress_table)(AlterTableCmd *cmd, Hypertable *ht,
								   WithClauseResult *with_clause_options);
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

	Datum (*data_node_add)(PG_FUNCTION_ARGS);
	Datum (*data_node_delete)(PG_FUNCTION_ARGS);
	Datum (*data_node_attach)(PG_FUNCTION_ARGS);
	Datum (*data_node_ping)(PG_FUNCTION_ARGS);
	Datum (*data_node_detach)(PG_FUNCTION_ARGS);
	Datum (*data_node_allow_new_chunks)(PG_FUNCTION_ARGS);
	Datum (*data_node_block_new_chunks)(PG_FUNCTION_ARGS);

	Datum (*chunk_set_default_data_node)(PG_FUNCTION_ARGS);
	Datum (*create_chunk)(PG_FUNCTION_ARGS);
	Datum (*show_chunk)(PG_FUNCTION_ARGS);
	List *(*get_and_validate_data_node_list)(ArrayType *nodearr);
	void (*hypertable_make_distributed)(Hypertable *ht, List *data_node_names);
	Datum (*timescaledb_fdw_handler)(PG_FUNCTION_ARGS);
	Datum (*timescaledb_fdw_validator)(PG_FUNCTION_ARGS);
	void (*cache_syscache_invalidate)(Datum arg, int cacheid, uint32 hashvalue);
	PGFunction remote_txn_id_in;
	PGFunction remote_txn_id_out;
	PGFunction remote_txn_heal_data_node;
	PGFunction remote_connection_cache_show;
	void (*create_chunk_on_data_nodes)(Chunk *chunk, Hypertable *ht);
	Path *(*data_node_dispatch_path_create)(PlannerInfo *root, ModifyTablePath *mtpath,
											Index hypertable_rti, int subpath_index);
	void (*distributed_copy)(const CopyStmt *stmt, uint64 *processed, CopyChunkState *ccstate,
							 List *attnums);
	bool (*set_distributed_id)(Datum id);
	void (*set_distributed_peer_id)(Datum id);
	bool (*is_frontend_session)(void);
	bool (*remove_from_distributed_db)(void);
	PGFunction dist_remote_hypertable_info;
	PGFunction dist_remote_chunk_info;
	void (*validate_as_data_node)(void);
	void (*func_call_on_data_nodes)(FunctionCallInfo fcinfo, List *data_node_oids);
	PGFunction distributed_exec;
	PGFunction chunk_get_relstats;
	PGFunction chunk_get_colstats;
	PGFunction hypertable_distributed_set_replication_factor;
} CrossModuleFunctions;

extern TSDLLEXPORT CrossModuleFunctions *ts_cm_functions;
extern TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default;

#endif /* TIMESCALEDB_CROSS_MODULE_FN_H */
