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
	void (*add_tsl_license_info_telemetry)(JsonbParseState *parseState);
	bool (*bgw_policy_job_execute)(BgwJob *job);
	bool (*continuous_agg_materialize)(int32 materialization_id, ContinuousAggMatOptions *options);
	Datum (*add_drop_chunks_policy)(PG_FUNCTION_ARGS);
	Datum (*add_reorder_policy)(PG_FUNCTION_ARGS);
	Datum (*add_compress_chunks_policy)(PG_FUNCTION_ARGS);
	Datum (*remove_drop_chunks_policy)(PG_FUNCTION_ARGS);
	Datum (*remove_reorder_policy)(PG_FUNCTION_ARGS);
	Datum (*remove_compress_chunks_policy)(PG_FUNCTION_ARGS);
	void (*create_upper_paths_hook)(PlannerInfo *, UpperRelationKind, RelOptInfo *, RelOptInfo *);
	void (*set_rel_pathlist_dml)(PlannerInfo *, RelOptInfo *, Index, RangeTblEntry *, Hypertable *);
	void (*set_rel_pathlist_query)(PlannerInfo *, RelOptInfo *, Index, RangeTblEntry *,
								   Hypertable *);
	void (*set_rel_pathlist)(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
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
	bool (*process_cagg_viewstmt)(ViewStmt *stmt, const char *query_string, void *pstmt,
								  WithClauseResult *with_clause_options);
	void (*continuous_agg_drop_chunks_by_chunk_id)(int32 raw_hypertable_id, Chunk **chunks,
												   Size num_chunks,

												   Datum older_than_datum, Datum newer_than_datum,
												   Oid older_than_type, Oid newer_than_type,
												   bool cascade, int32 log_level,
												   bool user_supplied_table_name);
	PGFunction continuous_agg_trigfn;
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

	PGFunction add_server;
	PGFunction delete_server;
	PGFunction attach_server;
	PGFunction server_ping;
	PGFunction create_chunk;
	PGFunction show_chunk;
	List *(*get_servername_list)(void);
	void (*hypertable_make_distributed)(Hypertable *ht, ArrayType *servers);
	PGFunction timescaledb_fdw_handler;
	PGFunction timescaledb_fdw_validator;
	void (*cache_syscache_invalidate)(Datum arg, int cacheid, uint32 hashvalue);
	PGFunction remote_txn_id_in;
	PGFunction remote_txn_id_out;
	PGFunction remote_txn_heal_server;
	void (*create_chunk_on_servers)(Chunk *chunk, Hypertable *ht);
	Path *(*server_dispatch_path_create)(PlannerInfo *root, ModifyTablePath *mtpath,
										 Index hypertable_rti, int subpath_index);
	void (*distributed_copy)(const CopyStmt *stmt, uint64 *processed, CopyChunkState *ccstate,
							 List *attnums);
	bool (*set_distributed_id)(Datum id);
	void (*set_distributed_peer_id)(Datum id);
	bool (*is_frontend_session)(void);
	bool (*remove_from_distributed_db)(void);
} CrossModuleFunctions;

extern TSDLLEXPORT CrossModuleFunctions *ts_cm_functions;
extern TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default;

#endif /* TIMESCALEDB_CROSS_MODULE_FN_H */
