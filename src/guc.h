/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_GUC_H
#define TIMESCALEDB_GUC_H

#include <postgres.h>
#include "export.h"
#include "config.h"

#ifdef USE_TELEMETRY
extern bool ts_telemetry_on(void);
#endif

extern bool ts_guc_enable_optimizations;
extern bool ts_guc_enable_constraint_aware_append;
extern bool ts_guc_enable_ordered_append;
extern bool ts_guc_enable_chunk_append;
extern bool ts_guc_enable_parallel_chunk_append;
extern bool ts_guc_enable_qual_propagation;
extern bool ts_guc_enable_runtime_exclusion;
extern bool ts_guc_enable_constraint_exclusion;
extern bool ts_guc_enable_cagg_reorder_groupby;
extern TSDLLEXPORT bool ts_guc_enable_transparent_decompression;
extern TSDLLEXPORT bool ts_guc_enable_per_data_node_queries;
extern TSDLLEXPORT bool ts_guc_enable_async_append;
extern TSDLLEXPORT bool ts_guc_enable_skip_scan;
extern bool ts_guc_restoring;
extern int ts_guc_max_open_chunks_per_insert;
extern int ts_guc_max_cached_chunks_per_hypertable;
#ifdef USE_TELEMETRY
extern int ts_guc_telemetry_level;
extern char *ts_telemetry_cloud;
#endif
extern TSDLLEXPORT char *ts_guc_license;
extern char *ts_last_tune_time;
extern char *ts_last_tune_version;
extern TSDLLEXPORT bool ts_guc_enable_2pc;
extern TSDLLEXPORT int ts_guc_max_insert_batch_size;
extern TSDLLEXPORT bool ts_guc_enable_connection_binary_data;
extern TSDLLEXPORT bool ts_guc_enable_client_ddl_on_data_nodes;
extern TSDLLEXPORT char *ts_guc_ssl_dir;
extern TSDLLEXPORT char *ts_guc_passfile;
extern TSDLLEXPORT bool ts_guc_enable_remote_explain;

typedef enum DataFetcherType
{
	CursorFetcherType,
	RowByRowFetcherType,
	AutoFetcherType,
} DataFetcherType;

extern TSDLLEXPORT DataFetcherType ts_guc_remote_data_fetcher;

#ifdef TS_DEBUG
extern bool ts_shutdown_bgw;
extern char *ts_current_timestamp_mock;
#else
#define ts_shutdown_bgw false
#endif

void _guc_init(void);
void _guc_fini(void);

#endif /* TIMESCALEDB_GUC_H */
