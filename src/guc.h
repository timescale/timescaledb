/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_GUC_H
#define TIMESCALEDB_GUC_H

#include <postgres.h>
#include "export.h"

extern bool ts_telemetry_on(void);

extern bool ts_guc_disable_optimizations;
extern bool ts_guc_optimize_non_hypertables;
extern bool ts_guc_constraint_aware_append;
extern bool ts_guc_enable_ordered_append;
extern bool ts_guc_enable_chunk_append;
extern bool ts_guc_enable_parallel_chunk_append;
extern bool ts_guc_enable_runtime_exclusion;
extern bool ts_guc_enable_constraint_exclusion;
extern TSDLLEXPORT bool ts_guc_enable_transparent_decompression;
extern bool ts_guc_restoring;
extern int ts_guc_max_open_chunks_per_insert;
extern int ts_guc_max_cached_chunks_per_hypertable;
extern int ts_guc_telemetry_level;
extern TSDLLEXPORT char *ts_guc_license_key;
extern char *ts_last_tune_time;
extern char *ts_last_tune_version;
extern char *ts_telemetry_cloud;

#ifdef TS_DEBUG
extern bool ts_shutdown_bgw;
#else
#define ts_shutdown_bgw false
#endif

void _guc_init(void);
void _guc_fini(void);

#endif /* TIMESCALEDB_GUC_H */
