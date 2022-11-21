/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_STATS_H
#define TIMESCALEDB_TELEMETRY_STATS_H

#include <postgres.h>

#include "utils.h"

typedef enum StatsRelType
{
	RELTYPE_HYPERTABLE,
	RELTYPE_DISTRIBUTED_HYPERTABLE,
	RELTYPE_DISTRIBUTED_HYPERTABLE_MEMBER,
	RELTYPE_MATERIALIZED_HYPERTABLE,
	RELTYPE_COMPRESSION_HYPERTABLE,
	RELTYPE_CONTINUOUS_AGG,
	RELTYPE_TABLE,
	RELTYPE_PARTITIONED_TABLE,
	RELTYPE_PARTITION,
	RELTYPE_VIEW,
	RELTYPE_MATVIEW,
	RELTYPE_CHUNK,
	RELTYPE_DISTRIBUTED_CHUNK,
	RELTYPE_DISTRIBUTED_CHUNK_MEMBER,
	RELTYPE_COMPRESSION_CHUNK,
	RELTYPE_MATERIALIZED_CHUNK,
	RELTYPE_OTHER,
} StatsRelType;

typedef enum StatsType
{
	STATS_TYPE_BASE,
	STATS_TYPE_STORAGE,
	STATS_TYPE_HYPER,
	STATS_TYPE_CAGG,
} StatsType;

typedef struct BaseStats
{
	int64 relcount;
	int64 reltuples;
} BaseStats;

typedef struct StorageStats
{
	BaseStats base;
	RelationSize relsize;
} StorageStats;

typedef struct HyperStats
{
	StorageStats storage;
	int64 replicated_hypertable_count;
	int64 child_count;
	int64 replica_chunk_count; /* only includes "additional" replica chunks */
	int64 compressed_chunk_count;
	int64 compressed_hypertable_count;
	int64 compressed_size;
	int64 compressed_heap_size;
	int64 compressed_indexes_size;
	int64 compressed_toast_size;
	int64 compressed_row_count;
	int64 uncompressed_heap_size;
	int64 uncompressed_indexes_size;
	int64 uncompressed_toast_size;
	int64 uncompressed_row_count;
} HyperStats;

typedef struct CaggStats
{
	HyperStats hyp; /* "hyper" as field name leads to name conflict on Windows compiler */
	int64 on_distributed_hypertable_count;
	int64 uses_real_time_aggregation_count;
	int64 finalized;
	int64 nested;
} CaggStats;

typedef struct TelemetryStats
{
	HyperStats hypertables;
	HyperStats distributed_hypertables;
	HyperStats distributed_hypertable_members;
	HyperStats partitioned_tables;
	StorageStats tables;
	StorageStats materialized_views;
	CaggStats continuous_aggs;
	BaseStats views;
} TelemetryStats;

typedef struct TelemetryJobStats
{
	int64 total_runs;
	int64 total_successes;
	int64 total_failures;
	int64 total_crashes;
	int32 max_consecutive_failures;
	int32 max_consecutive_crashes;
	Interval *total_duration;
	Interval *total_duration_failures;
} TelemetryJobStats;

extern void ts_telemetry_stats_gather(TelemetryStats *stats);

#endif /* TIMESCALEDB_TELEMETRY_STATS_H */
