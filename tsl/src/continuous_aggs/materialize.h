/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_MATERIALIZE_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_MATERIALIZE_H

#include <postgres.h>
#include <fmgr.h>
#include <nodes/pg_list.h>
#include "continuous_agg.h"

typedef struct SchemaAndName
{
	Name schema;
	Name name;
} SchemaAndName;

typedef struct Invalidation
{
	int64 modification_time;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
} Invalidation;

int64 invalidation_threshold_get(int32 raw_hypertable_id);
bool continuous_agg_materialize(int32 materialization_id, ContinuousAggMatOptions *options);
void continuous_agg_execute_materialization(int64 bucket_width, int32 hypertable_id,
											int32 materialization_id, SchemaAndName partial_view,
											int64 invalidation_range_start,
											int64 invalidation_range_end,
											int64 materialization_invalidation_threshold);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_MATERIALIZE_H */
