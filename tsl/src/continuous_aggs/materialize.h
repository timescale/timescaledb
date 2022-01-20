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
#include "ts_catalog/continuous_agg.h"

typedef struct SchemaAndName
{
	Name schema;
	Name name;
} SchemaAndName;

/***********************
 * Time ranges
 ***********************/

typedef struct TimeRange
{
	Oid type;
	Datum start;
	Datum end;
} TimeRange;

typedef struct InternalTimeRange
{
	Oid type;
	int64 start; /* inclusive */
	int64 end;   /* exclusive */
} InternalTimeRange;

InternalTimeRange continuous_agg_materialize_window_max(Oid timetype);
void continuous_agg_update_materialization(SchemaAndName partial_view,
										   SchemaAndName materialization_table,
										   const NameData *time_column_name,
										   InternalTimeRange new_materialization_range,
										   InternalTimeRange invalidation_range, int32 chunk_id);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_MATERIALIZE_H */
