/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_COMMON_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_COMMON_H

#include <postgres.h>
#include <fmgr.h>

#include <continuous_agg.h>

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

extern ContinuousAgg *continuous_agg_func_with_window(const FunctionCallInfo fcinfo,
													  InternalTimeRange *window);
extern InternalTimeRange compute_bucketed_refresh_window(const InternalTimeRange *refresh_window,
														 int64 bucket_width);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_COMMON_H */
