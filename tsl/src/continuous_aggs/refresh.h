/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_REFRESH_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_REFRESH_H

#include <postgres.h>
#include <fmgr.h>
#include "continuous_aggs/materialize.h"

#include "materialize.h"
#include "invalidation.h"

typedef enum CaggRefreshCallContext
{
	CAGG_REFRESH_CREATION,
	CAGG_REFRESH_WINDOW,
	CAGG_REFRESH_CHUNK,
	CAGG_REFRESH_POLICY,
} CaggRefreshCallContext;

extern Datum continuous_agg_refresh(PG_FUNCTION_ARGS);
extern Datum continuous_agg_refresh_chunk(PG_FUNCTION_ARGS);
extern void continuous_agg_calculate_merged_refresh_window(
	const InternalTimeRange *refresh_window, const InvalidationStore *invalidations,
	const int64 bucket_width, const ContinuousAggsBucketFunction *bucket_function,
	InternalTimeRange *merged_refresh_window);
extern void continuous_agg_refresh_internal(const ContinuousAgg *cagg,
											const InternalTimeRange *refresh_window,
											const CaggRefreshCallContext callctx);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_REFRESH_H */
