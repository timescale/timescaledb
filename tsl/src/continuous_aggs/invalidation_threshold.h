/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_H

#include <postgres.h>

typedef struct InternalTimeRange InternalTimeRange;
typedef struct ContinuousAgg ContinuousAgg;

extern int64 invalidation_threshold_get(int32 hypertable_id);
extern int64 invalidation_threshold_set_or_get(int32 raw_hypertable_id,
											   int64 invalidation_threshold);
extern void invalidation_threshold_lock(int32 raw_hypertable_id);
extern int64 invalidation_threshold_compute(const ContinuousAgg *cagg,
											const InternalTimeRange *refresh_window);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_H */
