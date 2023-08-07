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
typedef struct Hypertable Hypertable;

extern int64 invalidation_threshold_set_or_get(const ContinuousAgg *cagg,
											   const InternalTimeRange *refresh_window);
extern int64 invalidation_threshold_compute(const ContinuousAgg *cagg,
											const InternalTimeRange *refresh_window);
extern void invalidation_threshold_initialize(const ContinuousAgg *cagg);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_H */
