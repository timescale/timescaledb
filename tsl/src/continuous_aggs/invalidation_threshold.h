/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_H

#include <postgres.h>

extern int64 continuous_agg_invalidation_threshold_get(int32 hypertable_id);
extern bool continuous_agg_invalidation_threshold_set(int32 raw_hypertable_id,
													  int64 invalidation_threshold);
extern void continuous_agg_invalidation_threshold_lock(int32 raw_hypertable_id);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_H */
