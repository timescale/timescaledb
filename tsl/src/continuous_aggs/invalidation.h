/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_H

#include <postgres.h>

#include "continuous_agg.h"
#include "continuous_aggs/materialize.h"

extern void continuous_agg_invalidation_process(const ContinuousAgg *cagg,
												const InternalTimeRange *refresh_window);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_INVALIDATION_H */
