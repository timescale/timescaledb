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

extern Datum continuous_agg_refresh(PG_FUNCTION_ARGS);
extern void continuous_agg_refresh_internal(const ContinuousAgg *cagg,
											const InternalTimeRange *refresh_window);
extern void continuous_agg_refresh_all(const Hypertable *ht, int64 start, int64 end);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_REFRESH_H */
