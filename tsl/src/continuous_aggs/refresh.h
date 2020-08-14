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
void continuous_agg_refresh_internal(ContinuousAgg *cagg, InternalTimeRange *refresh_window);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_REFRESH_H */
