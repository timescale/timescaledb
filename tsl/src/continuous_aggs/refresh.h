/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_REFRESH_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_REFRESH_H

#include <postgres.h>
#include <fmgr.h>

extern Datum continuous_agg_refresh(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_REFRESH_H */
