/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Functions used by gapfill which are exported to SQL.
 * Shell functions for these are defined in src/gapfill.c
 */

#ifndef TIMESCALEDB_TSL_NODES_GAPFILL_FUNCTIONS_H
#define TIMESCALEDB_TSL_NODES_GAPFILL_FUNCTIONS_H

#include <postgres.h>
#include <fmgr.h>

extern Datum gapfill_marker(PG_FUNCTION_ARGS);
extern Datum gapfill_int16_time_bucket(PG_FUNCTION_ARGS);
extern Datum gapfill_int32_time_bucket(PG_FUNCTION_ARGS);
extern Datum gapfill_int64_time_bucket(PG_FUNCTION_ARGS);
extern Datum gapfill_timestamp_time_bucket(PG_FUNCTION_ARGS);
extern Datum gapfill_timestamptz_time_bucket(PG_FUNCTION_ARGS);
extern Datum gapfill_timestamptz_timezone_time_bucket(PG_FUNCTION_ARGS);
extern Datum gapfill_date_time_bucket(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_NODES_GAPFILL_FUNCTIONS_H */
