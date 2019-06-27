/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_HYPERTABLE
#define TIMESCALEDB_TSL_HYPERTABLE

#include "dimension.h"
#include "interval.h"
extern Datum hypertable_set_integer_now_func(PG_FUNCTION_ARGS);
extern Datum hypertable_valid_ts_interval(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_HYPERTABLE */
