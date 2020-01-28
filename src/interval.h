/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "catalog.h"
#include "dimension.h"

#ifndef TIMESCALEDB_INTERVAL
#define TIMESCALEDB_INTERVAL

#define TS_INTERVAL_TYPE_NAME "ts_interval"
TSDLLEXPORT FormData_ts_interval *ts_interval_from_tuple(Datum ts_interval_datum);
TSDLLEXPORT FormData_ts_interval *ts_interval_from_sql_input(Oid relid, Datum interval,
															 Oid interval_type,
															 const char *parameter_name,
															 const char *caller_name);
TSDLLEXPORT HeapTuple ts_interval_form_heaptuple(FormData_ts_interval *invl);
TSDLLEXPORT bool ts_interval_equal(FormData_ts_interval *invl1, FormData_ts_interval *invl2);
TSDLLEXPORT void ts_interval_now_func_validate(Oid now_func_oid, Oid open_dim_type);
TSDLLEXPORT Datum ts_interval_subtract_from_now(FormData_ts_interval *invl, Dimension *open_dim);
TSDLLEXPORT int64 ts_get_now_internal(Dimension *open_dim);
TSDLLEXPORT FormData_ts_interval *
ts_interval_from_sql_input_internal(Dimension *open_dim, Datum interval, Oid interval_type,
									const char *parameter_name, const char *caller_name);
#endif /* TIMESCALEDB_INTERVAL */
