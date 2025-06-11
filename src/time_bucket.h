/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

#include "export.h"

extern TSDLLEXPORT Datum ts_int16_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_int32_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_int64_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_date_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_timestamp_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_timestamptz_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_timestamptz_timezone_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT int64 ts_time_bucket_by_type(int64 interval, int64 timestamp, Oid type);
extern TSDLLEXPORT int64 ts_time_bucket_by_type_extended(int64 interval, int64 timestamp, Oid type,
														 NullableDatum offset,
														 NullableDatum origin);
