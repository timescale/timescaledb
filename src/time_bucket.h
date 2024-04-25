/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

#include "export.h"

#define TIME_BUCKET_NG_DEFAULT_ORIGIN_TIMESTAMPSTAMPTZ "2000-01-01 00:00:00+01"
#define TIME_BUCKET_NG_DEFAULT_ORIGIN_TIMESTAMP "2000-01-01 00:00:00"
#define TIME_BUCKET_NG_DEFAULT_ORIGIN_DATE "2000-01-01"

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
extern TSDLLEXPORT Datum ts_time_bucket_ng_date(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_time_bucket_ng_timestamp(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_time_bucket_ng_timestamptz(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_time_bucket_ng_timezone(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_time_bucket_ng_timezone_origin(PG_FUNCTION_ARGS);
