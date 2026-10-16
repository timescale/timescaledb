/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <datatype/timestamp.h>
#include <fmgr.h>

#include "export.h"

extern TSDLLEXPORT Datum ts_int16_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_int32_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_int64_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_date_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_timestamp_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_date_offset_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_timestamp_offset_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_timestamptz_offset_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_timestamptz_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_timestamptz_timezone_bucket(PG_FUNCTION_ARGS);
extern TSDLLEXPORT int64 ts_time_bucket_by_type(int64 interval, int64 timestamp, Oid type);
extern TSDLLEXPORT int64 ts_time_bucket_by_type_extended(int64 interval, int64 timestamp, Oid type,
														 NullableDatum offset,
														 NullableDatum origin);

#define JAN_3_2000 (2 * USECS_PER_DAY)

/*
 * The default origin is Monday 2000-01-03. We don't use PG epoch since it starts on a saturday.
 * This makes time-buckets by a week more intuitive and aligns it with date_trunc. Since month
 * bucketing ignores the day component this makes origin for month buckets 2000-01-01.
 */
#define DEFAULT_ORIGIN (JAN_3_2000)
