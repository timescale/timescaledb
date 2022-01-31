/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TIME_UTILS_H
#define TIMESCALEDB_TIME_UTILS_H

#include <postgres.h>

#include "export.h"

/* TimescaleDB-specific ranges for valid timestamps and dates: */
#define TS_EPOCH_DIFF (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)
#define TS_EPOCH_DIFF_MICROSECONDS (TS_EPOCH_DIFF * USECS_PER_DAY)

/* For Timestamps, we need to be able to go from UNIX epoch to POSTGRES epoch
 * and thus add the difference between the two epochs. This will constrain the
 * max supported timestamp by the same amount. */
#define TS_TIMESTAMP_MIN MIN_TIMESTAMP
#define TS_TIMESTAMP_MAX (TS_TIMESTAMP_END - 1)
#define TS_TIMESTAMP_END (END_TIMESTAMP - TS_EPOCH_DIFF_MICROSECONDS)
#define TS_TIMESTAMP_INTERNAL_MIN (TS_TIMESTAMP_MIN + TS_EPOCH_DIFF_MICROSECONDS)
#define TS_TIMESTAMP_INTERNAL_MAX (TS_TIMESTAMP_INTERNAL_END - 1)
#define TS_TIMESTAMP_INTERNAL_END (TS_TIMESTAMP_END + TS_EPOCH_DIFF_MICROSECONDS)

/* For Dates, we're limited by the timestamp range (since we internally first
 * convert dates to timestamps). Naturally the TimescaleDB-specific timestamp
 * limits apply as well. */
#define TS_DATE_MIN (DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE)
#define TS_DATE_MAX (TS_DATE_END - 1)
#define TS_DATE_END (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE - TS_EPOCH_DIFF)
#define TS_DATE_INTERNAL_MIN (TS_TIMESTAMP_MIN + TS_EPOCH_DIFF_MICROSECONDS)
#define TS_DATE_INTERNAL_MAX (TS_DATE_INTERNAL_END - 1)
#define TS_DATE_INTERNAL_END (TS_TIMESTAMP_END + TS_EPOCH_DIFF_MICROSECONDS)

/*
 * -Infinity and +Infinity in internal (Unix) time.
 */
#define TS_TIME_NOBEGIN (PG_INT64_MIN)
#define TS_TIME_NOEND (PG_INT64_MAX)

#define IS_INTEGER_TYPE(type) (type == INT2OID || type == INT4OID || type == INT8OID)
#define IS_TIMESTAMP_TYPE(type) (type == TIMESTAMPOID || type == TIMESTAMPTZOID || type == DATEOID)
#define IS_VALID_TIME_TYPE(type) (IS_INTEGER_TYPE(type) || IS_TIMESTAMP_TYPE(type))

#define TS_TIME_DATUM_IS_MIN(timeval, type) (timeval == ts_time_datum_get_min(type))
#define TS_TIME_DATUM_IS_MAX(timeval, type) (timeval == ts_time_datum_get_max(type))
#define TS_TIME_DATUM_IS_END(timeval, type)                                                        \
	(IS_TIMESTAMP_TYPE(type) && timeval == ts_time_datum_get_end(type)))
#define TS_TIME_DATUM_IS_NOBEGIN(timeval, type)                                                    \
	(IS_TIMESTAMP_TYPE(type) && (timeval == ts_time_datum_get_nobegin(type)))
#define TS_TIME_DATUM_IS_NOEND(timeval, type)                                                      \
	(IS_TIMESTAMP_TYPE(type) && (timeval == ts_time_datum_get_noend(type)))

#define TS_TIME_DATUM_NOT_FINITE(timeval, type)                                                    \
	(IS_INTEGER_TYPE(type) || TS_TIME_DATUM_IS_NOBEGIN(timeval, type) ||                           \
	 TS_TIME_DATUM_IS_NOEND(timeval, type))

#define TS_TIME_IS_MIN(timeval, type) (timeval == ts_time_get_min(type))
#define TS_TIME_IS_MAX(timeval, type) (timeval == ts_time_get_max(type))
#define TS_TIME_IS_END(timeval, type) (IS_TIMESTAMP_TYPE(type) && timeval == ts_time_get_end(type))
#define TS_TIME_IS_NOBEGIN(timeval, type)                                                          \
	(IS_TIMESTAMP_TYPE(type) && timeval == ts_time_get_nobegin(type))
#define TS_TIME_IS_NOEND(timeval, type)                                                            \
	(IS_TIMESTAMP_TYPE(type) && timeval == ts_time_get_noend(type))

#define TS_TIME_NOT_FINITE(timeval, type)                                                          \
	(IS_INTEGER_TYPE(type) || TS_TIME_IS_NOBEGIN(timeval, type) || TS_TIME_IS_NOEND(timeval, type))

extern TSDLLEXPORT int64 ts_time_value_from_arg(Datum arg, Oid argtype, Oid timetype);
extern TSDLLEXPORT Datum ts_time_datum_convert_arg(Datum arg, Oid *argtype, Oid timetype);
extern TSDLLEXPORT Datum ts_time_datum_get_min(Oid timetype);
extern TSDLLEXPORT Datum ts_time_datum_get_max(Oid timetype);
extern TSDLLEXPORT Datum ts_time_datum_get_end(Oid timetype);
extern TSDLLEXPORT Datum ts_time_datum_get_nobegin(Oid timetype);
extern TSDLLEXPORT Datum ts_time_datum_get_nobegin_or_min(Oid timetype);
extern TSDLLEXPORT Datum ts_time_datum_get_noend(Oid timetype);
extern TSDLLEXPORT int64 ts_time_get_min(Oid timetype);
extern TSDLLEXPORT int64 ts_time_get_max(Oid timetype);
extern TSDLLEXPORT int64 ts_time_get_end(Oid timetype);
extern TSDLLEXPORT int64 ts_time_get_end_or_max(Oid timetype);
extern TSDLLEXPORT int64 ts_time_get_nobegin(Oid timetype);
extern TSDLLEXPORT int64 ts_time_get_noend(Oid timetype);
extern TSDLLEXPORT int64 ts_time_get_noend_or_max(Oid timetype);
extern TSDLLEXPORT int64 ts_time_saturating_add(int64 timeval, int64 interval, Oid timetype);
extern TSDLLEXPORT int64 ts_time_saturating_sub(int64 timeval, int64 interval, Oid timetype);
extern TSDLLEXPORT int64 ts_subtract_integer_from_now_saturating(Oid now_func, int64 interval,
																 Oid timetype);
#ifdef TS_DEBUG
extern TSDLLEXPORT Datum ts_get_mock_time_or_current_time(void);
#endif

#endif /* TIMESCALEDB_TIME_UTILS_H */
