/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/datetime.h>
#include <utils/fmgrprotos.h>
#include <utils/timestamp.h>

#include "utils.h"
#include "time_bucket.h"

#define TIME_BUCKET(period, timestamp, offset, min, max, result)                                   \
	do                                                                                             \
	{                                                                                              \
		if (period <= 0)                                                                           \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),                                     \
					 errmsg("period must be greater than 0")));                                    \
		if (offset != 0)                                                                           \
		{                                                                                          \
			/* We need to ensure that the timestamp is in range _after_ the */                     \
			/* offset is applied: when the offset is positive we need to make */                   \
			/* sure the resultant time is at least min, and when negative that */                  \
			/* it is less than the max. */                                                         \
			offset = offset % period;                                                              \
			if ((offset > 0 && timestamp < min + offset) ||                                        \
				(offset < 0 && timestamp > max + offset))                                          \
				ereport(ERROR,                                                                     \
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),                              \
						 errmsg("timestamp out of range")));                                       \
			timestamp -= offset;                                                                   \
		}                                                                                          \
		result = (timestamp / period) * period;                                                    \
		if (timestamp < 0 && timestamp % period)                                                   \
		{                                                                                          \
			if (result < min + period)                                                             \
				ereport(ERROR,                                                                     \
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),                              \
						 errmsg("timestamp out of range")));                                       \
			else                                                                                   \
				result = result - period;                                                          \
		}                                                                                          \
		result += offset;                                                                          \
	} while (0)

TS_FUNCTION_INFO_V1(ts_int16_bucket);
TSDLLEXPORT Datum
ts_int16_bucket(PG_FUNCTION_ARGS)
{
	int16 result;
	int16 period = PG_GETARG_INT16(0);
	int16 timestamp = PG_GETARG_INT16(1);
	int16 offset = PG_NARGS() > 2 ? PG_GETARG_INT16(2) : 0;

	TIME_BUCKET(period, timestamp, offset, PG_INT16_MIN, PG_INT16_MAX, result);

	PG_RETURN_INT16(result);
}

TS_FUNCTION_INFO_V1(ts_int32_bucket);
TSDLLEXPORT Datum
ts_int32_bucket(PG_FUNCTION_ARGS)
{
	int32 result;
	int32 period = PG_GETARG_INT32(0);
	int32 timestamp = PG_GETARG_INT32(1);
	int32 offset = PG_NARGS() > 2 ? PG_GETARG_INT32(2) : 0;

	TIME_BUCKET(period, timestamp, offset, PG_INT32_MIN, PG_INT32_MAX, result);

	PG_RETURN_INT32(result);
}

TS_FUNCTION_INFO_V1(ts_int64_bucket);
TSDLLEXPORT Datum
ts_int64_bucket(PG_FUNCTION_ARGS)
{
	int64 result;
	int64 period = PG_GETARG_INT64(0);
	int64 timestamp = PG_GETARG_INT64(1);
	int64 offset = PG_NARGS() > 2 ? PG_GETARG_INT64(2) : 0;

	TIME_BUCKET(period, timestamp, offset, PG_INT64_MIN, PG_INT64_MAX, result);

	PG_RETURN_INT64(result);
}

#define JAN_3_2000 (2 * USECS_PER_DAY)

/*
 * The default origin is Monday 2000-01-03. We don't use PG epoch since it starts on a saturday.
 * This makes time-buckets by a week more intuitive and aligns it with
 * date_trunc.
 */
#define DEFAULT_ORIGIN (JAN_3_2000)
#define TIME_BUCKET_TS(period, timestamp, result, shift)                                           \
	do                                                                                             \
	{                                                                                              \
		if (period <= 0)                                                                           \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),                                     \
					 errmsg("period must be greater than 0")));                                    \
		/* shift = shift % period, but use TMODULO */                                              \
		TMODULO(shift, result, period);                                                            \
                                                                                                   \
		if ((shift > 0 && timestamp < DT_NOBEGIN + shift) ||                                       \
			(shift < 0 && timestamp > DT_NOEND + shift))                                           \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),                                  \
					 errmsg("timestamp out of range")));                                           \
		timestamp -= shift;                                                                        \
                                                                                                   \
		/* result = (timestamp / period) * period */                                               \
		TMODULO(timestamp, result, period);                                                        \
		if (timestamp < 0)                                                                         \
		{                                                                                          \
			/*                                                                                     \
			 * need to subtract another period if remainder < 0 this only happens                  \
			 * if timestamp is negative to begin with and there is a remainder                     \
			 * after division. Need to subtract another period since division                      \
			 * truncates toward 0 in C99.                                                          \
			 */                                                                                    \
			result = (result * period) - period;                                                   \
		}                                                                                          \
		else                                                                                       \
			result *= period;                                                                      \
                                                                                                   \
		result += shift;                                                                           \
	} while (0)

/* Returns the period in the same representation as Postgres Timestamps.
 * Note that this is not our internal representation (microseconds).
 * Always returns an exact value.*/
static inline int64
get_interval_period_timestamp_units(Interval *interval)
{
	if (interval->month != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval defined in terms of month, year, century etc. not supported")));
	}
	return interval->time + (interval->day * USECS_PER_DAY);
}

TS_FUNCTION_INFO_V1(ts_timestamp_bucket);

TSDLLEXPORT Datum
ts_timestamp_bucket(PG_FUNCTION_ARGS)
{
	Interval *interval = PG_GETARG_INTERVAL_P(0);
	Timestamp timestamp = PG_GETARG_TIMESTAMP(1);

	/*
	 * USE NARGS and not IS_NULL to differentiate a NULL argument from a call
	 * with 2 parameters
	 */
	Timestamp origin = (PG_NARGS() > 2 ? PG_GETARG_TIMESTAMP(2) : DEFAULT_ORIGIN);
	Timestamp result;
	int64 period = get_interval_period_timestamp_units(interval);

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	TIME_BUCKET_TS(period, timestamp, result, origin);

	PG_RETURN_TIMESTAMP(result);
}

TS_FUNCTION_INFO_V1(ts_timestamptz_bucket);

TSDLLEXPORT Datum
ts_timestamptz_bucket(PG_FUNCTION_ARGS)
{
	Interval *interval = PG_GETARG_INTERVAL_P(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);

	/*
	 * USE NARGS and not IS_NULL to differentiate a NULL argument from a call
	 * with 2 parameters
	 */
	TimestampTz origin = (PG_NARGS() > 2 ? PG_GETARG_TIMESTAMPTZ(2) : DEFAULT_ORIGIN);
	TimestampTz result;
	int64 period = get_interval_period_timestamp_units(interval);

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMPTZ(timestamp);

	TIME_BUCKET_TS(period, timestamp, result, origin);

	PG_RETURN_TIMESTAMPTZ(result);
}

static inline void
check_period_is_daily(int64 period)
{
	int64 day = USECS_PER_DAY;

	if (period < day)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval must not have sub-day precision")));
	}
	if (period % day != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval must be a multiple of a day")));
	}
}

TS_FUNCTION_INFO_V1(ts_date_bucket);

TSDLLEXPORT Datum
ts_date_bucket(PG_FUNCTION_ARGS)
{
	Interval *interval = PG_GETARG_INTERVAL_P(0);
	DateADT date = PG_GETARG_DATEADT(1);
	Timestamp origin = DEFAULT_ORIGIN;
	Timestamp timestamp, result;
	int64 period = -1;

	if (DATE_NOT_FINITE(date))
		PG_RETURN_DATEADT(date);

	period = get_interval_period_timestamp_units(interval);
	/* check the period aligns on a date */
	check_period_is_daily(period);

	/* convert to timestamp (NOT tz), bucket, convert back to date */
	timestamp = DatumGetTimestamp(DirectFunctionCall1(date_timestamp, PG_GETARG_DATUM(1)));
	if (PG_NARGS() > 2)
		origin = DatumGetTimestamp(DirectFunctionCall1(date_timestamp, PG_GETARG_DATUM(2)));

	Assert(!TIMESTAMP_NOT_FINITE(timestamp));

	TIME_BUCKET_TS(period, timestamp, result, origin);

	PG_RETURN_DATUM(DirectFunctionCall1(timestamp_date, TimestampGetDatum(result)));
}

/* when working with time_buckets stored in our catalog, we may not know ahead of time which
 * bucketing function to use, this function dynamically dispatches to the correct time_bucket_<foo>
 * based on an inputted timestamp_type*/
TSDLLEXPORT int64
ts_time_bucket_by_type(int64 interval, int64 timestamp, Oid timestamp_type)
{
	Datum timestamp_in_time_type = ts_internal_to_time_value(timestamp, timestamp_type);
	Datum interval_in_interval_type;
	Datum time_bucketed;
	Datum (*bucket_function)(PG_FUNCTION_ARGS);

	switch (timestamp_type)
	{
		case INT2OID:
			interval_in_interval_type = ts_internal_to_interval_value(interval, timestamp_type);
			bucket_function = ts_int16_bucket;
			break;
		case INT4OID:
			interval_in_interval_type = ts_internal_to_interval_value(interval, timestamp_type);
			bucket_function = ts_int32_bucket;
			break;
		case INT8OID:
			interval_in_interval_type = ts_internal_to_interval_value(interval, timestamp_type);
			bucket_function = ts_int64_bucket;
			break;
		case TIMESTAMPOID:
			interval_in_interval_type = ts_internal_to_interval_value(interval, INTERVALOID);
			bucket_function = ts_timestamp_bucket;
			break;
		case TIMESTAMPTZOID:
			interval_in_interval_type = ts_internal_to_interval_value(interval, INTERVALOID);
			bucket_function = ts_timestamptz_bucket;
			break;
		case DATEOID:
			interval_in_interval_type = ts_internal_to_interval_value(interval, INTERVALOID);
			bucket_function = ts_date_bucket;
			break;
		default:
			elog(ERROR, "invalid time_bucket type \"%s\"", format_type_be(timestamp_type));
	}

	time_bucketed =
		DirectFunctionCall2(bucket_function, interval_in_interval_type, timestamp_in_time_type);

	return ts_time_value_to_internal(time_bucketed, timestamp_type);
}

TS_FUNCTION_INFO_V1(ts_time_bucket_ng_timestamp);
TSDLLEXPORT Datum
ts_time_bucket_ng_timestamp(PG_FUNCTION_ARGS)
{
	DateADT ts_date;
	Interval *interval = PG_GETARG_INTERVAL_P(0);
	Timestamp timestamp = PG_GETARG_TIMESTAMP(1);

	if (interval->time != 0)
	{
		if (interval->month != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("interval can't combine months with minutes or hours")));
		}

		if (TIMESTAMP_NOT_FINITE(timestamp))
			PG_RETURN_TIMESTAMP(timestamp);

		/*
		 * Handle minutes, hours and days.
		 */
		Timestamp origin = (PG_NARGS() > 2 ? PG_GETARG_TIMESTAMP(2) : DEFAULT_ORIGIN);
		if (TIMESTAMP_NOT_FINITE(origin))
			PG_RETURN_TIMESTAMP(origin);

		Timestamp result;
		int64 period = get_interval_period_timestamp_units(interval);
		TIME_BUCKET_TS(period, timestamp, result, origin);

		PG_RETURN_TIMESTAMP(result);
	}

	/*
	 * Discard any time information and work with the date.
	 */
	ts_date = DatumGetDateADT(DirectFunctionCall1(timestamp_date, PG_GETARG_DATUM(1)));

	if (PG_NARGS() > 2)
	{
		DateADT result;
		DateADT origin = DatumGetDateADT(DirectFunctionCall1(timestamp_date, PG_GETARG_DATUM(2)));
		result = DatumGetDateADT(DirectFunctionCall3(ts_time_bucket_ng_date,
													 PG_GETARG_DATUM(0),
													 DateADTGetDatum(ts_date),
													 DateADTGetDatum(origin)));
		return DirectFunctionCall1(date_timestamp, DateADTGetDatum(result));
	}
	else
	{
		DateADT result;
		result = DatumGetDateADT(DirectFunctionCall2(ts_time_bucket_ng_date,
													 PG_GETARG_DATUM(0),
													 DateADTGetDatum(ts_date)));
		return DirectFunctionCall1(date_timestamp, DateADTGetDatum(result));
	}
}

TS_FUNCTION_INFO_V1(ts_time_bucket_ng_timestamptz);
TSDLLEXPORT Datum
ts_time_bucket_ng_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT result;
	Datum interval = PG_GETARG_DATUM(0);
	DateADT ts_date = DatumGetDateADT(DirectFunctionCall1(timestamptz_date, PG_GETARG_DATUM(1)));

	if (PG_NARGS() > 2)
	{
		DateADT origin = DatumGetDateADT(DirectFunctionCall1(timestamptz_date, PG_GETARG_DATUM(2)));
		result = DatumGetDateADT(DirectFunctionCall3(ts_time_bucket_ng_date,
													 interval,
													 DateADTGetDatum(ts_date),
													 DateADTGetDatum(origin)));
	}
	else
	{
		result = DatumGetDateADT(
			DirectFunctionCall2(ts_time_bucket_ng_date, interval, DateADTGetDatum(ts_date)));
	}

	return DirectFunctionCall1(date_timestamptz, DateADTGetDatum(result));
}

TS_FUNCTION_INFO_V1(ts_time_bucket_ng_date);
TSDLLEXPORT Datum
ts_time_bucket_ng_date(PG_FUNCTION_ARGS)
{
	Interval *interval = PG_GETARG_INTERVAL_P(0);
	DateADT date = PG_GETARG_DATEADT(1);
	DateADT origin_date = 0;
	int origin_year = 2000, origin_month = 1, origin_day = 1;
	int year, month, day;
	int delta, bucket_number;

	if ((interval->time != 0) || ((interval->month != 0) && (interval->day != 0)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval must be either days and weeks, or months and years")));
	}

	if ((interval->month == 0) && (interval->day == 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval must be at least one day")));
	}

	if (PG_NARGS() > 2)
	{
		origin_date = PG_GETARG_DATUM(2);
		if (DATE_NOT_FINITE(origin_date))
			PG_RETURN_DATEADT(origin_date);

		j2date(origin_date + POSTGRES_EPOCH_JDATE, &origin_year, &origin_month, &origin_day);
	}

	if ((origin_day != 1) && (interval->month != 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("origin must be the first day of the month"),
				 errhint("When using timestamptz-version of the function, 'origin' is "
						 "converted to provided 'timezone'.")));
	}

	if (DATE_NOT_FINITE(date))
		PG_RETURN_DATEADT(date);

	if (interval->month != 0)
	{
		/* Handle months and years */

		j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);

		if ((year < origin_year) || ((year == origin_year) && (month < origin_month)))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("origin must be before the given date")));
		}

		delta = (year * 12 + month) - (origin_year * 12 + origin_month);
		bucket_number = delta / interval->month;
		year = origin_year + (origin_month - 1 + bucket_number * interval->month) / 12;
		month =
			(((origin_year * 12 + (origin_month - 1)) + (bucket_number * interval->month)) % 12) +
			1;
		day = 1;

		date = date2j(year, month, day) - POSTGRES_EPOCH_JDATE;
	}
	else
	{
		/* Handle days and weeks */

		if (date < origin_date)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("origin must be before the given date")));
		}

		delta = date - origin_date;
		bucket_number = delta / interval->day;
		date = origin_date + bucket_number * interval->day;
	}

	PG_RETURN_DATEADT(date);
}

TS_FUNCTION_INFO_V1(ts_time_bucket_ng_timezone);
TSDLLEXPORT Datum
ts_time_bucket_ng_timezone(PG_FUNCTION_ARGS)
{
	Timestamp result;
	Datum timestamp;
	Datum interval = PG_GETARG_DATUM(0);
	Datum timestamptz = PG_GETARG_DATUM(1);
	Datum tzname = PG_GETARG_DATUM(2);

	/*
	 * Convert 'timestamptz' to TIMESTAMP at given 'tzname'.
	 * The code is equal to 'timestamptz AT TIME ZONE tzname'.
	 */
	timestamp = DirectFunctionCall2(timestamptz_zone, tzname, timestamptz);

	/* Then treat resulting timestamp as a regular one */
	result =
		DatumGetTimestamp(DirectFunctionCall2(ts_time_bucket_ng_timestamp, interval, timestamp));
	if (TIMESTAMP_NOT_FINITE(result))
		PG_RETURN_TIMESTAMP(result);

	PG_RETURN_DATUM(DirectFunctionCall2(timestamp_zone, tzname, TimestampGetDatum(result)));
}

TS_FUNCTION_INFO_V1(ts_time_bucket_ng_timezone_origin);
TSDLLEXPORT Datum
ts_time_bucket_ng_timezone_origin(PG_FUNCTION_ARGS)
{
	Timestamp result;
	Datum timestamp, origin;
	Datum interval = PG_GETARG_DATUM(0);
	Datum timestamptz = PG_GETARG_DATUM(1);
	Datum origintz = PG_GETARG_DATUM(2);
	Datum tzname = PG_GETARG_DATUM(3);

	/*
	 * Convert 'origin' to TIMESTAMP at given 'tzname'.
	 * The code is equal to 'origin AT TIME ZONE tzname'.
	 */
	origin = DirectFunctionCall2(timestamptz_zone, tzname, origintz);

	/* Same for 'timestamptz' */
	timestamp = DirectFunctionCall2(timestamptz_zone, tzname, timestamptz);

	/* Then treat resulting 'timestamp' and 'origin' as a regular ones */
	result = DatumGetTimestamp(
		DirectFunctionCall3(ts_time_bucket_ng_timestamp, interval, timestamp, origin));
	if (TIMESTAMP_NOT_FINITE(result))
		PG_RETURN_TIMESTAMP(result);

	PG_RETURN_DATUM(DirectFunctionCall2(timestamp_zone, tzname, TimestampGetDatum(result)));
}
