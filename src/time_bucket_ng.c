/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/date.h>
#include <utils/datetime.h>
#include <utils/fmgrprotos.h>

#include "time_bucket_ng.h"

TS_FUNCTION_INFO_V1(ts_time_bucket_ng);
TS_FUNCTION_INFO_V1(ts_time_bucket_ng_timestamp);
TS_FUNCTION_INFO_V1(ts_time_bucket_ng_timestamptz);

TSDLLEXPORT Datum
ts_time_bucket_ng_timestamp(PG_FUNCTION_ARGS)
{
	DateADT result;
	Datum interval = PG_GETARG_DATUM(0);
	DateADT ts_date = DatumGetDateADT(DirectFunctionCall1(timestamp_date, PG_GETARG_DATUM(1)));

	if (PG_NARGS() > 2)
	{
		DateADT origin = DatumGetDateADT(DirectFunctionCall1(timestamp_date, PG_GETARG_DATUM(2)));
		result = DatumGetDateADT(DirectFunctionCall3(ts_time_bucket_ng,
													 interval,
													 DateADTGetDatum(ts_date),
													 DateADTGetDatum(origin)));
	}
	else
	{
		result = DatumGetDateADT(
			DirectFunctionCall2(ts_time_bucket_ng, interval, DateADTGetDatum(ts_date)));
	}

	return DirectFunctionCall1(date_timestamp, DateADTGetDatum(result));
}

TSDLLEXPORT Datum
ts_time_bucket_ng_timestamptz(PG_FUNCTION_ARGS)
{
	DateADT result;
	Datum interval = PG_GETARG_DATUM(0);
	DateADT ts_date = DatumGetDateADT(DirectFunctionCall1(timestamptz_date, PG_GETARG_DATUM(1)));

	if (PG_NARGS() > 2)
	{
		DateADT origin = DatumGetDateADT(DirectFunctionCall1(timestamptz_date, PG_GETARG_DATUM(2)));
		result = DatumGetDateADT(DirectFunctionCall3(ts_time_bucket_ng,
													 interval,
													 DateADTGetDatum(ts_date),
													 DateADTGetDatum(origin)));
	}
	else
	{
		result = DatumGetDateADT(
			DirectFunctionCall2(ts_time_bucket_ng, interval, DateADTGetDatum(ts_date)));
	}

	return DirectFunctionCall1(date_timestamptz, DateADTGetDatum(result));
}

TSDLLEXPORT Datum
ts_time_bucket_ng(PG_FUNCTION_ARGS)
{
	Interval *interval = PG_GETARG_INTERVAL_P(0);
	DateADT date = PG_GETARG_DATEADT(1);
	DateADT origin_date = 0; // 2000-01-01
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
		/*
		 * This will be fixed in future versions of ts_time_bucket_ng().
		 * The reason why it's not yet implemented is that we want to start
		 * experimenting with variable-sized buckets as soon as possible.
		 * We know that fixed-sized buckets work OK and adding corresponding
		 * logic will be trivial.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("interval must be at least one day")));
	}

	if (PG_NARGS() > 2)
	{
		origin_date = PG_GETARG_DATUM(2);
		j2date(origin_date + POSTGRES_EPOCH_JDATE, &origin_year, &origin_month, &origin_day);
	}

	if ((origin_day != 1) && (interval->month != 0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("origin must be the first day of the month")));
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
		year = origin_year + (bucket_number * interval->month) / 12;
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
		date = bucket_number * interval->day;
	}

	PG_RETURN_DATEADT(date);
}
