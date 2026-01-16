/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <catalog/pg_type.h>
#include <common/int.h>
#include <datatype/timestamp.h>
#include <fmgr.h>
#include <nodes/nodes.h>
#include <pgtime.h>
#include <utils/date.h>
#include <utils/datetime.h>
#include <utils/fmgrprotos.h>
#include <utils/timestamp.h>

#include "chunk_range.h"

/*
 * Convert a timestamp Datum to a broken-down pg_tm structure.
 *
 * This utility function handles the conversion of a timestamp/timestamptz Datum
 * to its constituent date/time components in a pg_tm struct. It properly handles
 * timezone conversion for TIMESTAMPTZOID types.
 *
 * For TIMESTAMPTZ, pass the session timezone in attimezone.
 * For TIMESTAMP, pass NULL for attimezone.
 *
 * Returns 0 on success, non-zero on error.
 */
static int
timestamp_datum_to_tm(Datum timestamp, Oid type, struct pg_tm *tm, fsec_t *fsec, pg_tz *attimezone)
{
	int tz;
	const char *tzn;

	memset(tm, 0, sizeof(struct pg_tm));
	*fsec = 0;

	if (type == TIMESTAMPTZOID)
		return timestamp2tm(DatumGetTimestampTz(timestamp), &tz, tm, fsec, &tzn, attimezone);
	else
		return timestamp2tm(DatumGetTimestamp(timestamp), NULL, tm, fsec, NULL, NULL);
}

/*
 * Convert pg_tm structures to chunk range boundaries.
 *
 * Converts tm_start and tm_end to timestamps and returns a ChunkRange.
 * If conversion fails (out of range), uses -infinity/+infinity as fallbacks.
 * These will be properly clamped to DIMENSION_SLICE_MINVALUE/MAXVALUE
 * when converted to a DimensionSlice.
 */
static ChunkRange
tm_to_chunk_range(const struct pg_tm *tm_start, const struct pg_tm *tm_end, fsec_t fsec, Oid type,
				  pg_tz *attimezone)
{
	if (type == TIMESTAMPTZOID)
	{
		TimestampTz ts_start, ts_end;
		int tz_start = DetermineTimeZoneOffset((struct pg_tm *) tm_start, attimezone);
		if (tm2timestamp((struct pg_tm *) tm_start, fsec, &tz_start, &ts_start) != 0)
			ts_start = DT_NOBEGIN;

		int tz_end = DetermineTimeZoneOffset((struct pg_tm *) tm_end, attimezone);
		if (tm2timestamp((struct pg_tm *) tm_end, fsec, &tz_end, &ts_end) != 0)
			ts_end = DT_NOEND;

		return ts_chunk_range_from_timestamptz(ts_start, ts_end);
	}
	else
	{
		Timestamp ts_start, ts_end;
		Assert(type == TIMESTAMPOID);

		if (tm2timestamp((struct pg_tm *) tm_start, fsec, NULL, &ts_start) != 0)
			ts_start = DT_NOBEGIN;

		if (tm2timestamp((struct pg_tm *) tm_end, fsec, NULL, &ts_end) != 0)
			ts_end = DT_NOEND;

		return ts_chunk_range_from_timestamp(ts_start, ts_end);
	}
}

/*
 * Calculate the chunk range for month-based intervals.
 *
 * Aligns the timestamp to the beginning of a month bucket based on the origin.
 * Preserves the origin's day-of-month and time components.
 * Out-of-range chunk boundaries are handled by tm_to_chunk_range which clamps
 * to -infinity/+infinity when tm2timestamp fails.
 */
static ChunkRange
calc_month_chunk_range(Datum timestamp, Oid type, const Interval *interval, Datum origin)
{
	Assert(interval->month > 0 && interval->day == 0 && interval->time == 0);

	pg_tz *attimezone = (type == TIMESTAMPTZOID) ? session_timezone : NULL;
	struct pg_tm tt, *tm = &tt;
	struct pg_tm origin_tt, *origin_tm = &origin_tt;
	fsec_t fsec, origin_fsec;

	if (timestamp_datum_to_tm(origin, type, origin_tm, &origin_fsec, attimezone) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("origin timestamp out of range")));

	if (timestamp_datum_to_tm(timestamp, type, tm, &fsec, attimezone) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

	int year_diff = tm->tm_year - origin_tm->tm_year;
	int month_diff = tm->tm_mon - origin_tm->tm_mon;

	/*
	 * Check if the timestamp's day/time is before the origin's day/time within the
	 * same month. If so, we need to subtract one month from the total month difference
	 * because the timestamp falls in the previous bucket.
	 */
	bool before_origin_day_time =
		(tm->tm_mday < origin_tm->tm_mday) ||
		(tm->tm_mday == origin_tm->tm_mday &&
		 (tm->tm_hour < origin_tm->tm_hour ||
		  (tm->tm_hour == origin_tm->tm_hour &&
		   (tm->tm_min < origin_tm->tm_min ||
			(tm->tm_min == origin_tm->tm_min &&
			 (tm->tm_sec < origin_tm->tm_sec ||
			  (tm->tm_sec == origin_tm->tm_sec && fsec < origin_fsec)))))));
	if (before_origin_day_time)
		month_diff--;

	int total_month_diff = year_diff * MONTHS_PER_YEAR + month_diff;

	int bucket_num;
	if (total_month_diff >= 0)
		bucket_num = total_month_diff / interval->month;
	else
		bucket_num = (total_month_diff - interval->month + 1) / interval->month;

	int month_offset = bucket_num * interval->month;
	int total_months_from_jan = origin_tm->tm_mon - 1 + month_offset;

	int full_years, remaining_months;
	if (total_months_from_jan >= 0)
	{
		full_years = total_months_from_jan / MONTHS_PER_YEAR;
		remaining_months = total_months_from_jan % MONTHS_PER_YEAR;
	}
	else
	{
		full_years = (total_months_from_jan - MONTHS_PER_YEAR + 1) / MONTHS_PER_YEAR;
		remaining_months = total_months_from_jan - (full_years * MONTHS_PER_YEAR);
	}

	struct pg_tm tm_start;
	memset(&tm_start, 0, sizeof(tm_start));
	tm_start.tm_year = origin_tm->tm_year + full_years;
	tm_start.tm_mon = 1 + remaining_months;
	tm_start.tm_mday = origin_tm->tm_mday;
	tm_start.tm_hour = origin_tm->tm_hour;
	tm_start.tm_min = origin_tm->tm_min;
	tm_start.tm_sec = origin_tm->tm_sec;

	/*
	 * Adding interval->month can overflow if interval is extremely large.
	 * On overflow, clamp to INT_MAX - tm2timestamp will fail and we'll
	 * get +infinity for the chunk end.
	 */
	int end_total_months_from_jan;
	if (pg_add_s32_overflow(total_months_from_jan, interval->month, &end_total_months_from_jan))
		end_total_months_from_jan = PG_INT32_MAX;

	int end_full_years, end_remaining_months;
	if (end_total_months_from_jan >= 0)
	{
		end_full_years = end_total_months_from_jan / MONTHS_PER_YEAR;
		end_remaining_months = end_total_months_from_jan % MONTHS_PER_YEAR;
	}
	else
	{
		end_full_years = (end_total_months_from_jan - MONTHS_PER_YEAR + 1) / MONTHS_PER_YEAR;
		end_remaining_months = end_total_months_from_jan - (end_full_years * MONTHS_PER_YEAR);
	}

	struct pg_tm tm_end;
	memset(&tm_end, 0, sizeof(tm_end));
	tm_end.tm_year = origin_tm->tm_year + end_full_years;
	tm_end.tm_mon = 1 + end_remaining_months;
	tm_end.tm_mday = origin_tm->tm_mday;
	tm_end.tm_hour = origin_tm->tm_hour;
	tm_end.tm_min = origin_tm->tm_min;
	tm_end.tm_sec = origin_tm->tm_sec;

	return tm_to_chunk_range(&tm_start, &tm_end, origin_fsec, type, attimezone);
}

/*
 * Calculate the chunk range for day-based intervals.
 *
 * Uses Julian day numbers to align the timestamp to the beginning of a day bucket.
 * Preserves the origin's time-of-day components.
 * Out-of-range chunk boundaries are handled by tm_to_chunk_range which clamps
 * to -infinity/+infinity when tm2timestamp fails.
 */
static ChunkRange
calc_day_chunk_range(Datum timestamp, Oid type, const Interval *interval, Datum origin)
{
	Assert(interval->day > 0 && interval->month == 0 && interval->time == 0);

	pg_tz *attimezone = (type == TIMESTAMPTZOID) ? session_timezone : NULL;
	struct pg_tm tt, *tm = &tt;
	struct pg_tm origin_tt, *origin_tm = &origin_tt;
	fsec_t fsec, origin_fsec;

	if (timestamp_datum_to_tm(origin, type, origin_tm, &origin_fsec, attimezone) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("origin timestamp out of range")));

	if (timestamp_datum_to_tm(timestamp, type, tm, &fsec, attimezone) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

	int ts_julian = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
	int origin_julian = date2j(origin_tm->tm_year, origin_tm->tm_mon, origin_tm->tm_mday);

	/*
	 * Check if the timestamp's time-of-day is before the origin's time-of-day.
	 * If so, we need to subtract one day from the day count because the timestamp
	 * falls in the previous bucket.
	 */
	bool before_origin_time = (tm->tm_hour < origin_tm->tm_hour) ||
							  (tm->tm_hour == origin_tm->tm_hour &&
							   (tm->tm_min < origin_tm->tm_min ||
								(tm->tm_min == origin_tm->tm_min &&
								 (tm->tm_sec < origin_tm->tm_sec ||
								  (tm->tm_sec == origin_tm->tm_sec && fsec < origin_fsec)))));

	int days_since_origin = ts_julian - origin_julian;

	/* Adjust for time-of-day offset if timestamp is before origin time */
	if (before_origin_time)
		days_since_origin--;

	int bucket_num;
	if (days_since_origin >= 0)
		bucket_num = days_since_origin / interval->day;
	else
		bucket_num = (days_since_origin - interval->day + 1) / interval->day;

	int day_offset = bucket_num * interval->day;
	int start_julian = origin_julian + day_offset;

	struct pg_tm tm_start;
	memset(&tm_start, 0, sizeof(tm_start));
	j2date(start_julian, &tm_start.tm_year, &tm_start.tm_mon, &tm_start.tm_mday);
	tm_start.tm_hour = origin_tm->tm_hour;
	tm_start.tm_min = origin_tm->tm_min;
	tm_start.tm_sec = origin_tm->tm_sec;

	/*
	 * Adding interval->day can overflow if interval is extremely large.
	 * On overflow, clamp to INT_MAX - tm2timestamp will fail and we'll
	 * get +infinity for the chunk end.
	 */
	int end_julian;
	if (pg_add_s32_overflow(start_julian, interval->day, &end_julian))
		end_julian = PG_INT32_MAX;

	struct pg_tm tm_end;
	memset(&tm_end, 0, sizeof(tm_end));
	j2date(end_julian, &tm_end.tm_year, &tm_end.tm_mon, &tm_end.tm_mday);
	tm_end.tm_hour = origin_tm->tm_hour;
	tm_end.tm_min = origin_tm->tm_min;
	tm_end.tm_sec = origin_tm->tm_sec;

	return tm_to_chunk_range(&tm_start, &tm_end, origin_fsec, type, attimezone);
}

/*
 * Calculate the chunk range for time-based (sub-day) intervals.
 *
 * The function is always called with an Interval that is pure time (fixed/non-variable).
 *
 * Uses microsecond arithmetic for precise alignment.
 *
 * The bucket calculation logic below is borrowed from PostgreSQL's
 * timestamptz_bin() function in src/backend/utils/adt/timestamp.c
 */
static ChunkRange
calc_sub_day_chunk_range(Datum timestamp, Oid type, const Interval *interval, Datum origin)
{
	Assert(interval->time > 0 && interval->month == 0 && interval->day == 0);

	ChunkRange range = { .type = type };
	TimestampTz result, stride_usecs, tm_diff, tm_modulo, tm_delta;
	TimestampTz ts_val =
		(type == TIMESTAMPTZOID) ? DatumGetTimestampTz(timestamp) : DatumGetTimestamp(timestamp);
	TimestampTz origin_val =
		(type == TIMESTAMPTZOID) ? DatumGetTimestampTz(origin) : DatumGetTimestamp(origin);

	/*
	 * The function is always called with day==0 (see Assert), but keep this multiplication anyway
	 * to keep code similar to timestamptz_bin().
	 */
	if (unlikely(pg_mul_s64_overflow(interval->day, USECS_PER_DAY, &stride_usecs)) ||
		unlikely(pg_add_s64_overflow(stride_usecs, interval->time, &stride_usecs)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("interval out of range")));

	if (stride_usecs <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("stride must be greater than zero")));

	if (unlikely(pg_sub_s64_overflow(ts_val, origin_val, &tm_diff)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("interval out of range")));

	/* These calculations cannot overflow */
	tm_modulo = tm_diff % stride_usecs;
	tm_delta = tm_diff - tm_modulo;
	result = origin_val + tm_delta;

	/*
	 * We want to round towards -infinity, not 0, when tm_diff is negative and
	 * not a multiple of stride_usecs. This adjustment *can* cause overflow,
	 * since the result might now be out of the range origin .. timestamp.
	 */
	if (tm_modulo < 0)
	{
		if (unlikely(pg_sub_s64_overflow(result, stride_usecs, &result)) ||
			!IS_VALID_TIMESTAMP(result))
			ereport(ERROR,
					(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
					 errmsg("timestamp out of range")));
	}

	/* For time-only intervals, range_end is just result + time microseconds */
	TimestampTz range_end;
	if (pg_add_s64_overflow(result, interval->time, &range_end) || !IS_VALID_TIMESTAMP(range_end))
		range_end = DT_NOEND;

	if (type == TIMESTAMPTZOID)
		range = ts_chunk_range_from_timestamptz(result, range_end);
	else
		range = ts_chunk_range_from_timestamp(result, range_end);

	return range;
}

/*
 * Saturating timestamp + interval arithmetic.
 *
 * Similar to PostgreSQL's timestamp_pl_interval/timestamptz_pl_interval,
 * but instead of throwing errors on overflow, clamps to min/max timestamp.
 * This is needed for chunk range calculations where we iterate near time
 * boundaries and don't want to fail on overflow.
 *
 * For TIMESTAMPTZ, timezone conversion is handled using the provided timezone.
 * For TIMESTAMP, pass NULL for attimezone.
 *
 * Returns true if the operation succeeded without overflow, false if clamped.
 */
static bool
timestamp_interval_add_saturating(TimestampTz timestamp, const Interval *interval,
								  pg_tz *attimezone, TimestampTz *result)
{
	struct pg_tm tt, *tm = &tt;
	fsec_t fsec;
	int tz;

	/* Handle infinite timestamps */
	if (TIMESTAMP_NOT_FINITE(timestamp))
	{
		*result = timestamp;
		return true;
	}

	/* Decompose timestamp to broken-down time */
	if (attimezone != NULL)
	{
		/* TIMESTAMPTZ: convert with timezone */
		if (timestamp2tm(timestamp, &tz, tm, &fsec, NULL, attimezone) != 0)
		{
			*result = (timestamp < 0) ? DT_NOBEGIN : DT_NOEND;
			return false;
		}
	}
	else
	{
		/* TIMESTAMP: no timezone */
		if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) != 0)
		{
			*result = (timestamp < 0) ? DT_NOBEGIN : DT_NOEND;
			return false;
		}
	}

	/* Add months with overflow check */
	if (interval->month != 0)
	{
		int64 total_months = (int64) tm->tm_year * MONTHS_PER_YEAR + tm->tm_mon + interval->month;

		/* Check for year overflow */
		if (total_months / MONTHS_PER_YEAR > PG_INT32_MAX ||
			total_months / MONTHS_PER_YEAR < PG_INT32_MIN)
		{
			*result = (interval->month > 0) ? DT_NOEND : DT_NOBEGIN;
			return false;
		}

		tm->tm_year = (int) (total_months / MONTHS_PER_YEAR);
		tm->tm_mon = total_months % MONTHS_PER_YEAR;

		/* Handle negative month result */
		if (tm->tm_mon <= 0)
		{
			tm->tm_mon += MONTHS_PER_YEAR;
			tm->tm_year--;
		}

		/* Adjust day if it exceeds days in resulting month */
		int days_in_month = day_tab[isleap(tm->tm_year)][tm->tm_mon - 1];
		if (tm->tm_mday > days_in_month)
			tm->tm_mday = days_in_month;
	}

	/* Add days */
	if (interval->day != 0)
	{
		int julian = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
		int new_julian;

		if (pg_add_s32_overflow(julian, interval->day, &new_julian))
		{
			*result = (interval->day > 0) ? DT_NOEND : DT_NOBEGIN;
			return false;
		}

		j2date(new_julian, &tm->tm_year, &tm->tm_mon, &tm->tm_mday);
	}

	/* Convert back to timestamp */
	TimestampTz ts_result;
	if (attimezone != NULL)
	{
		/* TIMESTAMPTZ: determine timezone offset for result */
		tz = DetermineTimeZoneOffset(tm, attimezone);
		if (tm2timestamp(tm, fsec, &tz, &ts_result) != 0)
		{
			*result = (interval->month > 0 || interval->day > 0) ? DT_NOEND : DT_NOBEGIN;
			return false;
		}
	}
	else
	{
		/* TIMESTAMP: no timezone */
		if (tm2timestamp(tm, fsec, NULL, &ts_result) != 0)
		{
			*result = (interval->month > 0 || interval->day > 0) ? DT_NOEND : DT_NOBEGIN;
			return false;
		}
	}

	/* Add time component with overflow check */
	if (interval->time != 0)
	{
		if (pg_add_s64_overflow(ts_result, interval->time, &ts_result))
		{
			*result = (interval->time > 0) ? DT_NOEND : DT_NOBEGIN;
			return false;
		}
	}

	/* Final range check */
	if (!IS_VALID_TIMESTAMP(ts_result))
	{
		*result = (ts_result > 0) ? DT_NOEND : DT_NOBEGIN;
		return false;
	}

	*result = ts_result;
	return true;
}

/*
 * Saturating timestamp - interval arithmetic.
 * Equivalent to timestamp_interval_add_saturating with negated interval.
 */
static bool
timestamp_interval_sub_saturating(TimestampTz timestamp, const Interval *interval,
								  pg_tz *attimezone, TimestampTz *result)
{
	Interval neg_interval;
	neg_interval.month = -interval->month;
	neg_interval.day = -interval->day;
	neg_interval.time = -interval->time;
	return timestamp_interval_add_saturating(timestamp, &neg_interval, attimezone, result);
}

/*
 * Estimate the average duration of an interval in microseconds.
 * Used for optimizing the search by jumping close to the target.
 */
static int64
estimate_interval_usecs(const Interval *interval)
{
	/* Average days per month: 365.25 / 12 = 30.4375 */
	const int64 AVG_USECS_PER_MONTH = (int64) (30.4375 * USECS_PER_DAY);

	return interval->month * AVG_USECS_PER_MONTH + interval->day * USECS_PER_DAY + interval->time;
}

/*
 * Calculate chunk range using iterative interval arithmetic.
 *
 * This function handles arbitrary intervals by iteratively adding/subtracting
 * the interval until the bucket containing the timestamp is found. It works
 * for any valid interval, including mixed intervals (e.g., "1 month 15 days").
 *
 * Why this isn't used for all intervals:
 * - For pure month intervals, calc_month_chunk_range() uses direct month
 *   arithmetic which is O(1) and handles month-boundary edge cases correctly.
 * - For pure day intervals, calc_day_chunk_range() uses Julian day arithmetic
 *   which is O(1) and avoids DST-related issues.
 * - For pure time intervals, calc_sub_day_chunk_range() uses microsecond
 *   arithmetic which is O(1) and exact.
 *
 * This general function is used for mixed intervals where the components
 * interact in complex ways (e.g., adding "1 month 1 day" depends on which
 * month you start from). The iteration is optimized by first jumping close
 * to the target using an estimated interval duration, typically requiring
 * only 1-3 iterations to find the exact bucket.
 */
ChunkRange
ts_chunk_range_calculate_general(Datum timestamp, Oid type, const Interval *interval, Datum origin,
								 int64 *iterations_out)
{
	/* At least one component must be non-zero */
	Assert(interval->month != 0 || interval->day != 0 || interval->time != 0);

	ChunkRange range = { .type = type };
	TimestampTz ts_val =
		(type == TIMESTAMPTZOID) ? DatumGetTimestampTz(timestamp) : DatumGetTimestamp(timestamp);
	TimestampTz origin_val =
		(type == TIMESTAMPTZOID) ? DatumGetTimestampTz(origin) : DatumGetTimestamp(origin);
	TimestampTz bucket_start = origin_val;
	pg_tz *tz = (type == TIMESTAMPTZOID) ? session_timezone : NULL;

	/*
	 * Optimization: jump close to the target, adjusting if we overshoot.
	 *
	 * We start with an aggressive jump (100% of estimate), and if we overshoot,
	 * we back off progressively until we undershoot. This adaptive approach
	 * gets us very close while maintaining consistency.
	 *
	 * Note that we can only iterate in one direction depending on whether the
	 * point is before or after the origin. In other words, we cannot overshoot
	 * and then go "backwards" because mixed interval arithmetic is not
	 * reversible. Consider the following example:
	 *
	 * -- Adding 1 month to Jan 31 gives Feb 28 (not Feb 31 which doesn't
	 *  exist)
	 *
	 *  SELECT '2001-01-31'::date + '1 month'::interval;
	 *  2001-02-28
	 *
	 * -- Subtracting 1 month from Feb 28 gives Jan 28 (not Jan 31)
	 *
	 *  SELECT '2001-02-28'::date - '1 month'::interval;
	 *  2001-01-28
	 *
	 *  So: Jan 31 + 1 month - 1 month = Jan 28 ≠ Jan 31
	 */
	int64 avg_interval_usecs = estimate_interval_usecs(interval);

	if (avg_interval_usecs > 0)
	{
		int64 distance_usecs = ts_val - origin_val;
		int64 estimated_intervals = distance_usecs / avg_interval_usecs;
		int max_attempts = 10; /* Safety limit */

		/* Start aggressive, back off if overshooting */
		int64 jump_factor = 100; /* Start at 100% */

		while (max_attempts-- > 0 && (estimated_intervals > 100 || estimated_intervals < -100))
		{
			int64 jump_intervals = (estimated_intervals * jump_factor) / 100;
			int64 scaled_month = (int64) interval->month * jump_intervals;
			int64 scaled_day = (int64) interval->day * jump_intervals;
			int64 scaled_time;

			/* Check for overflow */
			if (scaled_month < PG_INT32_MIN || scaled_month > PG_INT32_MAX ||
				scaled_day < PG_INT32_MIN || scaled_day > PG_INT32_MAX ||
				pg_mul_s64_overflow(interval->time, jump_intervals, &scaled_time))
				break;

			Interval jump_interval;
			jump_interval.month = (int32) scaled_month;
			jump_interval.day = (int32) scaled_day;
			jump_interval.time = scaled_time;

			/*
			 * Note that when jumping backward the jump_interval is negative.
			 */
			TimestampTz jumped;
			if (!timestamp_interval_add_saturating(bucket_start, &jump_interval, tz, &jumped))
				break;

			/* Check if we overshot */
			bool overshot = (jump_intervals > 0) ? (jumped > ts_val) : (jumped < ts_val);

			if (overshot)
			{
				/* Back off: reduce jump factor and try again from current position */
				jump_factor = jump_factor * 9 / 10; /* Reduce by 10% each time */
				if (jump_factor < 50)
					break; /* Stop if we've backed off too much */
			}
			else
			{
				/* Good jump - accept it and continue */
				bucket_start = jumped;

				/* Recalculate remaining distance for next iteration */
				distance_usecs = ts_val - bucket_start;
				estimated_intervals = distance_usecs / avg_interval_usecs;

				/* Reset jump factor for next jump */
				jump_factor = 100;
			}
		}
	}

	int64 iterations = 0;
	TimestampTz range_end;

	if (ts_val >= bucket_start)
	{
		/*
		 * Search forward (timestamp at or after current bucket_start).
		 *
		 * We know the bucket's start and compute the end by adding the interval.
		 * We advance until ts_val < range_end (i.e., ts_val is within the bucket).
		 */
		if (!timestamp_interval_add_saturating(bucket_start, interval, tz, &range_end))
			range_end = DT_NOEND;

		while (ts_val >= range_end)
		{
			iterations++;
			bucket_start = range_end;
			TimestampTz next;
			if (!timestamp_interval_add_saturating(bucket_start, interval, tz, &next))
			{
				/* Overflow: this bucket extends to infinity */
				range_end = DT_NOEND;
				break;
			}
			range_end = next;
		}
	}
	else
	{
		/*
		 * Search backward (timestamp before current bucket_start).
		 *
		 * We know the bucket's end (it's the start of the next bucket) and
		 * compute the start by subtracting the interval. We advance until
		 * bucket_start <= ts_val (i.e., ts_val is within the bucket).
		 */
		range_end = bucket_start;

		while (bucket_start > ts_val)
		{
			iterations++;
			range_end = bucket_start;
			TimestampTz prev;
			if (!timestamp_interval_sub_saturating(bucket_start, interval, tz, &prev))
			{
				/* Overflow: this bucket starts at -infinity */
				bucket_start = DT_NOBEGIN;
				break;
			}
			bucket_start = prev;
		}
	}

	if (iterations_out)
		*iterations_out = iterations;

	if (type == TIMESTAMPTZOID)
		range = ts_chunk_range_from_timestamptz(bucket_start, range_end);
	else
		range = ts_chunk_range_from_timestamp(bucket_start, range_end);

	return range;
}

ChunkRange
ts_chunk_range_calculate(Datum timestamp, Oid type, const Interval *interval, Datum origin)
{
	/* Dispatch to the appropriate helper function based on interval type */
	if (interval->month > 0 && interval->day == 0 && interval->time == 0)
		return calc_month_chunk_range(timestamp, type, interval, origin);

	if (interval->day > 0 && interval->month == 0 && interval->time == 0)
		return calc_day_chunk_range(timestamp, type, interval, origin);

	if (interval->time > 0 && interval->month == 0 && interval->day == 0)
		return calc_sub_day_chunk_range(timestamp, type, interval, origin);

	/* Non-calendar-compatible (mixed) intervals */
	if (interval->month != 0 || interval->day != 0 || interval->time != 0)
		return ts_chunk_range_calculate_general(timestamp, type, interval, origin, NULL);

	/* Zero interval - return invalid range */
	ChunkRange range = { .type = InvalidOid };
	return range;
}

#ifdef TS_DEBUG
/*
 * Test wrapper for timestamp_interval_add_saturating.
 * Exposes the static function for unit testing.
 */
bool
ts_test_timestamp_interval_add_saturating(TimestampTz timestamp, const Interval *interval,
										  pg_tz *attimezone, TimestampTz *result)
{
	return timestamp_interval_add_saturating(timestamp, interval, attimezone, result);
}
#endif
