/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <datatype/timestamp.h>
#include <fmgr.h>
#include <pgtime.h>
#include <utils/timestamp.h>

#include "chunk_range.h"
#include "test_utils.h"

/* Unix epoch as PostgreSQL TimestampTz */
#define UNIX_EPOCH_TIMESTAMPTZ ((UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) * USECS_PER_DAY)

/*
 * Test saturating timestamp arithmetic with various overflow scenarios.
 */
static void
test_saturating_arithmetic(void)
{
	TimestampTz result;
	bool success;
	Interval interval;

	pg_tz *utc_tz = pg_tzset("UTC");
	TestAssertTrue(utc_tz != NULL);

	pg_tz *timezones[] = { NULL, utc_tz };

	/*
	 * INFINITY INPUT TESTS
	 * The function returns immediately for infinite timestamps,
	 * so we only need to verify each infinity is preserved once.
	 */
	memset(&interval, 0, sizeof(Interval));
	interval.month = 1;

	success = ts_test_timestamp_interval_add_saturating(DT_NOEND, &interval, NULL, &result);
	TestAssertTrue(success);
	TestAssertInt64Eq(result, DT_NOEND);

	success = ts_test_timestamp_interval_add_saturating(DT_NOBEGIN, &interval, NULL, &result);
	TestAssertTrue(success);
	TestAssertInt64Eq(result, DT_NOBEGIN);

	/*
	 * OVERFLOW TESTS
	 * Test all overflow cases × both timezones
	 */
	TimestampTz ts_2020 = 20 * 365 * USECS_PER_DAY; /* ~2020 from PG epoch */

	/* Test each overflow code path: month, day, time × positive, negative */
	struct
	{
		int32 month;
		int32 day;
		int64 time;
		TimestampTz expected;
	} overflow_cases[] = {
		{ INT32_MAX, 0, 0, DT_NOEND },									/* month overflow */
		{ INT32_MIN, 0, 0, DT_NOBEGIN }, { 0, INT32_MAX, 0, DT_NOEND }, /* day overflow */
		{ 0, INT32_MIN, 0, DT_NOBEGIN }, { 0, 0, INT64_MAX, DT_NOEND }, /* time overflow */
		{ 0, 0, INT64_MIN, DT_NOBEGIN },
	};

	for (size_t tz = 0; tz < lengthof(timezones); tz++)
	{
		for (size_t i = 0; i < lengthof(overflow_cases); i++)
		{
			memset(&interval, 0, sizeof(Interval));
			interval.month = overflow_cases[i].month;
			interval.day = overflow_cases[i].day;
			interval.time = overflow_cases[i].time;
			success = ts_test_timestamp_interval_add_saturating(ts_2020,
																&interval,
																timezones[tz],
																&result);
			TestAssertTrue(!success);
			TestAssertInt64Eq(result, overflow_cases[i].expected);
		}
	}

	/*
	 * INVALID INPUT TIMESTAMP TESTS
	 * Test timestamp2tm failure path - input outside valid range but not infinity
	 */
	TimestampTz invalid_high = END_TIMESTAMP + 1;
	TimestampTz invalid_low = MIN_TIMESTAMP - 1;

	for (size_t tz = 0; tz < lengthof(timezones); tz++)
	{
		memset(&interval, 0, sizeof(Interval));
		interval.day = 1;

		/* High invalid timestamp should saturate to DT_NOEND */
		success = ts_test_timestamp_interval_add_saturating(invalid_high,
															&interval,
															timezones[tz],
															&result);
		TestAssertTrue(!success);
		TestAssertInt64Eq(result, DT_NOEND);

		/* Low invalid timestamp should saturate to DT_NOBEGIN */
		success = ts_test_timestamp_interval_add_saturating(invalid_low,
															&interval,
															timezones[tz],
															&result);
		TestAssertTrue(!success);
		TestAssertInt64Eq(result, DT_NOBEGIN);
	}

	/*
	 * tm2timestamp FAILURE TESTS
	 * Test when adding months/days produces year outside valid range (~294276 AD)
	 * but doesn't trigger INT32_MAX year overflow check
	 */
	for (size_t tz = 0; tz < lengthof(timezones); tz++)
	{
		/* Add months to push year past valid range (without triggering INT32_MAX check) */
		memset(&interval, 0, sizeof(Interval));
		interval.month = 12 * 300000; /* ~300,000 years worth of months */

		success =
			ts_test_timestamp_interval_add_saturating(ts_2020, &interval, timezones[tz], &result);
		TestAssertTrue(!success);
		TestAssertInt64Eq(result, DT_NOEND);

		/* Negative months to push year before valid range */
		memset(&interval, 0, sizeof(Interval));
		interval.month = -12 * 300000;

		success =
			ts_test_timestamp_interval_add_saturating(ts_2020, &interval, timezones[tz], &result);
		TestAssertTrue(!success);
		TestAssertInt64Eq(result, DT_NOBEGIN);
	}

	/*
	 * FINAL RANGE CHECK TESTS
	 * Test IS_VALID_TIMESTAMP failure - result outside valid range after time addition
	 */
	for (size_t tz = 0; tz < lengthof(timezones); tz++)
	{
		memset(&interval, 0, sizeof(Interval));
		interval.time = USECS_PER_DAY * 10; /* Add 10 days worth of microseconds */

		/* Start near END_TIMESTAMP, push past it with time addition */
		success = ts_test_timestamp_interval_add_saturating(END_TIMESTAMP - USECS_PER_DAY,
															&interval,
															timezones[tz],
															&result);
		TestAssertTrue(!success);
		TestAssertInt64Eq(result, DT_NOEND);

		/* Start near MIN_TIMESTAMP, push below it with negative time */
		memset(&interval, 0, sizeof(Interval));
		interval.time = -USECS_PER_DAY * 10;
		success = ts_test_timestamp_interval_add_saturating(MIN_TIMESTAMP + USECS_PER_DAY,
															&interval,
															timezones[tz],
															&result);
		TestAssertTrue(!success);
		TestAssertInt64Eq(result, DT_NOBEGIN);
	}
}

/*
 * Test ts_chunk_range_calculate_general edge cases.
 * Normal functionality is covered by SQL regression tests in calendar_chunking.sql.
 */
static void
test_chunk_range_general(void)
{
	ChunkRange range;
	int64 iterations;

	TimestampTz origin = 20 * 365 * USECS_PER_DAY; /* ~2020 from PG epoch */

	/*
	 * JUMP OPTIMIZATION - Extreme timestamps should use few iterations
	 */
	Interval mixed = { .time = 0, .day = 15, .month = 1 };
	TimestampTz extreme[] = { origin + USECS_PER_DAY * 36500, origin - USECS_PER_DAY * 36500 };

	for (size_t i = 0; i < lengthof(extreme); i++)
	{
		range = ts_chunk_range_calculate_general(TimestampTzGetDatum(extreme[i]),
												 TIMESTAMPTZOID,
												 &mixed,
												 TimestampTzGetDatum(origin),
												 &iterations);
		TestAssertTrue(DatumGetTimestampTz(range.range_start) <= extreme[i]);
		TestAssertTrue(DatumGetTimestampTz(range.range_end) > extreme[i]);
		TestAssertTrue(iterations < 1000);
	}

	/*
	 * OVERFLOW TO INFINITY TESTS
	 * Test forward iteration overflow (range_end → DT_NOEND)
	 * and backward iteration overflow (bucket_start → DT_NOBEGIN)
	 */
	Interval large_interval = { .time = 0, .day = 0, .month = 12 * 1000 }; /* 1000 years */

	/* Forward: timestamp near END_TIMESTAMP should produce range_end = DT_NOEND */
	range =
		ts_chunk_range_calculate_general(TimestampTzGetDatum(END_TIMESTAMP - USECS_PER_DAY),
										 TIMESTAMPTZOID,
										 &large_interval,
										 TimestampTzGetDatum(END_TIMESTAMP - USECS_PER_DAY * 365),
										 &iterations);
	TestAssertTrue(range.type == TIMESTAMPTZOID);
	TestAssertInt64Eq(DatumGetTimestampTz(range.range_end), DT_NOEND);

	/* Backward: timestamp near MIN_TIMESTAMP should produce range_start = DT_NOBEGIN */
	range =
		ts_chunk_range_calculate_general(TimestampTzGetDatum(MIN_TIMESTAMP + USECS_PER_DAY),
										 TIMESTAMPTZOID,
										 &large_interval,
										 TimestampTzGetDatum(MIN_TIMESTAMP + USECS_PER_DAY * 365),
										 &iterations);
	TestAssertTrue(range.type == TIMESTAMPTZOID);
	TestAssertInt64Eq(DatumGetTimestampTz(range.range_start), DT_NOBEGIN);

	/*
	 * JUMP OPTIMIZATION DEFENSIVE PATHS
	 * Test scaled interval overflow and jump saturation
	 */

	/*
	 * Scaled interval overflow: timestamp very far from origin causes
	 * jump_intervals to be huge, making scaled_month overflow INT32.
	 * With 1-month interval and ~200k years distance, jump_intervals ≈ 2.4M
	 * and scaled_month = 1 * 2.4M overflows when multiplied further.
	 */
	Interval small_interval = { .time = 0, .day = 0, .month = 1 };
	TimestampTz very_far = END_TIMESTAMP - USECS_PER_DAY * 100;			 /* Near end */
	TimestampTz very_early_origin = MIN_TIMESTAMP + USECS_PER_DAY * 100; /* Near start */

	/* This should still work - jump optimization handles overflow gracefully */
	range = ts_chunk_range_calculate_general(TimestampTzGetDatum(very_far),
											 TIMESTAMPTZOID,
											 &small_interval,
											 TimestampTzGetDatum(very_early_origin),
											 &iterations);
	TestAssertTrue(range.type == TIMESTAMPTZOID);
	TestAssertTrue(DatumGetTimestampTz(range.range_start) <= very_far);
	TestAssertTrue(DatumGetTimestampTz(range.range_end) > very_far);

	/* Backward version */
	range = ts_chunk_range_calculate_general(TimestampTzGetDatum(very_early_origin + USECS_PER_DAY),
											 TIMESTAMPTZOID,
											 &small_interval,
											 TimestampTzGetDatum(very_far),
											 &iterations);
	TestAssertTrue(range.type == TIMESTAMPTZOID);
	TestAssertTrue(DatumGetTimestampTz(range.range_start) <= very_early_origin + USECS_PER_DAY);
}

/*
 * Test ts_chunk_range_calculate overflow cases for calendar-aligned intervals.
 *
 * These test the overflow handling in calc_month_chunk_range, calc_day_chunk_range,
 * and calc_sub_day_chunk_range which use saturating arithmetic and clamp out-of-range
 * results to DT_NOBEGIN/DT_NOEND.
 */
static void
test_chunk_range_overflow(void)
{
	ChunkRange range;
	Oid types[] = { TIMESTAMPTZOID, TIMESTAMPOID };

	/*
	 * MONTH INTERVAL OVERFLOW TESTS
	 *
	 * Test calc_month_chunk_range overflow paths:
	 * - Result year exceeds valid timestamp range → DT_NOBEGIN/DT_NOEND
	 *
	 * PostgreSQL valid timestamp range: 4713 BC to 294276 AD
	 * (year -4713 to 294276 in pg_tm terms)
	 */
	for (size_t t = 0; t < lengthof(types); t++)
	{
		Interval month_interval = { .time = 0, .day = 0, .month = 1 };

		/*
		 * Forward overflow: timestamp near END_TIMESTAMP with origin far in past
		 * causes range_end to overflow → DT_NOEND
		 */
		TimestampTz ts_near_end = END_TIMESTAMP - USECS_PER_DAY * 30;
		TimestampTz origin_far_past = 0; /* PG epoch 2000-01-01 */

		range = ts_chunk_range_calculate(TimestampTzGetDatum(ts_near_end),
										 types[t],
										 &month_interval,
										 TimestampTzGetDatum(origin_far_past));
		TestAssertTrue(range.type == types[t]);
		TestAssertTrue(DatumGetTimestampTz(range.range_start) <= ts_near_end);
		/* range_end should be DT_NOEND since adding 1 month to a date near END_TIMESTAMP overflows
		 */
		TestAssertInt64Eq(DatumGetTimestampTz(range.range_end), DT_NOEND);

		/*
		 * Backward case: timestamp near MIN_TIMESTAMP.
		 * Tests the backward month calculation path.
		 * range_start should be <= ts and range_end > ts (bucket contains timestamp)
		 * If bucket start would be before MIN_TIMESTAMP, it gets clamped to DT_NOBEGIN.
		 */
		TimestampTz ts_near_begin = MIN_TIMESTAMP + USECS_PER_DAY;
		TimestampTz origin_slightly_after = MIN_TIMESTAMP + USECS_PER_DAY * 15;

		range = ts_chunk_range_calculate(TimestampTzGetDatum(ts_near_begin),
										 types[t],
										 &month_interval,
										 TimestampTzGetDatum(origin_slightly_after));
		TestAssertTrue(range.type == types[t]);
		/* Bucket must contain the timestamp */
		TestAssertTrue(DatumGetTimestampTz(range.range_start) <= ts_near_begin);
		TestAssertTrue(DatumGetTimestampTz(range.range_end) > ts_near_begin);

		/*
		 * Test bucket_num * interval->month overflow path.
		 * With a 1000-year interval near END_TIMESTAMP (~294276 AD), adding 1000 years
		 * to find range_end would exceed the valid timestamp range → DT_NOEND.
		 */
		Interval large_month_interval = { .time = 0, .day = 0, .month = 12000 }; /* 1000 years */
		TimestampTz ts_far_future = END_TIMESTAMP - USECS_PER_DAY * 365;
		TimestampTz origin_far_past2 = MIN_TIMESTAMP + USECS_PER_DAY * 365;

		range = ts_chunk_range_calculate(TimestampTzGetDatum(ts_far_future),
										 types[t],
										 &large_month_interval,
										 TimestampTzGetDatum(origin_far_past2));
		TestAssertTrue(range.type == types[t]);
		TestAssertTrue(DatumGetTimestampTz(range.range_start) <= ts_far_future);
		/* Adding 1000 years to year ~294276 overflows → DT_NOEND */
		TestAssertInt64Eq(DatumGetTimestampTz(range.range_end), DT_NOEND);

		/*
		 * Test backward case with large interval - origin after timestamp.
		 * Timestamp near MIN_TIMESTAMP (~4713 BC), origin near END_TIMESTAMP (~294276 AD).
		 * Going back ~300 buckets of 1000 years each underflows → DT_NOBEGIN.
		 */
		range = ts_chunk_range_calculate(TimestampTzGetDatum(origin_far_past2),
										 types[t],
										 &large_month_interval,
										 TimestampTzGetDatum(ts_far_future));
		TestAssertTrue(range.type == types[t]);
		/* Subtracting ~300,000 years from year ~294276 underflows → DT_NOBEGIN */
		TestAssertInt64Eq(DatumGetTimestampTz(range.range_start), DT_NOBEGIN);
		TestAssertTrue(DatumGetTimestampTz(range.range_end) > origin_far_past2);
	}

	/*
	 * DAY INTERVAL OVERFLOW TESTS
	 *
	 * Test calc_day_chunk_range overflow paths:
	 * - Result date exceeds valid timestamp range → DT_NOBEGIN/DT_NOEND
	 */
	for (size_t t = 0; t < lengthof(types); t++)
	{
		Interval day_interval = { .time = 0, .day = 1, .month = 0 };

		/*
		 * Forward overflow: timestamp near END_TIMESTAMP
		 * Adding 1 day to range_start near end should overflow → DT_NOEND
		 */
		TimestampTz ts_near_end = END_TIMESTAMP - USECS_PER_HOUR;
		TimestampTz origin = 0;

		range = ts_chunk_range_calculate(TimestampTzGetDatum(ts_near_end),
										 types[t],
										 &day_interval,
										 TimestampTzGetDatum(origin));
		TestAssertTrue(range.type == types[t]);
		TestAssertTrue(DatumGetTimestampTz(range.range_start) <= ts_near_end);
		TestAssertInt64Eq(DatumGetTimestampTz(range.range_end), DT_NOEND);

		/*
		 * Backward case: timestamp near MIN_TIMESTAMP.
		 * Tests the backward day calculation path.
		 * If bucket start would be before MIN_TIMESTAMP, it gets clamped to DT_NOBEGIN.
		 */
		TimestampTz ts_near_begin = MIN_TIMESTAMP + USECS_PER_HOUR;
		TimestampTz origin_slightly_after = MIN_TIMESTAMP + USECS_PER_DAY * 2;

		range = ts_chunk_range_calculate(TimestampTzGetDatum(ts_near_begin),
										 types[t],
										 &day_interval,
										 TimestampTzGetDatum(origin_slightly_after));
		TestAssertTrue(range.type == types[t]);
		/* Bucket must contain the timestamp */
		TestAssertTrue(DatumGetTimestampTz(range.range_start) <= ts_near_begin);
		TestAssertTrue(DatumGetTimestampTz(range.range_end) > ts_near_begin);

		/*
		 * Test bucket_num * interval->day overflow path.
		 * With a 100-year day interval near END_TIMESTAMP (~294276 AD), adding 100 years
		 * to find range_end would exceed the valid timestamp range → DT_NOEND.
		 */
		Interval large_day_interval = { .time = 0, .day = 36500, .month = 0 }; /* ~100 years */
		TimestampTz ts_far_future = END_TIMESTAMP - USECS_PER_DAY * 365;
		TimestampTz origin_far_past = MIN_TIMESTAMP + USECS_PER_DAY * 365;

		range = ts_chunk_range_calculate(TimestampTzGetDatum(ts_far_future),
										 types[t],
										 &large_day_interval,
										 TimestampTzGetDatum(origin_far_past));
		TestAssertTrue(range.type == types[t]);
		TestAssertTrue(DatumGetTimestampTz(range.range_start) <= ts_far_future);
		/* Adding ~100 years to year ~294276 overflows → DT_NOEND */
		TestAssertInt64Eq(DatumGetTimestampTz(range.range_end), DT_NOEND);

		/*
		 * Test backward case with large interval - exercises negative bucket_num path.
		 * Timestamp near MIN_TIMESTAMP (~4713 BC), origin near END_TIMESTAMP (~294276 AD).
		 * Going back ~3000 buckets of 100 years each underflows → DT_NOBEGIN.
		 */
		range = ts_chunk_range_calculate(TimestampTzGetDatum(origin_far_past),
										 types[t],
										 &large_day_interval,
										 TimestampTzGetDatum(ts_far_future));
		TestAssertTrue(range.type == types[t]);
		/* Subtracting ~300,000 years from year ~294276 underflows → DT_NOBEGIN */
		TestAssertInt64Eq(DatumGetTimestampTz(range.range_start), DT_NOBEGIN);
		TestAssertTrue(DatumGetTimestampTz(range.range_end) > origin_far_past);

		/*
		 * Test with very large day interval (1000 years) near END_TIMESTAMP.
		 * Adding 1000 years to year ~294276 overflows → DT_NOEND.
		 */
		Interval very_large_day = { .time = 0, .day = 365000, .month = 0 }; /* ~1000 years */
		range = ts_chunk_range_calculate(TimestampTzGetDatum(ts_far_future),
										 types[t],
										 &very_large_day,
										 TimestampTzGetDatum(origin_far_past));
		TestAssertTrue(range.type == types[t]);
		TestAssertTrue(DatumGetTimestampTz(range.range_start) <= ts_far_future);
		TestAssertInt64Eq(DatumGetTimestampTz(range.range_end), DT_NOEND);
	}

	/*
	 * SUB-DAY (TIME) INTERVAL OVERFLOW TESTS
	 *
	 * Test calc_sub_day_chunk_range overflow path:
	 * - range_end = result + interval->time overflows → DT_NOEND
	 */
	for (size_t t = 0; t < lengthof(types); t++)
	{
		Interval time_interval = { .time = USECS_PER_HOUR, .day = 0, .month = 0 };

		/*
		 * Forward overflow: timestamp near END_TIMESTAMP
		 * Adding 1 hour to range_start should overflow → DT_NOEND
		 */
		TimestampTz ts_near_end = END_TIMESTAMP - USECS_PER_HOUR / 2;
		TimestampTz origin = 0;

		range = ts_chunk_range_calculate(TimestampTzGetDatum(ts_near_end),
										 types[t],
										 &time_interval,
										 TimestampTzGetDatum(origin));
		TestAssertTrue(range.type == types[t]);
		TestAssertTrue(DatumGetTimestampTz(range.range_start) <= ts_near_end);
		TestAssertInt64Eq(DatumGetTimestampTz(range.range_end), DT_NOEND);
	}
}

/*
 * Main entry point for chunk_range unit tests.
 */
TS_TEST_FN(ts_test_chunk_range)
{
	test_saturating_arithmetic();
	test_chunk_range_general();
	test_chunk_range_overflow();

	PG_RETURN_VOID();
}
