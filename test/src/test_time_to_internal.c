/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <utils/date.h>

#include "export.h"

#include "utils.h"

#include "test_utils.h"

TS_FUNCTION_INFO_V1(ts_test_time_to_internal_conversion);
TS_FUNCTION_INFO_V1(ts_test_interval_to_internal_conversion);

Datum
ts_test_time_to_internal_conversion(PG_FUNCTION_ARGS)
{
	int16 i16;
	int32 i32;
	int64 i64;

	/* test integer values */

	/* int16 */
	for (i16 = -100; i16 < 100; i16++)
	{
		TestAssertInt64Eq(i16, ts_time_value_to_internal(Int16GetDatum(i16), INT2OID));
		TestAssertInt64Eq(DatumGetInt16(ts_internal_to_time_value(i16, INT2OID)), i16);
	}

	TestAssertInt64Eq(PG_INT16_MAX,
					  ts_time_value_to_internal(Int16GetDatum(PG_INT16_MAX), INT2OID));
	TestAssertInt64Eq(DatumGetInt16(ts_internal_to_time_value(PG_INT16_MAX, INT2OID)),
					  PG_INT16_MAX);

	TestAssertInt64Eq(PG_INT16_MIN,
					  ts_time_value_to_internal(Int16GetDatum(PG_INT16_MIN), INT2OID));
	TestAssertInt64Eq(DatumGetInt16(ts_internal_to_time_value(PG_INT16_MIN, INT2OID)),
					  PG_INT16_MIN);

	/* int32 */
	for (i32 = -100; i32 < 100; i32++)
	{
		TestAssertInt64Eq(i32, ts_time_value_to_internal(Int32GetDatum(i32), INT4OID));
		TestAssertInt64Eq(DatumGetInt32(ts_internal_to_time_value(i32, INT4OID)), i32);
	}

	TestAssertInt64Eq(PG_INT16_MAX,
					  ts_time_value_to_internal(Int32GetDatum(PG_INT16_MAX), INT4OID));
	TestAssertInt64Eq(DatumGetInt32(ts_internal_to_time_value(PG_INT16_MAX, INT4OID)),
					  PG_INT16_MAX);

	TestAssertInt64Eq(PG_INT32_MAX,
					  ts_time_value_to_internal(Int32GetDatum(PG_INT32_MAX), INT4OID));
	TestAssertInt64Eq(DatumGetInt32(ts_internal_to_time_value(PG_INT32_MAX, INT4OID)),
					  PG_INT32_MAX);

	TestAssertInt64Eq(PG_INT32_MIN,
					  ts_time_value_to_internal(Int32GetDatum(PG_INT32_MIN), INT4OID));
	TestAssertInt64Eq(DatumGetInt32(ts_internal_to_time_value(PG_INT32_MIN, INT4OID)),
					  PG_INT32_MIN);

	/* int64 */
	for (i64 = -100; i64 < 100; i64++)
	{
		TestAssertInt64Eq(i64, ts_time_value_to_internal(Int64GetDatum(i64), INT8OID));
		TestAssertInt64Eq(DatumGetInt64(ts_internal_to_time_value(i64, INT8OID)), i64);
	}

	TestAssertInt64Eq(PG_INT16_MIN,
					  ts_time_value_to_internal(Int64GetDatum(PG_INT16_MIN), INT8OID));
	TestAssertInt64Eq(DatumGetInt64(ts_internal_to_time_value(PG_INT16_MIN, INT8OID)),
					  PG_INT16_MIN);

	TestAssertInt64Eq(PG_INT32_MAX,
					  ts_time_value_to_internal(Int64GetDatum(PG_INT32_MAX), INT8OID));
	TestAssertInt64Eq(DatumGetInt64(ts_internal_to_time_value(PG_INT32_MAX, INT8OID)),
					  PG_INT32_MAX);

	TestAssertInt64Eq(PG_INT64_MAX,
					  ts_time_value_to_internal(Int64GetDatum(PG_INT64_MAX), INT8OID));
	TestAssertInt64Eq(DatumGetInt64(ts_internal_to_time_value(PG_INT64_MAX, INT8OID)),
					  PG_INT64_MAX);

	TestAssertInt64Eq(PG_INT64_MIN,
					  ts_time_value_to_internal(Int64GetDatum(PG_INT64_MIN), INT8OID));
	TestAssertInt64Eq(DatumGetInt64(ts_internal_to_time_value(PG_INT64_MIN, INT8OID)),
					  PG_INT64_MIN);

	/* test time values round trip */

	/* TIMESTAMP */
	for (i64 = -100; i64 < 100; i64++)
		TestAssertInt64Eq(i64,
						  ts_time_value_to_internal(ts_internal_to_time_value(i64, TIMESTAMPOID),
													TIMESTAMPOID));

	for (i64 = -10000000; i64 < 100000000; i64 += 1000000)
		TestAssertInt64Eq(i64,
						  ts_time_value_to_internal(ts_internal_to_time_value(i64, TIMESTAMPOID),
													TIMESTAMPOID));

	for (i64 = -1000000000; i64 < 10000000000; i64 += 100000000)
		TestAssertInt64Eq(i64,
						  ts_time_value_to_internal(ts_internal_to_time_value(i64, TIMESTAMPOID),
													TIMESTAMPOID));

	TestEnsureError(ts_internal_to_time_value(PG_INT64_MIN, TIMESTAMPOID));
	TestEnsureError(ts_internal_to_time_value(TS_INTERNAL_TIMESTAMP_MIN - 1, TIMESTAMPOID));

	TestAssertInt64Eq(TS_INTERNAL_TIMESTAMP_MIN,
					  ts_time_value_to_internal(ts_internal_to_time_value(TS_INTERNAL_TIMESTAMP_MIN,
																		  TIMESTAMPOID),
												TIMESTAMPOID));

	TestAssertInt64Eq(PG_INT64_MAX - TS_EPOCH_DIFF_MICROSECONDS,
					  DatumGetInt64(ts_internal_to_time_value(PG_INT64_MAX, TIMESTAMPOID)));
	TestEnsureError(ts_time_value_to_internal(ts_internal_to_time_value(PG_INT64_MAX, TIMESTAMPOID),
											  TIMESTAMPOID));

	/* TIMESTAMPTZ */
	for (i64 = -100; i64 < 100; i64++)
		TestAssertInt64Eq(i64,
						  ts_time_value_to_internal(ts_internal_to_time_value(i64, TIMESTAMPTZOID),
													TIMESTAMPTZOID));

	for (i64 = -10000000; i64 < 100000000; i64 += 1000000)
		TestAssertInt64Eq(i64,
						  ts_time_value_to_internal(ts_internal_to_time_value(i64, TIMESTAMPTZOID),
													TIMESTAMPTZOID));

	for (i64 = -1000000000; i64 < 10000000000; i64 += 100000000)
		TestAssertInt64Eq(i64,
						  ts_time_value_to_internal(ts_internal_to_time_value(i64, TIMESTAMPTZOID),
													TIMESTAMPTZOID));

	TestEnsureError(ts_internal_to_time_value(PG_INT64_MIN, TIMESTAMPTZOID));
	TestEnsureError(ts_internal_to_time_value(TS_INTERNAL_TIMESTAMP_MIN - 1, TIMESTAMPTZOID));

	TestAssertInt64Eq(TS_INTERNAL_TIMESTAMP_MIN,
					  ts_time_value_to_internal(ts_internal_to_time_value(TS_INTERNAL_TIMESTAMP_MIN,
																		  TIMESTAMPTZOID),
												TIMESTAMPTZOID));

	TestAssertInt64Eq(PG_INT64_MAX - TS_EPOCH_DIFF_MICROSECONDS,
					  DatumGetInt64(ts_internal_to_time_value(PG_INT64_MAX, TIMESTAMPTZOID)));
	TestEnsureError(
		ts_time_value_to_internal(ts_internal_to_time_value(PG_INT64_MAX, TIMESTAMPTZOID),
								  TIMESTAMPTZOID));

	/* DATE */
	for (i64 = -100 * USECS_PER_DAY; i64 < 100 * USECS_PER_DAY; i64 += USECS_PER_DAY)
		TestAssertInt64Eq(i64,
						  ts_time_value_to_internal(ts_internal_to_time_value(i64, DATEOID),
													DATEOID));

	TestEnsureError(ts_internal_to_time_value(PG_INT64_MIN, DATEOID));
	TestAssertInt64Eq(106741034, DatumGetDateADT(ts_internal_to_time_value(PG_INT64_MAX, DATEOID)));

	TestEnsureError(ts_time_value_to_internal((DATEVAL_NOBEGIN + 1), DATEOID));
	TestEnsureError(ts_time_value_to_internal((DATEVAL_NOEND - 1), DATEOID));

	PG_RETURN_VOID();
};

Datum
ts_test_interval_to_internal_conversion(PG_FUNCTION_ARGS)
{
	int16 i16;
	int32 i32;
	int64 i64;

	/* test integer values */

	/* int16 */
	for (i16 = -100; i16 < 100; i16++)
	{
		TestAssertInt64Eq(i16, ts_interval_value_to_internal(Int16GetDatum(i16), INT2OID));
		TestAssertInt64Eq(DatumGetInt16(ts_internal_to_interval_value(i16, INT2OID)), i16);
	}

	TestAssertInt64Eq(PG_INT16_MAX,
					  ts_interval_value_to_internal(Int16GetDatum(PG_INT16_MAX), INT2OID));
	TestAssertInt64Eq(DatumGetInt16(ts_internal_to_interval_value(PG_INT16_MAX, INT2OID)),
					  PG_INT16_MAX);

	TestAssertInt64Eq(PG_INT16_MIN,
					  ts_interval_value_to_internal(Int16GetDatum(PG_INT16_MIN), INT2OID));
	TestAssertInt64Eq(DatumGetInt16(ts_internal_to_interval_value(PG_INT16_MIN, INT2OID)),
					  PG_INT16_MIN);

	/* int32 */
	for (i32 = -100; i32 < 100; i32++)
	{
		TestAssertInt64Eq(i32, ts_interval_value_to_internal(Int32GetDatum(i32), INT4OID));
		TestAssertInt64Eq(DatumGetInt32(ts_internal_to_interval_value(i32, INT4OID)), i32);
	}

	TestAssertInt64Eq(PG_INT16_MAX,
					  ts_interval_value_to_internal(Int32GetDatum(PG_INT16_MAX), INT4OID));
	TestAssertInt64Eq(DatumGetInt32(ts_internal_to_interval_value(PG_INT16_MAX, INT4OID)),
					  PG_INT16_MAX);

	TestAssertInt64Eq(PG_INT32_MAX,
					  ts_interval_value_to_internal(Int32GetDatum(PG_INT32_MAX), INT4OID));
	TestAssertInt64Eq(DatumGetInt32(ts_internal_to_interval_value(PG_INT32_MAX, INT4OID)),
					  PG_INT32_MAX);

	TestAssertInt64Eq(PG_INT32_MIN,
					  ts_interval_value_to_internal(Int32GetDatum(PG_INT32_MIN), INT4OID));
	TestAssertInt64Eq(DatumGetInt32(ts_internal_to_interval_value(PG_INT32_MIN, INT4OID)),
					  PG_INT32_MIN);

	/* int64 */
	for (i64 = -100; i64 < 100; i64++)
	{
		TestAssertInt64Eq(i64, ts_interval_value_to_internal(Int64GetDatum(i64), INT8OID));
		TestAssertInt64Eq(DatumGetInt64(ts_internal_to_interval_value(i64, INT8OID)), i64);
	}

	TestAssertInt64Eq(PG_INT16_MIN,
					  ts_interval_value_to_internal(Int64GetDatum(PG_INT16_MIN), INT8OID));
	TestAssertInt64Eq(DatumGetInt64(ts_internal_to_interval_value(PG_INT16_MIN, INT8OID)),
					  PG_INT16_MIN);

	TestAssertInt64Eq(PG_INT32_MAX,
					  ts_interval_value_to_internal(Int64GetDatum(PG_INT32_MAX), INT8OID));
	TestAssertInt64Eq(DatumGetInt64(ts_internal_to_interval_value(PG_INT32_MAX, INT8OID)),
					  PG_INT32_MAX);

	TestAssertInt64Eq(PG_INT64_MAX,
					  ts_interval_value_to_internal(Int64GetDatum(PG_INT64_MAX), INT8OID));
	TestAssertInt64Eq(DatumGetInt64(ts_internal_to_interval_value(PG_INT64_MAX, INT8OID)),
					  PG_INT64_MAX);

	TestAssertInt64Eq(PG_INT64_MIN,
					  ts_interval_value_to_internal(Int64GetDatum(PG_INT64_MIN), INT8OID));
	TestAssertInt64Eq(DatumGetInt64(ts_internal_to_interval_value(PG_INT64_MIN, INT8OID)),
					  PG_INT64_MIN);

	/* INTERVAL */
	for (i64 = -100; i64 < 100; i64++)
		TestAssertInt64Eq(i64,
						  ts_interval_value_to_internal(ts_internal_to_interval_value(i64,
																					  INTERVALOID),
														INTERVALOID));

	for (i64 = -10000000; i64 < 100000000; i64 += 1000000)
		TestAssertInt64Eq(i64,
						  ts_interval_value_to_internal(ts_internal_to_interval_value(i64,
																					  INTERVALOID),
														INTERVALOID));

	for (i64 = -1000000000; i64 < 10000000000; i64 += 100000000)
		TestAssertInt64Eq(i64,
						  ts_interval_value_to_internal(ts_internal_to_interval_value(i64,
																					  INTERVALOID),
														INTERVALOID));

	TestAssertInt64Eq(PG_INT64_MIN,
					  ts_interval_value_to_internal(ts_internal_to_interval_value(PG_INT64_MIN,
																				  INTERVALOID),
													INTERVALOID));
	TestAssertInt64Eq(PG_INT64_MAX,
					  ts_interval_value_to_internal(ts_internal_to_interval_value(PG_INT64_MAX,
																				  INTERVALOID),
													INTERVALOID));

	PG_RETURN_VOID();
}
