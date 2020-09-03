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

#include "time_utils.h"
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

	TestAssertInt64Eq(TS_TIME_NOBEGIN,
					  ts_time_value_to_internal(TimestampGetDatum(DT_NOBEGIN), TIMESTAMPOID));
	TestAssertInt64Eq(TS_TIME_NOEND,
					  ts_time_value_to_internal(TimestampGetDatum(DT_NOEND), TIMESTAMPOID));

	TestAssertInt64Eq(DT_NOBEGIN,
					  DatumGetTimestamp(ts_internal_to_time_value(PG_INT64_MIN, TIMESTAMPOID)));
	TestEnsureError(ts_internal_to_time_value(TS_TIMESTAMP_INTERNAL_MIN - 1, TIMESTAMPOID));

	TestAssertInt64Eq(TS_TIMESTAMP_INTERNAL_MIN,
					  ts_time_value_to_internal(ts_internal_to_time_value(TS_TIMESTAMP_INTERNAL_MIN,
																		  TIMESTAMPOID),
												TIMESTAMPOID));

	TestAssertInt64Eq(DT_NOEND,
					  (ts_time_value_to_internal(ts_internal_to_time_value(PG_INT64_MAX,
																		   TIMESTAMPOID),
												 TIMESTAMPOID)));

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

	TestAssertInt64Eq(TS_TIME_NOBEGIN,
					  ts_time_value_to_internal(TimestampTzGetDatum(DT_NOBEGIN), TIMESTAMPTZOID));
	TestAssertInt64Eq(TS_TIME_NOEND,
					  ts_time_value_to_internal(TimestampTzGetDatum(DT_NOEND), TIMESTAMPTZOID));

	TestAssertInt64Eq(DT_NOBEGIN,
					  DatumGetTimestampTz(ts_internal_to_time_value(PG_INT64_MIN, TIMESTAMPTZOID)));
	TestEnsureError(ts_internal_to_time_value(TS_TIMESTAMP_INTERNAL_MIN - 1, TIMESTAMPTZOID));

	TestAssertInt64Eq(TS_TIMESTAMP_INTERNAL_MIN,
					  ts_time_value_to_internal(ts_internal_to_time_value(TS_TIMESTAMP_INTERNAL_MIN,
																		  TIMESTAMPTZOID),
												TIMESTAMPTZOID));

	TestAssertInt64Eq(DT_NOEND,
					  ts_time_value_to_internal(ts_internal_to_time_value(PG_INT64_MAX,
																		  TIMESTAMPTZOID),
												TIMESTAMPTZOID));

	/* DATE */

	for (i64 = -100 * USECS_PER_DAY; i64 < 100 * USECS_PER_DAY; i64 += USECS_PER_DAY)
		TestAssertInt64Eq(i64,
						  ts_time_value_to_internal(ts_internal_to_time_value(i64, DATEOID),
													DATEOID));
	TestAssertInt64Eq(DATEVAL_NOBEGIN,
					  DatumGetDateADT(ts_internal_to_time_value(PG_INT64_MIN, DATEOID)));
	TestAssertInt64Eq(DATEVAL_NOEND,
					  DatumGetDateADT(ts_internal_to_time_value(PG_INT64_MAX, DATEOID)));
	TestEnsureError(ts_time_value_to_internal(DateADTGetDatum(DATEVAL_NOBEGIN + 1), DATEOID));
	TestEnsureError(ts_time_value_to_internal(DateADTGetDatum(DATEVAL_NOEND - 1), DATEOID));

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
