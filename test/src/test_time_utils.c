/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/date.h>
#include <fmgr.h>
#include <funcapi.h>

#include <time_utils.h>
#include <utils.h>

#include "test_utils.h"

/*
 * Functions to show TimescaleDB-specific limits of timestamps and dates:
 */

/*
 * TIMESTAMP WITH TIME ZONE
 */
TS_FUNCTION_INFO_V1(ts_timestamptz_pg_min);

Datum
ts_timestamptz_pg_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(MIN_TIMESTAMP);
}

TS_FUNCTION_INFO_V1(ts_timestamptz_pg_end);

Datum
ts_timestamptz_pg_end(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(END_TIMESTAMP);
}

TS_FUNCTION_INFO_V1(ts_timestamptz_min);

Datum
ts_timestamptz_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(TS_TIMESTAMP_MIN);
}

TS_FUNCTION_INFO_V1(ts_timestamptz_end);

Datum
ts_timestamptz_end(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMPTZ(TS_TIMESTAMP_END);
}

TS_FUNCTION_INFO_V1(ts_timestamptz_internal_min);

Datum
ts_timestamptz_internal_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(TS_TIMESTAMP_INTERNAL_MIN);
}

TS_FUNCTION_INFO_V1(ts_timestamptz_internal_end);

Datum
ts_timestamptz_internal_end(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(TS_TIMESTAMP_INTERNAL_END);
}

/*
 * TIMESTAMP
 */
TS_FUNCTION_INFO_V1(ts_timestamp_pg_min);

Datum
ts_timestamp_pg_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMP(MIN_TIMESTAMP);
}

TS_FUNCTION_INFO_V1(ts_timestamp_pg_end);

Datum
ts_timestamp_pg_end(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMP(END_TIMESTAMP);
}

TS_FUNCTION_INFO_V1(ts_timestamp_min);

Datum
ts_timestamp_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMP(TS_TIMESTAMP_MIN);
}

TS_FUNCTION_INFO_V1(ts_timestamp_end);

Datum
ts_timestamp_end(PG_FUNCTION_ARGS)
{
	PG_RETURN_TIMESTAMP(TS_TIMESTAMP_END);
}

TS_FUNCTION_INFO_V1(ts_timestamp_internal_min);

Datum
ts_timestamp_internal_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(TS_TIMESTAMP_INTERNAL_MIN);
}

TS_FUNCTION_INFO_V1(ts_timestamp_internal_end);

Datum
ts_timestamp_internal_end(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(TS_TIMESTAMP_INTERNAL_END);
}

/*
 * DATE
 */
TS_FUNCTION_INFO_V1(ts_date_pg_min);

Datum
ts_date_pg_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATEADT(DATETIME_MIN_JULIAN - POSTGRES_EPOCH_JDATE);
}

TS_FUNCTION_INFO_V1(ts_date_pg_end);

Datum
ts_date_pg_end(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATEADT(DATE_END_JULIAN - POSTGRES_EPOCH_JDATE);
}

TS_FUNCTION_INFO_V1(ts_date_min);

Datum
ts_date_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATEADT(TS_DATE_MIN);
}

TS_FUNCTION_INFO_V1(ts_date_end);

Datum
ts_date_end(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATEADT(TS_DATE_END);
}

TS_FUNCTION_INFO_V1(ts_date_internal_min);

Datum
ts_date_internal_min(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(TS_DATE_INTERNAL_MIN);
}

TS_FUNCTION_INFO_V1(ts_date_internal_end);

Datum
ts_date_internal_end(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(TS_DATE_INTERNAL_END);
}

TS_FUNCTION_INFO_V1(ts_test_time_utils);

Datum
ts_test_time_utils(PG_FUNCTION_ARGS)
{
	TestAssertInt64Eq(ts_time_get_min(INT8OID), PG_INT64_MIN);
	TestAssertInt64Eq(ts_time_get_max(INT8OID), PG_INT64_MAX);
	TestAssertInt64Eq(ts_time_get_end_or_max(INT8OID), PG_INT64_MAX);
	TestEnsureError(ts_time_get_end(INT8OID));
	TestEnsureError(ts_time_get_nobegin(INT8OID));
	TestEnsureError(ts_time_get_noend(INT8OID));
	TestAssertInt64Eq(DatumGetInt64(ts_time_datum_get_nobegin_or_min(INT8OID)), PG_INT64_MIN);
	TestAssertInt64Eq(DatumGetInt64(ts_time_datum_get_min(INT8OID)), PG_INT64_MIN);
	TestAssertInt64Eq(DatumGetInt64(ts_time_datum_get_max(INT8OID)), PG_INT64_MAX);
	TestEnsureError(ts_time_datum_get_end(INT8OID));
	TestEnsureError(ts_time_datum_get_nobegin(INT8OID));
	TestEnsureError(ts_time_datum_get_noend(INT8OID));

	TestAssertInt64Eq(ts_time_get_min(INT4OID), PG_INT32_MIN);
	TestAssertInt64Eq(ts_time_get_max(INT4OID), PG_INT32_MAX);
	TestAssertInt64Eq(ts_time_get_end_or_max(INT4OID), PG_INT32_MAX);
	TestEnsureError(ts_time_get_end(INT4OID));
	TestEnsureError(ts_time_get_nobegin(INT4OID));
	TestEnsureError(ts_time_get_noend(INT4OID));
	TestAssertInt64Eq(DatumGetInt32(ts_time_datum_get_nobegin_or_min(INT4OID)), PG_INT32_MIN);
	TestAssertInt64Eq(DatumGetInt32(ts_time_datum_get_min(INT4OID)), PG_INT32_MIN);
	TestAssertInt64Eq(DatumGetInt32(ts_time_datum_get_max(INT4OID)), PG_INT32_MAX);
	TestEnsureError(ts_time_datum_get_end(INT4OID));
	TestEnsureError(ts_time_datum_get_nobegin(INT4OID));
	TestEnsureError(ts_time_datum_get_noend(INT4OID));

	TestAssertInt64Eq(ts_time_get_min(INT2OID), PG_INT16_MIN);
	TestAssertInt64Eq(ts_time_get_max(INT2OID), PG_INT16_MAX);
	TestAssertInt64Eq(ts_time_get_end_or_max(INT2OID), PG_INT16_MAX);
	TestEnsureError(ts_time_get_end(INT2OID));
	TestEnsureError(ts_time_get_nobegin(INT2OID));
	TestEnsureError(ts_time_get_noend(INT2OID));
	TestAssertInt64Eq(DatumGetInt16(ts_time_datum_get_nobegin_or_min(INT2OID)), PG_INT16_MIN);
	TestAssertInt64Eq(DatumGetInt16(ts_time_datum_get_min(INT2OID)), PG_INT16_MIN);
	TestAssertInt64Eq(DatumGetInt16(ts_time_datum_get_max(INT2OID)), PG_INT16_MAX);
	TestEnsureError(ts_time_datum_get_end(INT2OID));
	TestEnsureError(ts_time_datum_get_nobegin(INT2OID));
	TestEnsureError(ts_time_datum_get_noend(INT2OID));

	TestAssertInt64Eq(ts_time_get_min(TIMESTAMPOID), TS_TIMESTAMP_INTERNAL_MIN);
	TestAssertInt64Eq(ts_time_get_max(TIMESTAMPOID), TS_TIMESTAMP_INTERNAL_MAX);
	TestAssertInt64Eq(ts_time_get_end(TIMESTAMPOID), TS_TIMESTAMP_INTERNAL_END);
	TestAssertInt64Eq(ts_time_get_end_or_max(TIMESTAMPOID), TS_TIMESTAMP_INTERNAL_END);
	TestAssertInt64Eq(ts_time_get_nobegin(TIMESTAMPOID), TS_TIME_NOBEGIN);
	TestAssertInt64Eq(ts_time_get_noend(TIMESTAMPOID), TS_TIME_NOEND);
	TestAssertInt64Eq(DatumGetTimestamp(ts_time_datum_get_nobegin_or_min(TIMESTAMPOID)),
					  DT_NOBEGIN);
	TestAssertInt64Eq(DatumGetTimestamp(ts_time_datum_get_min(TIMESTAMPOID)), TS_TIMESTAMP_MIN);
	TestAssertInt64Eq(DatumGetTimestamp(ts_time_datum_get_max(TIMESTAMPOID)), TS_TIMESTAMP_MAX);
	TestAssertInt64Eq(DatumGetTimestamp(ts_time_datum_get_end(TIMESTAMPOID)), TS_TIMESTAMP_END);
	TestAssertInt64Eq(DatumGetTimestamp(ts_time_datum_get_nobegin(TIMESTAMPOID)), DT_NOBEGIN);
	TestAssertInt64Eq(DatumGetTimestamp(ts_time_datum_get_noend(TIMESTAMPOID)), DT_NOEND);

	TestAssertInt64Eq(ts_time_get_min(TIMESTAMPTZOID), TS_TIMESTAMP_INTERNAL_MIN);
	TestAssertInt64Eq(ts_time_get_max(TIMESTAMPTZOID), TS_TIMESTAMP_INTERNAL_MAX);
	TestAssertInt64Eq(ts_time_get_end(TIMESTAMPTZOID), TS_TIMESTAMP_INTERNAL_END);
	TestAssertInt64Eq(ts_time_get_end_or_max(TIMESTAMPTZOID), TS_TIMESTAMP_INTERNAL_END);
	TestAssertInt64Eq(ts_time_get_nobegin(TIMESTAMPTZOID), TS_TIME_NOBEGIN);
	TestAssertInt64Eq(ts_time_get_noend(TIMESTAMPTZOID), TS_TIME_NOEND);
	TestAssertInt64Eq(DatumGetTimestampTz(ts_time_datum_get_nobegin_or_min(TIMESTAMPTZOID)),
					  DT_NOBEGIN);
	TestAssertInt64Eq(DatumGetTimestampTz(ts_time_datum_get_min(TIMESTAMPTZOID)), TS_TIMESTAMP_MIN);
	TestAssertInt64Eq(DatumGetTimestampTz(ts_time_datum_get_max(TIMESTAMPTZOID)), TS_TIMESTAMP_MAX);
	TestAssertInt64Eq(DatumGetTimestampTz(ts_time_datum_get_end(TIMESTAMPTZOID)), TS_TIMESTAMP_END);
	TestAssertInt64Eq(DatumGetTimestampTz(ts_time_datum_get_nobegin(TIMESTAMPTZOID)), DT_NOBEGIN);
	TestAssertInt64Eq(DatumGetTimestampTz(ts_time_datum_get_noend(TIMESTAMPTZOID)), DT_NOEND);

	TestAssertInt64Eq(ts_time_get_min(DATEOID), TS_DATE_INTERNAL_MIN);
	TestAssertInt64Eq(ts_time_get_max(DATEOID), TS_DATE_INTERNAL_MAX);
	TestAssertInt64Eq(ts_time_get_end(DATEOID), TS_DATE_INTERNAL_END);
	TestAssertInt64Eq(ts_time_get_end_or_max(DATEOID), TS_DATE_INTERNAL_END);
	TestAssertInt64Eq(ts_time_get_nobegin(DATEOID), TS_TIME_NOBEGIN);
	TestAssertInt64Eq(ts_time_get_noend(DATEOID), TS_TIME_NOEND);
	TestAssertInt64Eq(DatumGetDateADT(ts_time_datum_get_nobegin_or_min(DATEOID)), DATEVAL_NOBEGIN);
	TestAssertInt64Eq(DatumGetDateADT(ts_time_datum_get_min(DATEOID)), TS_DATE_MIN);
	TestAssertInt64Eq(DatumGetDateADT(ts_time_datum_get_max(DATEOID)), TS_DATE_MAX);
	TestAssertInt64Eq(DatumGetDateADT(ts_time_datum_get_end(DATEOID)), TS_DATE_END);
	TestAssertInt64Eq(DatumGetDateADT(ts_time_datum_get_nobegin(DATEOID)), DATEVAL_NOBEGIN);
	TestAssertInt64Eq(DatumGetDateADT(ts_time_datum_get_noend(DATEOID)), DATEVAL_NOEND);

	/* Test boundary routines with unsupported time type */
	TestEnsureError(ts_time_get_min(NUMERICOID));
	TestEnsureError(ts_time_get_max(NUMERICOID));
	TestEnsureError(ts_time_get_end(NUMERICOID));
	TestEnsureError(ts_time_get_nobegin(NUMERICOID));
	TestEnsureError(ts_time_get_noend(NUMERICOID));
	TestEnsureError(ts_time_datum_get_nobegin_or_min(NUMERICOID));
	TestEnsureError(ts_time_datum_get_min(NUMERICOID));
	TestEnsureError(ts_time_datum_get_max(NUMERICOID));
	TestEnsureError(ts_time_datum_get_end(NUMERICOID));
	TestEnsureError(ts_time_datum_get_nobegin(NUMERICOID));
	TestEnsureError(ts_time_datum_get_noend(NUMERICOID));

	/* Test conversion of min, end, nobegin, and noend between native and
	 * internal (Unix) time */

	TestAssertInt64Eq(ts_time_value_to_internal(ts_time_datum_get_min(INT2OID), INT2OID),
					  ts_time_get_min(INT2OID));
	TestAssertInt64Eq(ts_time_value_to_internal(ts_time_datum_get_min(INT4OID), INT4OID),
					  ts_time_get_min(INT4OID));
	TestAssertInt64Eq(ts_time_value_to_internal(ts_time_datum_get_min(INT8OID), INT8OID),
					  ts_time_get_min(INT8OID));
	TestAssertInt64Eq(ts_time_value_to_internal(ts_time_datum_get_min(DATEOID), DATEOID),
					  ts_time_get_min(DATEOID));
	TestAssertInt64Eq(ts_time_value_to_internal(ts_time_datum_get_min(TIMESTAMPOID), TIMESTAMPOID),
					  ts_time_get_min(TIMESTAMPOID));
	TestAssertInt64Eq(ts_time_value_to_internal(ts_time_datum_get_min(TIMESTAMPTZOID),
												TIMESTAMPTZOID),
					  ts_time_get_min(TIMESTAMPTZOID));

	/* Test saturating addition */
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_max(INT2OID), 1, INT2OID),
					  ts_time_get_max(INT2OID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_max(INT4OID), 1, INT4OID),
					  ts_time_get_max(INT4OID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_max(INT8OID), 1, INT8OID),
					  ts_time_get_max(INT8OID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_max(DATEOID), 1, DATEOID),
					  ts_time_get_noend(DATEOID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_max(DATEOID), 1, DATEOID),
					  ts_time_get_noend(DATEOID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_max(TIMESTAMPOID), 1, TIMESTAMPOID),
					  ts_time_get_noend(TIMESTAMPOID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_max(TIMESTAMPTZOID), 1, TIMESTAMPOID),
					  ts_time_get_noend(TIMESTAMPOID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_end(DATEOID) - 2, 1, DATEOID),
					  ts_time_get_max(DATEOID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_end(TIMESTAMPOID) - 2, 1, TIMESTAMPOID),
					  ts_time_get_max(TIMESTAMPOID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_end(TIMESTAMPTZOID) - 2, 1, TIMESTAMPOID),
					  ts_time_get_max(TIMESTAMPOID));

	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_min(INT2OID), -1, INT2OID),
					  ts_time_get_min(INT2OID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_min(INT4OID), -1, INT4OID),
					  ts_time_get_min(INT4OID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_min(INT8OID), -1, INT8OID),
					  ts_time_get_min(INT8OID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_min(DATEOID), -1, DATEOID),
					  ts_time_get_nobegin(DATEOID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_min(DATEOID), -1, DATEOID),
					  ts_time_get_nobegin(DATEOID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_min(TIMESTAMPOID), -1, TIMESTAMPOID),
					  ts_time_get_nobegin(TIMESTAMPOID));
	TestAssertInt64Eq(ts_time_saturating_add(ts_time_get_min(TIMESTAMPTZOID), -1, TIMESTAMPOID),
					  ts_time_get_nobegin(TIMESTAMPOID));

	/* Test saturating subtraction */
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_min(INT2OID), 1, INT2OID),
					  ts_time_get_min(INT2OID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_min(INT4OID), 1, INT4OID),
					  ts_time_get_min(INT4OID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_min(INT8OID), 1, INT8OID),
					  ts_time_get_min(INT8OID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_min(DATEOID), 1, DATEOID),
					  ts_time_get_nobegin(DATEOID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_min(TIMESTAMPOID), 1, TIMESTAMPOID),
					  ts_time_get_nobegin(TIMESTAMPOID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_min(TIMESTAMPTZOID), 1, TIMESTAMPTZOID),
					  ts_time_get_nobegin(TIMESTAMPTZOID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_min(DATEOID) + 1, 1, DATEOID),
					  ts_time_get_min(DATEOID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_min(TIMESTAMPOID) + 1, 1, TIMESTAMPOID),
					  ts_time_get_min(TIMESTAMPOID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_min(TIMESTAMPTZOID) + 1,
											 1,
											 TIMESTAMPTZOID),
					  ts_time_get_min(TIMESTAMPTZOID));

	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_max(INT2OID), -1, INT2OID),
					  ts_time_get_max(INT2OID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_max(INT4OID), -1, INT4OID),
					  ts_time_get_max(INT4OID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_max(INT8OID), -1, INT8OID),
					  ts_time_get_max(INT8OID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_max(DATEOID), -1, DATEOID),
					  ts_time_get_noend(DATEOID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_max(TIMESTAMPOID), -1, TIMESTAMPOID),
					  ts_time_get_noend(TIMESTAMPOID));
	TestAssertInt64Eq(ts_time_saturating_sub(ts_time_get_max(TIMESTAMPTZOID), -1, TIMESTAMPTZOID),
					  ts_time_get_noend(TIMESTAMPTZOID));

	PG_RETURN_VOID();
}
