/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits.h>
#include <catalog/indexing.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <access/heapam.h>
#include <access/genam.h>
#include <nodes/makefuncs.h>
#include <parser/scansup.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/relcache.h>
#include <utils/fmgroids.h>
#include <utils/date.h>
#include <catalog/pg_cast.h>
#include <parser/parse_coerce.h>
#include <fmgr.h>
#include <access/xact.h>
#include <funcapi.h>

#include "chunk.h"
#include "utils.h"
#include "compat.h"

#if PG10
#include <utils/fmgrprotos.h>
#endif

TS_FUNCTION_INFO_V1(ts_pg_timestamp_to_microseconds);

/*
 * Convert a Postgres TIMESTAMP to BIGINT microseconds relative the Postgres epoch.
 */
Datum
ts_pg_timestamp_to_microseconds(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	int64		microseconds;

	if (!IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	microseconds = timestamp;
#else
	if (1)
	{
		int64		seconds = (int64) timestamp;

		microseconds = (seconds * USECS_PER_SEC) + ((timestamp - seconds) * USECS_PER_SEC);
	}
#endif
	PG_RETURN_INT64(microseconds);
}

TS_FUNCTION_INFO_V1(ts_pg_microseconds_to_timestamp);

/*
 * Convert BIGINT microseconds relative the UNIX epoch to a Postgres TIMESTAMP.
 */
Datum
ts_pg_microseconds_to_timestamp(PG_FUNCTION_ARGS)
{
	int64		microseconds = PG_GETARG_INT64(0);
	TimestampTz timestamp;

#ifdef HAVE_INT64_TIMESTAMP
	timestamp = microseconds;
#else
	timestamp = microseconds / USECS_PER_SEC;
#endif

	if (!IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMPTZ(timestamp);
}

TS_FUNCTION_INFO_V1(ts_pg_timestamp_to_unix_microseconds);

/*
 * Convert a Postgres TIMESTAMP to BIGINT microseconds relative the UNIX epoch.
 */
Datum
ts_pg_timestamp_to_unix_microseconds(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	int64		epoch_diff_microseconds = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY;
	int64		microseconds;

	if (timestamp < MIN_TIMESTAMP)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	if (timestamp >= (END_TIMESTAMP - epoch_diff_microseconds))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	microseconds = timestamp + epoch_diff_microseconds;
#else
	if (1)
	{
		int64		seconds = (int64) timestamp;

		microseconds = (seconds * USECS_PER_SEC) + ((timestamp - seconds) * USECS_PER_SEC) + epoch_diff_microseconds;
	}
#endif
	PG_RETURN_INT64(microseconds);
}

TS_FUNCTION_INFO_V1(ts_pg_unix_microseconds_to_timestamp);

/*
 * Convert BIGINT microseconds relative the UNIX epoch to a Postgres TIMESTAMP.
 */
Datum
ts_pg_unix_microseconds_to_timestamp(PG_FUNCTION_ARGS)
{
	int64		microseconds = PG_GETARG_INT64(0);
	TimestampTz timestamp;

	/*
	 * Test that the UNIX us timestamp is within bounds. Note that an int64 at
	 * UNIX epoch and microsecond precision cannot represent the upper limit
	 * of the supported date range (Julian end date), so INT64_MAX is the
	 * natural upper bound for this function.
	 */
	if (microseconds < ((int64) USECS_PER_DAY * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	timestamp = microseconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);
#else
	/* Shift the epoch using integer arithmetic to reduce precision errors */
	timestamp = microseconds / USECS_PER_SEC;	/* seconds */
	microseconds = microseconds - ((int64) timestamp * USECS_PER_SEC);
	timestamp = (float8) ((int64) seconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY))
		+ (float8) microseconds / USECS_PER_SEC;
#endif
	PG_RETURN_TIMESTAMPTZ(timestamp);
}

TS_FUNCTION_INFO_V1(ts_time_to_internal);

Datum
ts_time_to_internal(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	PG_RETURN_INT64(time_value_to_internal(PG_GETARG_DATUM(0), get_fn_expr_argtype(fcinfo->flinfo, 0), false));
}


/* Convert valid timescale time column type to internal representation */
int64
time_value_to_internal(Datum time_val, Oid type_oid, bool failure_ok)
{
	Datum		res,
				tz;

	switch (type_oid)
	{
		case INT8OID:
			return DatumGetInt64(time_val);
		case INT4OID:
			return (int64) DatumGetInt32(time_val);
		case INT2OID:
			return (int64) DatumGetInt16(time_val);
		case TIMESTAMPOID:

			/*
			 * for timestamps, ignore timezones, make believe the timestamp is
			 * at UTC
			 */
			res = DirectFunctionCall1(ts_pg_timestamp_to_unix_microseconds, time_val);

			return DatumGetInt64(res);
		case TIMESTAMPTZOID:
			res = DirectFunctionCall1(ts_pg_timestamp_to_unix_microseconds, time_val);

			return DatumGetInt64(res);
		case DATEOID:
			tz = DirectFunctionCall1(date_timestamp, time_val);
			res = DirectFunctionCall1(ts_pg_timestamp_to_unix_microseconds, tz);

			return DatumGetInt64(res);
		default:
			if (type_is_int8_binary_compatible(type_oid))
				return DatumGetInt64(time_val);
			if (!failure_ok)
				elog(ERROR, "unknown time type OID %d", type_oid);
			return -1;
	}
}

/*
 * Convert the difference of interval and current timestamp to internal representation
 * This function interprets the interval as distance in time dimension to the past.
 * Depending on the type of hypertable type column, the function applied the
 * necessary granularity to now() - interval and converts the resulting
 * time to internal int64 representation
 */
int64
interval_from_now_to_internal(Datum interval, Oid type_oid)
{
	TimestampTz now;
	Datum		res;

	/*
	 * This is really confusing but looks like it is how postgres works. Event
	 * though there is a function called Datum now() that calls
	 * GetCurrentTransactionStartTimestamp and returns TimestampTz, that is
	 * not the same as now() function in SQL and even though return type is
	 * Timestamp WITH TIMEZONE, GetCurrentTransactionStartTimestamp does not
	 * incorporate current session timezone infromation in the returned value.
	 * In order to take this timezone value set by the user into account, I
	 * convert timestamp to POSIX time structure using timestamp2tm *which has
	 * access to current timezone* setting through session_timezone variable
	 * and incorporates it in the returned structure. I then convert it back
	 * to TimestampTz through a similar function which takes this timezone
	 * information int account.
	 */
	int			tzoff;
	struct pg_tm tm;
	fsec_t		fsec;
	const char *tzn;

	timestamp2tm(GetCurrentTransactionStartTimestamp(),
				 &tzoff, &tm, &fsec, &tzn, NULL);
	tm2timestamp(&tm, fsec, NULL, &now);

	switch (type_oid)
	{
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			res = TimestampTzGetDatum(now);
			res = DirectFunctionCall2(timestamptz_mi_interval, res, interval);

			return time_value_to_internal(res, type_oid, false);
		case DATEOID:
			res = TimestampTzGetDatum(now);

			res = DirectFunctionCall2(timestamptz_mi_interval, res, interval);
			res = DirectFunctionCall1(timestamp_date, res);

			return time_value_to_internal(res, type_oid, false);
		case INT8OID:
		case INT4OID:
		case INT2OID:

			/*
			 * should never get here as the case is handled by
			 * time_dim_typecheck
			 */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("can only use this with an INTERVAL for "
							"TIMESTAMP, TIMESTAMPTZ, and DATE types")
					 ));
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown time type OID %d", type_oid)
					 ));

	}
}

/* Returns approximate period in microseconds */
int64
get_interval_period_approx(Interval *interval)
{
	return interval->time + (((interval->month * DAYS_PER_MONTH) + interval->day) * USECS_PER_DAY);
}

#define DAYS_PER_WEEK 7
#define DAYS_PER_QUARTER 89
#define YEARS_PER_DECADE 10
#define YEARS_PER_CENTURY 100
#define YEARS_PER_MILLENNIUM 1000

/* Returns approximate period in microseconds */
int64
date_trunc_interval_period_approx(text *units)
{
	int			decode_type,
				val;
	char	   *lowunits = downcase_truncate_identifier(VARDATA_ANY(units),
														VARSIZE_ANY_EXHDR(units),
														false);

	decode_type = DecodeUnits(0, lowunits, &val);

	if (decode_type != UNITS)
		return -1;

	switch (val)
	{
		case DTK_WEEK:
			return DAYS_PER_WEEK * USECS_PER_DAY;
		case DTK_MILLENNIUM:
			return YEARS_PER_MILLENNIUM * DAYS_PER_YEAR * USECS_PER_DAY;
		case DTK_CENTURY:
			return YEARS_PER_CENTURY * DAYS_PER_YEAR * USECS_PER_DAY;
		case DTK_DECADE:
			return YEARS_PER_DECADE * DAYS_PER_YEAR * USECS_PER_DAY;
		case DTK_YEAR:
			return 1 * DAYS_PER_YEAR * USECS_PER_DAY;
		case DTK_QUARTER:
			return DAYS_PER_QUARTER * USECS_PER_DAY;
		case DTK_MONTH:
			return DAYS_PER_MONTH * USECS_PER_DAY;
		case DTK_DAY:
			return USECS_PER_DAY;
		case DTK_HOUR:
			return USECS_PER_HOUR;
		case DTK_MINUTE:
			return USECS_PER_MINUTE;
		case DTK_SECOND:
			return USECS_PER_SEC;
		case DTK_MILLISEC:
			return USECS_PER_SEC / 1000;
		case DTK_MICROSEC:
			return 1;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("timestamp units \"%s\" not supported",
							lowunits)));
	}
	return -1;
}

Oid
inheritance_parent_relid(Oid relid)
{
	Relation	catalog;
	SysScanDesc scan;
	ScanKeyData skey;
	Oid			parent = InvalidOid;
	HeapTuple	tuple;

	catalog = heap_open(InheritsRelationId, AccessShareLock);
	ScanKeyInit(&skey, Anum_pg_inherits_inhrelid, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relid));
	scan = systable_beginscan(catalog, InheritsRelidSeqnoIndexId, true,
							  NULL, 1, &skey);
	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
		parent = ((Form_pg_inherits) GETSTRUCT(tuple))->inhparent;

	systable_endscan(scan);
	heap_close(catalog, AccessShareLock);

	return parent;
}


bool
type_is_int8_binary_compatible(Oid sourcetype)
{
	HeapTuple	tuple;
	Form_pg_cast castForm;
	bool		result;

	tuple = SearchSysCache2(CASTSOURCETARGET,
							ObjectIdGetDatum(sourcetype),
							ObjectIdGetDatum(INT8OID));
	if (!HeapTupleIsValid(tuple))
		return false;			/* no cast */
	castForm = (Form_pg_cast) GETSTRUCT(tuple);
	result = castForm->castmethod == COERCION_METHOD_BINARY;
	ReleaseSysCache(tuple);
	return result;
}

/*
 * Create a fresh struct pointer that will contain copied contents of the tuple.
 * Note that this function uses GETSTRUCT, which will not work correctly for tuple types
 * that might have variable lengths.
 */
void *
create_struct_from_tuple(HeapTuple tuple, MemoryContext mctx, size_t alloc_size, size_t copy_size)
{
	void	   *struct_ptr = MemoryContextAllocZero(mctx, alloc_size);

	memcpy(struct_ptr, GETSTRUCT(tuple), copy_size);

	return struct_ptr;
}
