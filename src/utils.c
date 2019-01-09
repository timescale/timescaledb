/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
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

#if !PG96
#include <utils/fmgrprotos.h>
#endif

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

/* Convert valid timescale time column type to internal representation */
int64
ts_time_value_to_internal(Datum time_val, Oid type_oid, bool failure_ok)
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
			if (ts_type_is_int8_binary_compatible(type_oid))
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
ts_interval_from_now_to_internal(Datum interval, Oid type_oid)
{
	Datum		res = TimestampTzGetDatum(GetCurrentTimestamp());

	switch (type_oid)
	{
		case TIMESTAMPOID:
			res = DirectFunctionCall1(timestamptz_timestamp, res);
			res = DirectFunctionCall2(timestamp_mi_interval, res, interval);

			return ts_time_value_to_internal(res, type_oid, false);
		case TIMESTAMPTZOID:
			res = DirectFunctionCall2(timestamptz_mi_interval, res, interval);

			return ts_time_value_to_internal(res, type_oid, false);
		case DATEOID:
			res = DirectFunctionCall1(timestamptz_timestamp, res);
			res = DirectFunctionCall2(timestamp_mi_interval, res, interval);
			res = DirectFunctionCall1(timestamp_date, res);

			return ts_time_value_to_internal(res, type_oid, false);
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
	/* suppress compiler warnings on MSVC */
	Assert(false);
	return 0;
}

/* Returns approximate period in microseconds */
int64
ts_get_interval_period_approx(Interval *interval)
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
ts_date_trunc_interval_period_approx(text *units)
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
ts_inheritance_parent_relid(Oid relid)
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
ts_type_is_int8_binary_compatible(Oid sourcetype)
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
ts_create_struct_from_tuple(HeapTuple tuple, MemoryContext mctx, size_t alloc_size, size_t copy_size)
{
	void	   *struct_ptr = MemoryContextAllocZero(mctx, alloc_size);

	memcpy(struct_ptr, GETSTRUCT(tuple), copy_size);

	return struct_ptr;
}

bool
ts_function_types_equal(Oid left[], Oid right[], int nargs)
{
	int			arg_index;

	for (arg_index = 0; arg_index < nargs; arg_index++)
	{
		if (left[arg_index] != right[arg_index])
			return false;
	}
	return true;
}

Oid
get_function_oid(char *name, char *schema_name, int nargs, Oid arg_types[])
{
	FuncCandidateList func_candidates;

	func_candidates = FuncnameGetCandidates(list_make2(makeString(schema_name), makeString(name)), nargs, NIL, false, false, false);
	while (func_candidates != NULL)
	{
		if (func_candidates->nargs == nargs && ts_function_types_equal(func_candidates->args, arg_types, nargs))
			return func_candidates->oid;
		func_candidates = func_candidates->next;
	}
	elog(ERROR, "failed to find function %s in schema %s with %d args", name, schema_name, nargs);
}
