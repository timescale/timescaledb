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
	int64 microseconds;

	if (timestamp < MIN_TIMESTAMP)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

	if (timestamp >= (END_TIMESTAMP - TS_EPOCH_DIFF_MICROSECONDS))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	microseconds = timestamp + TS_EPOCH_DIFF_MICROSECONDS;
#else
	if (1)
	{
		int64 seconds = (int64) timestamp;

		microseconds = (seconds * USECS_PER_SEC) + ((timestamp - seconds) * USECS_PER_SEC) +
					   epoch_diff_microseconds;
	}
#endif
	PG_RETURN_INT64(microseconds);
}

TS_FUNCTION_INFO_V1(ts_pg_unix_microseconds_to_timestamp);
TS_FUNCTION_INFO_V1(ts_pg_unix_microseconds_to_timestamp_without_timezone);
TS_FUNCTION_INFO_V1(ts_pg_unix_microseconds_to_date);

/*
 * Convert BIGINT microseconds relative the UNIX epoch to a Postgres TIMESTAMP.
 */
Datum
ts_pg_unix_microseconds_to_timestamp(PG_FUNCTION_ARGS)
{
	int64 microseconds = PG_GETARG_INT64(0);
	TimestampTz timestamp;

	/*
	 * Test that the UNIX us timestamp is within bounds. Note that an int64 at
	 * UNIX epoch and microsecond precision cannot represent the upper limit
	 * of the supported date range (Julian end date), so INT64_MAX is the
	 * natural upper bound for this function.
	 */
	if (microseconds < TS_INTERNAL_TIMESTAMP_MIN)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	timestamp = microseconds - TS_EPOCH_DIFF_MICROSECONDS;
#else
	/* Shift the epoch using integer arithmetic to reduce precision errors */
	timestamp = microseconds / USECS_PER_SEC; /* seconds */
	microseconds = microseconds - ((int64) timestamp * USECS_PER_SEC);
	timestamp =
		(float8)((int64) seconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY)) +
		(float8) microseconds / USECS_PER_SEC;
#endif
	PG_RETURN_TIMESTAMPTZ(timestamp);
}

Datum
ts_pg_unix_microseconds_to_date(PG_FUNCTION_ARGS)
{
	int64 microseconds = PG_GETARG_INT64(0);
	Datum res =
		DirectFunctionCall1(ts_pg_unix_microseconds_to_timestamp, Int64GetDatum(microseconds));
	res = DirectFunctionCall1(timestamp_date, res);
	PG_RETURN_DATUM(res);
}

static int64 ts_integer_to_internal(Datum time_val, Oid type_oid);

/* Convert valid timescale time column type to internal representation */
TSDLLEXPORT int64
ts_time_value_to_internal(Datum time_val, Oid type_oid)
{
	Datum res, tz;

	switch (type_oid)
	{
		case INT8OID:
		case INT4OID:
		case INT2OID:
			return ts_integer_to_internal(time_val, type_oid);
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

			elog(ERROR, "unknown time type OID %d", type_oid);
			return -1;
	}
}

TSDLLEXPORT int64
ts_interval_value_to_internal(Datum time_val, Oid type_oid)
{
	switch (type_oid)
	{
		case INT8OID:
		case INT4OID:
		case INT2OID:
			return ts_integer_to_internal(time_val, type_oid);
		case INTERVALOID:
		{
			Interval *interval = DatumGetIntervalP(time_val);
			if (interval->month != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("intervals must be defined in terms of days or smaller")));
			return interval->time + (interval->day * USECS_PER_DAY);
		}
		default:
			elog(ERROR, "unknown interval type OID %d", type_oid);
			return -1;
	}
}

static int64
ts_integer_to_internal(Datum time_val, Oid type_oid)
{
	switch (type_oid)
	{
		case INT8OID:
			return DatumGetInt64(time_val);
		case INT4OID:
			return (int64) DatumGetInt32(time_val);
		case INT2OID:
			return (int64) DatumGetInt16(time_val);
		default:
			elog(ERROR, "unknown interval type OID %d", type_oid);
			return -1;
	}
}

int64
ts_time_value_to_internal_or_infinite(Datum time_val, Oid type_oid,
									  TimevalInfinity *is_infinite_out)
{
	switch (type_oid)
	{
		case TIMESTAMPOID:
		{
			Timestamp ts = DatumGetTimestamp(time_val);
			if (TIMESTAMP_NOT_FINITE(ts))
			{
				if (TIMESTAMP_IS_NOBEGIN(ts))
				{
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalNegInfinity;
					return PG_INT64_MIN;
				}
				else
				{
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalPosInfinity;
					return PG_INT64_MAX;
				}
			}

			return ts_time_value_to_internal(time_val, type_oid);
		}
		case TIMESTAMPTZOID:
		{
			TimestampTz ts = DatumGetTimestampTz(time_val);
			if (TIMESTAMP_NOT_FINITE(ts))
			{
				if (TIMESTAMP_IS_NOBEGIN(ts))
				{
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalNegInfinity;
					return PG_INT64_MIN;
				}
				else
				{
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalPosInfinity;
					return PG_INT64_MAX;
				}
			}

			return ts_time_value_to_internal(time_val, type_oid);
		}
		case DATEOID:
		{
			DateADT d = DatumGetTimestamp(time_val);
			if (DATE_NOT_FINITE(d))
			{
				if (DATE_IS_NOBEGIN(d))
				{
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalNegInfinity;
					return PG_INT64_MIN;
				}
				else
				{
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalPosInfinity;
					return PG_INT64_MAX;
				}
			}

			return ts_time_value_to_internal(time_val, type_oid);
		}
	}

	return ts_time_value_to_internal(time_val, type_oid);
}

TS_FUNCTION_INFO_V1(ts_time_to_internal);
Datum
ts_time_to_internal(PG_FUNCTION_ARGS)
{
	Datum time = PG_GETARG_DATUM(0);
	Oid time_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
	int64 res = ts_time_value_to_internal(time, time_type);
	PG_RETURN_INT64(res);
}

static Datum ts_integer_to_internal_value(int64 value, Oid type);

/*
 * convert int64 to Datum according to type
 * internally we store all times as int64 in the
 * same format postgres does
 */
TSDLLEXPORT Datum
ts_internal_to_time_value(int64 value, Oid type)
{
	switch (type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return ts_integer_to_internal_value(value, type);
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			/* we continue ts_time_value_to_internal's incorrect handling of TIMESTAMPs for
			 * compatibility */
			return DirectFunctionCall1(ts_pg_unix_microseconds_to_timestamp, Int64GetDatum(value));
		case DATEOID:
			return DirectFunctionCall1(ts_pg_unix_microseconds_to_date, Int64GetDatum(value));
		default:
			if (ts_type_is_int8_binary_compatible(type))
				return Int64GetDatum(value);
			elog(ERROR, "unknown time type OID %d in ts_internal_to_time_value", type);
	}
}

TS_FUNCTION_INFO_V1(ts_pg_unix_microseconds_to_interval);

Datum
ts_pg_unix_microseconds_to_interval(PG_FUNCTION_ARGS)
{
	int64 microseconds = PG_GETARG_INT64(0);
	Interval *interval = palloc0(sizeof(*interval));
	interval->day = microseconds / USECS_PER_DAY;
	interval->time = microseconds % USECS_PER_DAY;
	PG_RETURN_INTERVAL_P(interval);
}

TSDLLEXPORT Datum
ts_internal_to_interval_value(int64 value, Oid type)
{
	switch (type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return ts_integer_to_internal_value(value, type);
		case INTERVALOID:
			return DirectFunctionCall1(ts_pg_unix_microseconds_to_interval, Int64GetDatum(value));
		default:
			elog(ERROR, "unknown time type OID %d in ts_internal_to_time_value", type);
	}
}

static Datum
ts_integer_to_internal_value(int64 value, Oid type)
{
	switch (type)
	{
		case INT2OID:
			return Int16GetDatum(value);
		case INT4OID:
			return Int32GetDatum(value);
		case INT8OID:
			return Int64GetDatum(value);
		default:
			elog(ERROR, "unknown time type OID %d in ts_internal_to_time_value", type);
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
	Datum res = TimestampTzGetDatum(GetCurrentTimestamp());

	switch (type_oid)
	{
		case TIMESTAMPOID:
			res = DirectFunctionCall1(timestamptz_timestamp, res);
			res = DirectFunctionCall2(timestamp_mi_interval, res, interval);

			return ts_time_value_to_internal(res, type_oid);
		case TIMESTAMPTZOID:
			res = DirectFunctionCall2(timestamptz_mi_interval, res, interval);

			return ts_time_value_to_internal(res, type_oid);
		case DATEOID:
			res = DirectFunctionCall1(timestamptz_timestamp, res);
			res = DirectFunctionCall2(timestamp_mi_interval, res, interval);
			res = DirectFunctionCall1(timestamp_date, res);

			return ts_time_value_to_internal(res, type_oid);
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
							"TIMESTAMP, TIMESTAMPTZ, and DATE types")));
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown time type OID %d", type_oid)));
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
	int decode_type, val;
	char *lowunits =
		downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units), false);

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
					 errmsg("timestamp units \"%s\" not supported", lowunits)));
	}
	return -1;
}

Oid
ts_inheritance_parent_relid(Oid relid)
{
	Relation catalog;
	SysScanDesc scan;
	ScanKeyData skey;
	Oid parent = InvalidOid;
	HeapTuple tuple;

	catalog = heap_open(InheritsRelationId, AccessShareLock);
	ScanKeyInit(&skey,
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(catalog, InheritsRelidSeqnoIndexId, true, NULL, 1, &skey);
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
	HeapTuple tuple;
	Form_pg_cast castForm;
	bool result;

	tuple =
		SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(sourcetype), ObjectIdGetDatum(INT8OID));
	if (!HeapTupleIsValid(tuple))
		return false; /* no cast */
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
ts_create_struct_from_tuple(HeapTuple tuple, MemoryContext mctx, size_t alloc_size,
							size_t copy_size)
{
	void *struct_ptr = MemoryContextAllocZero(mctx, alloc_size);

	memcpy(struct_ptr, GETSTRUCT(tuple), copy_size);

	return struct_ptr;
}

bool
ts_function_types_equal(Oid left[], Oid right[], int nargs)
{
	int arg_index;

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

	func_candidates = FuncnameGetCandidates(list_make2(makeString(schema_name), makeString(name)),
											nargs,
											NIL,
											false,
											false,
											false);
	while (func_candidates != NULL)
	{
		if (func_candidates->nargs == nargs &&
			ts_function_types_equal(func_candidates->args, arg_types, nargs))
			return func_candidates->oid;
		func_candidates = func_candidates->next;
	}
	elog(ERROR, "failed to find function %s in schema %s with %d args", name, schema_name, nargs);
}

/*
 * get_operator
 *
 *    finds an operator given an exact specification (name, namespace,
 *    left and right type IDs).
 */
Oid
get_operator(const char *name, Oid namespace, Oid left, Oid right)
{
	HeapTuple tup;
	Oid opoid = InvalidOid;

	tup = SearchSysCache4(OPERNAMENSP,
						  PointerGetDatum(name),
						  ObjectIdGetDatum(left),
						  ObjectIdGetDatum(right),
						  ObjectIdGetDatum(namespace));
	if (HeapTupleIsValid(tup))
	{
		opoid = HeapTupleGetOid(tup);
		ReleaseSysCache(tup);
	}

	return opoid;
}

/*
 * get_cast_func
 *
 * returns Oid of functions that implements cast from source to target
 */
Oid
get_cast_func(Oid source, Oid target)
{
	Oid result = InvalidOid;
	HeapTuple casttup;

	casttup = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(source), ObjectIdGetDatum(target));
	if (HeapTupleIsValid(casttup))
	{
		Form_pg_cast castform = (Form_pg_cast) GETSTRUCT(casttup);

		result = castform->castfunc;
		ReleaseSysCache(casttup);
	}

	return result;
}

AttrNumber
attno_find_by_attname(TupleDesc tupdesc, Name attname)
{
	int i;

	if (NULL == attname)
		return InvalidAttrNumber;

	for (i = 0; i < tupdesc->natts; i++)
	{
		FormData_pg_attribute *attr = TupleDescAttr(tupdesc, i);

		if (strncmp(NameStr(attr->attname), NameStr(*attname), NAMEDATALEN) == 0)
			return attr->attnum;
	}
	return InvalidAttrNumber;
}
