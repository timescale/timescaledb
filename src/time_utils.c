/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/pg_type.h>
#include <utils/timestamp.h>
#include <utils/date.h>
#include <utils/builtins.h>
#include <parser/parse_coerce.h>
#include <fmgr.h>

#include "time_utils.h"
#include "dimension.h"
#include "utils.h"

static Datum
subtract_interval_from_now(Oid timetype, const Interval *interval)
{
	Datum res = DirectFunctionCall1(now, 0);

	switch (timetype)
	{
		case TIMESTAMPOID:
			res = DirectFunctionCall1(timestamptz_timestamp, res);
			return DirectFunctionCall2(timestamp_mi_interval, res, IntervalPGetDatum(interval));
		case TIMESTAMPTZOID:
			return DirectFunctionCall2(timestamptz_mi_interval, res, IntervalPGetDatum(interval));
		case DATEOID:
			res = DirectFunctionCall1(timestamptz_timestamp, res);
			res = DirectFunctionCall2(timestamp_mi_interval, res, IntervalPGetDatum(interval));
			return DirectFunctionCall1(timestamp_date, res);
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unknown time type %s", format_type_be(timetype))));
	}

	return res;
}

/*
 * Get the internal time value from a pseudo-type function argument.
 *
 * API functions that take supported time types as arguments often use a
 * pseudo-type paremeter to represent these. For instance, the "any"
 * pseudo-type is often used to represent any of these supported types.
 *
 * The downside of "any", however, is that it lacks type information and often
 * forces users to add explicit type casts. For instance, with the following
 * API call
 *
 * drop_chunk('conditions', '2020-10-01');
 *
 * the argument type will be UNKNOWNOID. And the user would have to add
 * an explicit type cast:
 *
 * drop_chunks('conditions', '2020-10-01'::date);
 *
 * However, we can handle the UNKNOWNOID case since we have the time type
 * information in internal metadata (e.g., the time column type of a
 * hypertable) and we can try to convert the argument to that type.
 *
 * Thus, there are two cases:
 *
 * 1. An explicit cast was done --> the type is given in argtype.
 * 2. No cast was done --> we try to convert the argument to the known time
 *    type.
 *
 * If an unsupported type is given, or the typeless argument has a nonsensical
 * string, then there will be an error raised.
 */
int64
ts_time_value_from_arg(Datum arg, Oid argtype, Oid timetype)
{
	if (!OidIsValid(argtype) || argtype == UNKNOWNOID)
	{
		/* No explicit cast was done by the user. Try to convert the argument
		 * to the time type used by the continuous aggregate. */
		Oid infuncid = InvalidOid;
		Oid typeioparam;

		argtype = timetype;
		getTypeInputInfo(argtype, &infuncid, &typeioparam);

		switch (get_func_nargs(infuncid))
		{
			case 1:
				/* Functions that take one input argument, e.g., the Date function */
				arg = OidFunctionCall1(infuncid, arg);
				break;
			case 3:
				/* Timestamp functions take three input arguments */
				arg = OidFunctionCall3(infuncid,
									   arg,
									   ObjectIdGetDatum(InvalidOid),
									   Int32GetDatum(-1));
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid time argument"),
						 errhint("Time argument requires an explicit cast.")));
		}
	}

	if (argtype == INTERVALOID)
	{
		if (IS_INTEGER_TYPE(timetype))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg(
						 "can only use an INTERVAL for TIMESTAMP, TIMESTAMPTZ, and DATE types")));

		arg = subtract_interval_from_now(timetype, DatumGetIntervalP(arg));
		argtype = timetype;
	}
	else if (argtype != timetype && !can_coerce_type(1, &argtype, &timetype, COERCION_IMPLICIT))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid type of time argument"),
				 errhint("The argument type should be compatible with type \"%s\"",
						 format_type_be(timetype))));

	return ts_time_value_to_internal(arg, argtype);
}

/*
 * Try to coerce a type to a supported time type.
 *
 * To support custom time types in hypertables, we need to know the type's
 * boundaries in order to, e.g., construct dimensional chunk constraints. If
 * the type is IMPLICITLY coercible to one of the supported time types we can
 * just use the boundaries of the supported type. Thus, the custom time type
 * will inherit the time boundaries of the first compatible time type found.
 */
static Oid
coerce_to_time_type(Oid type)
{
	static const Oid supported_time_types[] = { DATEOID, TIMESTAMPOID, TIMESTAMPTZOID,
												INT2OID, INT4OID,	  INT8OID };

	int i;

	for (i = 0; i < lengthof(supported_time_types); i++)
	{
		Oid targettype = supported_time_types[i];

		if (can_coerce_type(1, &type, &targettype, COERCION_IMPLICIT))
			return targettype;
	}

	elog(ERROR, "unsupported time type \"%s\"", format_type_be(type));
	pg_unreachable();
}

/*
 * Get the min time datum for a time type.
 *
 * Note that the min is not the same the actual "min" of the underlying
 * storage for date and timestamps.
 */
Datum
ts_time_datum_get_min(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
			return DateADTGetDatum(TS_DATE_MIN);
		case TIMESTAMPOID:
			return TimestampGetDatum(TS_TIMESTAMP_MIN);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(TS_TIMESTAMP_MIN);
		case INT2OID:
			return Int16GetDatum(PG_INT16_MIN);
		case INT4OID:
			return Int32GetDatum(PG_INT32_MIN);
		case INT8OID:
			return Int64GetDatum(PG_INT64_MIN);
		default:
			break;
	}

	return ts_time_datum_get_min(coerce_to_time_type(timetype));
}

/*
 * Get the end time datum for a time type.
 *
 * Note that the end is not the same as "max" (hence not named max). The end
 * is exclusive for date and timestamps, and might not be the same as the max
 * value for the underlying storage type (e.g., TIMESTAMP_END is before
 * PG_INT64_MAX). Instead, the max value for dates and timestamps represent
 * -Infinity and +Infinity.
 */
Datum
ts_time_datum_get_end(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
			return DateADTGetDatum(TS_DATE_END);
		case TIMESTAMPOID:
			return TimestampGetDatum(TS_TIMESTAMP_END);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(TS_TIMESTAMP_END);
		case INT2OID:
		case INT4OID:
		case INT8OID:
			elog(ERROR, "END is not defined for \"%s\"", format_type_be(timetype));
			break;
		default:
			break;
	}

	return ts_time_datum_get_end(coerce_to_time_type(timetype));
}

Datum
ts_time_datum_get_max(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
			return DateADTGetDatum(TS_DATE_END - 1);
		case TIMESTAMPOID:
			return TimestampGetDatum(TS_TIMESTAMP_END - 1);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(TS_TIMESTAMP_END - 1);
		case INT2OID:
			return Int16GetDatum(PG_INT16_MAX);
		case INT4OID:
			return Int32GetDatum(PG_INT32_MAX);
		case INT8OID:
			return Int64GetDatum(PG_INT64_MAX);
			break;
		default:
			break;
	}

	return ts_time_datum_get_max(coerce_to_time_type(timetype));
}

Datum
ts_time_datum_get_nobegin(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
			return DateADTGetDatum(DATEVAL_NOBEGIN);
		case TIMESTAMPOID:
			return TimestampGetDatum(DT_NOBEGIN);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(DT_NOBEGIN);
		case INT2OID:
		case INT4OID:
		case INT8OID:
			elog(ERROR, "NOBEGIN is not defined for \"%s\"", format_type_be(timetype));
			break;
		default:
			break;
	}

	return ts_time_datum_get_nobegin(coerce_to_time_type(timetype));
}

Datum
ts_time_datum_get_nobegin_or_min(Oid timetype)
{
	if (TS_TIME_IS_INTEGER_TIME(timetype))
		return ts_time_datum_get_min(timetype);

	return ts_time_datum_get_nobegin(timetype);
}

Datum
ts_time_datum_get_noend(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
			return DateADTGetDatum(DATEVAL_NOEND);
		case TIMESTAMPOID:
			return TimestampGetDatum(DT_NOEND);
		case TIMESTAMPTZOID:
			return TimestampTzGetDatum(DT_NOEND);
		case INT2OID:
		case INT4OID:
		case INT8OID:
			elog(ERROR, "NOEND is not defined for \"%s\"", format_type_be(timetype));
			break;
		default:
			break;
	}

	return ts_time_datum_get_noend(coerce_to_time_type(timetype));
}

/*
 * Get the min for a time type in internal time.
 */
int64
ts_time_get_min(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
			return TS_DATE_INTERNAL_MIN;
		case TIMESTAMPOID:
			return TS_TIMESTAMP_INTERNAL_MIN;
		case TIMESTAMPTZOID:
			return TS_TIMESTAMP_INTERNAL_MIN;
		case INT2OID:
			return PG_INT16_MIN;
		case INT4OID:
			return PG_INT32_MIN;
		case INT8OID:
			return PG_INT64_MIN;
		default:
			break;
	}

	return ts_time_get_min(coerce_to_time_type(timetype));
}

/*
 * Get the max for a time type in internal time.
 */
int64
ts_time_get_max(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
			return TS_DATE_INTERNAL_END - 1;
		case TIMESTAMPOID:
			return TS_TIMESTAMP_INTERNAL_END - 1;
		case TIMESTAMPTZOID:
			return TS_TIMESTAMP_INTERNAL_END - 1;
		case INT2OID:
			return PG_INT16_MAX;
		case INT4OID:
			return PG_INT32_MAX;
		case INT8OID:
			return PG_INT64_MAX;
		default:
			break;
	}

	return ts_time_get_max(coerce_to_time_type(timetype));
}

/*
 * Get the end value time for a time type in internal time.
 *
 * The end is not a valid time value (it is exclusive).
 */
int64
ts_time_get_end(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
			return TS_DATE_INTERNAL_END;
		case TIMESTAMPOID:
			return TS_TIMESTAMP_INTERNAL_END;
		case TIMESTAMPTZOID:
			return TS_TIMESTAMP_INTERNAL_END;
		case INT2OID:
		case INT4OID:
		case INT8OID:
			elog(ERROR, "END is not defined for \"%s\"", format_type_be(timetype));
			break;
		default:
			break;
	}

	return ts_time_get_end(coerce_to_time_type(timetype));
}

/*
 * Return the end (exclusive) or fall back to max.
 *
 * Integer time types have no definition for END, so we fall back to max.
 */
int64
ts_time_get_end_or_max(Oid timetype)
{
	if (TS_TIME_IS_INTEGER_TIME(timetype))
		return ts_time_get_max(timetype);

	return ts_time_get_end(timetype);
}

int64
ts_time_get_nobegin(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return TS_TIME_NOBEGIN;
		case INT2OID:
		case INT4OID:
		case INT8OID:
			elog(ERROR, "-Infinity not defined for \"%s\"", format_type_be(timetype));
			break;
		default:
			break;
	}

	return ts_time_get_nobegin(coerce_to_time_type(timetype));
}

int64
ts_time_get_noend(Oid timetype)
{
	switch (timetype)
	{
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return TS_TIME_NOEND;
		case INT2OID:
		case INT4OID:
		case INT8OID:
			elog(ERROR, "+Infinity not defined for \"%s\"", format_type_be(timetype));
			break;
		default:
			break;
	}

	return ts_time_get_noend(coerce_to_time_type(timetype));
}

int64
ts_time_get_noend_or_max(Oid timetype)
{
	if (TS_TIME_IS_INTEGER_TIME(timetype))
		return ts_time_get_max(timetype);

	return ts_time_get_noend(timetype);
}

/*
 * Add an interval to a time value in a saturating way.
 *
 * In contrast to, e.g., PG's timestamp_pl_interval, this function adds an
 * interval in a saturating way without throwing an error in case of
 * overflow. Instead it clamps to max for integer types end NOEND for date and
 * timestamp types.
 */
int64
ts_time_saturating_add(int64 timeval, int64 interval, Oid timetype)
{
	if (TS_TIME_IS_INTEGER_TIME(timetype))
	{
		int64 time_max = ts_time_get_max(timetype);

		if (timeval > (time_max - interval))
			return time_max;
	}
	else if (timeval >= (ts_time_get_end(timetype) - interval))
		return ts_time_get_noend(timetype);

	return timeval + interval;
}

/*
 * Subtract an interval from a time value in a saturating way.
 *
 * In contrast to, e.g., PG's timestamp_mi_interval, this function subtracts
 * an interval in a saturating way without throwing an error in case of
 * overflow. Instead, it clamps to min for integer types and NOBEGIN for date
 * and timestamp types.
 */
int64
ts_time_saturating_sub(int64 timeval, int64 interval, Oid timetype)
{
	if (TS_TIME_IS_INTEGER_TIME(timetype))
	{
		int64 time_min = ts_time_get_min(timetype);

		if (timeval < (time_min + interval))
			return time_min;
	}
	else if (timeval < (ts_time_get_min(timetype) + interval))
		return ts_time_get_nobegin(timetype);

	return timeval - interval;
}
