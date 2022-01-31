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

#include "time_bucket.h"
#include "dimension.h"
#include "guc.h"
#include "time_utils.h"
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

Datum
ts_time_datum_convert_arg(Datum arg, Oid *argtype, Oid timetype)
{
	Oid type = *argtype;

	if (!OidIsValid(type) || type == UNKNOWNOID)
	{
		Oid infuncid = InvalidOid;
		Oid typeioparam;

		type = timetype;
		getTypeInputInfo(type, &infuncid, &typeioparam);

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

		*argtype = type;
	}

	return arg;
}

/*
 * Get the internal time value from a pseudo-type function argument.
 *
 * API functions that take supported time types as arguments often use a
 * pseudo-type parameter to represent these. For instance, the "any"
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
	/* If no explicit cast was done by the user, try to convert the argument
	 * to the time type used by the continuous aggregate. */
	arg = ts_time_datum_convert_arg(arg, &argtype, timetype);

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
				 errmsg("invalid time argument type \"%s\"", format_type_be(argtype)),
				 errhint("Try casting the argument to \"%s\".", format_type_be(timetype))));

	return ts_time_value_to_internal(arg, argtype);
}

/*
 * Try to coerce a type to a supported time type.
 *
 * To support custom time types in hypertables, we need to know the type's
 * boundaries in order to, e.g., construct dimensional chunk constraints. The
 * custom time type will inherit the valid time range of the supported time
 * type it can be casted to.
 *
 * Currently, we only support custom time types that are binary compatible
 * with bigint and then it also inherits the valid time range of a bigint.
 */
static Oid
coerce_to_time_type(Oid type)
{
	if (ts_type_is_int8_binary_compatible(type))
		return INT8OID;

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
	if (IS_TIMESTAMP_TYPE(timetype))
		return ts_time_datum_get_nobegin(timetype);

	return ts_time_datum_get_min(timetype);
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
	if (IS_TIMESTAMP_TYPE(timetype))
		return ts_time_get_end(timetype);

	return ts_time_get_max(timetype);
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

static int64
ts_time_get_nobegin_or_min(Oid timetype)
{
	if (IS_TIMESTAMP_TYPE(timetype))
		return ts_time_get_nobegin(timetype);

	return ts_time_get_min(timetype);
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
	if (IS_TIMESTAMP_TYPE(timetype))
		return ts_time_get_noend(timetype);

	return ts_time_get_max(timetype);
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
	if (timeval > 0 && interval > 0 && timeval > (ts_time_get_max(timetype) - interval))
		return ts_time_get_noend_or_max(timetype);

	if (timeval < 0 && interval < 0 && timeval < (ts_time_get_min(timetype) - interval))
		return ts_time_get_nobegin_or_min(timetype);

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
	if (timeval < 0 && interval > 0 && timeval < (ts_time_get_min(timetype) + interval))
		return ts_time_get_nobegin_or_min(timetype);

	if (timeval > 0 && interval < 0 && timeval > (ts_time_get_max(timetype) + interval))
		return ts_time_get_noend_or_max(timetype);

	return timeval - interval;
}

int64
ts_subtract_integer_from_now_saturating(Oid now_func, int64 interval, Oid timetype)
{
	Datum now = OidFunctionCall0(now_func);
	int64 time_min = ts_time_get_min(timetype);
	int64 time_max = ts_time_get_max(timetype);
	int64 nowval, res;

	Assert(IS_INTEGER_TYPE(timetype));
	switch (timetype)
	{
		case INT2OID:
		{
			nowval = DatumGetInt16(now);
			break;
		}
		case INT4OID:
		{
			nowval = DatumGetInt32(now);
			break;
		}
		case INT8OID:
		{
			nowval = DatumGetInt64(now);
			break;
		}
		default:
			elog(ERROR, "unsupported integer time type \"%s\"", format_type_be(timetype));
	}
	if (nowval > 0 && interval < 0 && nowval > time_max + interval)
		res = time_max;
	else if (nowval < 0 && interval > 0 && nowval < time_min + interval)
		res = time_min;
	else
		res = nowval - interval;
	return res;
}

#ifdef TS_DEBUG
/* return mock time for testing */
Datum
ts_get_mock_time_or_current_time(void)
{
	Datum res;
	if (ts_current_timestamp_mock != NULL && strlen(ts_current_timestamp_mock) != 0)
	{
		res = DirectFunctionCall3(timestamptz_in,
								  CStringGetDatum(ts_current_timestamp_mock),
								  0,
								  Int32GetDatum(-1));
		return res;
	}
	res = TimestampTzGetDatum(GetCurrentTimestamp());
	return res;
}
#endif
