/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <catalog/pg_type.h>
#include <utils/timestamp.h>
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
						 errmsg("invalid window parameter"),
						 errhint("The window parameter requires an explicit cast.")));
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
