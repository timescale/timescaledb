/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/lsyscache.h>

#include "gapfill_functions.h"
#include "time_bucket.h"

Datum
gapfill_marker(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
}

/*
 * Integer gapfill wrappers.
 * 4-arg form: (bucket_width, ts, start, finish)
 * Note: offset is not supported for integer variants due to function resolution conflicts.
 */
#define GAPFILL_INT_WRAPPER(datatype)                                                              \
	Datum gapfill_##datatype##_time_bucket(PG_FUNCTION_ARGS)                                       \
	{                                                                                              \
		if (PG_ARGISNULL(0) || PG_ARGISNULL(1))                                                    \
			PG_RETURN_NULL();                                                                      \
		return DirectFunctionCall2(ts_##datatype##_bucket,                                         \
								   PG_GETARG_DATUM(0),                                             \
								   PG_GETARG_DATUM(1));                                            \
	}

GAPFILL_INT_WRAPPER(int16);
GAPFILL_INT_WRAPPER(int32);
GAPFILL_INT_WRAPPER(int64);

/*
 * Timestamp gapfill wrappers (date, timestamp, timestamptz without timezone).
 * Old 4-arg form: (bucket_width, ts, start, finish)
 * New 6-arg form: (bucket_width, ts, start, finish, origin, offset)
 */
Datum
gapfill_date_time_bucket(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	/* New 6-arg form with origin and offset */
	if (PG_NARGS() >= 6)
	{
		/* Check if origin is provided */
		if (!PG_ARGISNULL(4))
		{
			return DirectFunctionCall3(ts_date_bucket,
									   PG_GETARG_DATUM(0),
									   PG_GETARG_DATUM(1),
									   PG_GETARG_DATUM(4));
		}
		/* Check if offset is provided */
		if (!PG_ARGISNULL(5))
		{
			return DirectFunctionCall3(ts_date_offset_bucket,
									   PG_GETARG_DATUM(0),
									   PG_GETARG_DATUM(1),
									   PG_GETARG_DATUM(5));
		}
	}
	return DirectFunctionCall2(ts_date_bucket, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1));
}

Datum
gapfill_timestamp_time_bucket(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	/* New 6-arg form with origin and offset */
	if (PG_NARGS() >= 6)
	{
		/* Check if origin is provided */
		if (!PG_ARGISNULL(4))
		{
			return DirectFunctionCall3(ts_timestamp_bucket,
									   PG_GETARG_DATUM(0),
									   PG_GETARG_DATUM(1),
									   PG_GETARG_DATUM(4));
		}
		/* Check if offset is provided */
		if (!PG_ARGISNULL(5))
		{
			return DirectFunctionCall3(ts_timestamp_offset_bucket,
									   PG_GETARG_DATUM(0),
									   PG_GETARG_DATUM(1),
									   PG_GETARG_DATUM(5));
		}
	}
	return DirectFunctionCall2(ts_timestamp_bucket, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1));
}

Datum
gapfill_timestamptz_time_bucket(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	/* New 6-arg form with origin and offset */
	if (PG_NARGS() >= 6)
	{
		/* Check if origin is provided */
		if (!PG_ARGISNULL(4))
		{
			return DirectFunctionCall3(ts_timestamptz_bucket,
									   PG_GETARG_DATUM(0),
									   PG_GETARG_DATUM(1),
									   PG_GETARG_DATUM(4));
		}
		/* Check if offset is provided */
		if (!PG_ARGISNULL(5))
		{
			return DirectFunctionCall3(ts_timestamptz_offset_bucket,
									   PG_GETARG_DATUM(0),
									   PG_GETARG_DATUM(1),
									   PG_GETARG_DATUM(5));
		}
	}
	return DirectFunctionCall2(ts_timestamptz_bucket, PG_GETARG_DATUM(0), PG_GETARG_DATUM(1));
}

/*
 * Timestamptz with timezone gapfill wrapper.
 * Old 5-arg form: (bucket_width, ts, timezone, start, finish)
 * New 7-arg form: (bucket_width, ts, timezone, start, finish, origin, offset)
 */
Datum
gapfill_timestamptz_timezone_time_bucket(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	/* Check if using new 7-arg form with origin and offset */
	bool have_origin = (PG_NARGS() >= 7) && !PG_ARGISNULL(5);
	bool have_offset = (PG_NARGS() >= 7) && !PG_ARGISNULL(6);

	/*
	 * ts_timestamptz_timezone_bucket takes 5 args: (bucket_width, ts, timezone, origin, offset)
	 * We need to use LOCAL_FCINFO to handle NULL values properly.
	 */
	LOCAL_FCINFO(fcinfo_inner, 5);
	InitFunctionCallInfoData(*fcinfo_inner, NULL, 5, PG_GET_COLLATION(), NULL, NULL);

	fcinfo_inner->args[0].value = PG_GETARG_DATUM(0);
	fcinfo_inner->args[0].isnull = false;
	fcinfo_inner->args[1].value = PG_GETARG_DATUM(1);
	fcinfo_inner->args[1].isnull = false;
	fcinfo_inner->args[2].value = PG_GETARG_DATUM(2);
	fcinfo_inner->args[2].isnull = false;

	if (have_origin)
	{
		fcinfo_inner->args[3].value = PG_GETARG_DATUM(5);
		fcinfo_inner->args[3].isnull = false;
	}
	else
	{
		fcinfo_inner->args[3].value = (Datum) 0;
		fcinfo_inner->args[3].isnull = true;
	}

	if (have_offset)
	{
		fcinfo_inner->args[4].value = PG_GETARG_DATUM(6);
		fcinfo_inner->args[4].isnull = false;
	}
	else
	{
		fcinfo_inner->args[4].value = (Datum) 0;
		fcinfo_inner->args[4].isnull = true;
	}

	return ts_timestamptz_timezone_bucket(fcinfo_inner);
}
