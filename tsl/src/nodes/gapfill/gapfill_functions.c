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
	{
		PG_RETURN_NULL();
	}
	else
	{
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
	}
}

/* offset can be passed in as 3rd argument, no origin for integer buckets */
#define GAPFILL_TIMEBUCKET_INT_WRAPPER(datatype)                                                   \
	Datum gapfill_##datatype##_time_bucket(PG_FUNCTION_ARGS)                                       \
	{                                                                                              \
		/*                                                                                         \
		 * since time_bucket is STRICT and time_bucket_gapfill                                     \
		 * is not we need to add explicit checks for NULL here                                     \
		 */                                                                                        \
		if (PG_ARGISNULL(0) || PG_ARGISNULL(1))                                                    \
			PG_RETURN_NULL();                                                                      \
		if (PG_NARGS() <= 4 || PG_ARGISNULL(2))                                                    \
			return DirectFunctionCall2(ts_##datatype##_bucket,                                     \
									   PG_GETARG_DATUM(0),                                         \
									   PG_GETARG_DATUM(1));                                        \
		else                                                                                       \
			return DirectFunctionCall3(ts_##datatype##_bucket,                                     \
									   PG_GETARG_DATUM(0),                                         \
									   PG_GETARG_DATUM(1),                                         \
									   PG_GETARG_DATUM(2));                                        \
	}

/* offset can be passed in as 3rd argument (can be NULL), origin as 4th argument (can be NULL) */
#define GAPFILL_TIMEBUCKET_OFFSET_WRAPPER(datatype)                                                \
	Datum gapfill_##datatype##_offset_bucket(PG_FUNCTION_ARGS)                                     \
	{                                                                                              \
		/*                                                                                         \
		 * since time_bucket is STRICT and time_bucket_gapfill                                     \
		 * is not we need to add explicit checks for NULL here                                     \
		 */                                                                                        \
		if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || (PG_ARGISNULL(2) && PG_ARGISNULL(3)))            \
			PG_RETURN_NULL();                                                                      \
		bool has_origin = !PG_ARGISNULL(3);                                                        \
		if (has_origin)                                                                            \
		{                                                                                          \
			return DirectFunctionCall3(ts_##datatype##_bucket,                                     \
									   PG_GETARG_DATUM(0),                                         \
									   PG_GETARG_DATUM(1),                                         \
									   PG_GETARG_DATUM(3));                                        \
		}                                                                                          \
		else                                                                                       \
		{                                                                                          \
			return DirectFunctionCall3(ts_##datatype##_offset_bucket,                              \
									   PG_GETARG_DATUM(0),                                         \
									   PG_GETARG_DATUM(1),                                         \
									   PG_GETARG_DATUM(2));                                        \
		}                                                                                          \
	}

#define GAPFILL_TIMEBUCKET_WRAPPER(datatype)                                                       \
	Datum gapfill_##datatype##_time_bucket(PG_FUNCTION_ARGS)                                       \
	{                                                                                              \
		/*                                                                                         \
		 * since time_bucket is STRICT and time_bucket_gapfill                                     \
		 * is not we need to add explicit checks for NULL here                                     \
		 */                                                                                        \
		if (PG_ARGISNULL(0) || PG_ARGISNULL(1))                                                    \
			PG_RETURN_NULL();                                                                      \
		return DirectFunctionCall2(ts_##datatype##_bucket,                                         \
								   PG_GETARG_DATUM(0),                                             \
								   PG_GETARG_DATUM(1));                                            \
	}

GAPFILL_TIMEBUCKET_INT_WRAPPER(int16);
GAPFILL_TIMEBUCKET_INT_WRAPPER(int32);
GAPFILL_TIMEBUCKET_INT_WRAPPER(int64);
GAPFILL_TIMEBUCKET_WRAPPER(date);
GAPFILL_TIMEBUCKET_WRAPPER(timestamp);
GAPFILL_TIMEBUCKET_WRAPPER(timestamptz);
GAPFILL_TIMEBUCKET_OFFSET_WRAPPER(date);
GAPFILL_TIMEBUCKET_OFFSET_WRAPPER(timestamp);
GAPFILL_TIMEBUCKET_OFFSET_WRAPPER(timestamptz);

Datum
gapfill_timestamptz_timezone_time_bucket(PG_FUNCTION_ARGS)
{
	/*
	 * since time_bucket is STRICT and time_bucket_gapfill
	 * is not we need to add explicit checks for NULL here
	 */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
	{
		PG_RETURN_NULL();
	}

	return DirectFunctionCall3(ts_timestamptz_timezone_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1),
							   PG_GETARG_DATUM(2));
}

/* offset can be passed in as 4th argument, origin as 5th argument (can be NULL) */
Datum
gapfill_timestamptz_timezone_offset_bucket(PG_FUNCTION_ARGS)
{
	/*
	 * since time_bucket is STRICT and time_bucket_gapfill
	 * is not we need to add explicit checks for NULL here
	 */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
	{
		PG_RETURN_NULL();
	}
	Datum origin = (!PG_ARGISNULL(4) ? PG_GETARG_DATUM(4) : Int64GetDatum(DEFAULT_ORIGIN));
	return DirectFunctionCall5(ts_timestamptz_timezone_bucket,
							   PG_GETARG_DATUM(0),
							   PG_GETARG_DATUM(1),
							   PG_GETARG_DATUM(2),
							   origin,
							   PG_GETARG_DATUM(3));
}
