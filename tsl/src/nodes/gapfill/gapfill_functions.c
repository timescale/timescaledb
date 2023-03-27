/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/lsyscache.h>
#include <signal.h>

#include "time_bucket.h"
#include "gapfill_functions.h"

Datum
gapfill_marker(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	else
		PG_RETURN_DATUM(PG_GETARG_DATUM(0));
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
		if (PG_NARGS() <= 4 || PG_ARGISNULL(4))                                                    \
			return DirectFunctionCall2(ts_##datatype##_bucket,                                     \
									   PG_GETARG_DATUM(0),                                         \
									   PG_GETARG_DATUM(1));                                        \
		else                                                                                       \
			return DirectFunctionCall3(ts_##datatype##_bucket,                                     \
									   PG_GETARG_DATUM(0),                                         \
									   PG_GETARG_DATUM(1),                                         \
									   PG_GETARG_DATUM(4));                                        \
	}

GAPFILL_TIMEBUCKET_WRAPPER(int16);
GAPFILL_TIMEBUCKET_WRAPPER(int32);
GAPFILL_TIMEBUCKET_WRAPPER(int64);
GAPFILL_TIMEBUCKET_WRAPPER(date);
GAPFILL_TIMEBUCKET_WRAPPER(timestamp);
GAPFILL_TIMEBUCKET_WRAPPER(timestamptz);

Datum
gapfill_timestamptz_timezone_origin_time_bucket(PG_FUNCTION_ARGS)
{
	/*
	 * since time_bucket is STRICT and time_bucket_gapfill
	 * is not we need to add explicit checks for NULL here
	 */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	if (PG_NARGS() <= 5 || PG_ARGISNULL(5))
		return DirectFunctionCall3(ts_timestamptz_timezone_bucket,
								   PG_GETARG_DATUM(0),
								   PG_GETARG_DATUM(1),
								   PG_GETARG_DATUM(2));
	else
		return DirectFunctionCall4(ts_timestamptz_timezone_bucket,
								   PG_GETARG_DATUM(0),
								   PG_GETARG_DATUM(1),
								   PG_GETARG_DATUM(2),
								   PG_GETARG_DATUM(5));
}
