/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>

#include "license_guc.h"
#include "cross_module_fn.h"
#include "compat/compat.h"
#include "export.h"

/*
 * stub function to trigger locf and interpolate in gapfill node
 */
TS_FUNCTION_INFO_V1(ts_gapfill_marker);
Datum
ts_gapfill_marker(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->gapfill_marker(fcinfo));
}

#define GAPFILL_TIMEBUCKET_WRAPPER(datatype)                                                       \
	TS_FUNCTION_INFO_V1(ts_gapfill_##datatype##_bucket);                                           \
	Datum ts_gapfill_##datatype##_bucket(PG_FUNCTION_ARGS)                      \
	{                                                                                              \
		return ts_cm_functions->gapfill_##datatype##_time_bucket(fcinfo);                          \
	}

GAPFILL_TIMEBUCKET_WRAPPER(int16);
GAPFILL_TIMEBUCKET_WRAPPER(int32);
GAPFILL_TIMEBUCKET_WRAPPER(int64);
GAPFILL_TIMEBUCKET_WRAPPER(date);
GAPFILL_TIMEBUCKET_WRAPPER(timestamp);
GAPFILL_TIMEBUCKET_WRAPPER(timestamptz);
