/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <fmgr.h>

#include "license_guc.h"
#include "cross_module_fn.h"
#include "compat.h"
#include "export.h"

/*
 * stub function to trigger locf and interpolate in gapfill node
 * the calls to these functions will be removed from the final plan
 * for a valid gapfill query. This function will only be called if
 * no timescale license is available or when they are used outside of
 * a valid gapfill query.
 */
TS_FUNCTION_INFO_V1(ts_gapfill_marker);
Datum
ts_gapfill_marker(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(ts_cm_functions->gapfill_marker(fcinfo));
}

#define GAPFILL_TIMEBUCKET_WRAPPER(datatype) \
TS_FUNCTION_INFO_V1(ts_gapfill_ ## datatype ## _bucket); \
Datum \
ts_gapfill_ ## datatype ## _bucket(PG_FUNCTION_ARGS) \
{ \
	return ts_cm_functions->gapfill_ ## datatype ## _time_bucket(fcinfo); \
}

GAPFILL_TIMEBUCKET_WRAPPER(int16);
GAPFILL_TIMEBUCKET_WRAPPER(int32);
GAPFILL_TIMEBUCKET_WRAPPER(int64);
GAPFILL_TIMEBUCKET_WRAPPER(date);
GAPFILL_TIMEBUCKET_WRAPPER(timestamp);
GAPFILL_TIMEBUCKET_WRAPPER(timestamptz);
