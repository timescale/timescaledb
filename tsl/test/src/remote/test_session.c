/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>

#include "export.h"
#include "dist_util.h"

TS_FUNCTION_INFO_V1(ts_test_is_frontend_session);

Datum
ts_test_is_frontend_session(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(dist_util_is_frontend_session());
}
