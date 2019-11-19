/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include "export.h"
#include "fdw/scan_exec.h"

TS_FUNCTION_INFO_V1(ts_test_override_pushdown_timestamptz);

Datum
ts_test_override_pushdown_timestamptz(PG_FUNCTION_ARGS)
{
#ifdef TS_DEBUG
	fdw_scan_debug_override_pushdown_timestamp(PG_GETARG_INT64(0));
	PG_RETURN_VOID();
#else
	elog(ERROR, "unable to handle ts_test_is_frontend_session without TS_DEBUG flag set");
	PG_RETURN_VOID();
#endif
}
