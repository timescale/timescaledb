/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <dist_commands.h>

TS_FUNCTION_INFO_V1(ts_test_direct_function_call);

/*
 * Test that calling distribute_exec via a direct function call works. Covers
 * a bug that was hit when the third argument was transactional=false and the
 * fcinfo didn't have the full metadata from the parsing stage (which is
 * typical when using direct function calls).
 */
Datum
ts_test_direct_function_call(PG_FUNCTION_ARGS)
{
	LOCAL_FCINFO(fcinfo2, 3);
	Datum result;

	/* Fill in FunctionCallInfo similar to DirectFunctionCall, but copy info
	 * from wrapper. */
	InitFunctionCallInfoData(*fcinfo2, NULL, 3, InvalidOid, NULL, NULL);

	fcinfo2->args[0].value = PG_GETARG_DATUM(0);
	fcinfo2->args[0].isnull = PG_ARGISNULL(0);
	fcinfo2->args[1].value = PG_GETARG_DATUM(1);
	fcinfo2->args[1].isnull = PG_ARGISNULL(1);
	fcinfo2->args[2].value = PG_GETARG_DATUM(2);
	fcinfo2->args[2].isnull = PG_ARGISNULL(2);

	result = ts_dist_cmd_exec(fcinfo2);
	Assert(!fcinfo2->isnull);

	return result;
}
