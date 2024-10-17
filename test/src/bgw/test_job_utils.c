/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>

#include "bgw/job.h"
#include "test_utils.h"

TS_TEST_FN(ts_test_bgw_job_function_call_string)
{
	int32 job_id = PG_GETARG_INT32(0);
	BgwJob *job = ts_bgw_job_find(job_id, CurrentMemoryContext, true);

	const char *stmt = ts_bgw_job_function_call_string(job);

	PG_RETURN_TEXT_P(cstring_to_text(stmt));
}
