/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <utils/timestamp.h>
#include <access/htup_details.h>

#include "bgw/job.h"
#include "export.h"
#include "bgw_policy/chunk_stats.h"

TS_FUNCTION_INFO_V1(ts_test_chunk_stats_insert);
TS_FUNCTION_INFO_V1(ts_test_bgw_job_delete_by_id);

Datum
ts_test_chunk_stats_insert(PG_FUNCTION_ARGS)
{
	int32 job_id = PG_GETARG_INT32(0);
	int32 chunk_id = PG_GETARG_INT32(1);
	int32 num_times_run = PG_GETARG_INT32(2);
	TimestampTz last_time_run = PG_ARGISNULL(3) ? 0 : PG_GETARG_TIMESTAMPTZ(3);

	BgwPolicyChunkStats stat = { .fd = {
									 .job_id = job_id,
									 .chunk_id = chunk_id,
									 .num_times_job_run = num_times_run,
									 .last_time_job_run = last_time_run,
								 } };

	ts_bgw_policy_chunk_stats_insert(&stat);

	PG_RETURN_NULL();
}

Datum
ts_test_bgw_job_delete_by_id(PG_FUNCTION_ARGS)
{
	ts_bgw_job_delete_by_id(PG_GETARG_INT32(0));
	PG_RETURN_NULL();
}
