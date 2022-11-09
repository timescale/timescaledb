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

static int
osm_insert_hook_mock(Oid ht_oid, int64 range_start, int64 range_end)
{
	/* always return true */
	return 1;
}

/*
 * Dummy function to mock OSM_INSERT hook called at chunk creation for tiered data
 */
TS_FUNCTION_INFO_V1(ts_setup_osm_hook);
Datum
ts_setup_osm_hook(PG_FUNCTION_ARGS)
{
	typedef int (*MOCK_OSM_INSERT_HOOK)(Oid, int64, int64);
	MOCK_OSM_INSERT_HOOK *var =
		(MOCK_OSM_INSERT_HOOK *) find_rendezvous_variable("osm_chunk_insert_check_hook");
	*var = osm_insert_hook_mock;
	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_undo_osm_hook);
Datum
ts_undo_osm_hook(PG_FUNCTION_ARGS)
{
	typedef int (*MOCK_OSM_INSERT_HOOK)(Oid, int64, int64);
	MOCK_OSM_INSERT_HOOK *var =
		(MOCK_OSM_INSERT_HOOK *) find_rendezvous_variable("osm_chunk_insert_check_hook");
	*var = NULL;
	PG_RETURN_NULL();
}
