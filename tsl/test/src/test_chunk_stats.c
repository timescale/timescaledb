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
#include "osm_callbacks.h"

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

typedef int (*chunk_insert_check_hook_type)(Oid, int64, int64);
typedef void (*hypertable_drop_hook_type)(const char *, const char *);

static int
osm_insert_hook_mock(Oid ht_oid, int64 range_start, int64 range_end)
{
	/* always return true */
	elog(NOTICE, "chunk_insert_check_hook");
	return 1;
}

static void
osm_ht_drop_hook_mock(const char *schema_name, const char *table_name)
{
	elog(NOTICE, "hypertable_drop_hook");
}

OsmCallbacks fake_osm_callbacks = { .chunk_insert_check_hook = osm_insert_hook_mock,
									.hypertable_drop_hook = osm_ht_drop_hook_mock };

/*
 * Dummy function to mock OSM_INSERT hook called at chunk creation for tiered data
 */
TS_FUNCTION_INFO_V1(ts_setup_osm_hook);
Datum
ts_setup_osm_hook(PG_FUNCTION_ARGS)
{
	OsmCallbacks **ptr = (OsmCallbacks **) find_rendezvous_variable("osm_callbacks");
	*ptr = &fake_osm_callbacks;

	PG_RETURN_NULL();
}

TS_FUNCTION_INFO_V1(ts_undo_osm_hook);
Datum
ts_undo_osm_hook(PG_FUNCTION_ARGS)
{
	OsmCallbacks **ptr = (OsmCallbacks **) find_rendezvous_variable("osm_callbacks");
	*ptr = NULL;

	PG_RETURN_NULL();
}
