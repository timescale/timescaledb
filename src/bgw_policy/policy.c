/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>

#include "bgw/job.h"
#include "bgw_policy/compress_chunks.h"
#include "bgw_policy/drop_chunks.h"
#include "policy.h"

void
ts_bgw_policy_delete_by_hypertable_id(int32 hypertable_id)
{
	List *jobs;
	ListCell *lc;
	/*
	 * Because this is used in a cascaded delete from hypertable deletion, we
	 * also need to delete the job. This means we don't actually call the
	 * delete on the individual policy, but call the bgw_job delete function.
	 */
	void *policy;

	policy = ts_bgw_policy_drop_chunks_find_by_hypertable(hypertable_id);

	if (policy)
		ts_bgw_job_delete_by_id(((BgwPolicyDropChunks *) policy)->job_id);

	policy = ts_bgw_policy_compress_chunks_find_by_hypertable(hypertable_id);

	if (policy)
		ts_bgw_job_delete_by_id(((BgwPolicyCompressChunks *) policy)->fd.job_id);

	jobs = ts_bgw_job_find_by_hypertable_id(hypertable_id);
	foreach (lc, jobs)
	{
		BgwJob *job = lfirst(lc);
		ts_bgw_job_delete_by_id(job->fd.id);
	}
}

/* This function does NOT cascade deletes to the bgw_job table. */
ScanTupleResult
ts_bgw_policy_delete_row_only_tuple_found(TupleInfo *ti, void *const data)
{
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

int32
ts_bgw_policy_reorder_count()
{
	List *jobs = ts_bgw_job_find_by_proc("policy_reorder", INTERNAL_SCHEMA_NAME);

	return list_length(jobs);
}
