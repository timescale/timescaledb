/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "bgw_policy/compress_chunks.h"
#include <postgres.h>

#include "policy.h"
#include "bgw_policy/reorder.h"
#include "bgw_policy/drop_chunks.h"
#include "bgw_policy/compress_chunks.h"
#include "bgw/job.h"

void
ts_bgw_policy_delete_by_hypertable_id(int32 hypertable_id)
{
	/*
	 * Because this is used in a cascaded delete from hypertable deletion, we
	 * also need to delete the job. This means we don't actually call the
	 * delete on the individual policy, but call the bgw_job delete function.
	 */
	void *policy = ts_bgw_policy_reorder_find_by_hypertable(hypertable_id);

	if (policy)
		ts_bgw_job_delete_by_id(((BgwPolicyReorder *) policy)->fd.job_id);

	policy = ts_bgw_policy_drop_chunks_find_by_hypertable(hypertable_id);

	if (policy)
		ts_bgw_job_delete_by_id(((BgwPolicyDropChunks *) policy)->job_id);

	policy = ts_bgw_policy_compress_chunks_find_by_hypertable(hypertable_id);

	if (policy)
		ts_bgw_job_delete_by_id(((BgwPolicyCompressChunks *) policy)->fd.job_id);
}

/* This function does NOT cascade deletes to the bgw_job table. */
ScanTupleResult
ts_bgw_policy_delete_row_only_tuple_found(TupleInfo *ti, void *const data)
{
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete(ti->scanrel, ti->tuple);
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}
