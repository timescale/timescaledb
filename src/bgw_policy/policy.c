/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <utils/builtins.h>

#include "bgw/job.h"
#include "policy.h"

void
ts_bgw_policy_delete_by_hypertable_id(int32 hypertable_id)
{
	List *jobs;
	ListCell *lc;

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

/* This function counts background worker jobs by type. */
BgwJobTypeCount
ts_bgw_job_type_counts()
{
	ListCell *lc;
	List *jobs = ts_bgw_job_get_all(sizeof(BgwJob), CurrentMemoryContext);
	BgwJobTypeCount counts = { 0 };

	foreach (lc, jobs)
	{
		BgwJob *job = lfirst(lc);

		if (namestrcmp(&job->fd.proc_schema, INTERNAL_SCHEMA_NAME) == 0)
		{
			if (namestrcmp(&job->fd.proc_name, "policy_refresh_continuous_aggregate") == 0)
				counts.policy_cagg++;
			else if (namestrcmp(&job->fd.proc_name, "policy_compression") == 0)
				counts.policy_compression++;
			else if (namestrcmp(&job->fd.proc_name, "policy_reorder") == 0)
				counts.policy_reorder++;
			else if (namestrcmp(&job->fd.proc_name, "policy_retention") == 0)
				counts.policy_retention++;
			else if (namestrcmp(&job->fd.proc_name, "policy_telemetry") == 0)
				counts.policy_telemetry++;
			else
				Assert(false);
		}
		else
		{
			counts.user_defined_action++;
		}
	}

	return counts;
}
