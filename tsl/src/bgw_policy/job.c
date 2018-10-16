/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */

#include <postgres.h>

#include "bgw_policy/recluster.h"
#include "bgw_policy/drop_chunks.h"
#include "errors.h"
#include "job.h"

bool
tsl_bgw_policy_job_execute(BgwJob *job)
{
	switch (job->bgw_type)
	{
		case JOB_TYPE_RECLUSTER:
			{
				/* Get the arguments from the recluster_policy table */
				BgwPolicyRecluster *args = ts_bgw_policy_recluster_find_by_job(job->fd.id);

				if (args == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_TS_INTERNAL_ERROR),
							 errmsg("could not run recluster policy #%d because no args in policy table",
									job->fd.id)));

				/* TODO: Call the recluster_main function */
				/* Call it on a chunk in the selected hypertable */
				/* cluster_rel( */
				elog(WARNING, "Hi, supposed to run a recluster job...");
				return true;
			}
		case JOB_TYPE_DROP_CHUNKS:
			{
				/* Get the arguments from the drop_chunks_policy table */
				BgwPolicyDropChunks *args = ts_bgw_policy_drop_chunks_find_by_job(job->fd.id);

				if (args == NULL)
					ereport(ERROR,
							(errcode(ERRCODE_TS_INTERNAL_ERROR),
							 errmsg("could not run drop_chunks policy #%d because no args in policy table",
									job->fd.id)));

				elog(WARNING, "Hi, supposed to run a drop_chunks job...");

				/*
				 * drop_chunks_main_wrapper(args->fd.hypertable_id,
				 * IntervalPGetDatum(&args->fd.older_than), args->fd.cascade);
				 */
				return true;
			}
		default:
			elog(ERROR, "scheduler tried to run an invalid enterprise job type: \"%s\"", NameStr(job->fd.job_type));
	}
}
