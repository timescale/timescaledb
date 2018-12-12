/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */

#include <postgres.h>
#include <funcapi.h>
#include <utils/timestamp.h>

#include "bgw_policy/recluster.h"
#include "bgw_policy/drop_chunks.h"
#include "errors.h"
#include "job.h"

#define ALTER_JOB_SCHEDULE_NUM_COLS	5

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

Datum
bgw_policy_alter_policy_schedule(PG_FUNCTION_ARGS)
{
	BgwJob	   *job;
	TupleDesc	tupdesc;
	Datum		values[ALTER_JOB_SCHEDULE_NUM_COLS];
	bool		nulls[ALTER_JOB_SCHEDULE_NUM_COLS] = {false};
	HeapTuple	tuple;

	int			job_id = PG_GETARG_INT32(0);
	bool		if_exists = PG_GETARG_BOOL(5);

	/* First get the job */
	job = ts_bgw_job_find(job_id, CurrentMemoryContext, false);

	if (!job)
	{
		if (if_exists)
		{
			ereport(NOTICE, (errmsg("cannot alter policy schedule, policy #%d not found, skipping", job_id)));
			PG_RETURN_NULL();
		}
		else
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("cannot alter policy schedule, policy #%d not found", job_id)));
	}

	if (!PG_ARGISNULL(1))
		job->fd.schedule_interval = *PG_GETARG_INTERVAL_P(1);
	if (!PG_ARGISNULL(2))
		job->fd.max_runtime = *PG_GETARG_INTERVAL_P(2);
	if (!PG_ARGISNULL(3))
		job->fd.max_retries = PG_GETARG_INT32(3);
	if (!PG_ARGISNULL(4))
		job->fd.retry_period = *PG_GETARG_INTERVAL_P(4);

	ts_bgw_job_update_by_id(job_id, job);

	/* Now look up the job and return it */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);
	values[0] = Int32GetDatum(job->fd.id);
	values[1] = IntervalPGetDatum(&job->fd.schedule_interval);
	values[2] = IntervalPGetDatum(&job->fd.max_runtime);
	values[3] = Int32GetDatum(job->fd.max_retries);
	values[4] = IntervalPGetDatum(&job->fd.retry_period);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	return HeapTupleGetDatum(tuple);
}
