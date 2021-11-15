/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <utils/acl.h>
#include <utils/builtins.h>

#include <bgw/job.h>
#include <bgw/job_stat.h>

#include "job.h"
#include "job_api.h"

/* Default max runtime for a custom job is unlimited for now */
#define DEFAULT_MAX_RUNTIME 0

/* Right now, there is an infinite number of retries for custom jobs */
#define DEFAULT_MAX_RETRIES -1
/* Default retry period for reorder_jobs is currently 5 minutes */
#define DEFAULT_RETRY_PERIOD 5 * USECS_PER_MINUTE

#define ALTER_JOB_NUM_COLS 8

/*
 * CREATE FUNCTION add_job(
 * 0 proc REGPROC,
 * 1 schedule_interval INTERVAL,
 * 2 config JSONB DEFAULT NULL,
 * 3 initial_start TIMESTAMPTZ DEFAULT NULL,
 * 4 scheduled BOOL DEFAULT true
 * ) RETURNS INTEGER
 */
Datum
job_add(PG_FUNCTION_ARGS)
{
	NameData application_name;
	NameData proc_name;
	NameData proc_schema;
	NameData owner_name;
	Interval max_runtime = { .time = DEFAULT_MAX_RUNTIME };
	Interval retry_period = { .time = DEFAULT_RETRY_PERIOD };
	int32 job_id;
	char *func_name = NULL;

	Oid owner = GetUserId();
	Oid proc = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Interval *schedule_interval = PG_ARGISNULL(1) ? NULL : PG_GETARG_INTERVAL_P(1);
	Jsonb *config = PG_ARGISNULL(2) ? NULL : PG_GETARG_JSONB_P(2);
	bool scheduled = PG_ARGISNULL(4) ? true : PG_GETARG_BOOL(4);

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("function or procedure cannot be NULL")));

	if (NULL == schedule_interval)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("schedule interval cannot be NULL")));

	func_name = get_func_name(proc);
	if (func_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("function or procedure with OID %u does not exist", proc)));

	if (pg_proc_aclcheck(proc, owner, ACL_EXECUTE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for function \"%s\"", func_name),
				 errhint("Job owner must have EXECUTE privilege on the function.")));

	/* Verify that the owner can create a background worker */
	ts_bgw_job_validate_job_owner(owner);

	/* Next, insert a new job into jobs table */
	namestrcpy(&application_name, "User-Defined Action");
	namestrcpy(&proc_schema, get_namespace_name(get_func_namespace(proc)));
	namestrcpy(&proc_name, func_name);
	namestrcpy(&owner_name, GetUserNameFromId(owner, false));

	if (config)
		job_config_check(&proc_schema, &proc_name, config);

	job_id = ts_bgw_job_insert_relation(&application_name,
										schedule_interval,
										&max_runtime,
										DEFAULT_MAX_RETRIES,
										&retry_period,
										&proc_schema,
										&proc_name,
										&owner_name,
										scheduled,
										0,
										config);

	if (!PG_ARGISNULL(3))
	{
		TimestampTz initial_start = PG_GETARG_TIMESTAMPTZ(3);
		ts_bgw_job_stat_upsert_next_start(job_id, initial_start);
	}

	PG_RETURN_INT32(job_id);
}

static BgwJob *
find_job(int32 job_id, bool null_job_id, bool missing_ok)
{
	BgwJob *job;

	if (null_job_id && !missing_ok)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("job ID cannot be NULL")));

	job = ts_bgw_job_find(job_id, CurrentMemoryContext, !missing_ok);

	if (NULL == job)
	{
		Assert(missing_ok);
		ereport(NOTICE,
				(errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("job %d not found, skipping", job_id)));
	}

	return job;
}

/*
 * CREATE OR REPLACE FUNCTION delete_job(job_id INTEGER) RETURNS VOID
 */
Datum
job_delete(PG_FUNCTION_ARGS)
{
	int32 job_id = PG_GETARG_INT32(0);
	BgwJob *job;
	Oid owner;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	job = find_job(job_id, PG_ARGISNULL(0), false);
	owner = get_role_oid(NameStr(job->fd.owner), false);

	if (!has_privs_of_role(GetUserId(), owner))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("insufficient permissions to delete job for user \"%s\"",
						NameStr(job->fd.owner))));

	ts_bgw_job_delete_by_id(job_id);

	PG_RETURN_VOID();
}

/*
 * CREATE OR REPLACE PROCEDURE run_job(job_id INTEGER)
 */
Datum
job_run(PG_FUNCTION_ARGS)
{
	int32 job_id = PG_GETARG_INT32(0);
	BgwJob *job = find_job(job_id, PG_ARGISNULL(0), false);

	job_execute(job);

	PG_RETURN_VOID();
}

/*
 * CREATE OR REPLACE FUNCTION alter_job(
 * 0    job_id INTEGER,
 * 1    schedule_interval INTERVAL = NULL,
 * 2    max_runtime INTERVAL = NULL,
 * 3    max_retries INTEGER = NULL,
 * 4    retry_period INTERVAL = NULL,
 * 5    scheduled BOOL = NULL,
 * 6    config JSONB = NULL,
 * 7    next_start TIMESTAMPTZ = NULL
 * 8    if_exists BOOL = FALSE,
 * ) RETURNS TABLE (
 *      job_id INTEGER,
 *      schedule_interval INTERVAL,
 *      max_runtime INTERVAL,
 *      max_retries INTEGER,
 *      retry_period INTERVAL,
 *      scheduled BOOL,
 *      config JSONB,
 *      next_start TIMESTAMPTZ
 * )
 */
Datum
job_alter(PG_FUNCTION_ARGS)
{
	BgwJobStat *stat;
	TupleDesc tupdesc;
	Datum values[ALTER_JOB_NUM_COLS] = { 0 };
	bool nulls[ALTER_JOB_NUM_COLS] = { false };
	HeapTuple tuple;
	TimestampTz next_start;
	int job_id = PG_GETARG_INT32(0);
	bool if_exists = PG_GETARG_BOOL(8);
	BgwJob *job;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	/* check that caller accepts tuple and abort early if that is not the
	 * case */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	job = find_job(job_id, PG_ARGISNULL(0), if_exists);
	if (job == NULL)
		PG_RETURN_NULL();

	ts_bgw_job_permission_check(job);

	if (!PG_ARGISNULL(1))
		job->fd.schedule_interval = *PG_GETARG_INTERVAL_P(1);
	if (!PG_ARGISNULL(2))
		job->fd.max_runtime = *PG_GETARG_INTERVAL_P(2);
	if (!PG_ARGISNULL(3))
		job->fd.max_retries = PG_GETARG_INT32(3);
	if (!PG_ARGISNULL(4))
		job->fd.retry_period = *PG_GETARG_INTERVAL_P(4);
	if (!PG_ARGISNULL(5))
		job->fd.scheduled = PG_GETARG_BOOL(5);
	if (!PG_ARGISNULL(6))
		job->fd.config = PG_GETARG_JSONB_P(6);

	ts_bgw_job_update_by_id(job_id, job);

	if (!PG_ARGISNULL(7))
		ts_bgw_job_stat_upsert_next_start(job_id, PG_GETARG_TIMESTAMPTZ(7));

	stat = ts_bgw_job_stat_find(job_id);
	if (stat != NULL)
		next_start = stat->fd.next_start;
	else
		next_start = DT_NOBEGIN;

	tupdesc = BlessTupleDesc(tupdesc);
	values[0] = Int32GetDatum(job->fd.id);
	values[1] = IntervalPGetDatum(&job->fd.schedule_interval);
	values[2] = IntervalPGetDatum(&job->fd.max_runtime);
	values[3] = Int32GetDatum(job->fd.max_retries);
	values[4] = IntervalPGetDatum(&job->fd.retry_period);
	values[5] = BoolGetDatum(job->fd.scheduled);

	if (job->fd.config == NULL)
		nulls[6] = true;
	else
		values[6] = JsonbPGetDatum(job->fd.config);

	values[7] = TimestampTzGetDatum(next_start);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	return HeapTupleGetDatum(tuple);
}
