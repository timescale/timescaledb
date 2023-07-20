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

#include <parser/parse_func.h>
#include <parser/parser.h>

#include <bgw/job.h>
#include <bgw/job_stat.h>

#include "job.h"
#include "job_api.h"
#include "hypertable_cache.h"
#include "bgw/timer.h"
#include "debug_assert.h"

/* Default max runtime for a custom job is unlimited for now */
#define DEFAULT_MAX_RUNTIME 0

/* Right now, there is an infinite number of retries for custom jobs */
#define DEFAULT_MAX_RETRIES (-1)
/* Default retry period for reorder_jobs is currently 5 minutes */
#define DEFAULT_RETRY_PERIOD (5 * USECS_PER_MINUTE)

#define ALTER_JOB_NUM_COLS 13

/*
 * This function ensures that the check function has the required signature
 * @param check A valid Oid
 */
static inline void
validate_check_signature(Oid check)
{
	Oid proc = InvalidOid;
	ObjectWithArgs *object;
	NameData check_name = { .data = { 0 } };
	NameData check_schema = { .data = { 0 } };

	namestrcpy(&check_schema, get_namespace_name(get_func_namespace(check)));
	namestrcpy(&check_name, get_func_name(check));

	object = makeNode(ObjectWithArgs);
	object->objname =
		list_make2(makeString(NameStr(check_schema)), makeString(NameStr(check_name)));
	object->objargs = list_make1(SystemTypeName("jsonb"));
	proc = LookupFuncWithArgs(OBJECT_ROUTINE, object, true);

	if (!OidIsValid(proc))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("function or procedure %s.%s(config jsonb) not found",
						NameStr(check_schema),
						NameStr(check_name)),
				 errhint("The check function's signature must be (config jsonb).")));
}

/*
 * CREATE FUNCTION add_job(
 * 0 proc REGPROC,
 * 1 schedule_interval INTERVAL,
 * 2 config JSONB DEFAULT NULL,
 * 3 initial_start TIMESTAMPTZ DEFAULT NULL,
 * 4 scheduled BOOL DEFAULT true
 * 5 check_config REGPROC DEFAULT NULL
 * 6 fixed_schedule BOOL DEFAULT TRUE
 * 7 timezone TEXT DEFAULT NULL
 * ) RETURNS INTEGER
 */
Datum
job_add(PG_FUNCTION_ARGS)
{
	NameData application_name;
	NameData proc_name;
	NameData proc_schema;
	NameData check_name = { .data = { 0 } };
	NameData check_schema = { .data = { 0 } };
	Interval max_runtime = { .time = DEFAULT_MAX_RUNTIME };
	Interval retry_period = { .time = DEFAULT_RETRY_PERIOD };
	int32 job_id;
	char *func_name = NULL;
	char *check_name_str = NULL;
	char *valid_timezone = NULL;
	TimestampTz initial_start = PG_ARGISNULL(3) ? DT_NOBEGIN : PG_GETARG_TIMESTAMPTZ(3);

	Oid owner = GetUserId();
	Oid proc = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Interval *schedule_interval = PG_ARGISNULL(1) ? NULL : PG_GETARG_INTERVAL_P(1);
	Jsonb *config = PG_ARGISNULL(2) ? NULL : PG_GETARG_JSONB_P(2);
	bool scheduled = PG_ARGISNULL(4) ? true : PG_GETARG_BOOL(4);
	Oid check = PG_ARGISNULL(5) ? InvalidOid : PG_GETARG_OID(5);
	bool fixed_schedule = PG_ARGISNULL(6) ? true : PG_GETARG_BOOL(6);
	text *timezone = PG_ARGISNULL(7) ? NULL : PG_GETARG_TEXT_PP(7);
	/* verify it's a valid timezone */
	if (timezone != NULL)
		valid_timezone = ts_bgw_job_validate_timezone(PG_GETARG_DATUM(7));

	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("function or procedure cannot be NULL")));

	if (NULL == schedule_interval)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("schedule interval cannot be NULL")));

	/* for fixed schedules, we use time_bucket in the calculation of next_start
	 Therefore, we cannot allow schedule intervals containing both month and day components */
	if (fixed_schedule)
		ts_bgw_job_validate_schedule_interval(schedule_interval);

	func_name = get_func_name(proc);
	if (func_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("function or procedure with OID %u does not exist", proc)));

	if (object_aclcheck(ProcedureRelationId, proc, owner, ACL_EXECUTE) != ACLCHECK_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied for function \"%s\"", func_name),
				 errhint("Job owner must have EXECUTE privilege on the function.")));

	if (OidIsValid(check))
	{
		check_name_str = get_func_name(check);
		if (check_name_str == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("function with OID %d does not exist", check)));

		if (object_aclcheck(ProcedureRelationId, check, owner, ACL_EXECUTE) != ACLCHECK_OK)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied for function \"%s\"", check_name_str),
					 errhint("Job owner must have EXECUTE privilege on the function.")));

		namestrcpy(&check_schema, get_namespace_name(get_func_namespace(check)));
		namestrcpy(&check_name, check_name_str);
	}

	/* if no initial_start was provided for a fixed schedule, use the current time */
	if (fixed_schedule && TIMESTAMP_NOT_FINITE(initial_start))
	{
		initial_start = ts_timer_get_current_timestamp();
		elog(DEBUG1,
			 "Using current time [%s] as initial start",
			 DatumGetCString(
				 DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(initial_start))));
	}

	/* Verify that the owner can create a background worker */
	ts_bgw_job_validate_job_owner(owner);

	/* Next, insert a new job into jobs table */
	namestrcpy(&application_name, "User-Defined Action");
	namestrcpy(&proc_schema, get_namespace_name(get_func_namespace(proc)));
	namestrcpy(&proc_name, func_name);

	/* The check exists but may not have the expected signature: (config jsonb) */
	if (OidIsValid(check))
		validate_check_signature(check);

	ts_bgw_job_run_config_check(check, 0, config);

	job_id = ts_bgw_job_insert_relation(&application_name,
										schedule_interval,
										&max_runtime,
										DEFAULT_MAX_RETRIES,
										&retry_period,
										&proc_schema,
										&proc_name,
										&check_schema,
										&check_name,
										owner,
										scheduled,
										fixed_schedule,
										0,
										config,
										initial_start,
										valid_timezone);

	if (!TIMESTAMP_NOT_FINITE(initial_start))
		ts_bgw_job_stat_upsert_next_start(job_id, initial_start);

	PG_RETURN_INT32(job_id);
}

static BgwJob *
find_job(int32 job_id, bool null_job_id, bool missing_ok)
{
	BgwJob *job;
	bool got_lock;
	LOCKTAG tag;

	if (null_job_id && !missing_ok)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("job ID cannot be NULL")));

	/* The job table manipulation functions in job.c use an advisory lock
	 * rather than a row lock to prevent the job from being modified while
	 * running. Since we use this function to find a job for either running
	 * manually or for altering it, we need to grab that advisory lock here as
	 * well. */
	got_lock = ts_lock_job_id(job_id,
							  RowShareLock,
							  /* session_lock */ false,
							  &tag,
							  /* block */ true);
	Ensure(got_lock, "could not get lock on job id %d", job_id);

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

	TS_PREVENT_FUNC_IF_READ_ONLY();

	job = find_job(job_id, PG_ARGISNULL(0), false);

	if (!has_privs_of_role(GetUserId(), job->fd.owner))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("insufficient permissions to delete job for user \"%s\"",
						GetUserNameFromId(job->fd.owner, false))));

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

	ts_bgw_job_permission_check(job, "run");

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
 * 9    check_config REGPROC = NULL
 * 10   fixed_schedule BOOL = NULL,
 * 11   initial_start TIMESTAMPTZ = NULL
 * 12   timezone TEXT = NULL
 * ) RETURNS TABLE (
 *      job_id INTEGER,
 *      schedule_interval INTERVAL,
 *      max_runtime INTERVAL,
 *      max_retries INTEGER,
 *      retry_period INTERVAL,
 *      scheduled BOOL,
 *      config JSONB,
 *      next_start TIMESTAMPTZ
 *      check_config TEXT
 *      fixed_schedule BOOL
 *      initial_start TIMESTAMPTZ
 *      timezone TEXT
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
	NameData check_name = { .data = { 0 } };
	NameData check_schema = { .data = { 0 } };
	Oid check = PG_ARGISNULL(9) ? InvalidOid : PG_GETARG_OID(9);
	char *check_name_str = NULL;
	/* Added space for period and NULL */
	char schema_qualified_check_name[2 * NAMEDATALEN + 2] = { 0 };
	bool unregister_check = (!PG_ARGISNULL(9) && !OidIsValid(check));
	TimestampTz initial_start = PG_ARGISNULL(11) ? DT_NOBEGIN : PG_GETARG_TIMESTAMPTZ(11);
	text *timezone = PG_ARGISNULL(12) ? NULL : PG_GETARG_TEXT_PP(12);
	char *valid_timezone = NULL;
	/* verify it's a valid timezone */
	if (timezone != NULL)
		valid_timezone = ts_bgw_job_validate_timezone(PG_GETARG_DATUM(12));

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

	ts_bgw_job_permission_check(job, "alter");

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

	if (!PG_ARGISNULL(9))
	{
		if (OidIsValid(check))
		{
			check_name_str = get_func_name(check);
			if (check_name_str == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("function with OID %d does not exist", check)));

			if (object_aclcheck(ProcedureRelationId, check, GetUserId(), ACL_EXECUTE) !=
				ACLCHECK_OK)
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied for function \"%s\"", check_name_str),
						 errhint("Job owner must have EXECUTE privilege on the function.")));

			namestrcpy(&check_schema, get_namespace_name(get_func_namespace(check)));
			namestrcpy(&check_name, check_name_str);

			/* The check exists but may not have the expected signature: (config jsonb) */
			validate_check_signature(check);

			namestrcpy(&job->fd.check_schema, NameStr(check_schema));
			namestrcpy(&job->fd.check_name, NameStr(check_name));
			snprintf(schema_qualified_check_name,
					 sizeof(schema_qualified_check_name) / sizeof(schema_qualified_check_name[0]),
					 "%s.%s",
					 NameStr(check_schema),
					 check_name_str);
		}
	}
	else
		snprintf(schema_qualified_check_name,
				 sizeof(schema_qualified_check_name) / sizeof(schema_qualified_check_name[0]),
				 "%s.%s",
				 NameStr(job->fd.check_schema),
				 NameStr(job->fd.check_name));

	if (unregister_check)
	{
		NameData empty_namedata = { .data = { 0 } };
		namestrcpy(&job->fd.check_schema, NameStr(empty_namedata));
		namestrcpy(&job->fd.check_name, NameStr(empty_namedata));
	}

	if (!PG_ARGISNULL(10))
	{
		bool fixed_schedule = PG_GETARG_BOOL(10);
		/*
		 * initial_start is a required argument for fixed schedules
		 * so we use the current timestamp if it's not provided
		 */
		if (fixed_schedule)
		{
			if (TIMESTAMP_NOT_FINITE(initial_start))
			{
				initial_start = ts_timer_get_current_timestamp();
				elog(NOTICE,
					 "Using current time [%s] as initial start for job %d",
					 DatumGetCString(
						 DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(initial_start))),
					 job->fd.id);
				job->fd.initial_start = initial_start;
			}
		}
		job->fd.fixed_schedule = fixed_schedule;
	}
	if (!PG_ARGISNULL(11))
	{
		/* user provided +- infinity as initial_start, this is not acceptable */
		if (TIMESTAMP_NOT_FINITE(initial_start))
		{
			initial_start = ts_timer_get_current_timestamp();
			elog(NOTICE,
				 "Using current time [%s] as initial start for job %d",
				 DatumGetCString(
					 DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(initial_start))),
				 job->fd.id);
		}
		job->fd.initial_start = initial_start;
	}

	if (valid_timezone != NULL)
		job->fd.timezone = cstring_to_text(valid_timezone);
	else
		job->fd.timezone = NULL;
	/* it's also possible to alter the fields initial_start and timezone without
	 * specifying fixed_schedule. In that case, update them and also update the
	 * next_start accordingly.
	 * If the job is not on a fixed schedule, then this has no effect on the next_start,
	 * so maybe print a message to the user
	 * that these changes are not really doing anything */

	/* this function will also update the next_start if the schedule interval is changed,
	but I'm not going to rely on this to change stuff */
	ts_bgw_job_update_by_id(job_id, job);
	/* one of the fields below changing necessitates a next_start update */
	if (!PG_ARGISNULL(10) || !TIMESTAMP_NOT_FINITE(initial_start) || (valid_timezone != NULL))
	{
		TimestampTz next_start_calculated;
		if (job->fd.fixed_schedule)
		{
			next_start_calculated =
				ts_get_next_scheduled_execution_slot(job, ts_timer_get_current_timestamp());
			ts_bgw_job_stat_update_next_start(job->fd.id, next_start_calculated, false);
		}
		else
		{
			/* last finish time plus schedule interval */
			BgwJobStat *stat = ts_bgw_job_stat_find(job->fd.id);

			if (stat != NULL)
			{
				next_start_calculated = DatumGetTimestampTz(
					DirectFunctionCall2(timestamptz_pl_interval,
										TimestampTzGetDatum(stat->fd.last_finish),
										IntervalPGetDatum(&job->fd.schedule_interval)));
				/* allow DT_NOBEGIN for next_start here through allow_unset=true in the case that
				 * last_finish is DT_NOBEGIN,
				 * This means the value is counted as unset which is what we want */
				ts_bgw_job_stat_update_next_start(job->fd.id, next_start_calculated, true);
			}
		}
	}

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

	if (unregister_check)
		nulls[8] = true;
	else if (strlen(NameStr(job->fd.check_schema)) > 0)
		values[8] = CStringGetTextDatum(schema_qualified_check_name);
	else
		nulls[8] = true;

	/* values/nulls[9]: fixed_schedule */
	values[9] = job->fd.fixed_schedule;
	/* values/nulls[10]: initial_start */
	if (TIMESTAMP_NOT_FINITE(job->fd.initial_start))
	{
		nulls[10] = true;
	}
	else
		values[10] = TimestampTzGetDatum(job->fd.initial_start);
	/* values/nulls[11]: timezone */
	if (valid_timezone)
		values[11] = CStringGetTextDatum(valid_timezone);
	else
		nulls[11] = true;

	tuple = heap_form_tuple(tupdesc, values, nulls);
	return HeapTupleGetDatum(tuple);
}

static Hypertable *
get_hypertable_from_oid(Cache **hcache, Oid table_oid)
{
	Hypertable *hypertable = NULL;
	hypertable = ts_hypertable_cache_get_cache_and_entry(table_oid, CACHE_FLAG_MISSING_OK, hcache);
	if (!hypertable)
	{
		const char *view_name = get_rel_name(table_oid);

		if (!view_name)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("relation is not a hypertable or continuous aggregate")));
		else
		{
			ContinuousAgg *ca = ts_continuous_agg_find_by_relid(table_oid);

			if (!ca)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("relation \"%s\" is not a hypertable or continuous aggregate",
								view_name)));
			hypertable = ts_hypertable_get_by_id(ca->data.mat_hypertable_id);
		}
	}
	Assert(hypertable != NULL);
	return hypertable;
}

Datum
job_alter_set_hypertable_id(PG_FUNCTION_ARGS)
{
	int32 job_id = PG_GETARG_INT32(0);
	Oid table_oid = PG_GETARG_OID(1);
	Cache *hcache = NULL;
	Hypertable *ht = NULL;

	TS_PREVENT_FUNC_IF_READ_ONLY();
	BgwJob *job = find_job(job_id, PG_ARGISNULL(0), false /* missing_ok */);
	if (job == NULL)
		PG_RETURN_NULL();

	ts_bgw_job_permission_check(job, "alter");

	if (!PG_ARGISNULL(1))
	{
		ht = get_hypertable_from_oid(&hcache, table_oid);
		ts_hypertable_permissions_check(ht->main_table_relid, GetUserId());
	}

	job->fd.hypertable_id = (ht != NULL ? ht->fd.id : 0);
	ts_bgw_job_update_by_id(job_id, job);
	if (hcache)
		ts_cache_release(hcache);
	PG_RETURN_INT32(job_id);
}
