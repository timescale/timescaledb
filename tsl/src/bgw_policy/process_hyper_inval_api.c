/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include "process_hyper_inval_api.h"

#include "bgw/job.h"
#include "bgw/job_stat.h"
#include "bgw/timer.h"
#include "bgw_policy/job.h"
#include "bgw_policy/job_api.h"
#include "guc.h"
#include "hypertable.h"
#include "jsonb_utils.h"
#include "policy_config.h"
#include <utils/elog.h>

#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("0"), InvalidOid, -1))

Datum
policy_process_hyper_inval_proc(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() != 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_VOID();

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	policy_process_hyper_inval_execute(PG_GETARG_INT32(0), PG_GETARG_JSONB_P(1));

	PG_RETURN_VOID();
}

Datum
policy_process_hyper_inval_check(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("config must not be NULL")));
	}

	policy_process_hyper_inval_read_and_validate_config(PG_GETARG_JSONB_P(0), NULL);

	PG_RETURN_VOID();
}

static int32
policy_process_hyper_inval_add_internal(Hypertable *ht, bool if_not_exists,
										Interval *schedule_interval, Oid owner_id,
										TimestampTz initial_start, const bool fixed_schedule,
										char *timezone)
{
	NameData application_name, proc_name, proc_schema, check_schema, check_name, owner;

	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_PROCESS_HYPER_INVAL_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   ht->fd.id);
	if (jobs != NIL)
	{
		Assert(list_length(jobs) == 1);
		if (!if_not_exists)
			ereport(ERROR,
					errcode(ERRCODE_DUPLICATE_OBJECT),
					errmsg("move hypertable invalidations policy already exists for \"%s\"",
						   get_rel_name(ht->main_table_relid)));
		else
			ereport(NOTICE,
					errmsg("move hypertable invalidations policy already exists for \"%s\", "
						   "skipping",
						   get_rel_name(ht->main_table_relid)));
		return -1;
	}

	namestrcpy(&application_name, "Move Hypertables Invalidation Policy");
	namestrcpy(&proc_name, POLICY_PROCESS_HYPER_INVAL_PROC_NAME);
	namestrcpy(&proc_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&check_name, POLICY_PROCESS_HYPER_INVAL_CHECK_NAME);
	namestrcpy(&check_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&owner, GetUserNameFromId(owner_id, false));

	JsonbParseState *parse_state = NULL;

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_int32(parse_state, POLICY_CONFIG_KEY_HYPERTABLE_ID, ht->fd.id);

	JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	Jsonb *config = JsonbValueToJsonb(result);

	return ts_bgw_job_insert_relation(&application_name,
									  schedule_interval,
									  DEFAULT_MAX_RUNTIME,
									  JOB_RETRY_UNLIMITED,
									  schedule_interval,
									  &proc_schema,
									  &proc_name,
									  &check_schema,
									  &check_name,
									  owner_id,
									  true,
									  fixed_schedule,
									  ht->fd.id,
									  config,
									  initial_start,
									  timezone);
}

Datum
policy_process_hyper_inval_add(PG_FUNCTION_ARGS)
{
	Oid ht_oid = PG_GETARG_OID(0);
	Interval *schedule_interval = PG_GETARG_INTERVAL_P(1);
	const bool if_not_exists = PG_GETARG_BOOL(2);
	TimestampTz initial_start = PG_ARGISNULL(3) ? DT_NOBEGIN : PG_GETARG_TIMESTAMPTZ(3);
	const bool fixed_schedule = !PG_ARGISNULL(3);
	char *valid_timezone = NULL;

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (fixed_schedule)
	{
		ts_bgw_job_validate_schedule_interval(schedule_interval);
		if (TIMESTAMP_NOT_FINITE(initial_start))
			initial_start = ts_timer_get_current_timestamp();
	}

	if (!PG_ARGISNULL(4))
		valid_timezone = ts_bgw_job_validate_timezone(PG_GETARG_DATUM(4));

	Cache *hcache;
	Hypertable *ht = ts_hypertable_cache_get_cache_and_entry(ht_oid, CACHE_FLAG_NONE, &hcache);

	/* Check that we have a continuous aggregate attached */
	List *caggs = ts_continuous_aggs_find_by_raw_table_id(ht->fd.id);
	if (list_length(caggs) == 0)
		ereport(ERROR,
				errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				errmsg("\"%s\" does not have an associated continuous aggregate",
					   get_rel_name(ht_oid)));

	Oid owner_id = ts_hypertable_permissions_check(ht_oid, GetUserId());
	ts_bgw_job_validate_job_owner(owner_id);

	int32 job_id = policy_process_hyper_inval_add_internal(ht,
														   if_not_exists,
														   schedule_interval,
														   owner_id,
														   initial_start,
														   fixed_schedule,
														   valid_timezone);

	ts_cache_release(&hcache);

	if (!TIMESTAMP_NOT_FINITE(initial_start))
		ts_bgw_job_stat_upsert_next_start(job_id, initial_start);

	PG_RETURN_INT32(job_id);
}

static void
policy_process_hyper_inval_remove_internal(Oid ht_oid, bool if_exists)
{
	Cache *hcache;
	Hypertable *ht = ts_hypertable_cache_get_cache_and_entry(ht_oid, CACHE_FLAG_NONE, &hcache);
	if (!ht)
		ereport(ERROR,
				errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg("\"%s\" is not a hypertable", get_rel_name(ht_oid)));

	/* Check permissions */
	Oid owner_id = ts_hypertable_permissions_check(ht_oid, GetUserId());
	ts_bgw_job_validate_job_owner(owner_id);

	/* Find jobs */
	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_PROCESS_HYPER_INVAL_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   ht->fd.id);
	if (!jobs && !if_exists)
		ereport(ERROR,
				errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("move invalidations policy for \"%s\" not found", get_rel_name(ht_oid)));

	if (!jobs && if_exists)
	{
		ereport(NOTICE,
				errmsg("move invalidations policy for \"%s\" not found, skipping",
					   get_rel_name(ht_oid)));
		ts_cache_release(&hcache);
		return;
	}

	Assert(list_length(jobs) == 1);
	BgwJob *job = linitial(jobs);

	/* Delete all jobs, not just the first one? */
	ts_bgw_job_delete_by_id(job->fd.id);

	ts_cache_release(&hcache);
}

Datum
policy_process_hyper_inval_remove(PG_FUNCTION_ARGS)
{
	ts_feature_flag_check(FEATURE_POLICY);
	policy_process_hyper_inval_remove_internal(PG_GETARG_OID(0), PG_GETARG_BOOL(1));
	PG_RETURN_VOID();
}
