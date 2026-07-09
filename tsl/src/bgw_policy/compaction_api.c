/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>

#include <compat/compat.h>
#include <hypertable_cache.h>
#include <jsonb_utils.h>

#include "bgw/job.h"
#include "bgw/job_stat.h"
#include "bgw/timer.h"
#include "bgw_policy/compaction_api.h"
#include "bgw_policy/job_api.h"
#include "bgw_policy/policies_v2.h"
#include "bgw_policy/policy_config.h"
#include "guc.h"
#include "hypertable.h"
#include "utils.h"

/* Compaction jobs run frequently, so default to a short schedule interval */
#define DEFAULT_SCHEDULE_INTERVAL                                                                  \
	{                                                                                              \
		.time = 5 * USECS_PER_MINUTE                                                               \
	}

/* Default max runtime for a compaction job is unlimited */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("0"), InvalidOid, -1))

/* Default retry period for compaction jobs is 5 minutes */
#define DEFAULT_RETRY_PERIOD                                                                       \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("5 min"), InvalidOid, -1))

Datum
policy_compaction_check(PG_FUNCTION_ARGS)
{
	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("config must not be NULL")));
	}

	Jsonb *config = PG_GETARG_JSONB_P(0);
	int32 htid = policy_config_get_hypertable_id(config);
	Hypertable *ht = ts_hypertable_get_by_id(htid);

	if (!ht)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("configuration hypertable id %d not found", htid)));
	}

	bool found;
	int32 max_chunks = ts_jsonb_get_int32_field(config, POL_COMPACTION_CONF_KEY_MAX_CHUNKS, &found);
	if (found && max_chunks < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must be greater than or equal to 0",
						POL_COMPACTION_CONF_KEY_MAX_CHUNKS)));
	}

	/* Reading the field parses it, raising on a malformed interval */
	Interval *inactive_for =
		ts_jsonb_get_interval_field(config, POL_COMPACTION_CONF_KEY_INACTIVE_FOR);
	if (inactive_for != NULL)
	{
		Interval zero = { 0 };
		if (DatumGetInt32(DirectFunctionCall2(interval_cmp,
											  IntervalPGetDatum(inactive_for),
											  IntervalPGetDatum(&zero))) < 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("%s must not be negative", POL_COMPACTION_CONF_KEY_INACTIVE_FOR)));
		}
	}

	PG_RETURN_VOID();
}

Datum
policy_compaction_add(PG_FUNCTION_ARGS)
{
	/* behave like a strict function for the required arguments */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		PG_RETURN_NULL();
	}

	NameData application_name;
	NameData proc_name, proc_schema, check_name, check_schema, owner;
	int32 job_id;
	Interval schedule_interval = DEFAULT_SCHEDULE_INTERVAL;
	Oid ht_oid = PG_GETARG_OID(0);
	bool if_not_exists = PG_GETARG_BOOL(1);
	bool user_defined_schedule_interval = !PG_ARGISNULL(2);
	int32 max_chunks = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT32(5);
	Interval *inactive_for = PG_ARGISNULL(6) ? NULL : PG_GETARG_INTERVAL_P(6);
	Cache *hcache;
	Hypertable *ht;
	int32 hypertable_id;
	Oid owner_id;
	List *jobs;
	TimestampTz initial_start = PG_ARGISNULL(3) ? DT_NOBEGIN : PG_GETARG_TIMESTAMPTZ(3);
	bool fixed_schedule = !PG_ARGISNULL(3);
	text *timezone = PG_ARGISNULL(4) ? NULL : PG_GETARG_TEXT_PP(4);
	char *valid_timezone = NULL;

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (user_defined_schedule_interval)
	{
		schedule_interval = *PG_GETARG_INTERVAL_P(2);
	}

	if (timezone != NULL)
	{
		valid_timezone = ts_bgw_job_validate_timezone(PG_GETARG_DATUM(4));
	}

	if (max_chunks < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_chunks must be greater than or equal to 0")));
	}

	ht = ts_hypertable_cache_get_cache_and_entry(ht_oid, CACHE_FLAG_NONE, &hcache);
	Assert(ht != NULL);
	hypertable_id = ht->fd.id;

	owner_id = ts_hypertable_permissions_check(ht_oid, GetUserId());

	if (!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("compaction policy requires compression to be enabled on hypertable \"%s\"",
						get_rel_name(ht_oid)),
				 errhint("Enable compression before adding a compaction policy.")));
	}

	ts_bgw_job_validate_job_owner(owner_id);

	/* Make sure that an existing compaction policy doesn't exist on this hypertable */
	jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_COMPACTION_PROC_NAME,
													 FUNCTIONS_SCHEMA_NAME,
													 hypertable_id);

	ts_cache_release(&hcache);

	if (jobs != NIL)
	{
		Assert(list_length(jobs) == 1);

		if (!if_not_exists)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("compaction policy already exists for hypertable \"%s\"",
							get_rel_name(ht_oid))));
		}

		ereport(NOTICE,
				(errmsg("compaction policy already exists on hypertable \"%s\", skipping",
						get_rel_name(ht_oid))));
		PG_RETURN_INT32(-1);
	}

	if (fixed_schedule)
	{
		ts_bgw_job_validate_schedule_interval(&schedule_interval);
		if (TIMESTAMP_NOT_FINITE(initial_start))
		{
			initial_start = ts_timer_get_current_timestamp();
		}
	}

	/* Next, insert a new job into jobs table */
	namestrcpy(&application_name, "Compaction Policy");
	namestrcpy(&proc_name, POLICY_COMPACTION_PROC_NAME);
	namestrcpy(&proc_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&check_name, POLICY_COMPACTION_CHECK_NAME);
	namestrcpy(&check_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&owner, GetUserNameFromId(owner_id, false));

	JsonbInState parse_state = { 0 };

	pushJsonbValueCompat(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_int32(&parse_state, POLICY_CONFIG_KEY_HYPERTABLE_ID, hypertable_id);
	if (max_chunks > 0)
	{
		ts_jsonb_add_int32(&parse_state, POL_COMPACTION_CONF_KEY_MAX_CHUNKS, max_chunks);
	}
	if (inactive_for != NULL)
	{
		ts_jsonb_add_interval(&parse_state, POL_COMPACTION_CONF_KEY_INACTIVE_FOR, inactive_for);
	}
	pushJsonbValueCompat(&parse_state, WJB_END_OBJECT, NULL);
	JsonbValue *result = parse_state.result;
	Jsonb *config = JsonbValueToJsonb(result);

	job_id = ts_bgw_job_insert_relation(&application_name,
										&schedule_interval,
										DEFAULT_MAX_RUNTIME,
										JOB_RETRY_UNLIMITED,
										DEFAULT_RETRY_PERIOD,
										&proc_schema,
										&proc_name,
										&check_schema,
										&check_name,
										owner_id,
										true,
										fixed_schedule,
										hypertable_id,
										config,
										initial_start,
										valid_timezone);

	if (!TIMESTAMP_NOT_FINITE(initial_start))
	{
		ts_bgw_job_stat_upsert_next_start(job_id, initial_start);
	}

	PG_RETURN_INT32(job_id);
}

Datum
policy_compaction_remove(PG_FUNCTION_ARGS)
{
	Oid hypertable_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);
	Hypertable *ht;
	Cache *hcache;

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	ht = ts_hypertable_cache_get_cache_and_entry(hypertable_oid, CACHE_FLAG_NONE, &hcache);
	int32 ht_id = ht->fd.id;
	ts_cache_release(&hcache);

	ts_hypertable_permissions_check(hypertable_oid, GetUserId());

	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_COMPACTION_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   ht_id);

	if (jobs == NIL)
	{
		if (!if_exists)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("compaction policy not found for hypertable \"%s\"",
							get_rel_name(hypertable_oid))));
		}

		ereport(NOTICE,
				(errmsg("compaction policy not found for hypertable \"%s\", skipping",
						get_rel_name(hypertable_oid))));
		PG_RETURN_NULL();
	}
	Assert(list_length(jobs) == 1);
	BgwJob *job = linitial(jobs);

	ts_bgw_job_delete_by_id(job->fd.id);

	PG_RETURN_NULL();
}
