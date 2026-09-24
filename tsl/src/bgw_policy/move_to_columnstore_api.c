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
#include "bgw_policy/compression_api.h"
#include "bgw_policy/job_api.h"
#include "bgw_policy/move_to_columnstore_api.h"
#include "bgw_policy/policies_v2.h"
#include "bgw_policy/policy_config.h"
#include "guc.h"
#include "hypertable.h"
#include "utils.h"

/* Default max runtime for a move job is unlimited */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("0"), InvalidOid, -1))

/* Default retry period for move jobs is 5 minutes */
#define DEFAULT_RETRY_PERIOD                                                                       \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("5 min"), InvalidOid, -1))

Datum
policy_move_to_columnstore_check(PG_FUNCTION_ARGS)
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
	int32 max_chunks = ts_jsonb_get_int32_field(config,
												POL_MOVE_TO_COLUMNSTORE_CONF_KEY_MAXCHUNKS_TO_MOVE,
												&found);
	if (found && max_chunks < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must be greater than or equal to 0",
						POL_MOVE_TO_COLUMNSTORE_CONF_KEY_MAXCHUNKS_TO_MOVE)));
	}

	PG_RETURN_VOID();
}

Datum
policy_move_to_columnstore_add_internal(Oid user_rel_oid, Interval *default_schedule_interval,
										bool if_not_exists, bool fixed_schedule,
										TimestampTz initial_start, const char *timezone,
										int32 max_chunks, bool allow_blocking_compression)
{
	NameData application_name;
	NameData proc_name, proc_schema, check_name, check_schema, owner;
	int32 job_id;
	Cache *hcache;
	Hypertable *ht;
	Oid owner_id;

	if (max_chunks < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s must be greater than or equal to 0",
						POL_MOVE_TO_COLUMNSTORE_CONF_KEY_MAXCHUNKS_TO_MOVE)));
	}

	ht = ts_hypertable_cache_get_cache_and_entry(user_rel_oid, CACHE_FLAG_NONE, &hcache);
	Assert(ht != NULL);
	int32 hypertable_id = ht->fd.id;

	owner_id = ts_hypertable_permissions_check(user_rel_oid, GetUserId());

	if (!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnstore not enabled on \"%s\"", get_rel_name(user_rel_oid)),
				 errhint("Enable columnstore before adding a move to columnstore policy.")));
	}

	/*
	 * Direct compress and this policy are alternative strategies.
	 */
	if (TS_HYPERTABLE_HAS_DIRECT_COMPRESS_ENABLED(ht))
	{
		ts_cache_release(&hcache);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"direct_compress\" is enabled on \"%s\"", get_rel_name(user_rel_oid)),
				 errhint("Disable direct_compress before adding this policy.")));
	}

	ts_bgw_job_validate_job_owner(owner_id);

	/* Make sure that an existing policy doesn't exist on this hypertable */
	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_MOVE_TO_COLUMNSTORE_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   hypertable_id);

	if (jobs != NIL)
	{
		Assert(list_length(jobs) == 1);

		ts_cache_release(&hcache);

		if (!if_not_exists)
		{
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("move to columnstore policy already exists for hypertable \"%s\"",
							get_rel_name(user_rel_oid)),
					 errhint("Set option \"if_not_exists\" to true to avoid error.")));
		}

		ereport(NOTICE,
				(errmsg("move to columnstore policy already exists for hypertable \"%s\", skipping",
						get_rel_name(user_rel_oid))));
		PG_RETURN_INT32(-1);
	}

	/*
	 * This policy replaces the compression policy
	 */
	if (ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_COMPRESSION_PROC_NAME,
												  FUNCTIONS_SCHEMA_NAME,
												  hypertable_id) != NIL)
	{
		policy_compression_remove_internal(user_rel_oid, true /* if_exists */);
	}

	if (fixed_schedule)
	{
		ts_bgw_job_validate_schedule_interval(default_schedule_interval);
		if (TIMESTAMP_NOT_FINITE(initial_start))
		{
			initial_start = ts_timer_get_current_timestamp();
		}
	}

	namestrcpy(&application_name, "Move To Columnstore Policy");
	namestrcpy(&proc_name, POLICY_MOVE_TO_COLUMNSTORE_PROC_NAME);
	namestrcpy(&proc_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&check_name, POLICY_MOVE_TO_COLUMNSTORE_CHECK_NAME);
	namestrcpy(&check_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&owner, GetUserNameFromId(owner_id, false));

	JsonbInState parse_state = { 0 };

	pushJsonbValueCompat(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_int32(&parse_state, POLICY_CONFIG_KEY_HYPERTABLE_ID, hypertable_id);

	if (max_chunks > 0)
	{
		ts_jsonb_add_int32(&parse_state,
						   POL_MOVE_TO_COLUMNSTORE_CONF_KEY_MAXCHUNKS_TO_MOVE,
						   max_chunks);
	}

	if (allow_blocking_compression)
	{
		ts_jsonb_add_bool(&parse_state,
						  POL_MOVE_TO_COLUMNSTORE_CONF_KEY_ALLOW_BLOCKING_COMPRESSION,
						  true);
	}

	pushJsonbValueCompat(&parse_state, WJB_END_OBJECT, NULL);
	Jsonb *config = JsonbValueToJsonb(parse_state.result);

	ts_cache_release(&hcache);

	job_id = ts_bgw_job_insert_relation(&application_name,
										default_schedule_interval,
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
										timezone);

	if (!TIMESTAMP_NOT_FINITE(initial_start))
	{
		ts_bgw_job_stat_upsert_next_start(job_id, initial_start);
	}

	PG_RETURN_INT32(job_id);
}

Datum
policy_move_to_columnstore_add(PG_FUNCTION_ARGS)
{
	/* behave like a strict function for the required arguments */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		ts_feature_flag_check(FEATURE_POLICY);
		PG_RETURN_NULL();
	}

	Oid user_rel_oid = PG_GETARG_OID(0);
	bool if_not_exists = PG_GETARG_BOOL(1);
	Interval *default_schedule_interval =
		PG_ARGISNULL(2) ?
			DatumGetIntervalP(
				DirectFunctionCall3(interval_in,
									CStringGetDatum(DEFAULT_MOVE_TO_COLUMNSTORE_SCHEDULE_INTERVAL),
									InvalidOid,
									-1)) :
			PG_GETARG_INTERVAL_P(2);
	TimestampTz initial_start = PG_ARGISNULL(3) ? DT_NOBEGIN : PG_GETARG_TIMESTAMPTZ(3);
	bool fixed_schedule = !PG_ARGISNULL(3);
	text *timezone = PG_ARGISNULL(4) ? NULL : PG_GETARG_TEXT_PP(4);
	char *valid_timezone = NULL;
	int32 max_chunks = PG_ARGISNULL(5) ? 0 : PG_GETARG_INT32(5);
	bool allow_blocking_compression = PG_ARGISNULL(6) ? false : PG_GETARG_BOOL(6);

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (timezone != NULL)
	{
		valid_timezone = ts_bgw_job_validate_timezone(PointerGetDatum(timezone));
	}

	return policy_move_to_columnstore_add_internal(user_rel_oid,
												   default_schedule_interval,
												   if_not_exists,
												   fixed_schedule,
												   initial_start,
												   valid_timezone,
												   max_chunks,
												   allow_blocking_compression);
}

bool
policy_move_to_columnstore_remove_internal(Oid hypertable_oid, bool if_exists)
{
	Cache *hcache;
	Hypertable *ht =
		ts_hypertable_cache_get_cache_and_entry(hypertable_oid, CACHE_FLAG_NONE, &hcache);
	int32 ht_id = ht->fd.id;
	ts_cache_release(&hcache);

	ts_hypertable_permissions_check(hypertable_oid, GetUserId());

	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_MOVE_TO_COLUMNSTORE_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   ht_id);

	if (jobs == NIL)
	{
		if (!if_exists)
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("move to columnstore policy not found for hypertable \"%s\"",
							get_rel_name(hypertable_oid))));
		}

		ereport(NOTICE,
				(errmsg("move to columnstore policy not found for hypertable \"%s\", skipping",
						get_rel_name(hypertable_oid))));
		return false;
	}

	Assert(list_length(jobs) == 1);
	BgwJob *job = linitial(jobs);

	ts_bgw_job_delete_by_id(job->fd.id);

	return true;
}

Datum
policy_move_to_columnstore_remove(PG_FUNCTION_ARGS)
{
	Oid hypertable_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	policy_move_to_columnstore_remove_internal(hypertable_oid, if_exists);

	PG_RETURN_NULL();
}
