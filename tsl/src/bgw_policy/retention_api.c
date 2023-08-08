/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <catalog/pg_type.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include <hypertable_cache.h>

#include "bgw/job.h"
#include "ts_catalog/continuous_agg.h"
#include "chunk.h"
#include "retention_api.h"
#include "errors.h"
#include "hypertable.h"
#include "dimension.h"
#include "policy_utils.h"
#include "utils.h"
#include "guc.h"
#include "jsonb_utils.h"
#include "bgw_policy/job.h"
#include "bgw_policy/policies_v2.h"
#include "bgw/job_stat.h"
#include "bgw/timer.h"

Datum
policy_retention_proc(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() != 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_VOID();

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	policy_retention_execute(PG_GETARG_INT32(0), PG_GETARG_JSONB_P(1));

	PG_RETURN_VOID();
}

Datum
policy_retention_check(PG_FUNCTION_ARGS)
{
	TS_PREVENT_FUNC_IF_READ_ONLY();

	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("config must not be NULL")));
	}

	policy_retention_read_and_validate_config(PG_GETARG_JSONB_P(0), NULL);

	PG_RETURN_VOID();
}

int32
policy_retention_get_hypertable_id(const Jsonb *config)
{
	bool found;
	int32 hypertable_id =
		ts_jsonb_get_int32_field(config, POL_RETENTION_CONF_KEY_HYPERTABLE_ID, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find hypertable_id in config for job")));

	return hypertable_id;
}

int64
policy_retention_get_drop_after_int(const Jsonb *config)
{
	bool found;
	int64 drop_after = ts_jsonb_get_int64_field(config, POL_RETENTION_CONF_KEY_DROP_AFTER, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find %s in config for job", POL_RETENTION_CONF_KEY_DROP_AFTER)));

	return drop_after;
}

Interval *
policy_retention_get_drop_after_interval(const Jsonb *config)
{
	Interval *interval = ts_jsonb_get_interval_field(config, POL_RETENTION_CONF_KEY_DROP_AFTER);

	if (interval == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find %s in config for job", POL_RETENTION_CONF_KEY_DROP_AFTER)));

	return interval;
}

static Hypertable *
validate_drop_chunks_hypertable(Cache *hcache, Oid user_htoid)
{
	ContinuousAggHypertableStatus status;
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, user_htoid, true /* missing_ok */);

	if (ht != NULL)
	{
		if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot add retention policy to compressed hypertable \"%s\"",
							get_rel_name(user_htoid)),
					 errhint("Please add the policy to the corresponding uncompressed hypertable "
							 "instead.")));
		}
		status = ts_continuous_agg_hypertable_status(ht->fd.id);
		if ((status == HypertableIsMaterialization || status == HypertableIsMaterializationAndRaw))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot add retention policy to materialized hypertable \"%s\" ",
							get_rel_name(user_htoid)),
					 errhint("Please add the policy to the corresponding continuous aggregate "
							 "instead.")));
		}
	}
	else
	{
		/*check if this is a cont aggregate view */
		int32 mat_id;
		ContinuousAgg *ca;

		ca = ts_continuous_agg_find_by_relid(user_htoid);

		if (ca == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
					 errmsg("\"%s\" is not a hypertable or a continuous aggregate",
							get_rel_name(user_htoid))));
		mat_id = ca->data.mat_hypertable_id;
		ht = ts_hypertable_get_by_id(mat_id);
	}
	Assert(ht != NULL);
	return ht;
}

Datum
policy_retention_add_internal(Oid ht_oid, Oid window_type, Datum window_datum,
							  Interval default_schedule_interval, bool if_not_exists,
							  bool fixed_schedule, TimestampTz initial_start, const char *timezone)
{
	NameData application_name;
	int32 job_id;
	Hypertable *hypertable;
	Cache *hcache;

	Oid owner_id = ts_hypertable_permissions_check(ht_oid, GetUserId());
	Oid partitioning_type;
	const Dimension *dim;
	/* Default scheduled interval for drop_chunks jobs is currently 1 day (24 hours) */
	/* Default max runtime should not be very long. Right now set to 5 minutes */
	Interval default_max_runtime = { .time = 5 * USECS_PER_MINUTE };
	/* Default retry period is currently 5 minutes */
	Interval default_retry_period = { .time = 5 * USECS_PER_MINUTE };
	/* Right now, there is an infinite number of retries for drop_chunks jobs */
	int default_max_retries = -1;

	/* Verify that the hypertable owner can create a background worker */
	ts_bgw_job_validate_job_owner(owner_id);

	/* Make sure that an existing policy doesn't exist on this hypertable */
	hcache = ts_hypertable_cache_pin();
	hypertable = validate_drop_chunks_hypertable(hcache, ht_oid);

	dim = hyperspace_get_open_dimension(hypertable->space, 0);
	partitioning_type = ts_dimension_get_partition_type(dim);

	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_RETENTION_PROC_NAME,
														   INTERNAL_SCHEMA_NAME,
														   hypertable->fd.id);

	if (jobs != NIL)
	{
		if (!if_not_exists)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("retention policy already exists for hypertable \"%s\"",
							get_rel_name(ht_oid))));

		Assert(list_length(jobs) == 1);
		BgwJob *existing = linitial(jobs);

		if (policy_config_check_hypertable_lag_equality(existing->fd.config,
														POL_RETENTION_CONF_KEY_DROP_AFTER,
														partitioning_type,
														window_type,
														window_datum))
		{
			/* If all arguments are the same, do nothing */
			ts_cache_release(hcache);
			ereport(NOTICE,
					(errmsg("retention policy already exists for hypertable \"%s\", skipping",
							get_rel_name(ht_oid))));
			PG_RETURN_INT32(-1);
		}
		else
		{
			ts_cache_release(hcache);
			ereport(WARNING,
					(errmsg("retention policy already exists for hypertable \"%s\"",
							get_rel_name(ht_oid)),
					 errdetail("A policy already exists with different arguments."),
					 errhint("Remove the existing policy before adding a new one.")));
			PG_RETURN_INT32(-1);
		}
	}

	if (IS_INTEGER_TYPE(partitioning_type) && !IS_INTEGER_TYPE(window_type))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for parameter %s", POL_RETENTION_CONF_KEY_DROP_AFTER),
				 errhint("Integer time duration is required for hypertables"
						 " with integer time dimension.")));

	if (IS_TIMESTAMP_TYPE(partitioning_type) && window_type != INTERVALOID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for parameter %s", POL_RETENTION_CONF_KEY_DROP_AFTER),
				 errhint("Interval time duration is required for hypertable"
						 " with timestamp-based time dimension.")));

	JsonbParseState *parse_state = NULL;

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_int32(parse_state, POL_RETENTION_CONF_KEY_HYPERTABLE_ID, hypertable->fd.id);

	switch (window_type)
	{
		case INTERVALOID:
			ts_jsonb_add_interval(parse_state,
								  POL_RETENTION_CONF_KEY_DROP_AFTER,
								  DatumGetIntervalP(window_datum));
			break;
		case INT2OID:
			ts_jsonb_add_int64(parse_state,
							   POL_RETENTION_CONF_KEY_DROP_AFTER,
							   DatumGetInt16(window_datum));
			break;
		case INT4OID:
			ts_jsonb_add_int64(parse_state,
							   POL_RETENTION_CONF_KEY_DROP_AFTER,
							   DatumGetInt32(window_datum));
			break;
		case INT8OID:
			ts_jsonb_add_int64(parse_state,
							   POL_RETENTION_CONF_KEY_DROP_AFTER,
							   DatumGetInt64(window_datum));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported datatype for %s: %s",
							POL_RETENTION_CONF_KEY_DROP_AFTER,
							format_type_be(window_type))));
	}

	JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	Jsonb *config = JsonbValueToJsonb(result);

	/* Next, insert a new job into jobs table */
	namestrcpy(&application_name, "Retention Policy");
	NameData proc_name, proc_schema, check_schema, check_name;
	namestrcpy(&proc_name, POLICY_RETENTION_PROC_NAME);
	namestrcpy(&proc_schema, INTERNAL_SCHEMA_NAME);
	namestrcpy(&check_name, POLICY_RETENTION_CHECK_NAME);
	namestrcpy(&check_schema, INTERNAL_SCHEMA_NAME);

	job_id = ts_bgw_job_insert_relation(&application_name,
										&default_schedule_interval,
										&default_max_runtime,
										default_max_retries,
										&default_retry_period,
										&proc_schema,
										&proc_name,
										&check_schema,
										&check_name,
										owner_id,
										true,
										fixed_schedule,
										hypertable->fd.id,
										config,
										initial_start,
										timezone);

	ts_cache_release(hcache);

	PG_RETURN_INT32(job_id);
}

Datum
policy_retention_add(PG_FUNCTION_ARGS)
{
	/* behave like a strict function */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		PG_RETURN_NULL();

	Oid ht_oid = PG_GETARG_OID(0);
	Datum window_datum = PG_GETARG_DATUM(1);
	bool if_not_exists = PG_GETARG_BOOL(2);
	Oid window_type = PG_ARGISNULL(1) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 1);
	Interval default_schedule_interval =
		PG_ARGISNULL(3) ? (Interval) DEFAULT_RETENTION_SCHEDULE_INTERVAL : *PG_GETARG_INTERVAL_P(3);
	TimestampTz initial_start = PG_ARGISNULL(4) ? DT_NOBEGIN : PG_GETARG_TIMESTAMPTZ(4);
	bool fixed_schedule = !PG_ARGISNULL(4);
	text *timezone = PG_ARGISNULL(5) ? NULL : PG_GETARG_TEXT_PP(5);
	char *valid_timezone = NULL;

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	Datum retval;
	/* if users pass in -infinity for initial_start, then use the current_timestamp instead */
	if (fixed_schedule)
	{
		ts_bgw_job_validate_schedule_interval(&default_schedule_interval);
		if (TIMESTAMP_NOT_FINITE(initial_start))
			initial_start = ts_timer_get_current_timestamp();
	}

	if (timezone != NULL)
		valid_timezone = ts_bgw_job_validate_timezone(PG_GETARG_DATUM(5));

	retval = policy_retention_add_internal(ht_oid,
										   window_type,
										   window_datum,
										   default_schedule_interval,
										   if_not_exists,
										   fixed_schedule,
										   initial_start,
										   valid_timezone);
	if (!TIMESTAMP_NOT_FINITE(initial_start))
	{
		int32 job_id = DatumGetInt32(retval);
		ts_bgw_job_stat_upsert_next_start(job_id, initial_start);
	}
	return retval;
}

Datum
policy_retention_remove_internal(Oid table_oid, bool if_exists)
{
	Cache *hcache;
	Hypertable *hypertable;

	hypertable = ts_hypertable_cache_get_cache_and_entry(table_oid, CACHE_FLAG_MISSING_OK, &hcache);
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
	int32 ht_id = hypertable->fd.id;
	ts_cache_release(hcache);
	ts_hypertable_permissions_check(table_oid, GetUserId());

	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_RETENTION_PROC_NAME,
														   INTERNAL_SCHEMA_NAME,
														   ht_id);
	if (jobs == NIL)
	{
		if (!if_exists)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("retention policy not found for hypertable \"%s\"",
							get_rel_name(table_oid))));
		else
		{
			ereport(NOTICE,
					(errmsg("retention policy not found for hypertable \"%s\", skipping",
							get_rel_name(table_oid))));
			PG_RETURN_BOOL(false);
		}
	}
	Assert(list_length(jobs) == 1);
	BgwJob *job = linitial(jobs);

	ts_bgw_job_delete_by_id(job->fd.id);

	PG_RETURN_BOOL(true);
}

Datum
policy_retention_remove(PG_FUNCTION_ARGS)
{
	Oid table_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	return policy_retention_remove_internal(table_oid, if_exists);
}
