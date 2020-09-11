/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <miscadmin.h>
#include <parser/parse_coerce.h>

#include <jsonb_utils.h>
#include <utils/builtins.h>
#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/job.h"
#include "bgw/job.h"
#include "continuous_agg.h"
#include "continuous_aggs/materialize.h"
#include "dimension.h"
#include "hypertable_cache.h"
#include "time_utils.h"
#include "policy_utils.h"

#define POLICY_REFRESH_CAGG_PROC_NAME "policy_refresh_continuous_aggregate"
#define CONFIG_KEY_MAT_HYPERTABLE_ID "mat_hypertable_id"
#define CONFIG_KEY_START_INTERVAL "start_interval"
#define CONFIG_KEY_END_INTERVAL "end_interval"

/* Default max runtime for a continuous aggregate jobs is unlimited for now */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("0"), InvalidOid, -1))

/* infinite number of retries for continuous aggregate jobs */
#define DEFAULT_MAX_RETRIES (-1)

int32
policy_continuous_aggregate_get_mat_hypertable_id(const Jsonb *config)
{
	bool found;
	int32 mat_hypertable_id =
		ts_jsonb_get_int32_field(config, CONFIG_KEY_MAT_HYPERTABLE_ID, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find \"%s\" in config for job", CONFIG_KEY_MAT_HYPERTABLE_ID)));

	return mat_hypertable_id;
}

static int64
get_interval_from_config(const Dimension *dim, const Jsonb *config, const char *json_label,
						 bool *isnull)
{
	Oid partitioning_type = ts_dimension_get_partition_type(dim);
	*isnull = false;
	if (IS_INTEGER_TYPE(partitioning_type))
	{
		bool found;
		int64 interval_val = ts_jsonb_get_int64_field(config, json_label, &found);
		if (!found)
		{
			*isnull = true;
			return (Datum) 0;
		}
		Oid now_func = ts_get_integer_now_func(dim);

		Assert(now_func);

		return subtract_integer_from_now(interval_val, partitioning_type, now_func);
	}
	else
	{
		Datum res;
		Interval *interval_val = ts_jsonb_get_interval_field(config, json_label);
		if (!interval_val)
		{
			*isnull = true;
			return 0;
		}
		res = subtract_interval_from_now(interval_val, partitioning_type);
		return ts_time_value_to_internal(res, partitioning_type);
	}
}

int64
policy_refresh_cagg_get_refresh_start(const Dimension *dim, const Jsonb *config, bool *start_isnull)
{
	return get_interval_from_config(dim, config, CONFIG_KEY_START_INTERVAL, start_isnull);
}

int64
policy_refresh_cagg_get_refresh_end(const Dimension *dim, const Jsonb *config, bool *end_isnull)
{
	return get_interval_from_config(dim, config, CONFIG_KEY_END_INTERVAL, end_isnull);
}

Datum
policy_refresh_cagg_proc(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() != 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_VOID();

	PreventCommandIfReadOnly("policy_refresh_continuous_aggregate()");
	policy_refresh_cagg_execute(PG_GETARG_INT32(0), PG_GETARG_JSONB_P(1));

	PG_RETURN_VOID();
}

static Oid
ts_cagg_permissions_check(Oid cagg_oid, Oid userid)
{
	Oid ownerid = ts_rel_get_owner(cagg_oid);

	if (!has_privs_of_role(userid, ownerid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of continuous aggregate \"%s\"", get_rel_name(cagg_oid))));

	return ownerid;
}

static void
json_add_dim_interval_value(JsonbParseState *parse_state, const char *json_label, Oid dim_type,
							Datum value)
{
	switch (dim_type)
	{
		case INTERVALOID:
			ts_jsonb_add_interval(parse_state, json_label, DatumGetIntervalP(value));
			break;
		case INT2OID:
			ts_jsonb_add_int64(parse_state, json_label, DatumGetInt16(value));
			break;
		case INT4OID:
			ts_jsonb_add_int64(parse_state, json_label, DatumGetInt32(value));
			break;
		case INT8OID:
			ts_jsonb_add_int64(parse_state, json_label, DatumGetInt64(value));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported interval argument type, expected type : %s",
							format_type_be(dim_type))));
	}
}

static Datum
convert_interval_arg(Oid dim_type, Datum interval, Oid *interval_type, const char *str_msg)
{
	Oid convert_to = dim_type;

	if (*interval_type != convert_to)
	{
		if (IS_TIMESTAMP_TYPE(dim_type))
			convert_to = INTERVALOID;

		if (!can_coerce_type(1, interval_type, &convert_to, COERCION_IMPLICIT))
		{
			if (IS_INTEGER_TYPE(dim_type))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid parameter value for %s", str_msg),
						 errhint("Use time interval of type %s with the continuous aggregate.",
								 format_type_be(dim_type))));
			else if (IS_TIMESTAMP_TYPE(dim_type))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid parameter value for %s", str_msg),
						 errhint("Use time interval with a continuous aggregate using "
								 "timestamp-based time "
								 "bucket.")));
		}
	}

	return ts_time_datum_convert_arg(interval, interval_type, convert_to);
}

static void
check_valid_interval_values(Oid interval_type, Datum start_interval, Datum end_interval)
{
	bool valid = true;
	if (IS_INTEGER_TYPE(interval_type))
	{
		switch (interval_type)
		{
			case INT2OID:
			{
				if (DatumGetInt16(start_interval) <= DatumGetInt16(end_interval))
					valid = false;
				break;
			}
			case INT4OID:
			{
				if (DatumGetInt32(start_interval) <= DatumGetInt32(end_interval))
					valid = false;
				break;
			}
			case INT8OID:
			{
				if (DatumGetInt64(start_interval) <= DatumGetInt64(end_interval))
					valid = false;
				break;
			}
		}
	}
	else
	{
		Assert(interval_type == INTERVALOID);
		valid = DatumGetBool(DirectFunctionCall2(interval_gt, start_interval, end_interval));
	}
	if (!valid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("start interval should be greater than end interval")));
	}
}

Datum
policy_refresh_cagg_add(PG_FUNCTION_ARGS)
{
	NameData application_name;
	NameData refresh_name;
	NameData proc_name, proc_schema, owner;
	Cache *hcache;
	Hypertable *mat_ht;
	Dimension *dim;
	ContinuousAgg *cagg;
	int32 job_id, mat_htid;
	Datum start_interval, end_interval;
	Interval refresh_interval;
	Oid dim_type, start_interval_type, end_interval_type;
	Oid cagg_oid, owner_id;
	List *jobs;
	JsonbParseState *parse_state = NULL;
	bool if_not_exists, start_isnull, end_isnull;

	/* Verify that the owner can create a background worker */
	cagg_oid = PG_GETARG_OID(0);
	owner_id = ts_cagg_permissions_check(cagg_oid, GetUserId());
	ts_bgw_job_validate_job_owner(owner_id);

	cagg = ts_continuous_agg_find_by_relid(cagg_oid);
	if (!cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(cagg_oid))));

	hcache = ts_hypertable_cache_pin();
	mat_htid = cagg->data.mat_hypertable_id;
	mat_ht = ts_hypertable_cache_get_entry_by_id(hcache, mat_htid);
	dim = hyperspace_get_open_dimension(mat_ht->space, 0);
	dim_type = ts_dimension_get_partition_type(dim);
	ts_cache_release(hcache);

	/* Try to convert the argument to the time type used by the
	 * continuous aggregate */
	start_interval = PG_GETARG_DATUM(1);
	end_interval = PG_GETARG_DATUM(2);
	start_isnull = PG_ARGISNULL(1);
	end_isnull = PG_ARGISNULL(2);
	start_interval_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	end_interval_type = get_fn_expr_argtype(fcinfo->flinfo, 2);

	if (!start_isnull)
		start_interval =
			convert_interval_arg(dim_type, start_interval, &start_interval_type, "start_interval");

	if (!end_isnull)
		end_interval =
			convert_interval_arg(dim_type, end_interval, &end_interval_type, "end_interval");

	if (!start_isnull && !end_isnull)
	{
		Assert(start_interval_type == end_interval_type);
		check_valid_interval_values(start_interval_type, start_interval, end_interval);
	}

	if (PG_ARGISNULL(3))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot use NULL schedule interval")));
	refresh_interval = *PG_GETARG_INTERVAL_P(3);
	if_not_exists = PG_GETARG_BOOL(4);

	/* Make sure there is only 1 refresh policy on the cagg */
	jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_REFRESH_CAGG_PROC_NAME,
													 INTERNAL_SCHEMA_NAME,
													 mat_htid);

	if (jobs != NIL)
	{
		Assert(list_length(jobs) == 1);
		if (!if_not_exists)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("refresh policy already exists for continuous aggregate \"%s\"",
							get_rel_name(cagg_oid))));
		BgwJob *existing = linitial(jobs);

		if (policy_config_check_hypertable_lag_equality(existing->fd.config,
														CONFIG_KEY_START_INTERVAL,
														dim_type,
														start_interval_type,
														start_interval) &&
			policy_config_check_hypertable_lag_equality(existing->fd.config,
														CONFIG_KEY_END_INTERVAL,
														dim_type,
														end_interval_type,
														end_interval))
		{
			/* If all arguments are the same, do nothing */
			ereport(NOTICE,
					(errmsg("refresh policy already exists on continuous aggregate \"%s\", "
							"skipping",
							get_rel_name(cagg_oid))));
			PG_RETURN_INT32(-1);
		}
		else
		{
			elog(WARNING,
				 "could not add refresh policy due to existing policy on continuous aggregate with "
				 "different arguments");
			PG_RETURN_INT32(-1);
		}
	}

	/* Next, insert a new job into jobs table */
	namestrcpy(&application_name, "Refresh Continuous Aggregate Policy");
	namestrcpy(&refresh_name, "custom");
	namestrcpy(&proc_name, POLICY_REFRESH_CAGG_PROC_NAME);
	namestrcpy(&proc_schema, INTERNAL_SCHEMA_NAME);
	namestrcpy(&owner, GetUserNameFromId(owner_id, false));

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_int32(parse_state, CONFIG_KEY_MAT_HYPERTABLE_ID, mat_htid);
	if (!start_isnull)
		json_add_dim_interval_value(parse_state,
									CONFIG_KEY_START_INTERVAL,
									start_interval_type,
									start_interval);
	else
		ts_jsonb_add_null(parse_state, CONFIG_KEY_START_INTERVAL);
	if (!end_isnull)
		json_add_dim_interval_value(parse_state,
									CONFIG_KEY_END_INTERVAL,
									end_interval_type,
									end_interval);
	else
		ts_jsonb_add_null(parse_state, CONFIG_KEY_END_INTERVAL);
	JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	Jsonb *config = JsonbValueToJsonb(result);

	job_id = ts_bgw_job_insert_relation(&application_name,
										&refresh_name,
										&refresh_interval,
										DEFAULT_MAX_RUNTIME,
										DEFAULT_MAX_RETRIES,
										&refresh_interval,
										&proc_schema,
										&proc_name,
										&owner,
										true,
										mat_htid,
										config);

	PG_RETURN_INT32(job_id);
}

Datum
policy_refresh_cagg_remove(PG_FUNCTION_ARGS)
{
	Oid cagg_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);
	int32 mat_htid;

	ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(cagg_oid);
	if (!cagg)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(cagg_oid))));
	}

	ts_cagg_permissions_check(cagg_oid, GetUserId());
	mat_htid = cagg->data.mat_hypertable_id;
	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_REFRESH_CAGG_PROC_NAME,
														   INTERNAL_SCHEMA_NAME,
														   mat_htid);
	if (jobs == NIL)
	{
		if (!if_exists)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 (errmsg("refresh policy does not exist on continuous aggregate "
							 "\"%s\"",
							 get_rel_name(cagg_oid)))));
		else
		{
			ereport(NOTICE,
					(errmsg("refresh policy does not exist on continuous aggregate \"%s\", "
							"skipping",
							get_rel_name(cagg_oid))));
			PG_RETURN_VOID();
		}
	}
	Assert(list_length(jobs) == 1);
	BgwJob *job = linitial(jobs);

	ts_bgw_job_delete_by_id(job->fd.id);

	PG_RETURN_VOID();
}
