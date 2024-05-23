/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <common/int128.h>
#include <miscadmin.h>
#include <parser/parse_coerce.h>
#include <utils/acl.h>

#include <jsonb_utils.h>
#include <utils/builtins.h>

#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/job_api.h"
#include "bgw_policy/job.h"
#include "bgw_policy/policies_v2.h"
#include "bgw_policy/policy_utils.h"
#include "bgw/job_stat.h"
#include "bgw/job.h"
#include "bgw/timer.h"
#include "continuous_aggs/materialize.h"
#include "dimension.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "policy_utils.h"
#include "time_utils.h"
#include "ts_catalog/continuous_agg.h"

/* Default max runtime for a continuous aggregate jobs is unlimited for now */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("0"), InvalidOid, -1))

int32
policy_continuous_aggregate_get_mat_hypertable_id(const Jsonb *config)
{
	bool found;
	int32 mat_hypertable_id =
		ts_jsonb_get_int32_field(config, POL_REFRESH_CONF_KEY_MAT_HYPERTABLE_ID, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find \"%s\" in config for job",
						POL_REFRESH_CONF_KEY_MAT_HYPERTABLE_ID)));

	return mat_hypertable_id;
}

static int64
get_time_from_interval(const Dimension *dim, Datum interval, Oid type)
{
	Oid partitioning_type = ts_dimension_get_partition_type(dim);

	if (IS_INTEGER_TYPE(type))
	{
		Oid now_func = ts_get_integer_now_func(dim, true);
		int64 value = ts_interval_value_to_internal(interval, type);

		Assert(now_func);

		return ts_subtract_integer_from_now_saturating(now_func, value, partitioning_type);
	}
	else if (type == INTERVALOID)
	{
		Datum res = subtract_interval_from_now(DatumGetIntervalP(interval), partitioning_type);
		return ts_time_value_to_internal(res, partitioning_type);
	}
	else
		elog(ERROR, "unsupported offset type for continuous aggregate policy");

	pg_unreachable();

	return 0;
}

static int64
get_time_from_config(const Dimension *dim, const Jsonb *config, const char *json_label,
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
			return 0;
		}
		return get_time_from_interval(dim, Int64GetDatum(interval_val), INT8OID);
	}
	else
	{
		Interval *interval_val = ts_jsonb_get_interval_field(config, json_label);

		if (!interval_val)
		{
			*isnull = true;
			return 0;
		}

		return get_time_from_interval(dim, IntervalPGetDatum(interval_val), INTERVALOID);
	}
}

int64
policy_refresh_cagg_get_refresh_start(const ContinuousAgg *cagg, const Dimension *dim,
									  const Jsonb *config, bool *start_isnull)
{
	int64 res = get_time_from_config(dim, config, POL_REFRESH_CONF_KEY_START_OFFSET, start_isnull);

	/* interpret NULL as min value for that type */
	if (*start_isnull)
	{
		Assert(cagg->partition_type == ts_dimension_get_partition_type(dim));
		return cagg_get_time_min(cagg);
	}

	return res;
}

int64
policy_refresh_cagg_get_refresh_end(const Dimension *dim, const Jsonb *config, bool *end_isnull)
{
	int64 res = get_time_from_config(dim, config, POL_REFRESH_CONF_KEY_END_OFFSET, end_isnull);

	if (*end_isnull)
		return ts_time_get_end_or_max(ts_dimension_get_partition_type(dim));
	return res;
}

/* returns false if a policy could not be found */
bool
policy_refresh_cagg_exists(int32 materialization_id)
{
	Hypertable *mat_ht = ts_hypertable_get_by_id(materialization_id);

	if (!mat_ht)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("configuration materialization hypertable id %d not found",
						materialization_id)));

	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_REFRESH_CAGG_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   materialization_id);
	if (jobs == NIL)
		return false;

	/* only 1 cont. aggregate policy job allowed */
	Assert(list_length(jobs) == 1);
	return true;
}

/* Compare passed in interval value with refresh_start parameter
 * of cagg policy.
 */
bool
policy_refresh_cagg_refresh_start_lt(int32 materialization_id, Oid cmp_type, Datum cmp_interval)
{
	Hypertable *mat_ht = ts_hypertable_get_by_id(materialization_id);
	bool ret = false;

	if (!mat_ht)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("configuration materialization hypertable id %d not found",
						materialization_id)));

	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_REFRESH_CAGG_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   materialization_id);
	if (jobs == NIL)
		return false;

	/* only 1 cont. aggregate policy job allowed */
	BgwJob *cagg_job = linitial(jobs);
	Jsonb *cagg_config = cagg_job->fd.config;

	const Dimension *open_dim = get_open_dimension_for_hypertable(mat_ht, true);
	Oid dim_type = ts_dimension_get_partition_type(open_dim);
	if (IS_INTEGER_TYPE(dim_type))
	{
		bool found;
		Assert(IS_INTEGER_TYPE(cmp_type));
		int64 cmpval = ts_interval_value_to_internal(cmp_interval, cmp_type);
		int64 refresh_start =
			ts_jsonb_get_int64_field(cagg_config, POL_REFRESH_CONF_KEY_START_OFFSET, &found);
		if (!found) /*this is a null value */
			return false;
		ret = (refresh_start < cmpval);
	}
	else
	{
		Assert(cmp_type == INTERVALOID);
		Interval *refresh_start =
			ts_jsonb_get_interval_field(cagg_config, POL_REFRESH_CONF_KEY_START_OFFSET);
		if (refresh_start == NULL) /* NULL refresh_start */
			return false;
		Datum res =
			DirectFunctionCall2(interval_lt, IntervalPGetDatum(refresh_start), cmp_interval);
		ret = DatumGetBool(res);
	}
	return ret;
}

Datum
policy_refresh_cagg_proc(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() != 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_VOID();

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	policy_refresh_cagg_execute(PG_GETARG_INT32(0), PG_GETARG_JSONB_P(1));

	PG_RETURN_VOID();
}

Datum
policy_refresh_cagg_check(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("config must not be NULL")));
	}

	policy_refresh_cagg_read_and_validate_config(PG_GETARG_JSONB_P(0), NULL);

	PG_RETURN_VOID();
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
	Datum converted;

	if (IS_TIMESTAMP_TYPE(dim_type))
		convert_to = INTERVALOID;

	if (*interval_type != convert_to)
	{
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

	converted = ts_time_datum_convert_arg(interval, interval_type, convert_to);

	/* For integer types, first convert all types to int64 to get on a common
	 * type. Then check valid time ranges against the partition/dimension
	 * type */
	switch (*interval_type)
	{
		case INT2OID:
			converted = Int64GetDatum((int64) DatumGetInt16(converted));
			break;
		case INT4OID:
			converted = Int64GetDatum((int64) DatumGetInt32(converted));
			break;
		case INT8OID:
			break;
		case INTERVALOID:
			/* For timestamp types, we only support Interval, so nothing further
			 * to do. */
			return converted;
		default:
			pg_unreachable();
			break;
	}

	/* Cap at min and max */
	if (DatumGetInt64(converted) < ts_time_get_min(dim_type))
		converted = ts_time_get_min(dim_type);
	else if (DatumGetInt64(converted) > ts_time_get_max(dim_type))
		converted = ts_time_get_max(dim_type);

	/* Convert to the desired integer type */
	switch (dim_type)
	{
		case INT2OID:
			converted = Int16GetDatum((int16) DatumGetInt64(converted));
			break;
		case INT4OID:
			converted = Int32GetDatum((int32) DatumGetInt64(converted));
			break;
		case INT8OID:
			/* Already int64, so nothing to do. */
			break;
		default:
			pg_unreachable();
			break;
	}

	*interval_type = dim_type;

	return converted;
}

/*
 * Convert an interval to a 128 integer value.
 *
 * Based on PostgreSQL's interval_cmp_value().
 */
static inline INT128
interval_to_int128(const Interval *interval)
{
	INT128 span;
	int64 dayfraction;
	int64 days;

	/*
	 * Separate time field into days and dayfraction, then add the month and
	 * day fields to the days part.  We cannot overflow int64 days here.
	 */
	dayfraction = interval->time % USECS_PER_DAY;
	days = interval->time / USECS_PER_DAY;
	days += interval->month * INT64CONST(30);
	days += interval->day;

	/* Widen dayfraction to 128 bits */
	span = int64_to_int128(dayfraction);

	/* Scale up days to microseconds, forming a 128-bit product */
	int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);

	return span;
}

int64
interval_to_int64(Datum interval, Oid type)
{
	switch (type)
	{
		case INT2OID:
			return DatumGetInt16(interval);
		case INT4OID:
			return DatumGetInt32(interval);
		case INT8OID:
			return DatumGetInt64(interval);
		case INTERVALOID:
		{
			const int64 max = ts_time_get_max(TIMESTAMPTZOID);
			const int64 min = ts_time_get_min(TIMESTAMPTZOID);
			INT128 bigres = interval_to_int128(DatumGetIntervalP(interval));

			if (int128_compare(bigres, int64_to_int128(max)) >= 0)
				return max;
			else if (int128_compare(bigres, int64_to_int128(min)) <= 0)
				return min;
			else
				return int128_to_int64(bigres);
		}
		default:
			break;
	}

	pg_unreachable();

	return 0;
}

/*
 * Enforce that a policy has a refresh window of at least two buckets to
 * ensure we materialize at least one bucket each run.
 *
 * Why two buckets? Note that the policy probably won't execute at at time
 * that exactly aligns with a bucket boundary, so a window of one bucket
 * might not cover a full bucket that we want to materialize:
 *
 * Refresh window:                   [-----)
 * Materialized buckets:   |-----|-----|-----|
 */
static void
validate_window_size(const ContinuousAgg *cagg, const CaggPolicyConfig *config)
{
	int64 start_offset;
	int64 end_offset;
	int64 bucket_width;

	if (config->offset_start.isnull)
		start_offset = ts_time_get_max(cagg->partition_type);
	else
		start_offset = interval_to_int64(config->offset_start.value, config->offset_start.type);

	if (config->offset_end.isnull)
		end_offset = ts_time_get_min(cagg->partition_type);
	else
		end_offset = interval_to_int64(config->offset_end.value, config->offset_end.type);

	if (cagg->bucket_function->bucket_fixed_interval == false)
	{
		/*
		 * There are several cases of variable-sized buckets:
		 * 1. Monthly buckets
		 * 2. Buckets with timezones
		 * 3. Cases 1 and 2 at the same time
		 *
		 * For months we simply take 30 days like on interval_to_int64 and
		 * multiply this number by the number of months in the bucket. This
		 * reduces the task to days/hours/minutes scenario.
		 *
		 * Days/hours/minutes case is handled the same way as for fixed-sized
		 * buckets. The refresh window at least two buckets in size is adequate
		 * for such corner cases as DST.
		 */

		/* bucket_function should always be specified for variable-sized buckets */
		Assert(cagg->bucket_function != NULL);
		/* ... and bucket_function->bucket_time_width too */
		Assert(cagg->bucket_function->bucket_time_width != NULL);

		/* Make a temporary copy of bucket_width */
		Interval interval = *cagg->bucket_function->bucket_time_width;
		interval.day += 30 * interval.month;
		interval.month = 0;
		bucket_width = ts_interval_value_to_internal(IntervalPGetDatum(&interval), INTERVALOID);
	}
	else
	{
		bucket_width = ts_continuous_agg_fixed_bucket_width(cagg->bucket_function);
	}

	Assert(bucket_width > 0);

	if (ts_time_saturating_add(end_offset, bucket_width * 2, INT8OID) > start_offset)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("policy refresh window too small"),
				 errdetail("The start and end offsets must cover at least"
						   " two buckets in the valid time range of type \"%s\".",
						   format_type_be(cagg->partition_type))));
}

static void
parse_offset_arg(const ContinuousAgg *cagg, Oid offset_type, NullableDatum arg,
				 CaggPolicyOffset *offset)
{
	offset->isnull = arg.isnull;

	if (!offset->isnull)
	{
		offset->value =
			convert_interval_arg(cagg->partition_type, arg.value, &offset_type, offset->name);
		offset->type = offset_type;
	}
}

static void
parse_cagg_policy_config(const ContinuousAgg *cagg, Oid start_offset_type,
						 NullableDatum start_offset, Oid end_offset_type, NullableDatum end_offset,
						 CaggPolicyConfig *config)
{
	MemSet(config, 0, sizeof(CaggPolicyConfig));
	config->partition_type = cagg->partition_type;
	/* This might seem backwards, but since we are dealing with offsets, start
	 * actually translates to max and end to min for maximum window. */
	config->offset_start.value = ts_time_datum_get_max(config->partition_type);
	config->offset_end.value = ts_time_datum_get_min(config->partition_type);
	config->offset_start.type = config->offset_end.type =
		IS_TIMESTAMP_TYPE(cagg->partition_type) ? INTERVALOID : cagg->partition_type;
	config->offset_start.name = POL_REFRESH_CONF_KEY_START_OFFSET;
	config->offset_end.name = POL_REFRESH_CONF_KEY_END_OFFSET;
	parse_offset_arg(cagg, start_offset_type, start_offset, &config->offset_start);
	parse_offset_arg(cagg, end_offset_type, end_offset, &config->offset_end);

	Assert(config->offset_start.type == config->offset_end.type);
	validate_window_size(cagg, config);
}

Datum
policy_refresh_cagg_add_internal(Oid cagg_oid, Oid start_offset_type, NullableDatum start_offset,
								 Oid end_offset_type, NullableDatum end_offset,
								 Interval refresh_interval, bool if_not_exists, bool fixed_schedule,
								 TimestampTz initial_start, const char *timezone)
{
	NameData application_name;
	NameData proc_name, proc_schema, check_name, check_schema, owner;
	ContinuousAgg *cagg;
	CaggPolicyConfig policyconf;
	int32 job_id;
	Oid owner_id;
	List *jobs;
	JsonbParseState *parse_state = NULL;

	/* Verify that the owner can create a background worker */
	owner_id = ts_cagg_permissions_check(cagg_oid, GetUserId());
	ts_bgw_job_validate_job_owner(owner_id);

	cagg = ts_continuous_agg_find_by_relid(cagg_oid);
	if (!cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(cagg_oid))));

	if (!start_offset.isnull)
		start_offset.isnull =
			ts_if_offset_is_infinity(start_offset.value, start_offset_type, true /* is_start */);
	if (!end_offset.isnull)
		end_offset.isnull =
			ts_if_offset_is_infinity(end_offset.value, end_offset_type, false /* is_start */);

	parse_cagg_policy_config(cagg,
							 start_offset_type,
							 start_offset,
							 end_offset_type,
							 end_offset,
							 &policyconf);

	/* Make sure there is only 1 refresh policy on the cagg */
	jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_REFRESH_CAGG_PROC_NAME,
													 FUNCTIONS_SCHEMA_NAME,
													 cagg->data.mat_hypertable_id);

	if (jobs != NIL)
	{
		Assert(list_length(jobs) == 1);
		if (!if_not_exists)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("continuous aggregate policy already exists for \"%s\"",
							get_rel_name(cagg_oid)),
					 errdetail("Only one continuous aggregate policy can be created per continuous "
							   "aggregate and a policy with job id %d already exists for \"%s\".",
							   ((BgwJob *) linitial(jobs))->fd.id,
							   get_rel_name(cagg_oid))));
		BgwJob *existing = linitial(jobs);

		if (policy_config_check_hypertable_lag_equality(existing->fd.config,
														POL_REFRESH_CONF_KEY_START_OFFSET,
														cagg->partition_type,
														policyconf.offset_start.type,
														policyconf.offset_start.value,
														policyconf.offset_start.isnull) &&
			policy_config_check_hypertable_lag_equality(existing->fd.config,
														POL_REFRESH_CONF_KEY_END_OFFSET,
														cagg->partition_type,
														policyconf.offset_end.type,
														policyconf.offset_end.value,
														policyconf.offset_end.isnull))
		{
			/* If all arguments are the same, do nothing */
			ereport(NOTICE,
					(errmsg("continuous aggregate policy already exists for \"%s\", "
							"skipping",
							get_rel_name(cagg_oid))));
			PG_RETURN_INT32(-1);
		}
		else
		{
			ereport(WARNING,
					(errmsg("continuous aggregate policy already exists for \"%s\"",
							get_rel_name(cagg_oid)),
					 errdetail("A policy already exists with different arguments."),
					 errhint("Remove the existing policy before adding a new one.")));
			PG_RETURN_INT32(-1);
		}
	}

	/* Next, insert a new job into jobs table */
	namestrcpy(&application_name, "Refresh Continuous Aggregate Policy");
	namestrcpy(&proc_name, POLICY_REFRESH_CAGG_PROC_NAME);
	namestrcpy(&proc_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&check_name, POLICY_REFRESH_CAGG_CHECK_NAME);
	namestrcpy(&check_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&owner, GetUserNameFromId(owner_id, false));

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_int32(parse_state,
					   POL_REFRESH_CONF_KEY_MAT_HYPERTABLE_ID,
					   cagg->data.mat_hypertable_id);
	if (!policyconf.offset_start.isnull)
		json_add_dim_interval_value(parse_state,
									POL_REFRESH_CONF_KEY_START_OFFSET,
									policyconf.offset_start.type,
									policyconf.offset_start.value);
	else
		ts_jsonb_add_null(parse_state, POL_REFRESH_CONF_KEY_START_OFFSET);
	if (!policyconf.offset_end.isnull)
		json_add_dim_interval_value(parse_state,
									POL_REFRESH_CONF_KEY_END_OFFSET,
									policyconf.offset_end.type,
									policyconf.offset_end.value);
	else
		ts_jsonb_add_null(parse_state, POL_REFRESH_CONF_KEY_END_OFFSET);
	JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	Jsonb *config = JsonbValueToJsonb(result);

	job_id = ts_bgw_job_insert_relation(&application_name,
										&refresh_interval,
										DEFAULT_MAX_RUNTIME,
										JOB_RETRY_UNLIMITED,
										&refresh_interval,
										&proc_schema,
										&proc_name,
										&check_schema,
										&check_name,
										owner_id,
										true,
										fixed_schedule,
										cagg->data.mat_hypertable_id,
										config,
										initial_start,
										timezone);

	PG_RETURN_INT32(job_id);
}

Datum
policy_refresh_cagg_add(PG_FUNCTION_ARGS)
{
	Oid cagg_oid, start_offset_type, end_offset_type;
	Interval refresh_interval;
	bool if_not_exists;
	NullableDatum start_offset, end_offset;

	ts_feature_flag_check(FEATURE_POLICY);

	cagg_oid = PG_GETARG_OID(0);

	if (PG_ARGISNULL(3))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot use NULL refresh_schedule_interval")));

	start_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	start_offset.value = PG_GETARG_DATUM(1);
	start_offset.isnull = PG_ARGISNULL(1);
	end_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
	end_offset.value = PG_GETARG_DATUM(2);
	end_offset.isnull = PG_ARGISNULL(2);
	refresh_interval = *PG_GETARG_INTERVAL_P(3);
	if_not_exists = PG_GETARG_BOOL(4);
	TimestampTz initial_start = PG_ARGISNULL(5) ? DT_NOBEGIN : PG_GETARG_TIMESTAMPTZ(5);
	bool fixed_schedule = !PG_ARGISNULL(5);
	text *timezone = PG_ARGISNULL(6) ? NULL : PG_GETARG_TEXT_PP(6);
	char *valid_timezone = NULL;

	Datum retval;
	/* if users pass in -infinity for initial_start, then use the current_timestamp instead */
	if (fixed_schedule)
	{
		ts_bgw_job_validate_schedule_interval(&refresh_interval);
		if (TIMESTAMP_NOT_FINITE(initial_start))
			initial_start = ts_timer_get_current_timestamp();
	}

	if (timezone != NULL)
		valid_timezone = ts_bgw_job_validate_timezone(PG_GETARG_DATUM(6));

	retval = policy_refresh_cagg_add_internal(cagg_oid,
											  start_offset_type,
											  start_offset,
											  end_offset_type,
											  end_offset,
											  refresh_interval,
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
policy_refresh_cagg_remove_internal(Oid cagg_oid, bool if_exists)
{
	int32 mat_htid;

	ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(cagg_oid);

	if (!cagg)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(cagg_oid))));

	ts_cagg_permissions_check(cagg_oid, GetUserId());
	mat_htid = cagg->data.mat_hypertable_id;
	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_REFRESH_CAGG_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   mat_htid);
	if (jobs == NIL)
	{
		if (!if_exists)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 (errmsg("continuous aggregate policy not found for \"%s\"",
							 get_rel_name(cagg_oid)))));
		else
		{
			ereport(NOTICE,
					(errmsg("continuous aggregate policy not found for \"%s\", skipping",
							get_rel_name(cagg_oid))));
			PG_RETURN_BOOL(false);
		}
	}
	Assert(list_length(jobs) == 1);
	BgwJob *job = linitial(jobs);

	ts_bgw_job_delete_by_id(job->fd.id);
	PG_RETURN_BOOL(true);
}

Datum
policy_refresh_cagg_remove(PG_FUNCTION_ARGS)
{
	Oid cagg_oid = PG_GETARG_OID(0);
	bool if_not_exists = PG_GETARG_BOOL(1); /* Deprecating this argument */
	bool if_exists;

	/* For backward compatibility, we use IF_NOT_EXISTS when IF_EXISTS is not given */
	if_exists = PG_ARGISNULL(2) ? if_not_exists : PG_GETARG_BOOL(2);

	ts_feature_flag_check(FEATURE_POLICY);
	(void) policy_refresh_cagg_remove_internal(cagg_oid, if_exists);
	PG_RETURN_VOID();
}
