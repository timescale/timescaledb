/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <miscadmin.h>
#include <utils/builtins.h>

#include "compression_api.h"

#include "bgw/job.h"
#include "bgw/job_stat.h"
#include "bgw/timer.h"
#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/job.h"
#include "bgw_policy/job_api.h"
#include "bgw_policy/policies_v2.h"
#include "errors.h"
#include "guc.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "jsonb_utils.h"
#include "policy_utils.h"
#include "utils.h"
#include <utils/elog.h>

/* Default max runtime is unlimited for compress chunks */
#define DEFAULT_MAX_RUNTIME                                                                        \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("0"), InvalidOid, -1))

/* Default retry period for reorder_jobs is currently 1 hour */
#define DEFAULT_RETRY_PERIOD                                                                       \
	DatumGetIntervalP(DirectFunctionCall3(interval_in, CStringGetDatum("1 hour"), InvalidOid, -1))

/* Default max schedule period for the compression policy is 12 hours. The actual schedule period
 * will be chunk_interval/2 if the chunk_interval is < 12 hours. */
#define DEFAULT_MAX_SCHEDULE_PERIOD (int64)(12 * 3600 * 1000 * (int64) 1000)

static Hypertable *validate_compress_chunks_hypertable(Cache *hcache, Oid user_htoid,
													   bool *is_cagg);

int32
policy_compression_get_maxchunks_per_job(const Jsonb *config)
{
	bool found;
	int32 maxchunks =
		ts_jsonb_get_int32_field(config, POL_COMPRESSION_CONF_KEY_MAXCHUNKS_TO_COMPRESS, &found);
	return (found && maxchunks > 0) ? maxchunks : 0;
}

int32
policy_compression_get_hypertable_id(const Jsonb *config)
{
	bool found;
	int32 hypertable_id =
		ts_jsonb_get_int32_field(config, POL_COMPRESSION_CONF_KEY_HYPERTABLE_ID, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find hypertable_id in config for job")));

	return hypertable_id;
}

int64
policy_recompression_get_recompress_after_int(const Jsonb *config)
{
	bool found;
	int64 compress_after =
		ts_jsonb_get_int64_field(config, POL_RECOMPRESSION_CONF_KEY_RECOMPRESS_AFTER, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find %s in config for job",
						POL_RECOMPRESSION_CONF_KEY_RECOMPRESS_AFTER)));

	return compress_after;
}

Interval *
policy_recompression_get_recompress_after_interval(const Jsonb *config)
{
	Interval *interval =
		ts_jsonb_get_interval_field(config, POL_RECOMPRESSION_CONF_KEY_RECOMPRESS_AFTER);

	if (interval == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find %s in config for job",
						POL_RECOMPRESSION_CONF_KEY_RECOMPRESS_AFTER)));

	return interval;
}

Datum
policy_recompression_proc(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() != 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_VOID();

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	policy_recompression_execute(PG_GETARG_INT32(0), PG_GETARG_JSONB_P(1));

	PG_RETURN_VOID();
}

static void
validate_compress_after_type(const Dimension *dim, Oid partitioning_type, Oid compress_after_type)
{
	Oid expected_type = InvalidOid;
	if (IS_INTEGER_TYPE(partitioning_type))
	{
		Oid now_func = ts_get_integer_now_func(dim, false);

		if (!IS_INTEGER_TYPE(compress_after_type) && OidIsValid(now_func))
			expected_type = partitioning_type;
	}
	else if (compress_after_type != INTERVALOID)
	{
		expected_type = INTERVALOID;
	}
	if (OidIsValid(expected_type))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unsupported compress_after argument type, expected type : %s",
						format_type_be(expected_type))));
	}
}

Datum
policy_compression_check(PG_FUNCTION_ARGS)
{
	PolicyCompressionData policy_data;

	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("config must not be NULL")));
	}

	policy_compression_read_and_validate_config(PG_GETARG_JSONB_P(0), &policy_data);
	ts_cache_release(policy_data.hcache);

	PG_RETURN_VOID();
}

/* compression policies are added to hypertables or continuous aggregates */
Datum
policy_compression_add_internal(Oid user_rel_oid, Datum compress_after_datum,
								Oid compress_after_type, Interval *created_before,
								Interval *default_schedule_interval,
								bool user_defined_schedule_interval, bool if_not_exists,
								bool fixed_schedule, TimestampTz initial_start,
								const char *timezone, const char *compress_using)
{
	NameData application_name;
	NameData proc_name, proc_schema, check_schema, check_name, owner;
	int32 job_id;
	Hypertable *hypertable;
	Cache *hcache;
	const Dimension *dim;
	Oid owner_id;
	bool is_cagg = false;

	hcache = ts_hypertable_cache_pin();
	hypertable = validate_compress_chunks_hypertable(hcache, user_rel_oid, &is_cagg);

	/* creation time usage not supported with caggs yet */
	if (is_cagg && created_before != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot use \"compress_created_before\" with continuous aggregate \"%s\" ",
						get_rel_name(user_rel_oid))));

	owner_id = ts_hypertable_permissions_check(user_rel_oid, GetUserId());
	ts_bgw_job_validate_job_owner(owner_id);

	/* Make sure that an existing policy doesn't exist on this hypertable */
	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_COMPRESSION_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   hypertable->fd.id);

	dim = hyperspace_get_open_dimension(hypertable->space, 0);
	Oid partitioning_type = ts_dimension_get_partition_type(dim);

	if (jobs != NIL)
	{
		bool is_equal = false;

		if (!if_not_exists)
		{
			ts_cache_release(hcache);
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_OBJECT),
					 errmsg("compression policy already exists for hypertable or continuous "
							"aggregate \"%s\"",
							get_rel_name(user_rel_oid)),
					 errhint("Set option \"if_not_exists\" to true to avoid error.")));
		}
		Assert(list_length(jobs) == 1);
		BgwJob *existing = linitial(jobs);

		if (OidIsValid(compress_after_type))
			is_equal =
				policy_config_check_hypertable_lag_equality(existing->fd.config,
															POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER,
															partitioning_type,
															compress_after_type,
															compress_after_datum,
															false /* isnull */);
		else
		{
			Assert(created_before != NULL);
			is_equal = policy_config_check_hypertable_lag_equality(
				existing->fd.config,
				POL_COMPRESSION_CONF_KEY_COMPRESS_CREATED_BEFORE,
				partitioning_type,
				INTERVALOID,
				IntervalPGetDatum(created_before),
				false /* isnull */);
		}

		if (is_equal)
		{
			/* If all arguments are the same, do nothing */
			ts_cache_release(hcache);
			ereport(NOTICE,
					(errmsg("compression policy already exists for hypertable \"%s\", skipping",
							get_rel_name(user_rel_oid))));
			PG_RETURN_INT32(-1);
		}
		else
		{
			ts_cache_release(hcache);
			ereport(WARNING,
					(errmsg("compression policy already exists for hypertable \"%s\"",
							get_rel_name(user_rel_oid)),
					 errdetail("A policy already exists with different arguments."),
					 errhint("Remove the existing policy before adding a new one.")));
			PG_RETURN_INT32(-1);
		}
	}

	if (created_before)
	{
		Assert(!OidIsValid(compress_after_type));
		compress_after_type = INTERVALOID;
	}

	if (!is_cagg && IS_INTEGER_TYPE(partitioning_type) && !IS_INTEGER_TYPE(compress_after_type) &&
		created_before == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for parameter %s", POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER),
				 errhint("Integer duration in \"compress_after\" or interval time duration"
						 " in \"compress_created_before\" is required for hypertables with integer "
						 "time dimension.")));

	if (dim && IS_TIMESTAMP_TYPE(ts_dimension_get_partition_type(dim)) &&
		!user_defined_schedule_interval)
	{
		int64 hypertable_schedule_interval = dim->fd.interval_length / 2;

		/* On hypertables with a small chunk_time_interval, schedule the compression job more often
		 * than DEFAULT_MAX_SCHEDULE_PERIOD */
		if (DEFAULT_MAX_SCHEDULE_PERIOD > hypertable_schedule_interval)
		{
			default_schedule_interval = DatumGetIntervalP(
				ts_internal_to_interval_value(hypertable_schedule_interval, INTERVALOID));
		}
		else
		{
			default_schedule_interval = DatumGetIntervalP(
				ts_internal_to_interval_value(DEFAULT_MAX_SCHEDULE_PERIOD, INTERVALOID));
		}
	}

	if (compress_using != NULL && strcmp(compress_using, "heap") != 0 &&
		strcmp(compress_using, TS_HYPERCORE_TAM_NAME) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("can only compress using \"heap\" or \"%s\"", TS_HYPERCORE_TAM_NAME)));

	/* insert a new job into jobs table */
	namestrcpy(&application_name, "Compression Policy");
	namestrcpy(&proc_name, POLICY_COMPRESSION_PROC_NAME);
	namestrcpy(&proc_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&check_name, POLICY_COMPRESSION_CHECK_NAME);
	namestrcpy(&check_schema, FUNCTIONS_SCHEMA_NAME);
	namestrcpy(&owner, GetUserNameFromId(owner_id, false));

	JsonbParseState *parse_state = NULL;

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_int32(parse_state, POL_COMPRESSION_CONF_KEY_HYPERTABLE_ID, hypertable->fd.id);
	validate_compress_after_type(dim, partitioning_type, compress_after_type);

	if (NULL != compress_using)
		ts_jsonb_add_str(parse_state, POL_COMPRESSION_CONF_KEY_COMPRESS_USING, compress_using);

	switch (compress_after_type)
	{
		case INTERVALOID:
			if (created_before)
				ts_jsonb_add_interval(parse_state,
									  POL_COMPRESSION_CONF_KEY_COMPRESS_CREATED_BEFORE,
									  created_before);
			else
				ts_jsonb_add_interval(parse_state,
									  POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER,
									  DatumGetIntervalP(compress_after_datum));
			break;
		case INT2OID:
			ts_jsonb_add_int64(parse_state,
							   POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER,
							   DatumGetInt16(compress_after_datum));
			break;
		case INT4OID:
			ts_jsonb_add_int64(parse_state,
							   POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER,
							   DatumGetInt32(compress_after_datum));
			break;
		case INT8OID:
			ts_jsonb_add_int64(parse_state,
							   POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER,
							   DatumGetInt64(compress_after_datum));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported datatype for %s: %s",
							POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER,
							format_type_be(compress_after_type))));
	}
	/*
	 * If this is a compression policy for a cagg, verify that
	 * compress_after > refresh_start of cagg policy. We do not want
	 * to compress regions that can be refreshed by the cagg policy.
	 */
	if (is_cagg && !policy_refresh_cagg_refresh_start_lt(hypertable->fd.id,
														 compress_after_type,
														 compress_after_datum))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("compress_after value for compression policy should be greater than the "
						"start of the refresh window of continuous aggregate policy for %s",
						get_rel_name(user_rel_oid))));
	}

	JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	Jsonb *config = JsonbValueToJsonb(result);

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
										hypertable->fd.id,
										config,
										initial_start,
										timezone);

	ts_cache_release(hcache);
	PG_RETURN_INT32(job_id);
}

/* compression policies are added to hypertables or continuous aggregates */
Datum
policy_compression_add(PG_FUNCTION_ARGS)
{
	/*
	 * The function is not STRICT but we can't allow required args to be NULL
	 * so we need to act like a strict function in those cases
	 */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(2))
	{
		ts_feature_flag_check(FEATURE_POLICY);
		PG_RETURN_NULL();
	}

	Oid user_rel_oid = PG_GETARG_OID(0);
	Datum compress_after_datum = PG_GETARG_DATUM(1);
	Oid compress_after_type = PG_ARGISNULL(1) ? InvalidOid : get_fn_expr_argtype(fcinfo->flinfo, 1);
	bool if_not_exists = PG_GETARG_BOOL(2);
	bool user_defined_schedule_interval = !(PG_ARGISNULL(3));
	Interval *default_schedule_interval =
		PG_ARGISNULL(3) ? DEFAULT_COMPRESSION_SCHEDULE_INTERVAL : PG_GETARG_INTERVAL_P(3);
	TimestampTz initial_start = PG_ARGISNULL(4) ? DT_NOBEGIN : PG_GETARG_TIMESTAMPTZ(4);
	// if not providing initial_start, then we still get the old behavior
	bool fixed_schedule = !PG_ARGISNULL(4);
	text *timezone = PG_ARGISNULL(5) ? NULL : PG_GETARG_TEXT_PP(5);
	char *valid_timezone = NULL;
	Interval *created_before = PG_GETARG_INTERVAL_P(6);
	Name compress_using = PG_ARGISNULL(7) ? NULL : PG_GETARG_NAME(7);

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	/* compress_after and created_before cannot be specified [or omitted] together */
	if ((PG_ARGISNULL(1) && PG_ARGISNULL(6)) || (!PG_ARGISNULL(1) && !PG_ARGISNULL(6)))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg(
					 "need to specify one of \"compress_after\" or \"compress_created_before\"")));

	/* if users pass in -infinity for initial_start, then use the current_timestamp instead */
	if (fixed_schedule)
	{
		ts_bgw_job_validate_schedule_interval(default_schedule_interval);
		if (TIMESTAMP_NOT_FINITE(initial_start))
			initial_start = ts_timer_get_current_timestamp();
	}

	if (timezone != NULL)
		valid_timezone = ts_bgw_job_validate_timezone(PG_GETARG_DATUM(5));

	Datum retval;
	retval = policy_compression_add_internal(user_rel_oid,
											 compress_after_datum,
											 compress_after_type,
											 created_before,
											 default_schedule_interval,
											 user_defined_schedule_interval,
											 if_not_exists,
											 fixed_schedule,
											 initial_start,
											 valid_timezone,
											 compress_using ? NameStr(*compress_using) : NULL);

	if (!TIMESTAMP_NOT_FINITE(initial_start))
	{
		int32 job_id = DatumGetInt32(retval);
		ts_bgw_job_stat_upsert_next_start(job_id, initial_start);
	}
	return retval;
}

bool
policy_compression_remove_internal(Oid user_rel_oid, bool if_exists)
{
	Hypertable *ht;
	Cache *hcache;

	ht = ts_hypertable_cache_get_cache_and_entry(user_rel_oid, CACHE_FLAG_MISSING_OK, &hcache);
	if (!ht)
	{
		const char *view_name = get_rel_name(user_rel_oid);

		if (!view_name)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("relation is not a hypertable or continuous aggregate")));
		else
		{
			ContinuousAgg *ca = ts_continuous_agg_find_by_relid(user_rel_oid);

			if (!ca)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("relation \"%s\" is not a hypertable or continuous aggregate",
								view_name)));
			ht = ts_hypertable_get_by_id(ca->data.mat_hypertable_id);
		}
	}

	List *jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_COMPRESSION_PROC_NAME,
														   FUNCTIONS_SCHEMA_NAME,
														   ht->fd.id);

	ts_cache_release(hcache);

	if (jobs == NIL)
	{
		if (!if_exists)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("compression policy not found for hypertable \"%s\"",
							get_rel_name(user_rel_oid))));
		else
		{
			ereport(NOTICE,
					(errmsg("compression policy not found for hypertable \"%s\", skipping",
							get_rel_name(user_rel_oid))));
			PG_RETURN_BOOL(false);
		}
	}

	ts_hypertable_permissions_check(user_rel_oid, GetUserId());

	Assert(list_length(jobs) == 1);
	BgwJob *job = linitial(jobs);

	ts_bgw_job_delete_by_id(job->fd.id);

	PG_RETURN_BOOL(true);
}

/* remove compression policy from ht or cagg */
Datum
policy_compression_remove(PG_FUNCTION_ARGS)
{
	Oid user_rel_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);

	ts_feature_flag_check(FEATURE_POLICY);
	TS_PREVENT_FUNC_IF_READ_ONLY();

	return policy_compression_remove_internal(user_rel_oid, if_exists);
}

/* compare cagg job config  with compression job config. If there is an overlap, then
 * throw an error. We do this since we cannot refresh compressed
 * regions. We do not want cont. aggregate jobs to fail
 */

/* If this is a cagg, then mark it as a cagg */
static Hypertable *
validate_compress_chunks_hypertable(Cache *hcache, Oid user_htoid, bool *is_cagg)
{
	ContinuousAggHypertableStatus status;
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, user_htoid, true /* missing_ok */);
	*is_cagg = false;

	if (ht != NULL)
	{
		if (!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("compression not enabled on hypertable \"%s\"",
							get_rel_name(user_htoid)),
					 errhint("Enable compression before adding a compression policy.")));
		}
		status = ts_continuous_agg_hypertable_status(ht->fd.id);
		if ((status == HypertableIsMaterialization || status == HypertableIsMaterializationAndRaw))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot add compression policy to materialized hypertable \"%s\" ",
							get_rel_name(user_htoid)),
					 errhint("Please add the policy to the corresponding continuous aggregate "
							 "instead.")));
		}
	}
	else
	{
		/*check if this is a cont aggregate view */
		int32 mat_id;
		bool found;

		ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(user_htoid);

		if (cagg == NULL)
		{
			ts_cache_release(hcache);
			const char *relname = get_rel_name(user_htoid);
			if (relname)
				ereport(ERROR,
						(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
						 errmsg("\"%s\" is not a hypertable or a continuous aggregate", relname)));
			else
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("object with id \"%u\" not found", user_htoid)));
		}
		*is_cagg = true;
		mat_id = cagg->data.mat_hypertable_id;
		ht = ts_hypertable_get_by_id(mat_id);

		found = policy_refresh_cagg_exists(mat_id);
		if (!found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("continuous aggregate policy does not exist for \"%s\"",
							get_rel_name(user_htoid)),
					 errmsg("setup a refresh policy for \"%s\" before setting up a compression "
							"policy",
							get_rel_name(user_htoid))));
		}
		if (!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("compression not enabled on continuous aggregate \"%s\"",
							get_rel_name(user_htoid)),
					 errhint("Enable compression before adding a compression policy.")));
		}
	}
	Assert(ht != NULL);
	return ht;
}
