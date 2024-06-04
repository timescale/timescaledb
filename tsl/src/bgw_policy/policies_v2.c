/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <miscadmin.h>
#include <parser/parse_coerce.h>
#include <utils/builtins.h>
#include <utils/float.h>

#include "compat/compat.h"
#include "bgw/job.h"
#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/job.h"
#include "bgw_policy/policies_v2.h"
#include "compression_api.h"
#include "errors.h"
#include "funcapi.h"
#include "guc.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "jsonb_utils.h"
#include "policy_utils.h"
#include "utils.h"
#if PG16_GE
#include "nodes/miscnodes.h"
#endif

/* Check if the provided argument is infinity */
bool
ts_if_offset_is_infinity(Datum arg, Oid argtype, bool is_start)
{
	if (OidIsValid(argtype) && argtype != UNKNOWNOID && argtype != FLOAT8OID)
		return false;

	if (argtype != FLOAT8OID)
	{
		double val;
		char *num = DatumGetCString(arg);
#if PG16_LT
		bool have_error = false;
		val = float8in_internal_opt_error(num, NULL, "double precision", num, &have_error);

		if (have_error)
			return false;
#else
		ErrorSaveContext escontext = { .type = T_ErrorSaveContext };
		val = float8in_internal(num, NULL, "double precision", num, (Node *) &escontext);

		if (escontext.error_occurred)
			return false;
#endif

		arg = Float8GetDatum(val);
	}
	float8 result = DatumGetFloat8(arg);
	return ((result == -get_float8_infinity() && is_start) ||
			(result == get_float8_infinity() && !is_start));
}

static void
emit_error(const char *err)
{
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("%s", err)));
}

static int64
offset_to_int64(NullableDatum arg, Oid argtype, Oid partition_type, bool is_start)
{
	if (arg.isnull || ts_if_offset_is_infinity(arg.value, argtype, is_start))
	{
		if (is_start)
			return ts_time_get_max(partition_type);
		else
			return ts_time_get_min(partition_type);
	}
	else
		return interval_to_int64(arg.value, argtype);
}
/*
 * Check different conditions if the requested policy parameters
 * are compatible with each other and then create/update the
 * provided policies.
 */
bool
validate_and_create_policies(policies_info all_policies, bool if_exists)
{
	int refresh_job_id = 0, compression_job_id = 0, retention_job_id = 0;
	int64 refresh_interval = 0, compress_after = 0, drop_after = 0, drop_after_HT = 0;
	int64 start_offset = 0, end_offset = 0, refresh_total_interval = 0;
	List *jobs = NIL;
	BgwJob *orig_ht_reten_job = NULL;

	char *err_gap_refresh = "there are gaps in refresh policy";
	char *err_refresh_compress_overlap = "refresh and compression policies overlap";
	char *err_refresh_reten_overlap = "refresh and retention policies overlap";
	char *err_refresh_reten_ht_overlap = "refresh policy of continuous aggregate and retention "
										 "policy of underlying hypertable overlap";
	char *err_compress_reten_overlap = "compression and retention policies overlap";

	jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_RETENTION_PROC_NAME,
													 FUNCTIONS_SCHEMA_NAME,
													 all_policies.original_HT);
	if (jobs != NIL)
	{
		Assert(list_length(jobs) == 1);
		orig_ht_reten_job = linitial(jobs);
	}

	if (all_policies.refresh)
	{
		start_offset = offset_to_int64(all_policies.refresh->start_offset,
									   all_policies.refresh->start_offset_type,
									   all_policies.partition_type,
									   true);
		end_offset = offset_to_int64(all_policies.refresh->end_offset,
									 all_policies.refresh->end_offset_type,
									 all_policies.partition_type,
									 false);
		refresh_interval =
			interval_to_int64(IntervalPGetDatum(&all_policies.refresh->schedule_interval),
							  INTERVALOID);
		refresh_total_interval = start_offset;
		if (!IS_INTEGER_TYPE(all_policies.partition_type) &&
			refresh_total_interval != ts_time_get_max(all_policies.partition_type))
			refresh_total_interval += refresh_interval;
	}

	if (all_policies.compress)
		compress_after = interval_to_int64(all_policies.compress->compress_after,
										   all_policies.compress->compress_after_type);
	if (all_policies.retention)
		drop_after = interval_to_int64(all_policies.retention->drop_after,
									   all_policies.retention->drop_after_type);

	if (orig_ht_reten_job)
	{
		if (IS_INTEGER_TYPE(all_policies.partition_type))
		{
			bool found_drop_after = false;
			drop_after_HT = ts_jsonb_get_int64_field(orig_ht_reten_job->fd.config,
													 POL_RETENTION_CONF_KEY_DROP_AFTER,
													 &found_drop_after);
		}
		else
		{
			drop_after_HT = interval_to_int64(
				IntervalPGetDatum(ts_jsonb_get_interval_field(orig_ht_reten_job->fd.config,
															  POL_RETENTION_CONF_KEY_DROP_AFTER)),
				INTERVALOID);
		}
	}

	/* Per policy checks */
	if (all_policies.refresh && !IS_INTEGER_TYPE(all_policies.partition_type))
	{
		/*
		 * Check if there are any gaps in the refresh policy. The below code is
		 * a little suspect. But since we are planning to do away with the
		 * add_policies/remove_policies APIs there's no need to spend a lot
		 * of time on fixing it below.
		 */
		int64 refresh_window_size;
		if (start_offset == ts_time_get_max(all_policies.partition_type) ||
			end_offset == ts_time_get_min(all_policies.partition_type) ||
			end_offset > start_offset ||
			pg_sub_s64_overflow(start_offset, end_offset, &refresh_window_size))
			refresh_window_size = start_offset;

		/* if refresh_interval is greater than half of refresh_window_size, then there are gaps */
		if (refresh_interval > (refresh_window_size / 2))
			emit_error(err_gap_refresh);

		/* Disallow refreshed data to be deleted */
		if (orig_ht_reten_job)
		{
			if (refresh_total_interval > drop_after_HT)
				emit_error(err_refresh_reten_ht_overlap);
		}
	}

	/* Cross policy checks */
	if (all_policies.refresh && all_policies.compress)
	{
		/* Check if refresh policy does not overlap with compression */
		if (refresh_total_interval > compress_after)
			emit_error(err_refresh_compress_overlap);
	}
	if (all_policies.refresh && all_policies.retention)
	{
		/* Check if refresh policy does not overlap with retention */
		if (refresh_total_interval > drop_after)
			emit_error(err_refresh_reten_overlap);
	}
	if (all_policies.retention && all_policies.compress)
	{
		/* Check if compression and retention policy overlap */
		if (compress_after == drop_after)
			emit_error(err_compress_reten_overlap);
	}

	/* Create policies as required, delete the old ones if coming from alter */
	if (all_policies.refresh && all_policies.refresh->create_policy)
	{
		if (all_policies.is_alter_policy)
			policy_refresh_cagg_remove_internal(all_policies.rel_oid, if_exists);
		refresh_job_id = policy_refresh_cagg_add_internal(all_policies.rel_oid,
														  all_policies.refresh->start_offset_type,
														  all_policies.refresh->start_offset,
														  all_policies.refresh->end_offset_type,
														  all_policies.refresh->end_offset,
														  all_policies.refresh->schedule_interval,
														  false,
														  false,
														  DT_NOBEGIN,
														  NULL);
	}
	if (all_policies.compress && all_policies.compress->create_policy)
	{
		if (all_policies.is_alter_policy)
			policy_compression_remove_internal(all_policies.rel_oid, if_exists);
		compression_job_id =
			policy_compression_add_internal(all_policies.rel_oid,
											all_policies.compress->compress_after,
											all_policies.compress->compress_after_type,
											NULL,
											DEFAULT_COMPRESSION_SCHEDULE_INTERVAL,
											false,
											if_exists,
											false,
											DT_NOBEGIN,
											NULL,
											all_policies.compress->compress_using);
	}

	if (all_policies.retention && all_policies.retention->create_policy)
	{
		if (all_policies.is_alter_policy)
			policy_retention_remove_internal(all_policies.rel_oid, if_exists);
		retention_job_id =
			policy_retention_add_internal(all_policies.rel_oid,
										  all_policies.retention->drop_after_type,
										  all_policies.retention->drop_after,
										  NULL,
										  (Interval) DEFAULT_RETENTION_SCHEDULE_INTERVAL,
										  false,
										  false,
										  DT_NOBEGIN,
										  NULL);
	}
	return (refresh_job_id || compression_job_id || retention_job_id);
}

Datum
policies_add(PG_FUNCTION_ARGS)
{
	Oid rel_oid;
	bool if_not_exists;
	ContinuousAgg *cagg;
	policies_info all_policies = { .refresh = NULL, .compress = NULL, .retention = NULL };
	refresh_policy ref;
	compression_policy comp;
	retention_policy ret;

	ts_feature_flag_check(FEATURE_POLICY);

	rel_oid = PG_GETARG_OID(0);
	if_not_exists = PG_GETARG_BOOL(1);

	cagg = ts_continuous_agg_find_by_relid(rel_oid);
	if (!cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(rel_oid))));

	all_policies.rel_oid = rel_oid;
	all_policies.is_alter_policy = false;
	all_policies.original_HT = cagg->data.raw_hypertable_id;
	all_policies.partition_type = cagg->partition_type;

	if (!PG_ARGISNULL(2) || !PG_ARGISNULL(3))
	{
		NullableDatum start_offset, end_offset;
		Oid start_offset_type, end_offset_type;

		start_offset.isnull = PG_ARGISNULL(2);
		end_offset.isnull = PG_ARGISNULL(3);
		start_offset.value = PG_GETARG_DATUM(2);
		end_offset.value = PG_GETARG_DATUM(3);
		start_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
		end_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 3);

		refresh_policy tmp = {
			.create_policy = true,
			.start_offset = start_offset,
			.end_offset = end_offset,
			.schedule_interval = *DEFAULT_REFRESH_SCHEDULE_INTERVAL,
			.start_offset_type = start_offset_type,
			.end_offset_type = end_offset_type,
		};
		ref = tmp;
		all_policies.refresh = &ref;
	}
	if (!PG_ARGISNULL(4))
	{
		compression_policy tmp = {
			.create_policy = true,
			.compress_after = PG_GETARG_DATUM(4),
			.compress_after_type = get_fn_expr_argtype(fcinfo->flinfo, 4),
			.compress_using = PG_ARGISNULL(6) ? NULL : NameStr(*PG_GETARG_NAME(6)),
		};
		comp = tmp;
		all_policies.compress = &comp;
	}
	if (!PG_ARGISNULL(5))
	{
		retention_policy tmp = { .create_policy = true,
								 .drop_after = PG_GETARG_DATUM(5),
								 .drop_after_type = get_fn_expr_argtype(fcinfo->flinfo, 5) };
		ret = tmp;
		all_policies.retention = &ret;
	}
	PG_RETURN_BOOL(validate_and_create_policies(all_policies, if_not_exists));
}
Datum
policies_remove(PG_FUNCTION_ARGS)
{
	Oid cagg_oid = PG_GETARG_OID(0);
	ArrayType *policy_array = PG_ARGISNULL(2) ? NULL : PG_GETARG_ARRAYTYPE_P(2);
	bool if_exists = PG_GETARG_BOOL(1);
	Datum *policy;
	int npolicies, failures = 0;
	int i;
	bool success = false;

	ts_feature_flag_check(FEATURE_POLICY);

	if (policy_array == NULL)
		PG_RETURN_BOOL(false);

	deconstruct_array(policy_array, TEXTOID, -1, false, TYPALIGN_INT, &policy, NULL, &npolicies);

	for (i = 0; i < npolicies; i++)
	{
		char *curr_policy = VARDATA(policy[i]);

		if (pg_strcasecmp(curr_policy, POLICY_REFRESH_CAGG_PROC_NAME) == 0)
			success = policy_refresh_cagg_remove_internal(cagg_oid, if_exists);
		else if (pg_strcasecmp(curr_policy, POLICY_COMPRESSION_PROC_NAME) == 0)
			success = policy_compression_remove_internal(cagg_oid, if_exists);
		else if (pg_strncasecmp(curr_policy,
								POLICY_RETENTION_PROC_NAME,
								strlen(POLICY_RETENTION_PROC_NAME)) == 0)
			success = policy_retention_remove_internal(cagg_oid, if_exists);
		else
			ereport(NOTICE, (errmsg("No relevant policy found")));
		if (!success)
			++failures;
	}
	PG_RETURN_BOOL(success && (0 == failures));
}

Datum
policies_remove_all(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_BOOL(false);

	Oid cagg_oid = PG_GETARG_OID(0);
	bool if_exists = PG_GETARG_BOOL(1);
	List *jobs;
	ListCell *lc;
	bool success = if_exists;
	int failures = 0;
	ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(cagg_oid);

	ts_feature_flag_check(FEATURE_POLICY);

	if (!cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(cagg_oid))));

	jobs = ts_bgw_job_find_by_hypertable_id(cagg->data.mat_hypertable_id);
	foreach (lc, jobs)
	{
		BgwJob *job = lfirst(lc);
		if (namestrcmp(&(job->fd.proc_name), POLICY_REFRESH_CAGG_PROC_NAME) == 0)
			success = policy_refresh_cagg_remove_internal(cagg_oid, if_exists);
		else if (namestrcmp(&(job->fd.proc_name), POLICY_COMPRESSION_PROC_NAME) == 0)
			success = policy_compression_remove_internal(cagg_oid, if_exists);
		else if (namestrcmp(&(job->fd.proc_name), POLICY_RETENTION_PROC_NAME) == 0)
			success = policy_retention_remove_internal(cagg_oid, if_exists);
		else
			ereport(NOTICE, (errmsg("Ignoring custom job")));
		if (!success)
			++failures;
	}
	PG_RETURN_BOOL(success && (0 == failures));
}

Datum
policies_alter(PG_FUNCTION_ARGS)
{
	Oid rel_oid = PG_GETARG_OID(0);
	ContinuousAgg *cagg;
	List *jobs;
	ListCell *lc;
	bool if_exists = false, found, start_found, end_found;
	policies_info all_policies = { .refresh = NULL, .compress = NULL, .retention = NULL };
	refresh_policy ref;
	compression_policy comp;
	retention_policy ret;

	ts_feature_flag_check(FEATURE_POLICY);

	cagg = ts_continuous_agg_find_by_relid(rel_oid);
	if (!cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(rel_oid))));

	all_policies.is_alter_policy = true;
	all_policies.rel_oid = rel_oid;
	all_policies.original_HT = cagg->data.raw_hypertable_id;
	all_policies.partition_type = cagg->partition_type;

	jobs = ts_bgw_job_find_by_hypertable_id(cagg->data.mat_hypertable_id);
	if ((NIL == jobs))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no jobs found")));
	foreach (lc, jobs)
	{
		BgwJob *job = lfirst(lc);
		if (namestrcmp(&(job->fd.proc_name), POLICY_REFRESH_CAGG_PROC_NAME) == 0)
		{
			refresh_policy tmp = { .create_policy = false,
								   .end_offset = { 0 },
								   .end_offset_type = InvalidOid,
								   .start_offset = { 0 },
								   .start_offset_type = InvalidOid,
								   .schedule_interval = job->fd.schedule_interval };
			ref = tmp;
			all_policies.refresh = &ref;
			if (IS_INTEGER_TYPE(cagg->partition_type))
			{
				int64 start_value = ts_jsonb_get_int64_field(job->fd.config,
															 POL_REFRESH_CONF_KEY_START_OFFSET,
															 &start_found);
				int64 end_value = ts_jsonb_get_int64_field(job->fd.config,
														   POL_REFRESH_CONF_KEY_END_OFFSET,
														   &end_found);
				/*
				 * If there is job then start_offset has to be there because policy is
				 * not created without it. However if found it to be NULL, then we
				 * want to keep it to NULL in this alter command also.
				 */
				all_policies.refresh->start_offset.isnull = !start_found;
				all_policies.refresh->start_offset_type = cagg->partition_type;
				all_policies.refresh->end_offset.isnull = !end_found;
				all_policies.refresh->end_offset_type = cagg->partition_type;
				switch (all_policies.refresh->start_offset_type)
				{
					case INT2OID:
						all_policies.refresh->start_offset.value =
							Int16GetDatum((int16) start_value);
						all_policies.refresh->end_offset.value = Int16GetDatum((int16) end_value);
						break;
					case INT4OID:
						all_policies.refresh->start_offset.value =
							Int32GetDatum((int32) start_value);
						all_policies.refresh->end_offset.value = Int32GetDatum((int32) end_value);
						break;
					case INT8OID:
						all_policies.refresh->start_offset.value = Int64GetDatum(start_value);
						all_policies.refresh->end_offset.value = Int64GetDatum(end_value);
						break;
					default:
						Assert(0);
				}
			}
			else
			{
				all_policies.refresh->start_offset.value = IntervalPGetDatum(
					ts_jsonb_get_interval_field(job->fd.config, POL_REFRESH_CONF_KEY_START_OFFSET));
				all_policies.refresh->start_offset.isnull =
					(DatumGetIntervalP(all_policies.refresh->start_offset.value) == NULL);
				all_policies.refresh->start_offset_type = INTERVALOID;

				all_policies.refresh->end_offset.value = IntervalPGetDatum(
					ts_jsonb_get_interval_field(job->fd.config, POL_REFRESH_CONF_KEY_END_OFFSET));
				all_policies.refresh->end_offset.isnull =
					(DatumGetIntervalP(all_policies.refresh->end_offset.value) == NULL);
				all_policies.refresh->end_offset_type = INTERVALOID;
			}
		}
		else if (namestrcmp(&(job->fd.proc_name), POLICY_COMPRESSION_PROC_NAME) == 0)
		{
			compression_policy tmp = { .compress_after = 0,
									   .compress_after_type = InvalidOid,
									   .create_policy = false };
			comp = tmp;
			all_policies.compress = &comp;

			if (IS_INTEGER_TYPE(cagg->partition_type))
			{
				int64 compress_value =
					ts_jsonb_get_int64_field(job->fd.config,
											 POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER,
											 &found);
				all_policies.compress->compress_after_type = cagg->partition_type;
				switch (all_policies.compress->compress_after_type)
				{
					case INT2OID:
						all_policies.compress->compress_after =
							Int16GetDatum((int16) compress_value);
						break;
					case INT4OID:
						all_policies.compress->compress_after =
							Int32GetDatum((int32) compress_value);
						break;
					case INT8OID:
						all_policies.compress->compress_after = Int64GetDatum(compress_value);
						break;
					default:
						Assert(0);
				}
			}
			else
			{
				all_policies.compress->compress_after = IntervalPGetDatum(
					ts_jsonb_get_interval_field(job->fd.config,
												POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER));
				all_policies.compress->compress_after_type = INTERVALOID;
			}
		}
		else if (namestrcmp(&(job->fd.proc_name), POLICY_RETENTION_PROC_NAME) == 0)
		{
			retention_policy tmp = { .create_policy = false,
									 .drop_after = 0,
									 .drop_after_type = InvalidOid };
			ret = tmp;
			all_policies.retention = &ret;
			if (IS_INTEGER_TYPE(cagg->partition_type))
			{
				int64 drop_value = ts_jsonb_get_int64_field(job->fd.config,
															POL_RETENTION_CONF_KEY_DROP_AFTER,
															&found);
				all_policies.retention->drop_after_type = cagg->partition_type;
				switch (all_policies.retention->drop_after_type)
				{
					case INT2OID:
						all_policies.retention->drop_after = Int16GetDatum((int16) drop_value);
						break;
					case INT4OID:
						all_policies.retention->drop_after = Int32GetDatum((int32) drop_value);
						break;
					case INT8OID:
						all_policies.retention->drop_after = Int64GetDatum(drop_value);
						break;
					default:
						Assert(0);
				}
			}
			else
			{
				all_policies.retention->drop_after = IntervalPGetDatum(
					ts_jsonb_get_interval_field(job->fd.config, POL_RETENTION_CONF_KEY_DROP_AFTER));
				all_policies.retention->drop_after_type = INTERVALOID;
			}
		}
	}
	if (!PG_ARGISNULL(2))
	{
		if (!all_policies.refresh)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no refresh job found")));
		all_policies.refresh->start_offset.value = PG_GETARG_DATUM(2);
		all_policies.refresh->start_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
		all_policies.refresh->start_offset.isnull = false;
		all_policies.refresh->create_policy = true;
	}
	if (!PG_ARGISNULL(3))
	{
		if (!all_policies.refresh)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no refresh job found")));
		all_policies.refresh->end_offset.value = PG_GETARG_DATUM(3);
		all_policies.refresh->end_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 3);
		all_policies.refresh->end_offset.isnull = false;
		all_policies.refresh->create_policy = true;
	}
	if (!PG_ARGISNULL(4))
	{
		if (!all_policies.compress)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no compress job found")));
		all_policies.compress->compress_after = PG_GETARG_DATUM(4);
		all_policies.compress->compress_after_type = get_fn_expr_argtype(fcinfo->flinfo, 4);
		all_policies.compress->create_policy = true;
	}
	if (!PG_ARGISNULL(5))
	{
		if (!all_policies.retention)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("no retention job found")));
		all_policies.retention->drop_after = PG_GETARG_DATUM(5);
		all_policies.retention->drop_after_type = get_fn_expr_argtype(fcinfo->flinfo, 5);
		all_policies.retention->create_policy = true;
	}

	PG_RETURN_BOOL(validate_and_create_policies(all_policies, if_exists));
}

static void
push_to_json(Oid type, JsonbParseState *parse_state, BgwJob *job, char *json_label,
			 char *show_config)
{
	if (IS_INTEGER_TYPE(type))
	{
		bool found;
		int64 value = ts_jsonb_get_int64_field(job->fd.config, json_label, &found);
		if (!found)
			ts_jsonb_add_null(parse_state, show_config);
		else
			ts_jsonb_add_int64(parse_state, show_config, value);
	}
	else
	{
		Interval *value = ts_jsonb_get_interval_field(job->fd.config, json_label);
		if (value == NULL)
			ts_jsonb_add_null(parse_state, show_config);
		else
			ts_jsonb_add_interval(parse_state, show_config, value);
	}
}

Datum
policies_show(PG_FUNCTION_ARGS)
{
	Oid rel_oid = PG_GETARG_OID(0);
	Oid type;
	ContinuousAgg *cagg;
	ListCell *lc;
	FuncCallContext *funcctx;
	static List *jobs;
	JsonbParseState *parse_state = NULL;

	ts_feature_flag_check(FEATURE_POLICY);

	cagg = ts_continuous_agg_find_by_relid(rel_oid);
	if (!cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(rel_oid))));

	type = IS_TIMESTAMP_TYPE(cagg->partition_type) ? INTERVALOID : cagg->partition_type;

	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Use top-level memory context to preserve the global static list */
		jobs = ts_bgw_job_find_by_hypertable_id(cagg->data.mat_hypertable_id);

		funcctx->user_fctx = list_head(jobs);
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	lc = (ListCell *) funcctx->user_fctx;

	/*
	 * clang doesn't understand that jobs won't be NIL due to the above FIRSTCALL. However
	 * it's also possible that the ts_bgw_job_find_by_hypertable_id function above doesn't
	 * find a job for this hypertable
	 */
	if (lc == NULL || jobs == NULL)
		SRF_RETURN_DONE(funcctx);
	else
	{
		BgwJob *job = lfirst(lc);

		if (!namestrcmp(&(job->fd.proc_name), POLICY_REFRESH_CAGG_PROC_NAME))
		{
			ts_jsonb_add_str(parse_state,
							 SHOW_POLICY_KEY_POLICY_NAME,
							 POLICY_REFRESH_CAGG_PROC_NAME);
			push_to_json(type,
						 parse_state,
						 job,
						 POL_REFRESH_CONF_KEY_START_OFFSET,
						 SHOW_POLICY_KEY_REFRESH_START_OFFSET);
			push_to_json(type,
						 parse_state,
						 job,
						 POL_REFRESH_CONF_KEY_END_OFFSET,
						 SHOW_POLICY_KEY_REFRESH_END_OFFSET);
			ts_jsonb_add_interval(parse_state,
								  SHOW_POLICY_KEY_REFRESH_INTERVAL,
								  &(job->fd.schedule_interval));
		}
		else if (!namestrcmp(&(job->fd.proc_name), POLICY_COMPRESSION_PROC_NAME))
		{
			ts_jsonb_add_str(parse_state,
							 SHOW_POLICY_KEY_POLICY_NAME,
							 POLICY_COMPRESSION_PROC_NAME);
			push_to_json(type,
						 parse_state,
						 job,
						 POL_COMPRESSION_CONF_KEY_COMPRESS_AFTER,
						 SHOW_POLICY_KEY_COMPRESS_AFTER);
			/* POL_COMPRESSION_CONF_KEY_COMPRESS_CREATED_BEFORE not supported with caggs */
			ts_jsonb_add_interval(parse_state,
								  SHOW_POLICY_KEY_COMPRESS_INTERVAL,
								  &(job->fd.schedule_interval));
		}
		else if (!namestrcmp(&(job->fd.proc_name), POLICY_RETENTION_PROC_NAME))
		{
			ts_jsonb_add_str(parse_state, SHOW_POLICY_KEY_POLICY_NAME, POLICY_RETENTION_PROC_NAME);
			push_to_json(type,
						 parse_state,
						 job,
						 POL_RETENTION_CONF_KEY_DROP_AFTER,
						 SHOW_POLICY_KEY_DROP_AFTER);
			/* POL_RETENTION_CONF_KEY_DROP_CREATED_BEFORE not supported with caggs */
			ts_jsonb_add_interval(parse_state,
								  SHOW_POLICY_KEY_RETENTION_INTERVAL,
								  &(job->fd.schedule_interval));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("\"%s\" unsupported proc", NameStr(job->fd.proc_name))));

		JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);

		funcctx->user_fctx = lnext(jobs, (ListCell *) funcctx->user_fctx);
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(JsonbValueToJsonb(result)));
	}
	PG_RETURN_NULL();
}
