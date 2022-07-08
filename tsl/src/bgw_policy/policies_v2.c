/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <parser/parse_coerce.h>

#include "compression_api.h"
#include "errors.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "policy_utils.h"
#include "utils.h"
#include "jsonb_utils.h"
#include "bgw/job.h"
#include "bgw_policy/job.h"
#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/policies_v2.h"
#include "funcapi.h"
#include "compat/compat.h"

/* Check if the provided argument is infinity */
bool
ts_if_offset_is_infinity(Datum arg, Oid argtype, Oid timetype)
{
	int64 ret;

	arg = ts_time_datum_convert_arg(arg, &argtype, timetype);
	if (argtype == INTERVALOID)
		ret = ts_interval_value_to_internal(arg, argtype);
	else
		ret = ts_time_value_to_internal(arg, argtype);
	return (ret == PG_INT64_MIN || ret == PG_INT64_MAX);
}

void
emit_error(const char *err)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				errmsg(err)));
}

int64
offset_to_int64(NullableDatum arg, Oid argtype, Oid partition_type, bool is_start)
{
	if (arg.isnull || ts_if_offset_is_infinity(arg.value, argtype, partition_type))
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
valdiate_and_create_policies(policies_info all_policies, bool if_exists)
{
	int refresh_job_id = 0, compression_job_id = 0, retention_job_id = 0;
	bool error = false;
	int64 refresh_interval, compress_after, drop_after, drop_after_HT;
	int64 start_offset, end_offset, refresh_window_size, refresh_total_interval;
	List *jobs = NIL;
	BgwJob *orig_ht_reten_job = NULL;

	char *err_gap_refresh = "there are gaps in refresh policy";
	char *err_refresh_compress_overlap = "refresh and compression policies overlap";
	char *err_refresh_reten_overlap = "refresh and retention policies overlap";
	char *err_refresh_reten_ht_overlap = "do not allow refreshed data to be deleted";
	char *err_compress_reten_overlap = "compression and retention policies overlap";

	jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_RETENTION_PROC_NAME, EXPERIMENTAL_SCHEMA_NAME, all_policies.original_HT);
	if (jobs != NIL)
	{
		Assert(list_length(jobs) == 1);
		orig_ht_reten_job = linitial(jobs);
	}

	if (all_policies.refresh)
	{
		start_offset = offset_to_int64(all_policies.refresh->start_offset, all_policies.refresh->start_offset_type, all_policies.partition_type, true);
		end_offset = offset_to_int64(all_policies.refresh->end_offset, all_policies.refresh->end_offset_type, all_policies.partition_type, false);
		refresh_interval =  interval_to_int64(IntervalPGetDatum(&all_policies.refresh->schedule_interval), INTERVALOID);
		refresh_total_interval = start_offset == ts_time_get_max(all_policies.partition_type)? start_offset: start_offset + refresh_interval;
	}
	if(IS_INTEGER_TYPE(all_policies.partition_type))
	{
		if (all_policies.refresh)
			refresh_total_interval = start_offset;
		if (all_policies.compress)
			compress_after = DatumGetInt64(all_policies.compress->compress_after);
		if (all_policies.retention)
			drop_after = DatumGetInt64(all_policies.retention->drop_after);
		if (orig_ht_reten_job)
			drop_after_HT = ts_jsonb_get_int64_field(orig_ht_reten_job->fd.config, CONFIG_KEY_DROP_AFTER, false);
	}
	else
	{
		if (all_policies.compress)
			compress_after = interval_to_int64(all_policies.compress->compress_after, all_policies.compress->compress_after_type);
		if (all_policies.retention)
			drop_after = interval_to_int64(all_policies.retention->drop_after, all_policies.retention->drop_after_type);
		if (orig_ht_reten_job)
			drop_after_HT = interval_to_int64(IntervalPGetDatum(ts_jsonb_get_interval_field(orig_ht_reten_job->fd.config, CONFIG_KEY_DROP_AFTER)), INTERVALOID);
	}

	/* Per policy checks */
	if (all_policies.refresh && !IS_INTEGER_TYPE(all_policies.partition_type))
	{
		/* Check if there are any gaps in the refresh policy */
		refresh_window_size = (start_offset == ts_time_get_max(all_policies.partition_type) || end_offset == ts_time_get_min(all_policies.partition_type))? start_offset: start_offset - end_offset;

		/* if refresh_interval is greater than half of refresh_window_size, then there are gaps */
		if (refresh_interval > refresh_window_size/2)
			emit_error(err_gap_refresh);

		/* Disallow refreshed data to be deleted */
		if (orig_ht_reten_job)
		{
			if (refresh_total_interval > drop_after_HT)
				emit_error(err_refresh_reten_ht_overlap);
		}
	}

	/* Cross policy checks */
	if  (all_policies.refresh && all_policies.compress)
	{
		/* Check if refresh policy does not overlap with compression */
		if(IS_INTEGER_TYPE(all_policies.partition_type))
		{
			if (refresh_total_interval > compress_after)
				error = true;
		}
		else
		{
			if (refresh_total_interval > compress_after)
				error = true;
		}
		if (error)
			emit_error(err_refresh_compress_overlap);
	}
	if (all_policies.refresh && all_policies.retention)
	{
		/* Check if refresh policy does not overlap with compression */
		if(IS_INTEGER_TYPE(all_policies.partition_type))
		{
			if (refresh_total_interval > drop_after)
				error = true;
		}
		else
		{
			if (refresh_total_interval > drop_after)
				error = true;
		}
		if (error)
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
														false, true);

	}
	if (all_policies.compress && all_policies.compress->create_policy)
	{
		if (all_policies.is_alter_policy)
			policy_compression_remove_internal(all_policies.rel_oid, if_exists);
		compression_job_id = policy_compression_add_internal(all_policies.rel_oid,
															all_policies.compress->compress_after,
															all_policies.compress->compress_after_type,
															DEFAULT_COMPRESSION_SCHEDULE_INTERVAL,
															false,
															if_exists);
	}
	if (all_policies.retention && all_policies.retention->create_policy)
	{
		if (all_policies.is_alter_policy)
			policy_retention_remove_internal(all_policies.rel_oid, if_exists);
		retention_job_id =
			policy_retention_add_internal(all_policies.rel_oid, all_policies.retention->drop_after_type, all_policies.retention->drop_after,
										(Interval) DEFAULT_RETENTION_SCHEDULE_INTERVAL, false);
	}
	return (refresh_job_id || compression_job_id || retention_job_id);
}

Datum
policies_add(PG_FUNCTION_ARGS)
{
	Oid rel_oid;
	bool if_not_exists;
	ContinuousAgg *cagg;
	policies_info all_policies = {
									.refresh = NULL,
									.compress = NULL,
									.retention = NULL
								};

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


		if (!start_offset.isnull)
			start_offset.isnull = ts_if_offset_is_infinity(start_offset.value, start_offset_type, cagg->partition_type);
		if (!end_offset.isnull)
			end_offset.isnull = ts_if_offset_is_infinity(end_offset.value, end_offset_type, cagg->partition_type);

		refresh_policy ref = {
			.create_policy = true,
			.start_offset = start_offset,
			.end_offset = end_offset,
			.schedule_interval = *DEFAULT_REFRESH_SCHEDULE_INTERVAL,
			.start_offset_type = start_offset_type,
			.end_offset_type = end_offset_type,
		};
		all_policies.refresh = &ref;
	}
	if (!PG_ARGISNULL(4))
	{
		compression_policy comp ={
			.create_policy = true,
			.compress_after = PG_GETARG_DATUM(4),
			.compress_after_type = get_fn_expr_argtype(fcinfo->flinfo, 4)
		};
		all_policies.compress = &comp;
	}
	if (!PG_ARGISNULL(5))
	{
		retention_policy ret ={
			.create_policy = true,
			.drop_after = PG_GETARG_DATUM(5),
			.drop_after_type = get_fn_expr_argtype(fcinfo->flinfo, 5)
		};
		all_policies.retention = &ret;

	}
	PG_RETURN_BOOL(valdiate_and_create_policies(all_policies, if_not_exists));
}
Datum
policies_remove(PG_FUNCTION_ARGS)
{
	Oid cagg_oid = PG_GETARG_OID(0);
	ArrayType *policy_array = PG_ARGISNULL(2) ? NULL : PG_GETARG_ARRAYTYPE_P(2);
	bool if_exists = PG_GETARG_BOOL(1);
	Datum *policy;
	int npolicies;
	int i;
	bool success = false;

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
	}
	PG_RETURN_BOOL(success);
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
	bool success = false;
	ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(cagg_oid);

	if (!cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(cagg_oid))));


	jobs = ts_bgw_job_find_by_hypertable_id(cagg->data.mat_hypertable_id);
	foreach(lc, jobs)
	{
		BgwJob *job = lfirst(lc);
		if (namestrcmp(&(job->fd.proc_name), POLICY_REFRESH_CAGG_PROC_NAME) == 0)
			success = policy_refresh_cagg_remove_internal(cagg_oid, if_exists);
		else if (namestrcmp(&(job->fd.proc_name), POLICY_COMPRESSION_PROC_NAME) == 0)
			success = policy_compression_remove_internal(cagg_oid, if_exists);
		else if (namestrcmp(&(job->fd.proc_name),
								POLICY_RETENTION_PROC_NAME) == 0)
			success = policy_retention_remove_internal(cagg_oid, if_exists);
		else
			ereport(NOTICE, (errmsg("No relevant policy found")));
	}
	PG_RETURN_BOOL(success);
}

Datum
policies_alter(PG_FUNCTION_ARGS)
{
	Oid rel_oid = PG_GETARG_OID(0);
	ContinuousAgg *cagg;
	List *jobs;
	ListCell *lc;
	bool if_exists = false, found;
	policies_info all_policies = {
									.refresh = NULL,
									.compress = NULL,
									.retention = NULL
								};

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
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("no jobs found")));
	foreach(lc, jobs)
	{
		BgwJob *job = lfirst(lc);
		if (namestrcmp(&(job->fd.proc_name), POLICY_REFRESH_CAGG_PROC_NAME) == 0)
		{
			refresh_policy ref = {
				.create_policy = false,
				.end_offset =  {},
				.end_offset_type = InvalidOid,
				.start_offset = {},
				.start_offset_type = InvalidOid,
				.schedule_interval = job->fd.schedule_interval};
			all_policies.refresh = &ref;
			if (IS_INTEGER_TYPE(cagg->partition_type))
			{
				int64 start_value =
					ts_jsonb_get_int64_field(job->fd.config, CONFIG_KEY_START_OFFSET, &found);
				int64 end_value =
					ts_jsonb_get_int64_field(job->fd.config, CONFIG_KEY_END_OFFSET, &found);
			   /*
				* If there is job then start_offset has to be there because policy is
				* not created without it. However if found it to be NULL, then we
				* want to keep it to NULL in this alter command also.
				*/
				all_policies.refresh->start_offset.isnull = !found;
				all_policies.refresh->start_offset_type = cagg->partition_type;
				all_policies.refresh->end_offset.isnull = !found;
				all_policies.refresh->end_offset_type = cagg->partition_type;
				switch (all_policies.refresh->start_offset_type)
				{
					case INT2OID:
						all_policies.refresh->start_offset.value = Int16GetDatum((int16) start_value);
						all_policies.refresh->end_offset.value = Int16GetDatum((int16) end_value);
						break;
					case INT4OID:
						all_policies.refresh->start_offset.value = Int32GetDatum((int32) start_value);
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
					ts_jsonb_get_interval_field(job->fd.config, CONFIG_KEY_START_OFFSET));
				all_policies.refresh->start_offset.isnull = (DatumGetIntervalP(all_policies.refresh->start_offset.value) == NULL);
				all_policies.refresh->start_offset_type = INTERVALOID;

				all_policies.refresh->end_offset.value = IntervalPGetDatum(
					ts_jsonb_get_interval_field(job->fd.config, CONFIG_KEY_END_OFFSET));
				all_policies.refresh->end_offset.isnull = (DatumGetIntervalP(all_policies.refresh->end_offset.value) == NULL);
				all_policies.refresh->end_offset_type = INTERVALOID;
			}
		}
		else if (namestrcmp(&(job->fd.proc_name), POLICY_COMPRESSION_PROC_NAME) == 0)
		{
			compression_policy comp = {
				.compress_after = 0,
				.compress_after_type = InvalidOid,
				.create_policy = false
			};
			all_policies.compress = &comp;

			if (IS_INTEGER_TYPE(cagg->partition_type))
			{
				int64 compress_value = ts_jsonb_get_int64_field(job->fd.config, CONFIG_KEY_COMPRESS_AFTER, &found);
				all_policies.compress->compress_after_type = cagg->partition_type;
				switch (all_policies.compress->compress_after_type)
				{
					case INT2OID:
						all_policies.compress->compress_after = Int16GetDatum((int16)compress_value);
						break;
					case INT4OID:
						all_policies.compress->compress_after = Int32GetDatum((int32)compress_value);
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
				all_policies.compress->compress_after = IntervalPGetDatum(ts_jsonb_get_interval_field(job->fd.config, CONFIG_KEY_COMPRESS_AFTER));
				all_policies.compress->compress_after_type = INTERVALOID;
			}
		}
		else if (namestrcmp(&(job->fd.proc_name),
								POLICY_RETENTION_PROC_NAME) == 0)
		{
			retention_policy ret = {
				.create_policy = false,
				.drop_after = 0,
				.drop_after_type = InvalidOid
			};
			all_policies.retention = &ret;
			if (IS_INTEGER_TYPE(cagg->partition_type))
			{
				int64 drop_value = ts_jsonb_get_int64_field(job->fd.config, CONFIG_KEY_DROP_AFTER, &found);
				all_policies.retention->drop_after_type = cagg->partition_type;
				switch (all_policies.retention->drop_after_type)
				{
					case INT2OID:
						all_policies.retention->drop_after = Int16GetDatum((int16)drop_value);
						break;
					case INT4OID:
						all_policies.retention->drop_after = Int32GetDatum((int32)drop_value);
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
				all_policies.retention->drop_after = IntervalPGetDatum(ts_jsonb_get_interval_field(job->fd.config, CONFIG_KEY_DROP_AFTER));
				all_policies.retention->drop_after_type = INTERVALOID;
			}
		}
	}
	if (!PG_ARGISNULL(2))
	{
		if (!all_policies.refresh)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("no refresh job found")));
		all_policies.refresh->start_offset.value = PG_GETARG_DATUM(2);
		all_policies.refresh->start_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
		all_policies.refresh->start_offset.isnull = false;
		all_policies.refresh->create_policy = true;
	}
	if (!PG_ARGISNULL(3))
	{
		if (!all_policies.refresh)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("no refresh job found")));
		all_policies.refresh->end_offset.value = PG_GETARG_DATUM(3);
		all_policies.refresh->end_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 3);
		all_policies.refresh->end_offset.isnull = false;
		all_policies.refresh->create_policy = true;
	}
	if (!PG_ARGISNULL(4))
	{
		if (!all_policies.compress)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("no compress job found")));
		all_policies.compress->compress_after = PG_GETARG_DATUM(4);
		all_policies.compress->compress_after_type = get_fn_expr_argtype(fcinfo->flinfo, 4);
		all_policies.compress->create_policy = true;
	}
	if (!PG_ARGISNULL(5))
	{
		if (!all_policies.retention)
			ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("no retention job found")));
		all_policies.retention->drop_after = PG_GETARG_DATUM(5);
		all_policies.retention->drop_after_type = get_fn_expr_argtype(fcinfo->flinfo, 5);
		all_policies.retention->create_policy = true;
	}

	PG_RETURN_BOOL(valdiate_and_create_policies(all_policies, if_exists));
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

	if (lc == NULL)
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
						 CONFIG_KEY_START_OFFSET,
						 SHOW_POLICY_KEY_REFRESH_START_OFFSET);
			push_to_json(type,
						 parse_state,
						 job,
						 CONFIG_KEY_END_OFFSET,
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
						 CONFIG_KEY_COMPRESS_AFTER,
						 SHOW_POLICY_KEY_COMPRESS_AFTER);
			ts_jsonb_add_interval(parse_state,
								  SHOW_POLICY_KEY_COMPRESS_INTERVAL,
								  &(job->fd.schedule_interval));
		}
		else if (!namestrcmp(&(job->fd.proc_name), POLICY_RETENTION_PROC_NAME))
		{
			ts_jsonb_add_str(parse_state, SHOW_POLICY_KEY_POLICY_NAME, POLICY_RETENTION_PROC_NAME);
			push_to_json(type, parse_state, job, CONFIG_KEY_DROP_AFTER, SHOW_POLICY_KEY_DROP_AFTER);
			ts_jsonb_add_interval(parse_state,
								  SHOW_POLICY_KEY_RETENTION_INTERVAL,
								  &(job->fd.schedule_interval));
		}

		JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);

		funcctx->user_fctx = lnext_compat(jobs, (ListCell *) funcctx->user_fctx);
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(JsonbValueToJsonb(result)));
	}
	PG_RETURN_NULL();
}
