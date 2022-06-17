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

Datum
policies_add(PG_FUNCTION_ARGS)
{
	Oid rel_oid;
	bool if_not_exists;
	int refresh_job_id = 0, compression_job_id = 0, retention_job_id = 0;

	rel_oid = PG_GETARG_OID(0);
	if_not_exists = PG_GETARG_BOOL(1);

	if (!PG_ARGISNULL(2) || !PG_ARGISNULL(3) || !PG_ARGISNULL(4))
	{
		NullableDatum start_offset, end_offset;
		Interval refresh_interval = *DEFAULT_REFRESH_SCHEDULE_INTERVAL;
		Oid start_offset_type, end_offset_type;

		start_offset.value = PG_GETARG_DATUM(2);
		start_offset.isnull = PG_ARGISNULL(2);
		start_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
		end_offset.value = PG_GETARG_DATUM(3);
		end_offset.isnull = PG_ARGISNULL(3);
		end_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 3);

		refresh_job_id = policy_refresh_cagg_add_internal(rel_oid,
														  start_offset_type,
														  start_offset,
														  end_offset_type,
														  end_offset,
														  refresh_interval,
														  if_not_exists);
	}
	if (!PG_ARGISNULL(4))
	{
		Datum compress_after_datum = PG_GETARG_DATUM(4);
		Oid compress_after_type = get_fn_expr_argtype(fcinfo->flinfo, 4);
		compression_job_id = policy_compression_add_internal(rel_oid,
															 compress_after_datum,
															 compress_after_type,
															 DEFAULT_COMPRESSION_SCHEDULE_INTERVAL,
															 false, if_not_exists);
	}
	if (!PG_ARGISNULL(5))
	{
		Datum drop_after_datum = PG_GETARG_DATUM(5);
		Oid drop_after_type = get_fn_expr_argtype(fcinfo->flinfo, 5);

		retention_job_id = policy_retention_add_internal(rel_oid,
														 drop_after_type,
														 drop_after_datum,
														 (Interval)DEFAULT_RETENTION_SCHEDULE_INTERVAL,
														 if_not_exists);
	}
	PG_RETURN_BOOL(refresh_job_id || compression_job_id || retention_job_id);
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
	bool if_exists = false, found;
	int refresh_job_id = 0, compression_job_id = 0, retention_job_id = 0;

	cagg = ts_continuous_agg_find_by_relid(rel_oid);
	if (!cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a continuous aggregate", get_rel_name(rel_oid))));

	if (!PG_ARGISNULL(2) || !PG_ARGISNULL(3))
	{
		Interval refresh_interval;
		NullableDatum start_offset, end_offset;
		Oid start_offset_type, end_offset_type;

		jobs = ts_bgw_job_find_by_proc_and_hypertable_id(POLICY_REFRESH_CAGG_PROC_NAME,
														 INTERNAL_SCHEMA_NAME,
														 cagg->data.mat_hypertable_id);
		/* refresh_interval must either exist in policy config or be given by the user */
		if ((NIL == jobs))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot use NULL refresh_schedule_interval")));

		BgwJob *job = (NIL == jobs) ? NULL : linitial(jobs);

		refresh_interval = job->fd.schedule_interval;

		policy_refresh_cagg_remove_internal(rel_oid, if_exists);

		if (job && PG_ARGISNULL(2))
		{
			if (IS_INTEGER_TYPE(cagg->partition_type))
			{
				int64 value =
					ts_jsonb_get_int64_field(job->fd.config, CONFIG_KEY_START_OFFSET, &found);
				start_offset.isnull = !found;
				start_offset_type = cagg->partition_type;
				switch (start_offset_type)
				{
					case INT2OID:
						start_offset.value = Int16GetDatum((int16) value);
						break;
					case INT4OID:
						start_offset.value = Int32GetDatum((int32) value);
						break;
					case INT8OID:
						start_offset.value = Int64GetDatum(value);
						break;
					default:
						Assert(0);
				}
			}
			else
			{
				start_offset.value = IntervalPGetDatum(
					ts_jsonb_get_interval_field(job->fd.config, CONFIG_KEY_START_OFFSET));
				start_offset.isnull = (DatumGetIntervalP(start_offset.value) == NULL);
				start_offset_type = INTERVALOID;
			}
		}
		else
		{
			start_offset.value = PG_GETARG_DATUM(2);
			start_offset.isnull = PG_ARGISNULL(2);
			start_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
		}
		if (job && PG_ARGISNULL(3))
		{
			if (IS_INTEGER_TYPE(cagg->partition_type))
			{
				int64 value =
					ts_jsonb_get_int64_field(job->fd.config, CONFIG_KEY_END_OFFSET, &found);
				end_offset.isnull = !found;
				end_offset_type = cagg->partition_type;
				switch (end_offset_type)
				{
					case INT2OID:
						end_offset.value = Int16GetDatum((int16) value);
						break;
					case INT4OID:
						end_offset.value = Int32GetDatum((int32) value);
						break;
					case INT8OID:
						end_offset.value = Int64GetDatum(value);
						break;
					default:
						Assert(0);
				}
			}
			else
			{
				end_offset.value = IntervalPGetDatum(
					ts_jsonb_get_interval_field(job->fd.config, CONFIG_KEY_END_OFFSET));
				end_offset.isnull = (DatumGetIntervalP(end_offset.value) == NULL);
				end_offset_type = INTERVALOID;
			}
		}
		else
		{
			end_offset.value = PG_GETARG_DATUM(3);
			end_offset.isnull = PG_ARGISNULL(3);
			end_offset_type = get_fn_expr_argtype(fcinfo->flinfo, 3);
		}

		refresh_job_id = policy_refresh_cagg_add_internal(rel_oid,
														  start_offset_type,
														  start_offset,
														  end_offset_type,
														  end_offset,
														  refresh_interval,
														  false);
	}
	if (!PG_ARGISNULL(4))
	{
		Datum compress_after_datum = PG_GETARG_DATUM(4);
		Oid compress_after_type = get_fn_expr_argtype(fcinfo->flinfo, 4);

		policy_compression_remove_internal(rel_oid, if_exists);
		compression_job_id = policy_compression_add_internal(rel_oid,
															 compress_after_datum,
															 compress_after_type,
															 false,
															 DEFAULT_COMPRESSION_SCHEDULE_INTERVAL,
															 false);
	}
	if (!PG_ARGISNULL(5))
	{
		Datum drop_after_datum = PG_GETARG_DATUM(5);
		Oid drop_after_type = get_fn_expr_argtype(fcinfo->flinfo, 5);

		policy_retention_remove_internal(rel_oid, if_exists);
		retention_job_id =
			policy_retention_add_internal(rel_oid, drop_after_type, drop_after_datum,
										  (Interval) DEFAULT_RETENTION_SCHEDULE_INTERVAL, false);
	}

	PG_RETURN_BOOL(refresh_job_id || compression_job_id || retention_job_id);
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
		}
		else if (!namestrcmp(&(job->fd.proc_name), POLICY_RETENTION_PROC_NAME))
		{
			ts_jsonb_add_str(parse_state, SHOW_POLICY_KEY_POLICY_NAME, POLICY_RETENTION_PROC_NAME);
			push_to_json(type, parse_state, job, CONFIG_KEY_DROP_AFTER, SHOW_POLICY_KEY_DROP_AFTER);
		}

		JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);

		funcctx->user_fctx = lnext_compat(jobs, (ListCell *) funcctx->user_fctx);
		SRF_RETURN_NEXT(funcctx, PointerGetDatum(JsonbValueToJsonb(result)));
	}
	PG_RETURN_NULL();
}
