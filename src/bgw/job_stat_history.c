/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <access/xact.h>
#include <utils/jsonb.h>

#include "compat/compat.h"
#include "guc.h"
#include "hypertable.h"
#include "job_stat_history.h"
#include "jsonb_utils.h"
#include "timer.h"
#include "utils.h"

typedef struct BgwJobStatHistoryContext
{
	BgwJob *job;
	JobResult result;
	Jsonb *edata;
} BgwJobStatHistoryContext;

static Jsonb *
build_job_info(BgwJob *job)
{
	JsonbParseState *parse_state = NULL;
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	/* all fields that is possible to change with `alter_job` API */
	ts_jsonb_add_interval(parse_state, "schedule_interval", &job->fd.schedule_interval);
	ts_jsonb_add_interval(parse_state, "max_runtime", &job->fd.max_runtime);
	ts_jsonb_add_int32(parse_state, "max_retries", job->fd.max_retries);
	ts_jsonb_add_interval(parse_state, "retry_period", &job->fd.retry_period);
	ts_jsonb_add_str(parse_state, "proc_schema", NameStr(job->fd.proc_schema));
	ts_jsonb_add_str(parse_state, "proc_name", NameStr(job->fd.proc_name));
	ts_jsonb_add_str(parse_state, "owner", GetUserNameFromId(job->fd.owner, false));
	ts_jsonb_add_bool(parse_state, "scheduled", job->fd.scheduled);
	ts_jsonb_add_bool(parse_state, "fixed_schedule", job->fd.fixed_schedule);

	if (job->fd.initial_start)
		ts_jsonb_add_interval(parse_state, "initial_start", &job->fd.retry_period);

	if (job->fd.hypertable_id != INVALID_HYPERTABLE_ID)
		ts_jsonb_add_int32(parse_state, "hypertable_id", job->fd.hypertable_id);

	if (job->fd.config != NULL)
	{
		/* config information jsonb*/
		JsonbValue value = { 0 };
		JsonbToJsonbValue(job->fd.config, &value);
		ts_jsonb_add_value(parse_state, "config", &value);
	}

	if (strlen(NameStr(job->fd.check_schema)) > 0)
		ts_jsonb_add_str(parse_state, "check_schema", NameStr(job->fd.check_schema));

	if (strlen(NameStr(job->fd.check_name)) > 0)
		ts_jsonb_add_str(parse_state, "check_name", NameStr(job->fd.check_name));

	if (job->fd.timezone != NULL)
		ts_jsonb_add_str(parse_state, "timezone", text_to_cstring(job->fd.timezone));

	return JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL));
}

static Jsonb *
ts_bgw_job_stat_history_build_data_info(BgwJobStatHistoryContext *context)
{
	JsonbParseState *parse_state = NULL;
	JsonbValue value = { 0 };
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);

	Assert(context != NULL && context->job != NULL);

	/* job information jsonb */
	JsonbToJsonbValue(build_job_info(context->job), &value);
	ts_jsonb_add_value(parse_state, "job", &value);

	if (context->edata != NULL)
	{
		/* error information jsonb */
		JsonbToJsonbValue(context->edata, &value);
		ts_jsonb_add_value(parse_state, "error_data", &value);
	}

	return JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL));
}

static void
ts_bgw_job_stat_history_insert(BgwJobStatHistoryContext *context)
{
	Assert(context != NULL);

	Relation rel = table_open(catalog_get_table_id(ts_catalog_get(), BGW_JOB_STAT_HISTORY),
							  ShareRowExclusiveLock);
	TupleDesc desc = RelationGetDescr(rel);
	NullableDatum values[Natts_bgw_job_stat_history] = { { 0 } };
	CatalogSecurityContext sec_ctx;

	ts_datum_set_int32(Anum_bgw_job_stat_history_job_id, values, context->job->fd.id, false);
	ts_datum_set_int32(Anum_bgw_job_stat_history_pid, values, 0, true);
	ts_datum_set_timestamptz(Anum_bgw_job_stat_history_execution_start,
							 values,
							 context->job->job_history.execution_start,
							 false);
	ts_datum_set_timestamptz(Anum_bgw_job_stat_history_execution_finish, values, 0, true);
	ts_datum_set_timestamptz(Anum_bgw_job_stat_history_execution_finish,
							 values,
							 ts_timer_get_current_timestamp(),
							 false);
	ts_datum_set_jsonb(Anum_bgw_job_stat_history_data,
					   values,
					   ts_bgw_job_stat_history_build_data_info(context));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	if (context->job->job_history.id == INVALID_BGW_JOB_STAT_HISTORY_ID)
	{
		/* We need to get a new job id to mark the end later */
		context->job->job_history.id =
			ts_catalog_table_next_seq_id(ts_catalog_get(), BGW_JOB_STAT_HISTORY);
	}
	ts_datum_set_int64(Anum_bgw_job_stat_history_id, values, context->job->job_history.id, false);

	ts_catalog_insert_datums(rel, desc, values);
	ts_catalog_restore_user(&sec_ctx);

	table_close(rel, NoLock);
}

void
ts_bgw_job_stat_history_mark_start(BgwJob *job)
{
	/* Don't mark the start in case of the GUC be disabled */
	if (!ts_guc_enable_job_execution_logging)
		return;

	BgwJobStatHistoryContext context = {
		.job = job,
	};

	ts_bgw_job_stat_history_insert(&context);
}

static bool
bgw_job_stat_history_scan_one(int indexid, ScanKeyData scankey[], int nkeys,
							  tuple_found_func tuple_found, tuple_filter_func tuple_filter,
							  void *data, LOCKMODE lockmode)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, BGW_JOB_STAT_HISTORY),
		.index = catalog_get_index(catalog, BGW_JOB_STAT_HISTORY, indexid),
		.nkeys = nkeys,
		.scankey = scankey,
		.flags = SCANNER_F_KEEPLOCK,
		.tuple_found = tuple_found,
		.filter = tuple_filter,
		.data = data,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	return ts_scanner_scan_one(&scanctx, false, "bgw job stat");
}

static inline bool
bgw_job_stat_history_scan_id(int64 bgw_job_history_id, tuple_found_func tuple_found,
							 tuple_filter_func tuple_filter, void *data, LOCKMODE lockmode)
{
	if (bgw_job_history_id == INVALID_BGW_JOB_STAT_HISTORY_ID)
		return true;

	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_job_stat_history_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT8EQ,
				Int64GetDatum(bgw_job_history_id));

	return bgw_job_stat_history_scan_one(BGW_JOB_STAT_HISTORY_PKEY_IDX,
										 scankey,
										 1,
										 tuple_found,
										 tuple_filter,
										 data,
										 lockmode);
}

static ScanTupleResult
bgw_job_stat_history_tuple_mark_end(TupleInfo *ti, void *const data)
{
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	BgwJobStatHistoryContext *context = (BgwJobStatHistoryContext *) data;

	Datum values[Natts_bgw_job_stat_history] = { 0 };
	bool nulls[Natts_bgw_job_stat_history] = { 0 };
	bool doReplace[Natts_bgw_job_stat_history] = { 0 };

	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_pid)] = Int32GetDatum(MyProcPid);
	doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_pid)] = true;

	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_execution_finish)] =
		TimestampTzGetDatum(ts_timer_get_current_timestamp());
	doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_execution_finish)] = true;

	values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_succeeded)] =
		BoolGetDatum((context->result == JOB_SUCCESS));
	doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_succeeded)] = true;

	Jsonb *job_history_data = ts_bgw_job_stat_history_build_data_info(context);

	if (job_history_data != NULL)
	{
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_data)] =
			JsonbPGetDatum(job_history_data);
		doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_data)] = true;
	}

	HeapTuple new_tuple =
		heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls, doReplace);

	ts_catalog_update(ti->scanrel, new_tuple);

	heap_freetuple(new_tuple);

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
}

void
ts_bgw_job_stat_history_mark_end(BgwJob *job, JobResult result, Jsonb *edata)
{
	/* Don't execute in case of the GUC is false and the job succeeded, because failures are always
	 * logged
	 */
	if (!ts_guc_enable_job_execution_logging && result == JOB_SUCCESS)
		return;

	/* Re-read the job information because it can change during the execution by using the
	 * `alter_job` API inside the function/procedure (i.e. job config) */
	BgwJob *new_job = ts_bgw_job_find(job->fd.id, CurrentMemoryContext, true);

	/* Set the job history information  */
	new_job->job_history = job->job_history;

	BgwJobStatHistoryContext context = {
		.job = new_job,
		.result = result,
		.edata = edata,
	};

	/* Failures are always logged so in case of the GUC is false and a failure happens then we need
	 * to insert all the information in the job error history table */
	if (!ts_guc_enable_job_execution_logging && result != JOB_SUCCESS)
	{
		ts_bgw_job_stat_history_insert(&context);
	}
	else
	{
		/* Mark the end of the previous inserted start execution */
		if (!bgw_job_stat_history_scan_id(new_job->job_history.id,
										  bgw_job_stat_history_tuple_mark_end,
										  NULL,
										  &context,
										  ShareRowExclusiveLock))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unable to find job history " INT64_FORMAT, new_job->job_history.id)));
	}
}
