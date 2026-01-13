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
	JobResult result;
	BgwJobStatHistoryUpdateType update_type;
	BgwJob *job;
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
bgw_job_stat_history_insert(BgwJobStatHistoryContext *context, bool track_only_errors)
{
	Assert(context != NULL);

	Relation rel = table_open(catalog_get_table_id(ts_catalog_get(), BGW_JOB_STAT_HISTORY),
							  ShareRowExclusiveLock);
	TupleDesc desc = RelationGetDescr(rel);
	NullableDatum values[Natts_bgw_job_stat_history] = { { 0 } };
	CatalogSecurityContext sec_ctx;

	ts_datum_set_int32(Anum_bgw_job_stat_history_job_id, values, context->job->fd.id, false);
	ts_datum_set_timestamptz(Anum_bgw_job_stat_history_execution_start,
							 values,
							 context->job->job_history.execution_start,
							 false);
	if (track_only_errors)
	{
		/* In case of logging only ERRORs */
		ts_datum_set_int32(Anum_bgw_job_stat_history_pid, values, MyProcPid, false);
		ts_datum_set_timestamptz(Anum_bgw_job_stat_history_execution_finish,
								 values,
								 ts_timer_get_current_timestamp(),
								 false);
		ts_datum_set_bool(Anum_bgw_job_stat_history_succeeded, values, false, false);
	}
	else
	{
		/* When tracking history first we INSERT the job without the FINISH execution timestamp,
		 * PID and SUCCEED flag because it will be marked once the job finishes */
		ts_datum_set_int32(Anum_bgw_job_stat_history_pid, values, 0, true);
		ts_datum_set_timestamptz(Anum_bgw_job_stat_history_execution_finish, values, 0, true);
		ts_datum_set_bool(Anum_bgw_job_stat_history_succeeded, values, false, true);
	}

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

static void
bgw_job_stat_history_mark_start(BgwJobStatHistoryContext *context)
{
	/* Don't mark the start in case of the GUC be disabled */
	if (!ts_guc_enable_job_execution_logging)
		return;

	bgw_job_stat_history_insert(context, false);
}

static void
bgw_job_stat_history_update_entry(int64 bgw_job_history_id, tuple_found_func tuple_found,
								  tuple_filter_func tuple_filter, void *data, LOCKMODE lockmode)
{
	if (bgw_job_history_id == INVALID_BGW_JOB_STAT_HISTORY_ID)
		return;

	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_job_stat_history_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT8EQ,
				Int64GetDatum(bgw_job_history_id));

	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, BGW_JOB_STAT_HISTORY),
		.index = catalog_get_index(catalog, BGW_JOB_STAT_HISTORY, BGW_JOB_STAT_HISTORY_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.flags = SCANNER_F_KEEPLOCK,
		.tuple_found = tuple_found,
		.filter = tuple_filter,
		.data = data,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	int num_found = ts_scanner_scan(&scanctx);

	/* We do not want to raise an error in case there is something wrong with history entries */
	if (num_found == 0)
		/* This might happen due to job history retention deleting entries */
		ereport(DEBUG1,
				(errmsg("could not find job stat history entry with id " INT64_FORMAT,
						bgw_job_history_id)));
	else if (num_found > 1)
		ereport(DEBUG1,
				(errmsg("found multiple job stat history entries with id " INT64_FORMAT,
						bgw_job_history_id)));
}

static ScanTupleResult
bgw_job_stat_history_tuple_update(TupleInfo *ti, void *const data)
{
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	BgwJobStatHistoryContext *context = (BgwJobStatHistoryContext *) data;
	Jsonb *job_history_data = NULL;

	Datum values[Natts_bgw_job_stat_history] = { 0 };
	bool nulls[Natts_bgw_job_stat_history] = { 0 };
	bool doReplace[Natts_bgw_job_stat_history] = { 0 };

	switch (context->update_type)
	{
		case JOB_STAT_HISTORY_UPDATE_PID:
		{
			values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_pid)] =
				Int32GetDatum(MyProcPid);
			doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_pid)] = true;
			break;
		}

		case JOB_STAT_HISTORY_UPDATE_END:
		{
			values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_execution_finish)] =
				TimestampTzGetDatum(ts_timer_get_current_timestamp());
			doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_execution_finish)] = true;

			values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_succeeded)] =
				BoolGetDatum((context->result == JOB_SUCCESS));
			doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_succeeded)] = true;

			job_history_data = ts_bgw_job_stat_history_build_data_info(context);

			if (job_history_data != NULL)
			{
				values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_data)] =
					JsonbPGetDatum(job_history_data);
				doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_data)] = true;
			}
			break;
		}

		case JOB_STAT_HISTORY_UPDATE_START:
			pg_unreachable();
			break;
	}

	HeapTuple new_tuple =
		heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls, doReplace);

	ts_catalog_update(ti->scanrel, new_tuple);

	heap_freetuple(new_tuple);

	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
}

static void
bgw_job_stat_history_update(BgwJobStatHistoryContext *context)
{
	/* Don't execute in case of the GUC is false and the job succeeded, because failures are always
	 * logged
	 */
	if (!ts_guc_enable_job_execution_logging && context->result == JOB_SUCCESS)
		return;

	/* Re-read the job information because it can change during the execution by using the
	 * `alter_job` API inside the function/procedure (i.e. job config) */
	BgwJob *new_job = ts_bgw_job_find(context->job->fd.id, CurrentMemoryContext, true);

	/* Set the job history information */
	new_job->job_history = context->job->job_history;

	/* Use the newly loaded job in the current context to use this information to register the
	 * execution history */
	context->job = new_job;

	/* Failures are always logged so in case of the GUC is false and a failure happens then we need
	 * to insert all the information in the job error history table */
	if (!ts_guc_enable_job_execution_logging && context->result != JOB_SUCCESS)
	{
		bgw_job_stat_history_insert(context, true);
	}
	else
	{
		/* Mark the end of the previous inserted start execution */
		bgw_job_stat_history_update_entry(new_job->job_history.id,
										  bgw_job_stat_history_tuple_update,
										  NULL,
										  context,
										  RowExclusiveLock);
	}
}

void
ts_bgw_job_stat_history_update(BgwJobStatHistoryUpdateType update_type, BgwJob *job,
							   JobResult result, Jsonb *edata)
{
	BgwJobStatHistoryContext context = {
		.result = result,
		.update_type = update_type,
		.job = job,
		.edata = edata,
	};

	switch (update_type)
	{
		case JOB_STAT_HISTORY_UPDATE_START:
			bgw_job_stat_history_mark_start(&context);
			break;
		case JOB_STAT_HISTORY_UPDATE_END:
		case JOB_STAT_HISTORY_UPDATE_PID:
			bgw_job_stat_history_update(&context);
			break;
	}
}
