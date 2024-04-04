/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <access/xact.h>
#include <utils/jsonb.h>

#include "guc.h"
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

static void
ts_bgw_job_stat_history_insert(BgwJob *job, BgwJobStatHistoryContext *context)
{
	Relation rel = table_open(catalog_get_table_id(ts_catalog_get(), BGW_JOB_STAT_HISTORY),
							  ShareRowExclusiveLock);
	TupleDesc desc = RelationGetDescr(rel);
	NullableDatum values[Natts_bgw_job_stat_history] = { { 0 } };
	CatalogSecurityContext sec_ctx;

	ts_datum_set_int32(Anum_bgw_job_stat_history_job_id, values, job->fd.id, false);
	ts_datum_set_int32(Anum_bgw_job_stat_history_pid, values, 0, true);
	ts_datum_set_timestamptz(Anum_bgw_job_stat_history_execution_start,
							 values,
							 job->job_history.execution_start,
							 false);
	ts_datum_set_timestamptz(Anum_bgw_job_stat_history_execution_finish, values, 0, true);
	ts_datum_set_bool(Anum_bgw_job_stat_history_succeeded, values, false);
	ts_datum_set_jsonb(Anum_bgw_job_stat_history_config, values, job->fd.config);

	if (job->fd.config != NULL)
	{
		ts_datum_set_jsonb(Anum_bgw_job_stat_history_config, values, job->fd.config);
	}

	/* In case of the GUC be disabled all errors are logged then the `context` will contain
	 * `error_data` information */
	if (context != NULL)
	{
		ts_datum_set_timestamptz(Anum_bgw_job_stat_history_execution_finish,
								 values,
								 ts_timer_get_current_timestamp(),
								 false);
		ts_datum_set_jsonb(Anum_bgw_job_stat_history_error_data, values, context->edata);
	}
	else
		ts_datum_set_jsonb(Anum_bgw_job_stat_history_error_data, values, NULL);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	if (job->job_history.id == INVALID_BGW_JOB_STAT_HISTORY_ID)
	{
		/* We need to get a new job id to mark the end later */
		job->job_history.id = ts_catalog_table_next_seq_id(ts_catalog_get(), BGW_JOB_STAT_HISTORY);
	}
	ts_datum_set_int64(Anum_bgw_job_stat_history_id, values, job->job_history.id, false);

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

	ts_bgw_job_stat_history_insert(job, NULL);
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

	if (context->job->fd.config != NULL)
	{
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_config)] =
			JsonbPGetDatum(context->job->fd.config);
		doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_config)] = true;
	}

	if (context->edata != NULL)
	{
		values[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_error_data)] =
			JsonbPGetDatum(context->edata);
		doReplace[AttrNumberGetAttrOffset(Anum_bgw_job_stat_history_error_data)] = true;
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
		ts_bgw_job_stat_history_insert(new_job, &context);
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
