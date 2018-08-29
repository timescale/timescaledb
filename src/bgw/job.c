#include <postgres.h>
#include <utils/builtins.h>
#include <postmaster/bgworker.h>
#include <access/xact.h>
#include <pgstat.h>
#include <utils/memutils.h>
#include <miscadmin.h>

#include "job.h"
#include "scanner.h"
#include "extension.h"
#include "compat.h"
#include "job_stat.h"
#include "telemetry/telemetry.h"

const char *job_type_names[_MAX_JOB_TYPE] = {
	[JOB_TYPE_VERSION_CHECK] = "telemetry_and_version_check_if_enabled",
	[JOB_TYPE_UNKNOWN] = "unknown"
};

static unknown_job_type_hook_type unknown_job_type_hook = NULL;
static char *job_entrypoint_function_name = "ts_bgw_job_entrypoint";

BackgroundWorkerHandle *
bgw_start_worker(const char *function, const char *name, const char *extra)
{
	BackgroundWorker worker = {
		.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION,
		.bgw_start_time = BgWorkerStart_RecoveryFinished,
		.bgw_restart_time = BGW_NEVER_RESTART,
		.bgw_notify_pid = MyProcPid,
		.bgw_main_arg = ObjectIdGetDatum(MyDatabaseId),
	};
	BackgroundWorkerHandle *handle = NULL;

	StrNCpy(worker.bgw_name, name, BGW_MAXLEN);
	StrNCpy(worker.bgw_library_name, extension_get_so_name(), BGW_MAXLEN);
	StrNCpy(worker.bgw_function_name, function, BGW_MAXLEN);

	Assert(strlen(extra) < BGW_EXTRALEN);
	StrNCpy(worker.bgw_extra, extra, BGW_EXTRALEN);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		return NULL;
	return handle;
}

BackgroundWorkerHandle *
bgw_job_start(BgwJob *job)
{
	char	   *job_id_text;

	job_id_text = DatumGetCString(DirectFunctionCall1(int4out, Int32GetDatum(job->fd.id)));

	return bgw_start_worker(job_entrypoint_function_name, NameStr(job->fd.application_name), job_id_text);
}

static JobType
get_job_type_from_name(Name job_type_name)
{
	int			i;

	for (i = 0; i < _MAX_JOB_TYPE; i++)
		if (namestrcmp(job_type_name, job_type_names[i]) == 0)
			return i;
	return JOB_TYPE_UNKNOWN;
}

static BgwJob *
bgw_job_from_tuple(HeapTuple tuple, size_t alloc_size, MemoryContext mctx)
{
	BgwJob	   *job;

	/* allow for embedding */
	Assert(alloc_size >= sizeof(BgwJob));
	job = MemoryContextAllocZero(mctx, alloc_size);
	memcpy(&job->fd, GETSTRUCT(tuple), sizeof(FormData_bgw_job));
	job->bgw_type = get_job_type_from_name(&job->fd.job_type);

	return job;
}

typedef struct AccumData
{
	List	   *list;
	size_t		alloc_size;
} AccumData;

static bool
bgw_job_accum_tuple_found(TupleInfo *ti, void *data)
{
	AccumData  *list_data = data;
	BgwJob	   *job = bgw_job_from_tuple(ti->tuple, list_data->alloc_size, ti->mctx);
	MemoryContext orig = MemoryContextSwitchTo(ti->mctx);

	list_data->list = lappend(list_data->list, job);

	MemoryContextSwitchTo(orig);
	return true;
}


extern List *
bgw_job_get_all(size_t alloc_size, MemoryContext mctx)
{
	Catalog    *catalog = catalog_get();
	AccumData	list_data = {
		.list = NIL,
		.alloc_size = alloc_size,
	};
	int			num_tuples;
	ScannerCtx	scanctx = {
		.table = catalog->tables[BGW_JOB].id,
		.index = InvalidOid,
		.data = &list_data,
		.tuple_found = bgw_job_accum_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
		.result_mctx = mctx,
	};

	num_tuples = scanner_scan(&scanctx);
	Assert(list_length(list_data.list) == num_tuples);
	return list_data.list;
}

static void
bgw_job_scan_one(int indexid, ScanKeyData scankey[], int nkeys,
				 tuple_found_func tuple_found, tuple_filter_func tuple_filter, void *data, MemoryContext mctx, LOCKMODE lockmode)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanctx = {
		.table = catalog->tables[BGW_JOB].id,
		.index = CATALOG_INDEX(catalog, BGW_JOB, indexid),
		.nkeys = nkeys,
		.scankey = scankey,
		.tuple_found = tuple_found,
		.filter = tuple_filter,
		.data = data,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
		.result_mctx = mctx,
	};

	scanner_scan_one(&scanctx, true, "bgw job");
}

static inline void
bgw_job_scan_job_id(int32 bgw_job_id, tuple_found_func tuple_found, tuple_filter_func tuple_filter,
					void *data, MemoryContext mctx, LOCKMODE lockmode)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_job_stat_pkey_idx_job_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(bgw_job_id));
	bgw_job_scan_one(BGW_JOB_STAT_PKEY_IDX,
					 scankey, 1, tuple_found, tuple_filter, data, mctx, lockmode);
}

static bool
bgw_job_tuple_found(TupleInfo *ti, void *const data)
{
	BgwJob	  **job_pp = data;

	*job_pp = bgw_job_from_tuple(ti->tuple, sizeof(BgwJob), ti->mctx);

	/* return true since used with scan one */
	return true;
}

BgwJob *
bgw_job_find(int32 bgw_job_id, MemoryContext mctx)
{
	BgwJob	   *job_stat = NULL;

	bgw_job_scan_job_id(bgw_job_id, bgw_job_tuple_found, NULL, &job_stat, mctx, AccessShareLock);

	return job_stat;
}

bool
bgw_job_execute(BgwJob *job)
{
	switch (job->bgw_type)
	{
		case JOB_TYPE_VERSION_CHECK:
			telemetry_main();
			return true;
		case JOB_TYPE_UNKNOWN:
			if (unknown_job_type_hook != NULL)
				return unknown_job_type_hook(job);
			elog(ERROR, "unknown job type \"%s\"", NameStr(job->fd.job_type));
			break;
		case _MAX_JOB_TYPE:
			elog(ERROR, "unknown job type \"%s\"", NameStr(job->fd.job_type));
			break;
	}
	Assert(false);
	return false;
}

bool
bgw_job_has_timeout(BgwJob *job)
{
	Interval	zero_val = {.time = 0,};

	return DatumGetBool(DirectFunctionCall2(interval_gt, IntervalPGetDatum(&job->fd.max_runtime), IntervalPGetDatum(&zero_val)));
}

/* Return the timestamp at which to kill the job due to a timeout */
TimestampTz
bgw_job_timeout_at(BgwJob *job, TimestampTz start_time)
{
	/* timestamptz plus interval */
	return DatumGetTimestampTz(DirectFunctionCall2(
												   timestamptz_pl_interval,
												   TimestampTzGetDatum(start_time),
												   IntervalPGetDatum(&job->fd.max_runtime)));
}


TS_FUNCTION_INFO_V1(ts_bgw_job_entrypoint);

extern Datum
ts_bgw_job_entrypoint(PG_FUNCTION_ARGS)
{
	Oid			db_oid = DatumGetObjectId(MyBgworkerEntry->bgw_main_arg);
	int32		job_id = Int32GetDatum(DirectFunctionCall1(int4in, CStringGetDatum(MyBgworkerEntry->bgw_extra)));
	BgwJob	   *job;
	JobResult	res = JOB_FAILURE;

	BackgroundWorkerBlockSignals();
	/* Setup any signal handlers here */
	BackgroundWorkerUnblockSignals();

	elog(DEBUG1, "started background job %d", job_id);

	BackgroundWorkerInitializeConnectionByOid(db_oid, InvalidOid);

	StartTransactionCommand();
	job = bgw_job_find(job_id, TopMemoryContext);
	CommitTransactionCommand();

	if (job == NULL)
		elog(ERROR, "job %d not found", job_id);

	pgstat_report_appname(NameStr(job->fd.application_name));

	PG_TRY();
	{
		res = bgw_job_execute(job);
		/* The job is responsible for committing or aborting it's own txns */
		if (IsTransactionState())
			elog(ERROR, "TimescaleDB background job \"%s\" failed to end the transaction", NameStr(job->fd.application_name));


	}
	PG_CATCH();
	{
		if (IsTransactionState())
			/* If there was an error, rollback what was done before the error */
			AbortCurrentTransaction();
		StartTransactionCommand();

		/*
		 * Note that the mark_start happens in the scheduler right before the
		 * job is launched
		 */
		bgw_job_stat_mark_end(job, JOB_FAILURE);
		CommitTransactionCommand();

		/*
		 * the rethrow will log the error; but also log which job threw the
		 * error
		 */
		elog(DEBUG1, "job %d threw an error", job_id);
		PG_RE_THROW();
	}
	PG_END_TRY();

	Assert(!IsTransactionState());

	StartTransactionCommand();

	/*
	 * Note that the mark_start happens in the scheduler right before the job
	 * is launched
	 */
	bgw_job_stat_mark_end(job, res);
	CommitTransactionCommand();

	elog(DEBUG1, "exiting job %d with %s", job_id, (res == JOB_SUCCESS ? "success" : "failure"));

	PG_RETURN_VOID();
}

void
bgw_job_set_unknown_job_type_hook(unknown_job_type_hook_type hook)
{
	unknown_job_type_hook = hook;
}

void
bgw_job_set_job_entrypoint_function_name(char *func_name)
{
	job_entrypoint_function_name = func_name;
}
