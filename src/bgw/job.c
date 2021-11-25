/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <miscadmin.h>
#include <pgstat.h>
#include <access/xact.h>
#include <catalog/pg_authid.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <tcop/tcopprot.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/syscache.h>
#include <utils/timestamp.h>
#include <storage/lock.h>
#include <storage/proc.h>
#include <storage/procarray.h>
#include <storage/sinvaladt.h>
#include <utils/acl.h>
#include <utils/elog.h>
#include <utils/jsonb.h>

#include "job.h"
#include "config.h"
#include "scanner.h"
#include "extension.h"
#include "compat/compat.h"
#include "job_stat.h"
#include "license_guc.h"
#include "utils.h"
#ifdef USE_TELEMETRY
#include "telemetry/telemetry.h"
#endif
#include "bgw_policy/chunk_stats.h"
#include "bgw_policy/policy.h"
#include "scan_iterator.h"
#include "bgw/scheduler.h"

#include <cross_module_fn.h>

#define TELEMETRY_INITIAL_NUM_RUNS 12

static scheduler_test_hook_type scheduler_test_hook = NULL;
static char *job_entrypoint_function_name = "ts_bgw_job_entrypoint";
#ifdef USE_TELEMETRY
static bool is_telemetry_job(BgwJob *job);
#endif

typedef enum JobLockLifetime
{
	SESSION_LOCK = 0,
	TXN_LOCK,
} JobLockLifetime;

BackgroundWorkerHandle *
ts_bgw_job_start(BgwJob *job, Oid user_uid)
{
	int32 job_id = Int32GetDatum(job->fd.id);
	StringInfo si = makeStringInfo();
	BackgroundWorkerHandle *bgw_handle;

	/* Changing this requires changes to ts_bgw_job_entrypoint */
	appendStringInfo(si, "%u %d", user_uid, job_id);

	bgw_handle = ts_bgw_start_worker(job_entrypoint_function_name,
									 NameStr(job->fd.application_name),
									 si->data);

	pfree(si->data);
	pfree(si);
	return bgw_handle;
}

static BgwJob *
bgw_job_from_tupleinfo(TupleInfo *ti, size_t alloc_size)
{
	BgwJob *job;
	bool should_free;
	HeapTuple tuple;
	bool isnull;
	MemoryContext old_ctx;
	Datum value;

	/*
	 * allow for embedding with arbitrary alloc_size, which means we can't use
	 * the STRUCT_FROM_TUPLE macro
	 */
	Assert(alloc_size >= sizeof(BgwJob));
	job = MemoryContextAllocZero(ti->mctx, alloc_size);
	tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	memcpy(job, GETSTRUCT(tuple), sizeof(FormData_bgw_job));

	if (should_free)
		heap_freetuple(tuple);

	/*
	 * GETSTRUCT does not work with variable length types and NULLs so we have
	 * to do special handling for hypertable_id and the jsonb column
	 */
	value = slot_getattr(ti->slot, Anum_bgw_job_hypertable_id, &isnull);
	job->fd.hypertable_id = isnull ? 0 : DatumGetInt32(value);

	value = slot_getattr(ti->slot, Anum_bgw_job_config, &isnull);
	old_ctx = MemoryContextSwitchTo(ti->mctx);
	job->fd.config = isnull ? NULL : DatumGetJsonbP(value);
	MemoryContextSwitchTo(old_ctx);

	return job;
}

typedef struct AccumData
{
	List *list;
	size_t alloc_size;
} AccumData;

static ScanTupleResult
bgw_job_accum_tuple_found(TupleInfo *ti, void *data)
{
	AccumData *list_data = data;
	BgwJob *job = bgw_job_from_tupleinfo(ti, list_data->alloc_size);
	MemoryContext orig = MemoryContextSwitchTo(ti->mctx);

	list_data->list = lappend(list_data->list, job);

	MemoryContextSwitchTo(orig);
	return SCAN_CONTINUE;
}

static ScanFilterResult
bgw_job_filter_scheduled(const TupleInfo *ti, void *data)
{
	bool isnull;
	Datum scheduled = slot_getattr(ti->slot, Anum_bgw_job_scheduled, &isnull);
	Assert(!isnull);

	return DatumGetBool(scheduled);
}

/* This function is meant to be used by the scheduler only
 * it does not include the config field which saves us from
 * detoasting and makes memory management in the scheduler
 * simpler as otherwise the config field would have to be
 * freed separately when freeing jobs which would prevent
 * the use of list_free_deep.
 * The scheduler does not need the config field only the
 * individual jobs do.
 * The scheduler requires jobs to be sorted by id
 * which is guaranteed by the index scan on the primary key
 */
List *
ts_bgw_job_get_scheduled(size_t alloc_size, MemoryContext mctx)
{
	MemoryContext old_ctx;
	List *jobs = NIL;
	ScanIterator iterator = ts_scan_iterator_create(BGW_JOB, AccessShareLock, mctx);

	iterator.ctx.index = catalog_get_index(ts_catalog_get(), BGW_JOB, BGW_JOB_PKEY_IDX);
	iterator.ctx.filter = bgw_job_filter_scheduled;

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool should_free, isnull;
		Datum value;

		BgwJob *job = MemoryContextAllocZero(mctx, alloc_size);
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		memcpy(job, GETSTRUCT(tuple), sizeof(FormData_bgw_job));

		if (should_free)
			heap_freetuple(tuple);

#ifdef USE_TELEMETRY
		/* ignore telemetry jobs if telemetry is disabled */
		if (!ts_telemetry_on() && is_telemetry_job(job))
		{
			pfree(job);
			continue;
		}
#endif

		/* handle NULL columns */
		value = slot_getattr(ti->slot, Anum_bgw_job_hypertable_id, &isnull);
		job->fd.hypertable_id = isnull ? 0 : DatumGetInt32(value);

		/* skip config since the scheduler doesnt need it this saves us
		 * from detoasting and simplifies freeing job lists in the
		 * scheduler as otherwise the config field would have to be
		 * freed separately when freeing a job
		 */
		job->fd.config = NULL;

		old_ctx = MemoryContextSwitchTo(mctx);
		jobs = lappend(jobs, job);
		MemoryContextSwitchTo(old_ctx);
	}

	return jobs;
}

List *
ts_bgw_job_get_all(size_t alloc_size, MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	AccumData list_data = {
		.list = NIL,
		.alloc_size = sizeof(BgwJob),
	};
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, BGW_JOB),
		.data = &list_data,
		.tuple_found = bgw_job_accum_tuple_found,
		.lockmode = AccessShareLock,
		.result_mctx = mctx,
		.scandirection = ForwardScanDirection,
	};

	ts_scanner_scan(&scanctx);
	return list_data.list;
}

static void
init_scan_by_proc_name(ScanKeyData *scankey, const char *proc_name)
{
	ScanKeyInit(scankey,
				Anum_bgw_job_proc_hypertable_id_idx_proc_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(proc_name));
}

static void
init_scan_by_proc_schema(ScanKeyData *scankey, const char *proc_schema)
{
	ScanKeyInit(scankey,
				Anum_bgw_job_proc_hypertable_id_idx_proc_schema,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(proc_schema));
}

static void
init_scan_by_hypertable_id(ScanKeyData *scankey, int32 hypertable_id)
{
	ScanKeyInit(scankey,
				Anum_bgw_job_proc_hypertable_id_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));
}

List *
ts_bgw_job_find_by_proc_and_hypertable_id(const char *proc_name, const char *proc_schema,
										  int32 hypertable_id)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[3];
	AccumData list_data = {
		.list = NIL,
		.alloc_size = sizeof(BgwJob),
	};
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, BGW_JOB),
		.index = catalog_get_index(ts_catalog_get(), BGW_JOB, BGW_JOB_PROC_HYPERTABLE_ID_IDX),
		.data = &list_data,
		.scankey = scankey,
		.nkeys = sizeof(scankey) / sizeof(*scankey),
		.tuple_found = bgw_job_accum_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	init_scan_by_proc_schema(&scankey[0], proc_schema);
	init_scan_by_proc_name(&scankey[1], proc_name);
	init_scan_by_hypertable_id(&scankey[2], hypertable_id);

	ts_scanner_scan(&scanctx);
	return list_data.list;
}

List *
ts_bgw_job_find_by_proc(const char *proc_name, const char *proc_schema)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[2];
	AccumData list_data = {
		.list = NIL,
		.alloc_size = sizeof(BgwJob),
	};
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, BGW_JOB),
		.index = catalog_get_index(ts_catalog_get(), BGW_JOB, BGW_JOB_PROC_HYPERTABLE_ID_IDX),
		.data = &list_data,
		.scankey = scankey,
		.nkeys = sizeof(scankey) / sizeof(*scankey),
		.tuple_found = bgw_job_accum_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	init_scan_by_proc_schema(&scankey[0], proc_schema);
	init_scan_by_proc_name(&scankey[1], proc_name);

	ts_scanner_scan(&scanctx);
	return list_data.list;
}

List *
ts_bgw_job_find_by_hypertable_id(int32 hypertable_id)
{
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	AccumData list_data = {
		.list = NIL,
		.alloc_size = sizeof(BgwJob),
	};
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, BGW_JOB),
		.index = catalog_get_index(ts_catalog_get(), BGW_JOB, BGW_JOB_PROC_HYPERTABLE_ID_IDX),
		.data = &list_data,
		.scankey = scankey,
		.nkeys = sizeof(scankey) / sizeof(*scankey),
		.tuple_found = bgw_job_accum_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	init_scan_by_hypertable_id(&scankey[0], hypertable_id);

	ts_scanner_scan(&scanctx);
	return list_data.list;
}

static void
init_scan_by_job_id(ScanIterator *iterator, int32 job_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(), BGW_JOB, BGW_JOB_PKEY_IDX);
	ts_scan_iterator_scan_key_init(iterator,
								   Anum_bgw_job_pkey_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(job_id));
}

/* Lock a job tuple using an advisory lock. Advisory job locks are
 * used to lock the job row while a job is running to prevent a job from being
 * modified while in the middle of a run. This lock should be taken before
 * bgw_job table lock to avoid deadlocks.
 *
 * We use an advisory lock instead of a tuple lock because we want the lock on the job id
 * and not on the tid of the row (in case it is vacuumed or updated in some way). We don't
 * want the job modified while it is running for safety reasons. Finally, we use this lock
 * to be able to send a signal to the PID of the running job. This is used by delete because,
 * a job deletion sends a SIGINT to the running job to cancel it.
 *
 * We acquire a SHARE lock on the job during scheduling and when the job is running so that it
 * cannot be deleted during those times and an EXCLUSIVE lock when deleting.
 *
 * returns whether or not the lock was obtained (false return only possible if block==false)
 */

static bool
lock_job(int32 job_id, LOCKMODE mode, JobLockLifetime lock_type, LOCKTAG *tag, bool block)
{
	/* Use a special pseudo-random field 4 value to avoid conflicting with user-advisory-locks */
	TS_SET_LOCKTAG_ADVISORY(*tag, MyDatabaseId, job_id, 0);

	return LockAcquire(tag, mode, lock_type == SESSION_LOCK, !block) != LOCKACQUIRE_NOT_AVAIL;
}

static BgwJob *
ts_bgw_job_find_with_lock(int32 bgw_job_id, MemoryContext mctx, LOCKMODE tuple_lock_mode,
						  JobLockLifetime lock_type, bool block, bool *got_lock)
{
	/* Take a share lock on the table to prevent concurrent data changes during scan. This lock will
	 * be released after the scan */
	ScanIterator iterator = ts_scan_iterator_create(BGW_JOB, ShareLock, mctx);
	int num_found = 0;
	BgwJob *job = NULL;
	LOCKTAG tag;

	/* take advisory lock before relation lock */
	if (!(*got_lock = lock_job(bgw_job_id, tuple_lock_mode, lock_type, &tag, block)))
	{
		/* return NULL if lock could not be acquired */
		Assert(!block);
		return NULL;
	}

	init_scan_by_job_id(&iterator, bgw_job_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		job = bgw_job_from_tupleinfo(ti, sizeof(BgwJob));
		Assert(num_found == 0);
		num_found++;
	}

	return job;
}

/* Take a lock on the job for the duration of the txn. This prevents
 *  the job from being deleted.
 *
 *  Returns true if the job is found ( we block till we can acquire a lock
 *                               so we will always lock here)
 *          false if the job is missing.
 */
bool
ts_bgw_job_get_share_lock(int32 bgw_job_id, MemoryContext mctx)
{
	bool got_lock;
	/* note the mode here is equivalent to FOR SHARE row locks */
	BgwJob *job = ts_bgw_job_find_with_lock(bgw_job_id,
											mctx,
											RowShareLock,
											TXN_LOCK,
											true /* block */
											,
											&got_lock);
	if (job != NULL)
	{
		if (!got_lock)
		{
			/* since we blocked for a lock , this is an unexpected condition */
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not acquire share lock for job=%d", bgw_job_id)));
		}
		pfree(job);
		return true;
	}
	return false;
}

BgwJob *
ts_bgw_job_find(int32 bgw_job_id, MemoryContext mctx, bool fail_if_not_found)
{
	ScanIterator iterator = ts_scan_iterator_create(BGW_JOB, AccessShareLock, mctx);
	int num_found = 0;
	BgwJob *job = NULL;

	init_scan_by_job_id(&iterator, bgw_job_id);

	ts_scanner_foreach(&iterator)
	{
		Assert(num_found == 0);
		job = bgw_job_from_tupleinfo(ts_scan_iterator_tuple_info(&iterator), sizeof(BgwJob));
		num_found++;
	}

	if (num_found == 0 && fail_if_not_found)
		elog(ERROR, "job %d not found", bgw_job_id);

	return job;
	;
}

static void
get_job_lock_for_delete(int32 job_id)
{
	LOCKTAG tag;
	bool got_lock;

	/* Try getting an exclusive lock on the tuple in a non-blocking manner. Note this is the
	 * equivalent of a row-based FOR UPDATE lock */
	got_lock = lock_job(job_id,
						AccessExclusiveLock,
						TXN_LOCK,
						&tag,
						/* block */ false);
	if (!got_lock)
	{
		/* If I couldn't get a lock, try killing the background worker that's running the job.
		 * This is probably not bulletproof but best-effort is good enough here. */
		VirtualTransactionId *vxid = GetLockConflicts(&tag, AccessExclusiveLock, NULL);
		PGPROC *proc;

		if (VirtualTransactionIdIsValid(*vxid))
		{
			proc = BackendIdGetProc(vxid->backendId);
			if (proc != NULL && proc->isBackgroundWorker)
			{
				elog(NOTICE,
					 "cancelling the background worker for job %d (pid %d)",
					 job_id,
					 proc->pid);
				DirectFunctionCall1(pg_cancel_backend, Int32GetDatum(proc->pid));
			}
		}

		/* We have to grab this lock before proceeding so grab it in a blocking manner now */
		got_lock = lock_job(job_id,
							AccessExclusiveLock,
							TXN_LOCK,
							&tag,
							/* block */ true);
	}
	Assert(got_lock);
}

static ScanTupleResult
bgw_job_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;
	bool isnull;
	Datum datum = slot_getattr(ti->slot, Anum_bgw_job_id, &isnull);
	int32 job_id = DatumGetInt32(datum);

	Assert(!isnull);

	/* Also delete the bgw_stat entry */
	ts_bgw_job_stat_delete(job_id);

	/* Delete any stats in bgw_policy_chunk_stats related to this job */
	ts_bgw_policy_chunk_stats_delete_row_only_by_job_id(job_id);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_catalog_restore_user(&sec_ctx);

	return SCAN_CONTINUE;
}

static bool
bgw_job_delete_scan(ScanKeyData *scankey, int32 job_id)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx;

	/* get job lock before relation lock */
	get_job_lock_for_delete(job_id);

	scanctx = (ScannerCtx){
		.table = catalog_get_table_id(catalog, BGW_JOB),
		.index = catalog_get_index(catalog, BGW_JOB, BGW_JOB_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.data = NULL,
		.limit = 1,
		.tuple_found = bgw_job_tuple_delete,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
		.result_mctx = CurrentMemoryContext,
	};

	return ts_scanner_scan(&scanctx);
}

/*
 * This function will try to delete the job identified by `job_id`. If the job is currently running,
 * this function will send a `SIGINT` to the job, and wait for the job to terminate before deleting
 * the job. In the event that it cannot  send the signal (for instance, if the job is not in a
 * transaction, we have no way to send the signal), it will still wait for the job to terminate and
 * release the job lock, or will ERROR due to a lock or deadlock timeout. In this case,  the user
 * has to  manually determine the `pid` of the BGW and send an `SIGINT` or a `SIGKILL`.
 */
bool
ts_bgw_job_delete_by_id(int32 job_id)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_job_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));

	return bgw_job_delete_scan(scankey, job_id);
}

/* This function only updates the fields modifiable with alter_job. */
static ScanTupleResult
bgw_job_tuple_update_by_id(TupleInfo *ti, void *const data)
{
	BgwJob *updated_job = (BgwJob *) data;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	HeapTuple new_tuple;

	Datum values[Natts_bgw_job] = { 0 };
	bool isnull[Natts_bgw_job] = { 0 };
	bool repl[Natts_bgw_job] = { 0 };

	Datum old_schedule_interval =
		slot_getattr(ti->slot, Anum_bgw_job_schedule_interval, &isnull[0]);
	Assert(!isnull[0]);

	/* when we update the schedule interval, modify the next start time as well*/
	if (!DatumGetBool(DirectFunctionCall2(interval_eq,
										  old_schedule_interval,
										  IntervalPGetDatum(&updated_job->fd.schedule_interval))))
	{
		BgwJobStat *stat = ts_bgw_job_stat_find(updated_job->fd.id);

		if (stat != NULL)
		{
			TimestampTz next_start = DatumGetTimestampTz(
				DirectFunctionCall2(timestamptz_pl_interval,
									TimestampTzGetDatum(stat->fd.last_finish),
									IntervalPGetDatum(&updated_job->fd.schedule_interval)));
			/* allow DT_NOBEGIN for next_start here through allow_unset=true in the case that
			 * last_finish is DT_NOBEGIN,
			 * This means the value is counted as unset which is what we want */
			ts_bgw_job_stat_update_next_start(updated_job->fd.id, next_start, true);
		}
		values[AttrNumberGetAttrOffset(Anum_bgw_job_schedule_interval)] =
			IntervalPGetDatum(&updated_job->fd.schedule_interval);
		repl[AttrNumberGetAttrOffset(Anum_bgw_job_schedule_interval)] = true;
	}

	values[AttrNumberGetAttrOffset(Anum_bgw_job_max_runtime)] =
		IntervalPGetDatum(&updated_job->fd.max_runtime);
	repl[AttrNumberGetAttrOffset(Anum_bgw_job_max_runtime)] = true;

	values[AttrNumberGetAttrOffset(Anum_bgw_job_max_retries)] =
		Int32GetDatum(updated_job->fd.max_retries);
	repl[AttrNumberGetAttrOffset(Anum_bgw_job_max_retries)] = true;

	values[AttrNumberGetAttrOffset(Anum_bgw_job_retry_period)] =
		IntervalPGetDatum(&updated_job->fd.retry_period);
	repl[AttrNumberGetAttrOffset(Anum_bgw_job_retry_period)] = true;

	values[AttrNumberGetAttrOffset(Anum_bgw_job_scheduled)] =
		BoolGetDatum(updated_job->fd.scheduled);
	repl[AttrNumberGetAttrOffset(Anum_bgw_job_scheduled)] = true;

	repl[AttrNumberGetAttrOffset(Anum_bgw_job_config)] = true;
	if (updated_job->fd.config)
	{
		ts_cm_functions->job_config_check(&updated_job->fd.proc_schema,
										  &updated_job->fd.proc_name,
										  updated_job->fd.config);
		values[AttrNumberGetAttrOffset(Anum_bgw_job_config)] =
			JsonbPGetDatum(updated_job->fd.config);
	}
	else
		isnull[AttrNumberGetAttrOffset(Anum_bgw_job_config)] = true;

	new_tuple = heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, isnull, repl);

	ts_catalog_update(ti->scanrel, new_tuple);

	heap_freetuple(new_tuple);
	if (should_free)
		heap_freetuple(tuple);

	return SCAN_DONE;
}

/*
 * Overwrite job with specified job_id with the given fields
 *
 * This function only updates the fields modifiable with alter_job.
 */
bool
ts_bgw_job_update_by_id(int32 job_id, BgwJob *job)
{
	ScanKeyData scankey[1];
	Catalog *catalog = ts_catalog_get();
	ScanTupLock scantuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};
	ScannerCtx scanctx = { .table = catalog_get_table_id(catalog, BGW_JOB),
						   .index = catalog_get_index(catalog, BGW_JOB, BGW_JOB_PKEY_IDX),
						   .nkeys = 1,
						   .scankey = scankey,
						   .data = job,
						   .limit = 1,
						   .tuple_found = bgw_job_tuple_update_by_id,
						   .lockmode = RowExclusiveLock,
						   .scandirection = ForwardScanDirection,
						   .result_mctx = CurrentMemoryContext,
						   .tuplock = &scantuplock };

	ScanKeyInit(&scankey[0],
				Anum_bgw_job_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));

	return ts_scanner_scan(&scanctx);
}

static void
ts_bgw_job_check_max_retries(BgwJob *job)
{
	BgwJobStat *job_stat;

	job_stat = ts_bgw_job_stat_find(job->fd.id);

	/* stop to execute failing jobs after reached the "max_retries" option */
	if (job->fd.max_retries > 0 && job_stat->fd.consecutive_failures >= job->fd.max_retries)
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("job %d reached max_retries after %d consecutive failures",
						job->fd.id,
						job_stat->fd.consecutive_failures),
				 errdetail("Job %d unscheduled as max_retries reached %d, consecutive failures %d.",
						   job->fd.id,
						   job->fd.max_retries,
						   job_stat->fd.consecutive_failures),
				 errhint("Use alter_job(%d, scheduled => TRUE) SQL function to reschedule.",
						 job->fd.id)));

		if (job->fd.scheduled)
		{
			job->fd.scheduled = false;
			ts_bgw_job_update_by_id(job->fd.id, job);
		}
	}
}

void
ts_bgw_job_permission_check(BgwJob *job)
{
	Oid owner_oid = get_role_oid(NameStr(job->fd.owner), false);

	if (!has_privs_of_role(GetUserId(), owner_oid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("insufficient permissions to alter job %d", job->fd.id)));
}

void
ts_bgw_job_validate_job_owner(Oid owner)
{
	HeapTuple role_tup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(owner));
	Form_pg_authid rform = (Form_pg_authid) GETSTRUCT(role_tup);

	if (!rform->rolcanlogin)
	{
		ReleaseSysCache(role_tup);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("permission denied to start background process as role \"%s\"",
						rform->rolname.data),
				 errhint("Hypertable owner must have LOGIN permission to run background tasks.")));
	}
	ReleaseSysCache(role_tup);
}

#ifdef USE_TELEMETRY
static bool
is_telemetry_job(BgwJob *job)
{
	return namestrcmp(&job->fd.proc_schema, INTERNAL_SCHEMA_NAME) == 0 &&
		   namestrcmp(&job->fd.proc_name, "policy_telemetry") == 0;
}
#endif

bool
ts_bgw_job_execute(BgwJob *job)
{
#ifdef USE_TELEMETRY
	if (is_telemetry_job(job))
	{
		/*
		 * In the first 12 hours, we want telemetry to ping every
		 * hour. After that initial period, we default to the
		 * schedule_interval listed in the job table.
		 */
		Interval one_hour = { .time = 1 * USECS_PER_HOUR };
		return ts_bgw_job_run_and_set_next_start(job,
												 ts_telemetry_main_wrapper,
												 TELEMETRY_INITIAL_NUM_RUNS,
												 &one_hour);
	}
#endif

#ifdef TS_DEBUG
	if (scheduler_test_hook != NULL)
		return scheduler_test_hook(job);
#endif

	return ts_cm_functions->job_execute(job);
}

bool
ts_bgw_job_has_timeout(BgwJob *job)
{
	Interval zero_val = {
		.time = 0,
	};

	return DatumGetBool(DirectFunctionCall2(interval_gt,
											IntervalPGetDatum(&job->fd.max_runtime),
											IntervalPGetDatum(&zero_val)));
}

/* Return the timestamp at which to kill the job due to a timeout */
TimestampTz
ts_bgw_job_timeout_at(BgwJob *job, TimestampTz start_time)
{
	/* timestamptz plus interval */
	return DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
												   TimestampTzGetDatum(start_time),
												   IntervalPGetDatum(&job->fd.max_runtime)));
}

static void handle_sigterm(SIGNAL_ARGS)
{
	/*
	 * do not use a level >= ERROR because we don't want to exit here but
	 * rather only during CHECK_FOR_INTERRUPTS
	 */
	ereport(LOG,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating TimescaleDB background job \"%s\" due to administrator command",
					MyBgworkerEntry->bgw_name)));
	die(postgres_signal_arg);
}

TS_FUNCTION_INFO_V1(ts_bgw_job_entrypoint);

static void
zero_guc(const char *guc_name)
{
	int config_change =
		set_config_option(guc_name, "0", PGC_SUSET, PGC_S_SESSION, GUC_ACTION_SET, true, 0, false);

	if (config_change == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("guc \"%s\" does not exist", guc_name)));
	else if (config_change < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("could not set \"%s\" guc", guc_name)));
}

extern Datum
ts_bgw_job_entrypoint(PG_FUNCTION_ARGS)
{
	Oid db_oid = DatumGetObjectId(MyBgworkerEntry->bgw_main_arg);
	Oid user_uid;
	int32 job_id;
	BgwJob *job;
	JobResult res = JOB_FAILURE;
	bool got_lock;

	if (sscanf(MyBgworkerEntry->bgw_extra, "%u %d", &user_uid, &job_id) != 2)
		elog(ERROR, "job entrypoint got invalid bgw_extra");

	BackgroundWorkerBlockSignals();
	/* Setup any signal handlers here */

	/*
	 * do not use the default `bgworker_die` sigterm handler because it does
	 * not respect critical sections
	 */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	elog(DEBUG1, "started background job %d", job_id);

	BackgroundWorkerInitializeConnectionByOid(db_oid, user_uid, 0);

	ts_license_enable_module_loading();

	StartTransactionCommand();
	/* Grab a session lock on the job row to prevent concurrent deletes. Lock is released
	 * when the job process exits */
	job = ts_bgw_job_find_with_lock(job_id,
									TopMemoryContext,
									RowShareLock,
									SESSION_LOCK,
									/* block */ true,
									&got_lock);
	CommitTransactionCommand();

	if (job == NULL)
		elog(ERROR, "job %d not found when running the background worker", job_id);

	pgstat_report_appname(NameStr(job->fd.application_name));

	PG_TRY();
	{
		/*
		 * we do not necessarily have a valid parallel worker context in
		 * background workers, so disable parallel execution by default
		 */
		zero_guc("max_parallel_workers_per_gather");
		zero_guc("max_parallel_workers");
		zero_guc("max_parallel_maintenance_workers");

		res = ts_bgw_job_execute(job);
		/* The job is responsible for committing or aborting it's own txns */
		if (IsTransactionState())
			elog(ERROR,
				 "TimescaleDB background job \"%s\" failed to end the transaction",
				 NameStr(job->fd.application_name));
	}
	PG_CATCH();
	{
		if (IsTransactionState())
			/* If there was an error, rollback what was done before the error */
			AbortCurrentTransaction();
		StartTransactionCommand();

		/* Free the old job if it exists, it's no longer needed, and since it's
		 * in the TopMemoryContext it won't be freed otherwise.
		 */

		if (job != NULL)
		{
			pfree(job);
			job = NULL;
		}

		/*
		 * Note that the mark_start happens in the scheduler right before the
		 * job is launched. Try to get a lock on the job again. Because the error
		 * removed the session lock. Don't block and only record if the lock was actually
		 * obtained.
		 */
		job = ts_bgw_job_find_with_lock(job_id,
										TopMemoryContext,
										RowShareLock,
										TXN_LOCK,
										/* block */ false,
										&got_lock);
		if (job != NULL)
		{
			ts_bgw_job_stat_mark_end(job, JOB_FAILURE);
			ts_bgw_job_check_max_retries(job);
			pfree(job);
			job = NULL;
		}
		CommitTransactionCommand();

		/*
		 * the rethrow will log the error; but also log which job threw the
		 * error
		 */
		elog(LOG, "job %d threw an error", job_id);
		PG_RE_THROW();
	}
	PG_END_TRY();

	Assert(!IsTransactionState());

	StartTransactionCommand();

	/*
	 * Note that the mark_start happens in the scheduler right before the job
	 * is launched
	 */
	ts_bgw_job_stat_mark_end(job, res);

	CommitTransactionCommand();

	if (job != NULL)
	{
		pfree(job);
		job = NULL;
	}

	elog(DEBUG1, "exiting job %d with %s", job_id, (res == JOB_SUCCESS ? "success" : "failure"));

	PG_RETURN_VOID();
}

void
ts_bgw_job_set_scheduler_test_hook(scheduler_test_hook_type hook)
{
	scheduler_test_hook = hook;
}

void
ts_bgw_job_set_job_entrypoint_function_name(char *func_name)
{
	job_entrypoint_function_name = func_name;
}

bool
ts_bgw_job_run_and_set_next_start(BgwJob *job, job_main_func func, int64 initial_runs,
								  Interval *next_interval)
{
	BgwJobStat *job_stat;
	bool ret = func();

	/* Now update next_start. */
	StartTransactionCommand();

	job_stat = ts_bgw_job_stat_find(job->fd.id);

	/*
	 * Note that setting next_start explicitly from this function will
	 * override any backoff calculation due to failure.
	 */
	if (job_stat->fd.total_runs < initial_runs)
	{
		TimestampTz next_start =
			DatumGetTimestampTz(DirectFunctionCall2(timestamptz_pl_interval,
													TimestampTzGetDatum(job_stat->fd.last_start),
													IntervalPGetDatum(next_interval)));

		ts_bgw_job_stat_set_next_start(job->fd.id, next_start);
	}
	CommitTransactionCommand();

	return ret;
}

int
ts_bgw_job_insert_relation(Name application_name, Interval *schedule_interval,
						   Interval *max_runtime, int32 max_retries, Interval *retry_period,
						   Name proc_schema, Name proc_name, Name owner, bool scheduled,
						   int32 hypertable_id, Jsonb *config)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	Datum values[Natts_bgw_job];
	CatalogSecurityContext sec_ctx;
	bool nulls[Natts_bgw_job] = { false };
	int32 job_id;
	char app_name[NAMEDATALEN];

	rel = table_open(catalog_get_table_id(catalog, BGW_JOB), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	values[AttrNumberGetAttrOffset(Anum_bgw_job_schedule_interval)] =
		IntervalPGetDatum(schedule_interval);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_max_runtime)] = IntervalPGetDatum(max_runtime);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_max_retries)] = Int32GetDatum(max_retries);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_retry_period)] = IntervalPGetDatum(retry_period);

	values[AttrNumberGetAttrOffset(Anum_bgw_job_proc_schema)] = NameGetDatum(proc_schema);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_proc_name)] = NameGetDatum(proc_name);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_owner)] = NameGetDatum(owner);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_scheduled)] = BoolGetDatum(scheduled);
	if (hypertable_id == 0)
		nulls[AttrNumberGetAttrOffset(Anum_bgw_job_hypertable_id)] = true;
	else
		values[AttrNumberGetAttrOffset(Anum_bgw_job_hypertable_id)] = Int32GetDatum(hypertable_id);

	if (config == NULL)
		nulls[AttrNumberGetAttrOffset(Anum_bgw_job_config)] = true;
	else
		values[AttrNumberGetAttrOffset(Anum_bgw_job_config)] = JsonbPGetDatum(config);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	job_id = DatumGetInt32(ts_catalog_table_next_seq_id(catalog, BGW_JOB));
	snprintf(app_name, NAMEDATALEN, "%s [%d]", NameStr(*application_name), job_id);

	values[AttrNumberGetAttrOffset(Anum_bgw_job_id)] = Int32GetDatum(job_id);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_application_name)] = CStringGetDatum(app_name);

	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	table_close(rel, RowExclusiveLock);
	return values[AttrNumberGetAttrOffset(Anum_bgw_job_id)];
}
