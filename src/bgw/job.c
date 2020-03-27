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
#include <utils/elog.h>

#include "job.h"
#include "scanner.h"
#include "extension.h"
#include "compat.h"
#include "job_stat.h"
#include "license_guc.h"
#include "utils.h"
#include "telemetry/telemetry.h"
#include "bgw_policy/chunk_stats.h"
#include "bgw_policy/drop_chunks.h"
#include "bgw_policy/compress_chunks.h"
#include "bgw_policy/reorder.h"
#include "scan_iterator.h"

#include <cross_module_fn.h>

#define TELEMETRY_INITIAL_NUM_RUNS 12

#if PG12_LT
static VirtualTransactionId *
GetLockConflictsCompat(const LOCKTAG *locktag, LOCKMODE lockmode, int *countp)
{
	VirtualTransactionId *ids = GetLockConflicts(locktag, lockmode);
	if (countp != NULL)
	{
		for (*countp = 0; VirtualTransactionIdIsValid(ids[*countp]); (*countp)++)
		{
			/* Counts the number of virtual transactions ids into countp */
		}
	}
	return ids;
}
#else
#define GetLockConflictsCompat(locktag, lockmode, countp)                                          \
	GetLockConflicts(locktag, lockmode, countp)
#endif

static const char *job_type_names[_MAX_JOB_TYPE] = {
	[JOB_TYPE_VERSION_CHECK] = "telemetry_and_version_check_if_enabled",
	[JOB_TYPE_REORDER] = "reorder",
	[JOB_TYPE_DROP_CHUNKS] = "drop_chunks",
	[JOB_TYPE_CONTINUOUS_AGGREGATE] = "continuous_aggregate",
	[JOB_TYPE_COMPRESS_CHUNKS] = "compress_chunks",
	[JOB_TYPE_UNKNOWN] = "unknown",
};

static unknown_job_type_hook_type unknown_job_type_hook = NULL;
static unknown_job_type_owner_hook_type unknown_job_type_owner_hook = NULL;
static char *job_entrypoint_function_name = "ts_bgw_job_entrypoint";

typedef enum JobLockLifetime
{
	SESSION_LOCK = 0,
	TXN_LOCK,
} JobLockLifetime;

BackgroundWorkerHandle *
ts_bgw_start_worker(const char *function, const char *name, const char *extra)
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
	StrNCpy(worker.bgw_library_name, ts_extension_get_so_name(), BGW_MAXLEN);
	StrNCpy(worker.bgw_function_name, function, BGW_MAXLEN);

	Assert(strlen(extra) < BGW_EXTRALEN);
	StrNCpy(worker.bgw_extra, extra, BGW_EXTRALEN);

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
		return NULL;
	return handle;
}

Oid
ts_bgw_job_owner(BgwJob *job)
{
	switch (job->bgw_type)
	{
		case JOB_TYPE_VERSION_CHECK:
			return ts_catalog_database_info_get()->owner_uid;
		case JOB_TYPE_REORDER:
		{
			BgwPolicyReorder *policy = ts_bgw_policy_reorder_find_by_job(job->fd.id);

			if (policy == NULL)
				elog(ERROR, "reorder policy for job with id \"%d\" not found", job->fd.id);

			return ts_rel_get_owner(ts_hypertable_id_to_relid(policy->fd.hypertable_id));
		}
		case JOB_TYPE_DROP_CHUNKS:
		{
			BgwPolicyDropChunks *policy = ts_bgw_policy_drop_chunks_find_by_job(job->fd.id);

			if (policy == NULL)
				elog(ERROR, "drop_chunks policy for job with id \"%d\" not found", job->fd.id);

			return ts_rel_get_owner(ts_hypertable_id_to_relid(policy->hypertable_id));
		}
		case JOB_TYPE_CONTINUOUS_AGGREGATE:
		{
			ContinuousAgg *ca = ts_continuous_agg_find_by_job_id(job->fd.id);

			if (ca == NULL)
				elog(ERROR, "continuous aggregate for job with id \"%d\" not found", job->fd.id);

			return ts_rel_get_owner(ts_continuous_agg_get_user_view_oid(ca));
		}
		case JOB_TYPE_COMPRESS_CHUNKS:
		{
			BgwPolicyCompressChunks *policy = ts_bgw_policy_compress_chunks_find_by_job(job->fd.id);
			if (policy == NULL)
				elog(ERROR, "compress chunks policy for job with id \"%d\" not found", job->fd.id);
			return ts_rel_get_owner(ts_hypertable_id_to_relid(policy->fd.hypertable_id));
		}
		case JOB_TYPE_UNKNOWN:
			if (unknown_job_type_owner_hook != NULL)
				return unknown_job_type_owner_hook(job);
			break;
		case _MAX_JOB_TYPE:
			break;
	}
	elog(ERROR, "unknown job type \"%s\" in finding owner", NameStr(job->fd.job_type));
}

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

static JobType
get_job_type_from_name(Name job_type_name)
{
	int i;

	for (i = 0; i < _MAX_JOB_TYPE; i++)
		if (namestrcmp(job_type_name, job_type_names[i]) == 0)
			return i;
	return JOB_TYPE_UNKNOWN;
}

static BgwJob *
bgw_job_from_tuple(HeapTuple tuple, size_t alloc_size, MemoryContext mctx)
{
	BgwJob *job;

	/*
	 * allow for embedding with arbitrary alloc_size, which means we can't use
	 * the STRUCT_FROM_TUPLE macro
	 */
	Assert(alloc_size >= sizeof(BgwJob));
	job = (BgwJob *) ts_create_struct_from_tuple(tuple, mctx, alloc_size, sizeof(FormData_bgw_job));
	job->bgw_type = get_job_type_from_name(&job->fd.job_type);

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
	BgwJob *job = bgw_job_from_tuple(ti->tuple, list_data->alloc_size, ti->mctx);
	MemoryContext orig = MemoryContextSwitchTo(ti->mctx);

	list_data->list = lappend(list_data->list, job);

	MemoryContextSwitchTo(orig);
	return SCAN_CONTINUE;
}

extern List *
ts_bgw_job_get_all(size_t alloc_size, MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	AccumData list_data = {
		.list = NIL,
		.alloc_size = alloc_size,
	};
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, BGW_JOB),
		.index = InvalidOid,
		.data = &list_data,
		.tuple_found = bgw_job_accum_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
		.result_mctx = mctx,
	};

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
 *  used to lock the job row while a job is running to prevent a job from being
 *  modified while in the middle of a run. This lock should be taken before
 *  bgw_job table lock to avoid deadlocks.
 *
 * We use an advisory lock instead of a tuple lock because we want the lock on the job id
 * and not on the tid of the row (in case it is vacuumed or updated in some way). We don't
 * want the job modified while it is running for safety reasons. Finally, we use this lock
 * to be able to send a signal to the PID of the running job. This is used by delete because,
 * a job deletion sends a SIGINT to the running job to cancel it.
 *
 * We aquire a SHARE lock on the job during scheduling and when the job is running so that it
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
		HeapTuple heap = ts_scan_iterator_tuple(&iterator);
		job = bgw_job_from_tuple(heap, sizeof(BgwJob), mctx);

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
		job = bgw_job_from_tuple(ts_scan_iterator_tuple(&iterator), sizeof(BgwJob), mctx);
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
						/* session_lock */ false,
						&tag,
						/* block */ false);
	if (!got_lock)
	{
		/* If I couldn't get a lock, try killing the background worker that's running the job.
		 * This is probably not bulletproof but best-effort is good enough here. */
		VirtualTransactionId *vxid = GetLockConflictsCompat(&tag, AccessExclusiveLock, NULL);
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
							/* session lock */ false,
							&tag,
							/* block */ true);
	}
	Assert(got_lock);
}

static ScanTupleResult
bgw_job_tuple_delete(TupleInfo *ti, void *data)
{
	CatalogSecurityContext sec_ctx;
	int32 job_id = ((FormData_bgw_job *) GETSTRUCT(ti->tuple))->id;

	/* Also delete the bgw_stat entry */
	ts_bgw_job_stat_delete(job_id);

	/* Delete any policy args associated with this job */
	ts_bgw_policy_reorder_delete_row_only_by_job_id(job_id);
	ts_bgw_policy_drop_chunks_delete_row_only_by_job_id(job_id);
	ts_bgw_policy_compress_chunks_delete_row_only_by_job_id(job_id);

	/* Delete any stats in bgw_policy_chunk_stats related to this job */
	ts_bgw_policy_chunk_stats_delete_row_only_by_job_id(job_id);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_delete(ti->scanrel, ti->tuple);
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

void
ts_bgw_job_permission_check(BgwJob *job)
{
	Oid owner_oid = ts_bgw_job_owner(job);

	if (!has_privs_of_role(GetUserId(), owner_oid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("insufficient permissions to alter job %d", job->fd.id)));
}

void
ts_bgw_job_validate_job_owner(Oid owner, JobType type)
{
	HeapTuple role_tup = SearchSysCache1(AUTHOID, ObjectIdGetDatum(owner));
	Form_pg_authid rform = (Form_pg_authid) GETSTRUCT(role_tup);

	if (!rform->rolcanlogin)
	{
		ReleaseSysCache(role_tup);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION),
				 errmsg("permission denied to start %s background process as role \"%s\"",
						job_type_names[type],
						rform->rolname.data),
				 errhint("Hypertable owner must have LOGIN permission to run background tasks.")));
	}
	ReleaseSysCache(role_tup);
}

bool
ts_bgw_job_execute(BgwJob *job)
{
	switch (job->bgw_type)
	{
		case JOB_TYPE_VERSION_CHECK:
		{
			bool next_start_set;
			/*
			 * In the first 12 hours, we want telemetry to ping every
			 * hour. After that initial period, we default to the
			 * schedule_interval listed in the job table.
			 */
			Interval *one_hour = DatumGetIntervalP(DirectFunctionCall7(make_interval,
																	   Int32GetDatum(0),
																	   Int32GetDatum(0),
																	   Int32GetDatum(0),
																	   Int32GetDatum(0),
																	   Int32GetDatum(1),
																	   Int32GetDatum(0),
																	   Float8GetDatum(0)));

			next_start_set = ts_bgw_job_run_and_set_next_start(job,
															   ts_telemetry_main_wrapper,
															   TELEMETRY_INITIAL_NUM_RUNS,
															   one_hour);
			pfree(one_hour);
			return next_start_set;
		}
		case JOB_TYPE_REORDER:
		case JOB_TYPE_DROP_CHUNKS:
		case JOB_TYPE_CONTINUOUS_AGGREGATE:
		case JOB_TYPE_COMPRESS_CHUNKS:
			return ts_cm_functions->bgw_policy_job_execute(job);
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

	BackgroundWorkerInitializeConnectionByOidCompat(db_oid, user_uid);

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
#if !PG96
		zero_guc("max_parallel_workers");
#endif
#if PG11_GE
		zero_guc("max_parallel_maintenance_workers");
#endif

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
ts_bgw_job_set_unknown_job_type_hook(unknown_job_type_hook_type hook)
{
	unknown_job_type_hook = hook;
}

void
ts_bgw_job_set_unknown_job_type_owner_hook(unknown_job_type_owner_hook_type hook)
{
	unknown_job_type_owner_hook = hook;
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

		ts_bgw_job_stat_set_next_start(job, next_start);
	}
	CommitTransactionCommand();

	return ret;
}

int
ts_bgw_job_insert_relation(Name application_name, Name job_type, Interval *schedule_interval,
						   Interval *max_runtime, int32 max_retries, Interval *retry_period)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	Datum values[Natts_bgw_job];
	CatalogSecurityContext sec_ctx;
	bool nulls[Natts_bgw_job] = { false };

	rel = table_open(catalog_get_table_id(catalog, BGW_JOB), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	values[AttrNumberGetAttrOffset(Anum_bgw_job_application_name)] = NameGetDatum(application_name);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_job_type)] = NameGetDatum(job_type);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_schedule_interval)] =
		IntervalPGetDatum(schedule_interval);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_max_runtime)] = IntervalPGetDatum(max_runtime);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_max_retries)] = Int32GetDatum(max_retries);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_retry_period)] = IntervalPGetDatum(retry_period);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	values[AttrNumberGetAttrOffset(Anum_bgw_job_id)] =
		ts_catalog_table_next_seq_id(catalog, BGW_JOB);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	table_close(rel, RowExclusiveLock);
	return values[AttrNumberGetAttrOffset(Anum_bgw_job_id)];
}

static ScanTupleResult
bgw_job_tuple_update_by_id(TupleInfo *ti, void *const data)
{
	BgwJob *updated_job = (BgwJob *) data;
	HeapTuple tuple = heap_copytuple(ti->tuple);
	FormData_bgw_job *fd = (FormData_bgw_job *) GETSTRUCT(tuple);
	TimestampTz next_start;

	ts_bgw_job_permission_check(updated_job);
	/* when we update the schedule interval, modify the next start time as well*/
	if (!DatumGetBool(DirectFunctionCall2(interval_eq,
										  IntervalPGetDatum(&fd->schedule_interval),
										  IntervalPGetDatum(&updated_job->fd.schedule_interval))))
	{
		BgwJobStat *stat = ts_bgw_job_stat_find(fd->id);

		if (stat != NULL)
		{
			next_start = DatumGetTimestampTz(
				DirectFunctionCall2(timestamptz_pl_interval,
									TimestampTzGetDatum(stat->fd.last_finish),
									IntervalPGetDatum(&updated_job->fd.schedule_interval)));
			/* allow DT_NOBEGIN for next_start here through allow_unset=true in the case that
			 * last_finish is DT_NOBEGIN,
			 * This means the value is counted as unset which is what we want */
			ts_bgw_job_stat_update_next_start(updated_job, next_start, true);
		}
		fd->schedule_interval = updated_job->fd.schedule_interval;
	}
	fd->max_runtime = updated_job->fd.max_runtime;
	fd->max_retries = updated_job->fd.max_retries;
	fd->retry_period = updated_job->fd.retry_period;

	ts_catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);
	return SCAN_DONE;
}

static bool
bgw_job_update_scan(ScanKeyData *scankey, void *data)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = { .table = catalog_get_table_id(catalog, BGW_JOB),
						   .index = catalog_get_index(catalog, BGW_JOB, BGW_JOB_PKEY_IDX),
						   .nkeys = 1,
						   .scankey = scankey,
						   .data = data,
						   .limit = 1,
						   .tuple_found = bgw_job_tuple_update_by_id,
						   .lockmode = RowExclusiveLock,
						   .scandirection = ForwardScanDirection,
						   .result_mctx = CurrentMemoryContext,
						   .tuplock = {
							   .waitpolicy = LockWaitBlock,
							   .lockmode = LockTupleExclusive,
							   .enabled = false,
						   } };

	return ts_scanner_scan(&scanctx);
}

/* Overwrite job with specified job_id with the given fields */
void
ts_bgw_job_update_by_id(int32 job_id, BgwJob *job)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_bgw_job_pkey_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(job_id));

	bgw_job_update_scan(scankey, (void *) job);
}
