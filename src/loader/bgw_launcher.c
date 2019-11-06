/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

/* BGW includes below */
/* These are always necessary for a bgworker */
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>

/* for setting our wait event during waitlatch*/
#include <pgstat.h>

/* needed for getting database list*/
#include <access/heapam.h>
#include <access/htup_details.h>
#include <catalog/pg_database.h>
#include <utils/snapmgr.h>
#include <access/xact.h>

/* and checking db list for whether we're in a template*/
#include <utils/syscache.h>

/* for calling external function*/
#include <fmgr.h>

/* for signal handling (specifically die() function) */
#include <tcop/tcopprot.h>

/* for looking up sending proc information for message handling */
#include <storage/procarray.h>

/* for allocating the htab storage */
#include <utils/memutils.h>

/* for getting settings correct before loading the versioned scheduler */
#include "catalog/pg_db_role_setting.h"

#include "../compat.h"
#include "../extension_constants.h"
#include "loader.h"
#include "bgw_counter.h"
#include "bgw_message_queue.h"
#include "bgw_launcher.h"

#define BGW_DB_SCHEDULER_FUNCNAME "ts_bgw_scheduler_main"
#define BGW_ENTRYPOINT_FUNCNAME "ts_bgw_db_scheduler_entrypoint"

typedef enum AckResult
{
	ACK_FAILURE = 0,
	ACK_SUCCESS,
} AckResult;

/* See state machine in README.md */
typedef enum SchedulerState
{
	/* Scheduler should be started but has not been allocated or started */
	ENABLED = 0,
	/* The scheduler has been allocated a spot in timescaleDB's worker counter */
	ALLOCATED,
	/* Scheduler has been started */
	STARTED,

	/*
	 * Scheduler is stopped and should not be started automatically. START and
	 * RESTART messages can re-enable the scheduler.
	 */
	DISABLED
} SchedulerState;

#ifdef TS_DEBUG
#define BGW_LAUNCHER_RESTART_TIME_S 0
#else
#define BGW_LAUNCHER_RESTART_TIME_S 60
#endif

/* WaitLatch expects a long */
#ifdef TS_DEBUG
#define BGW_LAUNCHER_POLL_TIME_MS 10L
#else
#define BGW_LAUNCHER_POLL_TIME_MS 60000L
#endif

static volatile sig_atomic_t got_SIGHUP = false;

static void launcher_sighup(SIGNAL_ARGS)
{
	/* based on av_sighup_handler */
	int save_errno = errno;

	got_SIGHUP = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Main bgw launcher for the cluster.
 *
 * Run through the TimescaleDB loader, so needs to have a small footprint as
 * any interactions it has will need to remain backwards compatible for the
 * foreseeable future.
 *
 * Notes: multiple databases in an instance (PG cluster) can have TimescaleDB
 * installed. They are not necessarily the same version of TimescaleDB (though
 * they could be) Shared memory is allocated and background workers are
 * registered at shared_preload_libraries time We do not know what databases
 * exist, nor which databases TimescaleDB is installed in (if any) at
 * shared_preload_libraries time.
 */

TS_FUNCTION_INFO_V1(ts_bgw_cluster_launcher_main);
TS_FUNCTION_INFO_V1(ts_bgw_db_scheduler_entrypoint);
typedef struct DbHashEntry
{
	Oid db_oid;									 /* key for the hash table, must be first */
	BackgroundWorkerHandle *db_scheduler_handle; /* needed to shut down
												  * properly */
	SchedulerState state;
	VirtualTransactionId vxid;
	int state_transition_failures;
} DbHashEntry;

static void scheduler_state_trans_enabled_to_allocated(DbHashEntry *entry);

static void
bgw_on_postmaster_death(void)
{
	on_exit_reset(); /* don't call exit hooks cause we want to bail
					  * out quickly */
	ereport(FATAL,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("postmaster exited while TimescaleDB background worker launcher was working")));
}

static void
report_bgw_limit_exceeded(DbHashEntry *entry)
{
	if (entry->state_transition_failures == 0)
		ereport(LOG,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("TimescaleDB background worker limit of %d exceeded",
						ts_guc_max_background_workers),
				 errhint("Consider increasing timescaledb.max_background_workers.")));
	entry->state_transition_failures++;
}

static void
report_error_on_worker_register_failure(DbHashEntry *entry)
{
	if (entry->state_transition_failures == 0)
		ereport(LOG,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("no available background worker slots"),
				 errhint("Consider increasing max_worker_processes in tandem with "
						 "timescaledb.max_background_workers.")));
	entry->state_transition_failures++;
}

/*
 * Aliasing a few things in bgworker.h so that we exit correctly on postmaster
 * death so we don't have to duplicate code basically telling it we shouldn't
 * call exit hooks cause we want to bail out quickly - similar to how the
 * quickdie function works when we receive a sigquit. This should work
 * similarly because postmaster death is a similar severity of issue.
 * Additionally, we're wrapping these calls to make sure we never have a NULL
 * handle, if we have a null handle, we return normal things.
 */
static BgwHandleStatus
get_background_worker_pid(BackgroundWorkerHandle *handle, pid_t *pidp)
{
	BgwHandleStatus status;
	pid_t pid;

	if (handle == NULL)
		status = BGWH_STOPPED;
	else
	{
		status = GetBackgroundWorkerPid(handle, &pid);
		if (pidp != NULL)
			*pidp = pid;
	}

	if (status == BGWH_POSTMASTER_DIED)
		bgw_on_postmaster_death();
	return status;
}

static void
wait_for_background_worker_startup(BackgroundWorkerHandle *handle, pid_t *pidp)
{
	BgwHandleStatus status;

	if (handle == NULL)
		status = BGWH_STOPPED;
	else
		status = WaitForBackgroundWorkerStartup(handle, pidp);

	/*
	 * We don't care whether we get BGWH_STOPPED or BGWH_STARTED here, because
	 * the worker could have started and stopped very quickly before we read
	 * it. We can't get BGWH_NOT_YET_STARTED as that's what we're waiting for.
	 * We do care if the Postmaster died however.
	 */

	if (status == BGWH_POSTMASTER_DIED)
		bgw_on_postmaster_death();

	Assert(status == BGWH_STOPPED || status == BGWH_STARTED);
	return;
}

static void
wait_for_background_worker_shutdown(BackgroundWorkerHandle *handle)
{
	BgwHandleStatus status;

	if (handle == NULL)
		status = BGWH_STOPPED;
	else
		status = WaitForBackgroundWorkerShutdown(handle);

	/* We can only ever get BGWH_STOPPED stopped unless the Postmaster died. */
	if (status == BGWH_POSTMASTER_DIED)
		bgw_on_postmaster_death();

	Assert(status == BGWH_STOPPED);
	return;
}

static void
terminate_background_worker(BackgroundWorkerHandle *handle)
{
	if (handle == NULL)
		return;
	else
		TerminateBackgroundWorker(handle);
}

extern void
ts_bgw_cluster_launcher_register(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	/* set up worker settings for our main worker */
	snprintf(worker.bgw_name, BGW_MAXLEN, "TimescaleDB Background Worker Launcher");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_restart_time = BGW_LAUNCHER_RESTART_TIME_S;

	/*
	 * Starting at BgWorkerStart_RecoveryFinished means we won't ever get
	 * started on a hot_standby see
	 * https://www.postgresql.org/docs/10/static/bgworker.html as it's not
	 * documented in bgworker.c.
	 */
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_notify_pid = 0;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, EXTENSION_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "ts_bgw_cluster_launcher_main");
	RegisterBackgroundWorker(&worker);
}

/*
 * Register a background worker that calls the main TimescaleDB background
 * worker launcher library (i.e. loader) and uses the scheduler entrypoint
 * function.  The scheduler entrypoint will deal with starting a new worker,
 * and waiting on any txns that it needs to, if we pass along a vxid in the
 * bgw_extra field of the BgWorker.
 */
static bool
register_entrypoint_for_db(Oid db_id, VirtualTransactionId vxid, BackgroundWorkerHandle **handle)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "TimescaleDB Background Worker Scheduler");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	snprintf(worker.bgw_library_name, BGW_MAXLEN, EXTENSION_NAME);
	snprintf(worker.bgw_function_name, BGW_MAXLEN, BGW_ENTRYPOINT_FUNCNAME);
	worker.bgw_notify_pid = MyProcPid;
	worker.bgw_main_arg = ObjectIdGetDatum(db_id);
	memcpy(worker.bgw_extra, &vxid, sizeof(VirtualTransactionId));

	return RegisterDynamicBackgroundWorker(&worker, handle);
}

/* Initializes the launcher's hash table of schedulers.
 * Return value is guaranteed to be not-null, because otherwise the function
 * will have thrown an error.
 */
static HTAB *
init_database_htab(void)
{
	HASHCTL info = { .keysize = sizeof(Oid), .entrysize = sizeof(DbHashEntry) };

	return hash_create("launcher_db_htab",
					   ts_guc_max_background_workers,
					   &info,
					   HASH_BLOBS | HASH_ELEM);
}

/* Insert a scheduler entry into the hash table. Correctly set entry values. */
static DbHashEntry *
db_hash_entry_create_if_not_exists(HTAB *db_htab, Oid db_oid)
{
	DbHashEntry *db_he;
	bool found;

	db_he = (DbHashEntry *) hash_search(db_htab, &db_oid, HASH_ENTER, &found);
	if (!found)
	{
		db_he->db_scheduler_handle = NULL;
		db_he->state = ENABLED;
		SetInvalidVirtualTransactionId(db_he->vxid);
		db_he->state_transition_failures = 0;

		/*
		 * Try to allocate a spot right away to give schedulers priority over
		 * other bgws. This is especially important on initial server startup
		 * where we want to reserve slots for all schedulers before starting
		 * any. This is done so that background workers started by schedulers
		 * don't race for open slots with other schedulers on startup.
		 */
		scheduler_state_trans_enabled_to_allocated(db_he);
	}

	return db_he;
}

/*
 * Model this on autovacuum.c -> get_database_list.
 *
 * Note that we are not doing
 * all the things around memory context that they do, because the hashtable
 * we're using to store db entries is automatically created in its own memory
 * context (a child of TopMemoryContext) This can get called at two different
 * times 1) when the cluster launcher starts and is looking for dbs and 2) if
 * it restarts due to a postmaster signal.
 */
static void
populate_database_htab(HTAB *db_htab)
{
	Relation rel;
	TableScanDesc scan;
	HeapTuple tup;

	/*
	 * by this time we should already be connected to the db, and only have
	 * access to shared catalogs
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = table_open(DatabaseRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 0, NULL);

	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdb = (Form_pg_database) GETSTRUCT(tup);

		if (!pgdb->datallowconn || pgdb->datistemplate)
			continue; /* don't bother with dbs that don't allow
					   * connections or are templates */
#if PG12_LT
		db_hash_entry_create_if_not_exists(db_htab, HeapTupleGetOid(tup));
#else
		db_hash_entry_create_if_not_exists(db_htab, pgdb->oid);
#endif
	}
	heap_endscan(scan);
	table_close(rel, AccessShareLock);

	CommitTransactionCommand();
}

static void
scheduler_modify_state(DbHashEntry *entry, SchedulerState new_state)
{
	Assert(entry->state != new_state);
	entry->state_transition_failures = 0;
	entry->state = new_state;
}

/* TRANSITION FUNCTIONS */
static void
scheduler_state_trans_disabled_to_enabled(DbHashEntry *entry)
{
	Assert(entry->state == DISABLED);
	Assert(entry->db_scheduler_handle == NULL);
	scheduler_modify_state(entry, ENABLED);
}

static void
scheduler_state_trans_enabled_to_allocated(DbHashEntry *entry)
{
	Assert(entry->state == ENABLED);
	Assert(entry->db_scheduler_handle == NULL);
	/* Reserve a spot for this scheduler with BGW counter */
	if (!ts_bgw_total_workers_increment())
	{
		report_bgw_limit_exceeded(entry);
		return;
	}
	scheduler_modify_state(entry, ALLOCATED);
}

static void
scheduler_state_trans_started_to_allocated(DbHashEntry *entry)
{
	Assert(entry->state == STARTED);
	Assert(get_background_worker_pid(entry->db_scheduler_handle, NULL) == BGWH_STOPPED);
	if (entry->db_scheduler_handle != NULL)
	{
		pfree(entry->db_scheduler_handle);
		entry->db_scheduler_handle = NULL;
	}
	scheduler_modify_state(entry, ALLOCATED);
}

static void
scheduler_state_trans_allocated_to_started(DbHashEntry *entry)
{
	pid_t worker_pid;
	bool worker_registered;

	Assert(entry->state == ALLOCATED);
	Assert(entry->db_scheduler_handle == NULL);

	worker_registered =
		register_entrypoint_for_db(entry->db_oid, entry->vxid, &entry->db_scheduler_handle);

	if (!worker_registered)
	{
		report_error_on_worker_register_failure(entry);
		return;
	}
	wait_for_background_worker_startup(entry->db_scheduler_handle, &worker_pid);
	SetInvalidVirtualTransactionId(entry->vxid);
	scheduler_modify_state(entry, STARTED);
}

static void
scheduler_state_trans_enabled_to_disabled(DbHashEntry *entry)
{
	Assert(entry->state == ENABLED);
	Assert(entry->db_scheduler_handle == NULL);
	scheduler_modify_state(entry, DISABLED);
}

static void
scheduler_state_trans_allocated_to_disabled(DbHashEntry *entry)
{
	Assert(entry->state == ALLOCATED);
	Assert(entry->db_scheduler_handle == NULL);

	ts_bgw_total_workers_decrement();
	scheduler_modify_state(entry, DISABLED);
}

static void
scheduler_state_trans_started_to_disabled(DbHashEntry *entry)
{
	Assert(entry->state == STARTED);
	Assert(get_background_worker_pid(entry->db_scheduler_handle, NULL) == BGWH_STOPPED);

	ts_bgw_total_workers_decrement();
	if (entry->db_scheduler_handle != NULL)
	{
		pfree(entry->db_scheduler_handle);
		entry->db_scheduler_handle = NULL;
	}
	scheduler_modify_state(entry, DISABLED);
}

static void
scheduler_state_trans_automatic(DbHashEntry *entry)
{
	switch (entry->state)
	{
		case ENABLED:
			scheduler_state_trans_enabled_to_allocated(entry);
			if (entry->state == ALLOCATED)
				scheduler_state_trans_allocated_to_started(entry);
			break;
		case ALLOCATED:
			scheduler_state_trans_allocated_to_started(entry);
			break;
		case STARTED:
			if (get_background_worker_pid(entry->db_scheduler_handle, NULL) == BGWH_STOPPED)
				scheduler_state_trans_started_to_disabled(entry);
			break;
		case DISABLED:
			break;
	}
}

static void
scheduler_state_trans_automatic_all(HTAB *db_htab)
{
	HASH_SEQ_STATUS hash_seq;
	DbHashEntry *current_entry;

	hash_seq_init(&hash_seq, db_htab);
	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
		scheduler_state_trans_automatic(current_entry);
}

/* This is called when we're going to shut down so we don't leave things messy*/
static void
launcher_pre_shmem_cleanup(int code, Datum arg)
{
	HTAB *db_htab = *(HTAB **) DatumGetPointer(arg);
	HASH_SEQ_STATUS hash_seq;
	DbHashEntry *current_entry;

	/* db_htab will be NULL if we fail during init_database_htab */
	if (db_htab != NULL)
	{
		hash_seq_init(&hash_seq, db_htab);

		/*
		 * Stop everyone (or at least tell the Postmaster we don't care about
		 * them anymore)
		 */
		while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
		{
			if (current_entry->db_scheduler_handle != NULL)
			{
				terminate_background_worker(current_entry->db_scheduler_handle);
				pfree(current_entry->db_scheduler_handle);
			}
		}

		hash_destroy(db_htab);
	}

	/*
	 * Reset our pid in the queue so that others know we've died and don't
	 * wait forever
	 */
	ts_bgw_message_queue_shmem_cleanup();
}

/*
 *************
 * Actions for message types we could receive off of the bgw_message_queue.
 *************
 */

/*
 * This should be idempotent. If we find the background worker and it's not
 * stopped, do nothing. In order to maintain idempotency, a scheduler in the
 * ENABLED, ALLOCATED or STARTED state cannot get a new vxid to wait on. (We
 * cannot pass in a new vxid to wait on for an already-started scheduler in any
 * case). This means that actions like restart, which are not idempotent, will
 * not have their effects changed by subsequent start actions, no matter the
 * state they are in when the start action is received.
 */
static AckResult
message_start_action(HTAB *db_htab, BgwMessage *message)
{
	DbHashEntry *entry;

	entry = db_hash_entry_create_if_not_exists(db_htab, message->db_oid);

	if (entry->state == DISABLED)
		scheduler_state_trans_disabled_to_enabled(entry);

	scheduler_state_trans_automatic(entry);

	return (entry->state == STARTED ? ACK_SUCCESS : ACK_FAILURE);
}

static AckResult
message_stop_action(HTAB *db_htab, BgwMessage *message)
{
	DbHashEntry *entry;

	/*
	 * If the entry does not exist try to create it so we can put it in the
	 * DISABLED state. Otherwise, it will be created during the next poll and
	 * then will end up in the ENABLED state and proceed to being STARTED. But
	 * this is not the behavior we want.
	 */
	entry = db_hash_entry_create_if_not_exists(db_htab, message->db_oid);

	switch (entry->state)
	{
		case ENABLED:
			scheduler_state_trans_enabled_to_disabled(entry);
			break;
		case ALLOCATED:
			scheduler_state_trans_allocated_to_disabled(entry);
			break;
		case STARTED:
			terminate_background_worker(entry->db_scheduler_handle);
			wait_for_background_worker_shutdown(entry->db_scheduler_handle);
			scheduler_state_trans_started_to_disabled(entry);
			break;
		case DISABLED:
			break;
	}
	return entry->state == DISABLED ? ACK_SUCCESS : ACK_FAILURE;
}

/*
 * This function will stop and restart a scheduler in the STARTED state,  ENABLE
 * a scheduler if it does not exist or is in the DISABLED state and set the vxid
 * to wait on for a scheduler in any state. It is not idempotent. Additionally,
 * one might think that this function would simply be a combination of stop and
 * start above, but it is not as we maintain the worker's "slot" by never
 * releasing the worker from our "pool" of background workers as stopping and
 * starting would.  We don't want a race condition where some other db steals
 * the scheduler of the other by requesting a worker at the wrong time. (This is
 * accomplished by moving from STARTED to ALLOCATED after shutting down the
 * worker, never releasing the entry and transitioning all the way back to
 * ENABLED).
 */
static AckResult
message_restart_action(HTAB *db_htab, BgwMessage *message, VirtualTransactionId vxid)
{
	DbHashEntry *entry;

	entry = db_hash_entry_create_if_not_exists(db_htab, message->db_oid);

	entry->vxid = vxid;

	switch (entry->state)
	{
		case ENABLED:
			break;
		case ALLOCATED:
			break;
		case STARTED:
			terminate_background_worker(entry->db_scheduler_handle);
			wait_for_background_worker_shutdown(entry->db_scheduler_handle);
			scheduler_state_trans_started_to_allocated(entry);
			break;
		case DISABLED:
			scheduler_state_trans_disabled_to_enabled(entry);
	}

	scheduler_state_trans_automatic(entry);
	return entry->state == STARTED ? ACK_SUCCESS : ACK_FAILURE;
}

/*
 * Handle 1 message.
 */
static bool
launcher_handle_message(HTAB *db_htab)
{
	BgwMessage *message = ts_bgw_message_receive();
	PGPROC *sender;
	VirtualTransactionId vxid;
	AckResult action_result = ACK_FAILURE;

	if (message == NULL)
		return false;

	sender = BackendPidGetProc(message->sender_pid);
	if (sender == NULL)
	{
		ereport(LOG,
				(errmsg("TimescaleDB background worker launcher received message from non-existent "
						"backend")));
		return true;
	}

	GET_VXID_FROM_PGPROC(vxid, *sender);

	switch (message->message_type)
	{
		case START:
			action_result = message_start_action(db_htab, message);
			break;
		case STOP:
			action_result = message_stop_action(db_htab, message);
			break;
		case RESTART:
			action_result = message_restart_action(db_htab, message, vxid);
			break;
	}

	ts_bgw_message_send_ack(message, action_result);
	return true;
}

/*
 * Wrapper around normal postgres `die()` function to give more context on
 * sigterms. The default, `bgworker_die()`, can't be used due to the fact
 * that it handles signals synchronously, rather than waiting for a
 * CHECK_FOR_INTERRUPTS(). `die()` (which is arguably misnamed) sets flags
 * that will cause the backend to exit on the next call to
 * CHECK_FOR_INTERRUPTS(), which can happen either in our code or in
 * functions within the Postgres codebase that we call. This means that we
 * don't need to wait for the next time control is returned to our loop to
 * exit, which would be necessary if we set our own flag and checked it in
 * a loop condition. However, because it cannot exit 0, the launcher will be
 * restarted by the postmaster, even when it has received a SIGTERM, which
 * we decided was the proper behavior. If users want to disable the launcher,
 * they can set `timescaledb.max_background_workers = 0` and then we will
 * `proc_exit(0)` before doing anything else.
 */
static void launcher_sigterm(SIGNAL_ARGS)
{
	/*
	 * do not use a level >= ERROR because we don't want to exit here but
	 * rather only during CHECK_FOR_INTERRUPTS
	 */
	ereport(LOG,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating TimescaleDB background worker launcher due to administrator "
					"command")));
	die(postgres_signal_arg);
}

extern Datum
ts_bgw_cluster_launcher_main(PG_FUNCTION_ARGS)
{
	HTAB **htab_storage;

	HTAB *db_htab;

	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, launcher_sigterm);
	pqsignal(SIGHUP, launcher_sighup);

	/* Some SIGHUPS may already have been dropped, so we must load the file here */
	got_SIGHUP = false;
	ProcessConfigFile(PGC_SIGHUP);
	BackgroundWorkerUnblockSignals();
	ereport(DEBUG1, (errmsg("TimescaleDB background worker launcher started")));

	/* set counter back to zero on restart */
	ts_bgw_counter_reinit();
	if (!ts_bgw_total_workers_increment())
	{
		/*
		 * Should be the first thing happening so if we already exceeded our
		 * limits it means we have a limit of 0 and we should just exit We
		 * have to exit(0) because if we exit in error we get restarted by the
		 * postmaster.
		 */
		ereport(LOG,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("TimescaleDB background worker is set to 0"),
				 errhint("TimescaleDB background worker launcher shutting down.")));
		proc_exit(0);
	}
	/* Connect to the db, no db name yet, so can only access shared catalogs */
	BackgroundWorkerInitializeConnectionCompat(NULL, NULL);
	pgstat_report_appname(MyBgworkerEntry->bgw_name);
	ereport(LOG, (errmsg("TimescaleDB background worker launcher connected to shared catalogs")));

	htab_storage = MemoryContextAllocZero(TopMemoryContext, sizeof(*htab_storage));

	/*
	 * We must setup the cleanup function _before_ initializing any state it
	 * touches (specifically the bgw_message_queue and db_htab). Failing to do
	 * this can cause cascading failures when the launcher fails in
	 * init_database_htab (eg. due to running out of shared memory) but
	 * doesn't deregister itself from the shared bgw_message_queue.
	 */
	before_shmem_exit(launcher_pre_shmem_cleanup, PointerGetDatum(htab_storage));

	ts_bgw_message_queue_set_reader();

	db_htab = init_database_htab();
	*htab_storage = db_htab;

	populate_database_htab(db_htab);

	while (true)
	{
		int wl_rc;
		bool handled_msgs = false;

		CHECK_FOR_INTERRUPTS();
		populate_database_htab(db_htab);
		handled_msgs = launcher_handle_message(db_htab);
		scheduler_state_trans_automatic_all(db_htab);
		if (handled_msgs)
			continue;

		wl_rc = WaitLatchCompat(MyLatch,
								WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
								BGW_LAUNCHER_POLL_TIME_MS);
		ResetLatch(MyLatch);
		if (wl_rc & WL_POSTMASTER_DEATH)
			bgw_on_postmaster_death();

		if (got_SIGHUP)
		{
			got_SIGHUP = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}
	PG_RETURN_VOID();
}

/* Wrapper around `die()`, see note on `launcher_sigterm()` above for more info*/
static void entrypoint_sigterm(SIGNAL_ARGS)
{
	ereport(LOG,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating TimescaleDB scheduler entrypoint due to administrator command")));
	die(postgres_signal_arg);
}

/*
 * Inside the entrypoint, we must check again if we're in a template db
 * even though we excluded template dbs in populate_database_htab because
 * we can be called on, say, CREATE EXTENSION in a template db and then
 * we'll not stop til next server shutdown so if we hit this point and are
 * in a template db, we throw an error and shut down Check in the syscache
 * rather than searching through the entire database catalog again.
 * Modelled on autovacuum.c -> do_autovacuum.
 */
static void
database_is_template_check(void)
{
	Form_pg_database pgdb;
	HeapTuple tuple;

	tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR,
				(errmsg("TimescaleDB background worker failed to find entry for database in "
						"syscache")));

	pgdb = (Form_pg_database) GETSTRUCT(tuple);
	if (pgdb->datistemplate)
		ereport(ERROR,
				(errmsg("TimescaleDB background worker connected to template database, exiting")));

	ReleaseSysCache(tuple);
}

/*
 * Before we morph into the scheduler, we also need to reload configs from their
 * defaults if the database default has changed. Defaults are changed in the
 * post_restore function where we change the db default for the restoring guc
 * wait until the txn commits and then must see if the txn made the change.
 * Checks for changes are normally run at connection startup, but because we
 * have to connect in order to wait on the txn we have to re-run after the wait.
 * This function is based on the postgres function in postinit.c by the same
 * name.
 */

static void
process_settings(Oid databaseid)
{
	Relation relsetting;
	Snapshot snapshot;

	if (!IsUnderPostmaster)
		return;

	relsetting = heap_open(DbRoleSettingRelationId, AccessShareLock);

	/* read all the settings under the same snapshot for efficiency */
	snapshot = RegisterSnapshot(GetCatalogSnapshot(DbRoleSettingRelationId));

	/* Later settings are ignored if set earlier. */
	ApplySetting(snapshot, databaseid, InvalidOid, relsetting, PGC_S_DATABASE);
	ApplySetting(snapshot, InvalidOid, InvalidOid, relsetting, PGC_S_GLOBAL);

	UnregisterSnapshot(snapshot);
	heap_close(relsetting, AccessShareLock);
}

/*
 * This can be run either from the cluster launcher at db_startup time, or
 * in the case of an install/uninstall/update of the extension, in the
 * first case, we have no vxid that we're waiting on. In the second case,
 * we do, because we have to wait so that we see the effects of said txn.
 * So we wait for it to finish, then we  morph into the new db_scheduler
 * worker using whatever version is now installed (or exit gracefully if
 * no version is now installed).
 */
extern Datum
ts_bgw_db_scheduler_entrypoint(PG_FUNCTION_ARGS)
{
	Oid db_id = DatumGetObjectId(MyBgworkerEntry->bgw_main_arg);
	bool ts_installed = false;
	char version[MAX_VERSION_LEN];
	VirtualTransactionId vxid;

	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, entrypoint_sigterm);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOidCompat(db_id, InvalidOid);
	pgstat_report_appname(MyBgworkerEntry->bgw_name);

	/*
	 * Wait until whatever vxid that potentially called us finishes before we
	 * happens in a txn so it's cleaned up correctly if we get a sigkill in
	 * the meantime, but we will need stop after and take a new txn so we can
	 * see the correct state after its effects
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();
	memcpy(&vxid, MyBgworkerEntry->bgw_extra, sizeof(VirtualTransactionId));
	if (VirtualTransactionIdIsValid(vxid))
		VirtualXactLock(vxid, true);
	CommitTransactionCommand();

	/*
	 * now we can start our transaction and get the version currently
	 * installed
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	/*
	 * Check whether a database is a template database and raise an error if
	 * so, as we don't want to run in template dbs.
	 */
	database_is_template_check();
	/*  Process any config changes caused by an ALTER DATABASE */
	process_settings(MyDatabaseId);
	ts_installed = ts_loader_extension_exists();
	if (ts_installed)
		StrNCpy(version, ts_loader_extension_version(), MAX_VERSION_LEN);

	ts_loader_extension_check();
	CommitTransactionCommand();
	if (ts_installed)
	{
		char soname[MAX_SO_NAME_LEN];
		PGFunction versioned_scheduler_main;

		snprintf(soname, MAX_SO_NAME_LEN, "%s-%s", EXTENSION_SO, version);
		versioned_scheduler_main =
			load_external_function(soname, BGW_DB_SCHEDULER_FUNCNAME, false, NULL);
		if (versioned_scheduler_main == NULL)
			ereport(LOG,
					(errmsg("TimescaleDB version %s does not have a background worker, exiting",
							soname)));
		else /* essentially we morph into the versioned
			  * worker here */
			DirectFunctionCall1(versioned_scheduler_main, ObjectIdGetDatum(InvalidOid));
	}
	PG_RETURN_VOID();
}
