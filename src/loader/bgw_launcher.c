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

#ifdef TS_DEBUG
#define BGW_LAUNCHER_RESTART_TIME 0
#else
#define BGW_LAUNCHER_RESTART_TIME 60
#endif

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
	Oid			db_oid;			/* key for the hash table, must be first */
	BackgroundWorkerHandle *db_scheduler_handle;	/* needed to shut down
													 * properly */
} DbHashEntry;


static void
bgw_on_postmaster_death(void)
{
	on_exit_reset();			/* don't call exit hooks cause we want to bail
								 * out quickly */
	ereport(FATAL,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("postmaster exited while TimescaleDB background worker launcher launcher was working")));
}

static void
report_bgw_limit_exceeded(void)
{
	ereport(LOG, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				  errmsg("TimescaleDB background worker limit of %d exceeded", guc_max_background_workers),
				  errhint("Consider increasing timescaledb.max_background_workers.")));
}

/*
 * This error is thrown on failure to register a background worker with the
 * postmaster. This should be highly unusual and only happen when we run out of
 * background worker slots. In which case, it is okay to shut everything down
 * and hope that things are better when we restart
 */
static void
report_error_on_worker_register_failure(void)
{
	ereport(LOG, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				  errmsg("no available background worker slots"),
				  errhint("Consider increasing max_worker_processes in tandem with timescaledb.max_background_workers.")));

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

	if (handle == NULL)
		status = BGWH_STOPPED;
	else
		status = GetBackgroundWorkerPid(handle, pidp);

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
bgw_cluster_launcher_register(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));
	/* set up worker settings for our main worker */
	snprintf(worker.bgw_name, BGW_MAXLEN, "TimescaleDB Background Worker Launcher");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_restart_time = BGW_LAUNCHER_RESTART_TIME;

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
	HASHCTL		info = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(DbHashEntry)
	};

	return hash_create("launcher_db_htab", guc_max_background_workers, &info, HASH_BLOBS | HASH_ELEM);
}

/* Insert a scheduler entry into the hash table. Correctly set entry values. */
static DbHashEntry *
db_hash_entry_create(HTAB *db_htab, Oid db_oid)
{
	DbHashEntry *db_he;
	bool		found;

	db_he = (DbHashEntry *) hash_search(db_htab, &db_oid, HASH_ENTER, &found);
	db_he->db_scheduler_handle = NULL;

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
	Relation	rel;
	HeapScanDesc scan;
	HeapTuple	tup;
	ListCell   *lc;

	/*
	 * Used to store OIDs that can be assigned schedulers.
	 */
	List	   *db_oids = NIL;

	/*
	 * by this time we should already be connected to the db, and only have
	 * access to shared catalogs
	 */
	StartTransactionCommand();
	(void) GetTransactionSnapshot();

	rel = heap_open(DatabaseRelationId, AccessShareLock);
	scan = heap_beginscan_catalog(rel, 0, NULL);

	/*
	 * Do an initial scan just to figure out how many databases there are.
	 * This initial scan is necessary so that we can maintain the invariant
	 * that entries are not added to the hash table UNLESS they have already
	 * been accounted for in the bgw_counter.
	 */
	while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database pgdb = (Form_pg_database) GETSTRUCT(tup);

		if (!pgdb->datallowconn || pgdb->datistemplate)
			continue;			/* don't bother with dbs that don't allow
								 * connections or are templates */

		db_oids = lappend_oid(db_oids, HeapTupleGetOid(tup));
	}
	heap_endscan(scan);
	heap_close(rel, AccessShareLock);

	/*
	 * Now reserve slots for all schedulers via the bgw_counter. We want to
	 * avoid the race condition where we have enough workers allocated to
	 * start schedulers for all databases, but before we could get all of them
	 * started, the (say) first scheduler has started too many jobs and then
	 * we don't have enough schedulers for the dbs. So we need to increment
	 * our workers all at once so that schedulers don't start workers stealing
	 * other schedulers' spots.
	 */
	if (!bgw_total_workers_increment_by(list_length(db_oids)))
	{
		ereport(LOG, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
					  errmsg("total databases = %ld TimescaleDB background worker limit %d, so no schedulers allocated", hash_get_num_entries(db_htab), guc_max_background_workers),
					  errhint("You may start background workers manually by using the  _timescaledb_internal.start_background_workers() function in each database you would like to have a scheduler worker in.")));
		return;
	}

	/*
	 * All schedulers have been correctly accounted for, so go ahead and
	 * insert all into the actual hash table.
	 */
	foreach(lc, db_oids)
		(void) db_hash_entry_create(db_htab, lfirst_oid(lc));

	/*
	 * This commit is at the end of this function instead of after heap_close
	 * above because the db_oids list is allocated on this txn's memory
	 * context. We cannot free this context until we are done using the list.
	 */
	CommitTransactionCommand();
}

/* Scan our hash table of dbs and register a worker for each */
static void
start_db_schedulers(HTAB *db_htab)
{
	HASH_SEQ_STATUS hash_seq;
	DbHashEntry *current_entry;
	int			nstarted = 0;
	int			ndatabases = hash_get_num_entries(db_htab);

	hash_seq_init(&hash_seq, db_htab);
	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		bool		worker_registered = false;
		pid_t		worker_pid;
		VirtualTransactionId vxid;

		/* When called at server start, no need to wait on a vxid */
		SetInvalidVirtualTransactionId(vxid);

		worker_registered = register_entrypoint_for_db(current_entry->db_oid, vxid, &current_entry->db_scheduler_handle);
		if (!worker_registered)
		{
			hash_seq_term(&hash_seq);
			ereport(LOG, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						  errmsg("TimescaleDB background worker scheduler for at least one database unable to start"),
						  errhint("%d schedulers have been started, %d databases remain without scheduler. Increase max_worker_processes and restart the server.", nstarted, (ndatabases - nstarted))));

			/*
			 * We don't need to decrement as that will be handled by the
			 * stopped workers check.
			 */
			break;
		}
		wait_for_background_worker_startup(current_entry->db_scheduler_handle, &worker_pid);
		nstarted++;
	}
}

/* This is called when we're going to shut down so we don't leave things messy*/
static void
launcher_pre_shmem_cleanup(int code, Datum arg)
{
	HTAB	   *db_htab = (HTAB *) DatumGetPointer(arg);
	HASH_SEQ_STATUS hash_seq;
	DbHashEntry *current_entry;

	hash_seq_init(&hash_seq, db_htab);

	/*
	 * Stop everyone (or at least tell the Postmaster we don't care about them
	 * anymore)
	 */
	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
		terminate_background_worker(current_entry->db_scheduler_handle);

	hash_destroy(db_htab);

	/*
	 * Reset our pid in the queue so that others know we've died and don't
	 * wait forever
	 */
	bgw_message_queue_shmem_cleanup();
}

/*Garbage collector cleaning up any stopped schedulers*/
static void
stopped_db_schedulers_cleanup(HTAB *db_htab)
{
	HASH_SEQ_STATUS hash_seq;
	DbHashEntry *current_entry;
	bool		found;

	hash_seq_init(&hash_seq, db_htab);
	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		pid_t		worker_pid;

		if (get_background_worker_pid(current_entry->db_scheduler_handle, &worker_pid) == BGWH_STOPPED)
		{
			hash_search(db_htab, &current_entry->db_oid, HASH_REMOVE, &found);
			bgw_total_workers_decrement();
		}
	}
}

/* Returns the hashtable entry for a database's scheduler, allocating one if it does not exist. */
static DbHashEntry *
allocate_scheduler(HTAB *db_htab, Oid db_oid)
{
	bool		found;
	DbHashEntry *db_he;

	db_he = hash_search(db_htab, &db_oid, HASH_FIND, &found);
	if (!found)
	{
		/* Reserve a spot for this scheduler with BGW counter */
		if (!bgw_total_workers_increment())
		{
			report_bgw_limit_exceeded();
			return NULL;
		}

		/*
		 * Only insert into the hash table if we successfully get a counter
		 * spot
		 */
		db_he = db_hash_entry_create(db_htab, db_oid);
	}

	return db_he;
}

static AckResult
register_scheduler_handle(VirtualTransactionId vxid, DbHashEntry *db_he)
{
	pid_t		worker_pid;
	bool		worker_registered = register_entrypoint_for_db(db_he->db_oid, vxid, &db_he->db_scheduler_handle);

	if (!worker_registered)
	{
		report_error_on_worker_register_failure();
		return ACK_FAILURE;
	}
	wait_for_background_worker_startup(db_he->db_scheduler_handle, &worker_pid);
	return ACK_SUCCESS;
}

/*
 *************
 * Actions for message types we could receive off of the bgw_message_queue.
 * None of these action functions should do accounting clean-up, as this is done in exclusively
 * in stopped_db_schedulers.
 *************
 */

/*
 * This should be idempotent. If we find the background worker and it's
 * not stopped, do nothing. If we have a worker that stopped
 * but hasn't been cleaned up yet, simply restart the worker.
 */
static AckResult
message_start_action(HTAB *db_htab, BgwMessage *message, VirtualTransactionId vxid)
{
	DbHashEntry *db_he;
	pid_t		worker_pid;

	db_he = allocate_scheduler(db_htab, message->db_oid);
	if (db_he == NULL)
		return ACK_FAILURE;

	if (get_background_worker_pid(db_he->db_scheduler_handle, &worker_pid) == BGWH_STOPPED)
		return register_scheduler_handle(vxid, db_he);

	return ACK_SUCCESS;
}

static AckResult
message_stop_action(HTAB *db_htab, BgwMessage *message)
{
	DbHashEntry *db_he;
	bool		found;

	db_he = hash_search(db_htab, &message->db_oid, HASH_FIND, &found);
	if (found)
	{
		terminate_background_worker(db_he->db_scheduler_handle);
		wait_for_background_worker_shutdown(db_he->db_scheduler_handle);
	}
	return ACK_SUCCESS;
}

/*
 * This function will only restart an existing scheduler and will throw an error
 * if it is told to restart a nonexistent scheduler.
 *
 * One might think that this function would simply be a combination of stop and start above, however
 * we decided against that because we want to maintain the worker's "slot".
 * We don't want a race condition where some other db steals the scheduler of the other by requesting a worker at the wrong time.
*/
static AckResult
message_restart_action(HTAB *db_htab, BgwMessage *message, VirtualTransactionId vxid)
{
	DbHashEntry *db_he;
	bool		found;

	db_he = hash_search(db_htab, &message->db_oid, HASH_FIND, &found);
	if (!found)
	{
		ereport(WARNING, (errmsg("TimescaleDB background worker launcher instructed to restart a nonexistent scheduler BGW")));
		return ACK_FAILURE;
	}

	/*
	 * Stop the scheduler no matter what, so we can restart. Both helper
	 * functions correctly handle null scheduler_handles
	 */
	terminate_background_worker(db_he->db_scheduler_handle);
	wait_for_background_worker_shutdown(db_he->db_scheduler_handle);

	return register_scheduler_handle(vxid, db_he);
}

/*
 * Handle 1 message.
 */
static bool
launcher_handle_message(HTAB *db_htab)
{
	BgwMessage *message = bgw_message_receive();
	PGPROC	   *sender;
	VirtualTransactionId vxid;
	AckResult	action_result = ACK_FAILURE;

	if (message == NULL)
		return false;

	sender = BackendPidGetProc(message->sender_pid);
	if (sender == NULL)
	{
		ereport(LOG, (errmsg("TimescaleDB background worker launcher received message from non-existent backend")));
		return true;
	}

	GET_VXID_FROM_PGPROC(vxid, *sender);

	switch (message->message_type)
	{
		case START:
			action_result = message_start_action(db_htab, message, vxid);
			break;
		case STOP:
			action_result = message_stop_action(db_htab, message);
			break;
		case RESTART:
			action_result = message_restart_action(db_htab, message, vxid);
			break;
	}

	bgw_message_send_ack(message, action_result);
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
static void
launcher_sigterm(SIGNAL_ARGS)
{
	/*
	 * do not use a level >= ERROR because we don't want to exit here but
	 * rather only during CHECK_FOR_INTERRUPTS
	 */
	ereport(LOG,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("terminating TimescaleDB background worker launcher due to administrator command")));
	die(postgres_signal_arg);
}

extern Datum
ts_bgw_cluster_launcher_main(PG_FUNCTION_ARGS)
{

	HTAB	   *db_htab;

	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, launcher_sigterm);
	BackgroundWorkerUnblockSignals();
	ereport(DEBUG1, (errmsg("TimescaleDB background worker launcher started")));

	/* set counter back to zero on restart */
	bgw_counter_reinit();
	if (!bgw_total_workers_increment())
	{
		/*
		 * Should be the first thing happening so if we already exceeded our
		 * limits it means we have a limit of 0 and we should just exit We
		 * have to exit(0) because if we exit in error we get restarted by the
		 * postmaster.
		 */
		report_bgw_limit_exceeded();
		proc_exit(0);
	}
	/* Connect to the db, no db name yet, so can only access shared catalogs */
	BackgroundWorkerInitializeConnection(NULL, NULL);
	pgstat_report_appname(MyBgworkerEntry->bgw_name);
	ereport(LOG, (errmsg("TimescaleDB background worker launcher connected to shared catalogs")));

	bgw_message_queue_set_reader();
	db_htab = init_database_htab();
	populate_database_htab(db_htab);

	before_shmem_exit(launcher_pre_shmem_cleanup, PointerGetDatum(db_htab));

	start_db_schedulers(db_htab);

	while (true)
	{
		int			wl_rc;

		CHECK_FOR_INTERRUPTS();
		stopped_db_schedulers_cleanup(db_htab);
		if (launcher_handle_message(db_htab))
			continue;

#if PG96
		wl_rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0);
#else
		wl_rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH, 0, PG_WAIT_EXTENSION);
#endif
		ResetLatch(MyLatch);
		if (wl_rc & WL_POSTMASTER_DEATH)
			bgw_on_postmaster_death();
	}
	PG_RETURN_VOID();
}

/* Wrapper around `die()`, see note on `launcher_sigterm()` above for more info*/
static void
entrypoint_sigterm(SIGNAL_ARGS)
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
	HeapTuple	tuple;

	tuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));
	if (!HeapTupleIsValid(tuple))
		ereport(ERROR, (errmsg("TimescaleDB background worker failed to find entry for database in syscache")));

	pgdb = (Form_pg_database) GETSTRUCT(tuple);
	if (pgdb->datistemplate)
		ereport(ERROR, (errmsg("TimescaleDB background worker connected to template database, exiting")));

	ReleaseSysCache(tuple);
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
	Oid			db_id = DatumGetObjectId(MyBgworkerEntry->bgw_main_arg);
	bool		ts_installed = false;
	char		version[MAX_VERSION_LEN];
	VirtualTransactionId vxid;


	pqsignal(SIGINT, StatementCancelHandler);
	pqsignal(SIGTERM, entrypoint_sigterm);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnectionByOid(db_id, InvalidOid);
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
	ts_installed = loader_extension_exists();
	if (ts_installed)
		StrNCpy(version, loader_extension_version(), MAX_VERSION_LEN);

	loader_extension_check();
	CommitTransactionCommand();
	if (ts_installed)
	{
		char		soname[MAX_SO_NAME_LEN];
		PGFunction	versioned_scheduler_main;

		snprintf(soname, MAX_SO_NAME_LEN, "%s-%s", EXTENSION_NAME, version);
		versioned_scheduler_main = load_external_function(soname, BGW_DB_SCHEDULER_FUNCNAME, false, NULL);
		if (versioned_scheduler_main == NULL)
			ereport(LOG, (errmsg("TimescaleDB version %s does not have a background worker, exiting", soname)));
		else					/* essentially we morph into the versioned
								 * worker here */
			DirectFunctionCall1(versioned_scheduler_main, InvalidOid);
	}
	PG_RETURN_VOID();
}
