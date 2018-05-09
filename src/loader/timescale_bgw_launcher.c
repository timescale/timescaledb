/*
 * Main bgw launcher for the cluster. Run through the timescale loader, so needs to have a 
 * small footprint as any interactions it has will need to remain backwards compatible for 
 * the foreseeable future. 
 * 
 * Notes: multiple databases in a PG Cluster can have Timescale installed. They are not necessarily 
 * the same version of Timescale (though they could be)
 * Shared memory is allocated and background workers are registered at shared_preload_libraries time
 * We do not know what databases exist, nor which databases Timescale is installed in (if any) at
 * shared_preload_libraries time. 
 * 
 * It contains code that will be called by the loader at two points
 *  1) Initialize a shared memory segment that has 
 *  2) Start a cluster launcher that gets the dbs in the cluster, and starts a worker for each
 *   of them. 
 * 
 *
 * 
 * So how do we deal with dependent vs independent libraries for the timescale version?
 * one possible approach: cluster launcher launches per-db interrogator workers, these workers 
 * are only responsible for populating whether timescale is installed and what version it is installed
 * at in shared memory then they launch a new worker for the db that loads the proper library 
 * as its main arg and they die?
 * 
*/

#include <postgres.h>

/* These are always necessary for a bgworker */
#include <miscadmin.h>
#include <postmaster/bgworker.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <storage/lwlock.h>
#include <storage/proc.h>
#include <storage/shmem.h>             

/* needed for getting database list*/
#include <catalog/pg_database.h>
#include <access/xact.h>
#include <access/heapam.h>
#include <access/htup_details.h>

/* needed for initializing shared memory */
#include <utils/hsearch.h>


/* TODO: WRITE THE SHMEM STUFF*/


/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;


/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
timescale_bgw_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to tell the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
timescale_bgw_sighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}
/* Model this on autovacuum.c -> get_database_list */

extern void register_timescale_bgw_launcher(void) {
    BackgroundWorker worker;

    ereport(LOG, (errmsg("Registering Timescale BGW Launcher")));

    /*set up worker settings for our main worker */
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_notify_pid = 0;
    /* TODO: maybe pass in the library name, we know it, one assumes, as we are in 
    shared-preload-libraries when we call this. The problem could be that otherwise
    a specific versioned library of timescaledb in shared preload libraries will 
    break this otherwise?*/
    sprintf(worker.bgw_library_name, "timescaledb");
    sprintf(worker.bgw_function_name , "timescale_bgw_launcher_main");
    snprintf(worker.bgw_name, BGW_MAXLEN, "timescale_bgw_launcher");

    RegisterBackgroundWorker(&worker);
    ereport(LOG, (errmsg("Registered Timescale BGW Launcher"))); 
}

extern void timescale_bgw_launcher_main(void) {
    /* Establish signal handlers before unblocking signals. */
    ereport(LOG, (errmsg("Starting Timescale BGW Launcher")));
	pqsignal(SIGHUP, timescale_bgw_sighup);
	pqsignal(SIGTERM, timescale_bgw_sigterm);
    BackgroundWorkerUnblockSignals();
    /* Connect to the db, no db name yet, so can only access shared catalogs*/
    ereport(LOG, (errmsg("BGW Launcher Signals Unblocked"))); 
    BackgroundWorkerInitializeConnection(NULL, NULL);
    ereport(LOG, (errmsg("Timescale BGW Launcher Connected To DB")));

}