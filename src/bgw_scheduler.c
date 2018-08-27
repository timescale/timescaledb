/*
 * Contains code for the db_launcher background worker as well as the child workers
 * spawned to run various tasks.
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

/* for dealing with signals*/
#include <libpq/pqsignal.h>

#include "compat.h"
#include "extension.h"
#include "bgw_launcher_interface.h"
#include "bgw_scheduler.h"


/*
 * NOTE: We will throw an error if we have gotten a sigterm due to the default
 * signal handler for a bgworker (in bgworker.c) We don't block signals and
 * set up our signal handler, because we would like this to be the default
 * behavior.  Instead of exiting at the end of our loop, this will pull us out
 * of any waits or loops we happen to be in. We will also run our cleanup
 * function set in the before_shmem_exit -> this is where we will terminate
 * any child workers we have started.  In the case of postmaster death, we run
 * on_exit_reset (and should in any workers as well) and just get the hell out
 * of there. Trying to communicate with other workers will now be impossible
 * because the postmaster doesn't exist and it mediated that communication.
 */
static inline void
bgw_on_postmaster_death(void)
{
	on_exit_reset();			/* don't call exit hooks cause we want to bail
								 * out quickly */
	ereport(FATAL,
			(errcode(ERRCODE_ADMIN_SHUTDOWN),
			 errmsg("postmaster exited while timescale bgw was working")));
}

/* Note that this is a boilerplate/mock for now just so we can test the
 * launcher code */
extern Datum
bgw_db_scheduler_main(PG_FUNCTION_ARGS)
{
	int			num_wakes = 0;

	/* TODO: SETUP BEFORE_SHMEM_EXIT_CALLBACK */

	ereport(LOG, (errmsg("Versioned Worker started for Database id = %d with pid %d",
						 MyDatabaseId, MyProcPid)));
	CHECK_FOR_INTERRUPTS();

	while (true)
	{
		int			wl_rc;

		ereport(LOG, (errmsg("Database id = %d, Wake # %d ", MyDatabaseId, num_wakes)));
		ereport(LOG, (errmsg("Unreserved Workers = %d", bgw_num_unreserved())));
		num_wakes++;

#if PG96
		wl_rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 5 * 1000L);
#else
		wl_rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 5 * 1000L, PG_WAIT_EXTENSION);
#endif

		ResetLatch(MyLatch);

		if (wl_rc & WL_POSTMASTER_DEATH)
			bgw_on_postmaster_death();
		CHECK_FOR_INTERRUPTS();
	}
}
