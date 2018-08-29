/* needed for initializing shared memory and using various locks */
#include <postgres.h>

#include <miscadmin.h>
#include <storage/lwlock.h>
#include <utils/hsearch.h>
#include <storage/spin.h>
#include <storage/shmem.h>
#include <storage/ipc.h>
#include <storage/latch.h>
#include <utils/guc.h>

#include "bgw_counter.h"

#define BGW_COUNTER_STATE_NAME "ts_bgw_counter_state"

int			guc_max_background_workers = 8;

/*
 * We need a bit of shared state here to deal with keeping track of the total
 * number of background workers we've launched across the instance since we
 * don't want to exceed some configured value.  We considered, briefly, the
 * possibility of using pg_sema for this, unfortunately it does not appear to
 * be accessible to code outside of postgres core in any meaningful way. So
 * we're not using that.
 */
typedef struct CounterState
{
	/*
	 * Using an slock because we're only taking it for very brief periods to
	 * read a single value so no need for an lwlock
	 */
	slock_t		mutex;			/* controls modification of total_workers */
	int			total_workers;
} CounterState;


static CounterState *ct = NULL;

static void
bgw_counter_state_init()
{
	bool		found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ct = ShmemInitStruct(BGW_COUNTER_STATE_NAME, sizeof(CounterState), &found);
	if (!found)
	{
		memset(ct, 0, sizeof(CounterState));
		SpinLockInit(&ct->mutex);
		ct->total_workers = 0;
	}
	LWLockRelease(AddinShmemInitLock);
}

extern void
bgw_counter_setup_gucs(void)
{

	DefineCustomIntVariable("timescaledb.max_background_workers",
							"Maximum background worker processes allocated to TimescaleDB",
							"Max background worker processes allocated to TimescaleDB - set to at least 1 + number of databases in Postgres instance to use background workers ",
							&guc_max_background_workers,
							guc_max_background_workers,
							0,
							1000,	/* no reasonable way to have more than
									 * 1000 background workers */
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);
}

/*
 * This gets called by the loader (and therefore the postmaster) at
 * shared_preload_libraries time
 */
extern void
bgw_counter_shmem_alloc(void)
{
	RequestAddinShmemSpace(sizeof(CounterState));
}

extern void
bgw_counter_shmem_startup(void)
{
	bgw_counter_state_init();
}

extern void
bgw_counter_reinit(void)
{
	/* set counter back to zero on startup */
	SpinLockAcquire(&ct->mutex);
	ct->total_workers = 0;
	SpinLockRelease(&ct->mutex);
}

extern bool
bgw_total_workers_increment()
{
	bool		incremented = false;
	int			max_workers = guc_max_background_workers;

	SpinLockAcquire(&ct->mutex);
	if (ct->total_workers < max_workers)
	{
		ct->total_workers++;
		incremented = true;
	}
	SpinLockRelease(&ct->mutex);
	return incremented;
}

extern void
bgw_total_workers_decrement()
{
	/*
	 * Launcher is 1 worker, and when it dies we reinitialize, so we should
	 * never be below 1
	 */
	SpinLockAcquire(&ct->mutex);
	if (ct->total_workers > 1)
		ct->total_workers--;
	else
		ereport(ERROR, (errmsg("TimescaleDB background worker internal error: cannot decrement workers below 1"),
						errhint("The background worker scheduler is in an invalid state, please submit a bug report.")));
	SpinLockRelease(&ct->mutex);
}

extern bool
bgw_total_workers_increment_by(int increment_by)
{
	bool		incremented = false;
	int			max_workers = guc_max_background_workers;

	SpinLockAcquire(&ct->mutex);
	if (ct->total_workers + increment_by <= max_workers)
	{
		ct->total_workers += increment_by;
		incremented = true;
	}
	SpinLockRelease(&ct->mutex);
	return incremented;
}

extern void
bgw_total_workers_decrement_by(int decrement_by)
{
	SpinLockAcquire(&ct->mutex);

	/*
	 * Launcher is 1 worker, and when it dies we reinitialize, so we should
	 * never be below 1
	 */
	if (ct->total_workers - decrement_by >= 1)
		ct->total_workers -= decrement_by;
	else
		ereport(ERROR, (errmsg("TimescaleDB background worker internal error: cannot decrement workers below 1"),
						errhint("The background worker scheduler is in an invalid state, please submit a bug report.")));
	SpinLockRelease(&ct->mutex);
}
extern int
bgw_total_workers_get()
{
	int			nworkers;

	SpinLockAcquire(&ct->mutex);
	nworkers = ct->total_workers;
	SpinLockRelease(&ct->mutex);
	return nworkers;
}
