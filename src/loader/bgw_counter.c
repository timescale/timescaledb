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
	 * read a single value so no need for an lw_lock
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
	/* We don't actually care about found, we always init */
	ct = ShmemInitStruct(BGW_COUNTER_STATE_NAME, sizeof(CounterState), &found);
	memset(ct, 0, sizeof(CounterState));
	SpinLockInit(&ct->mutex);
	ct->total_workers = 0;
	LWLockRelease(AddinShmemInitLock);
}

extern void
bgw_counter_setup_gucs(void)
{

	DefineCustomIntVariable("timescaledb.max_bgw_processes",
							"Maximum background worker processes allocated to TimescaleDB",
							"Max background worker processes allocated to TimescaleDB - set to at least 1 + number of databases in Postgres instance to use background workers ",
							&guc_max_background_workers,
							guc_max_background_workers,
							0,
							max_worker_processes,
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
bgw_counter_shmem_cleanup(void)
{
	bgw_counter_state_init();
}

extern bool
bgw_total_workers_increment()
{
	bool		incremented = false;
	int			max_workers = guc_max_background_workers;

	SpinLockAcquire(&ct->mutex);
	if (ct->total_workers < max_workers)	/* result can be <= max_workers,
											 * so we test for less than */
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
	SpinLockAcquire(&ct->mutex);
	ct->total_workers--;
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
