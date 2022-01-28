/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
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

int ts_guc_max_background_workers = 16;

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
	slock_t mutex; /* controls modification of total_workers */
	int total_workers;
} CounterState;

static CounterState *ct = NULL;

static void
bgw_counter_state_init()
{
	bool found;

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
ts_bgw_counter_setup_gucs(void)
{
	DefineCustomIntVariable("timescaledb.max_background_workers",
							"Maximum background worker processes allocated to TimescaleDB",
							"Max background worker processes allocated to TimescaleDB - set to at "
							"least 1 + number of databases in Postgres instance to use background "
							"workers ",
							&ts_guc_max_background_workers,
							ts_guc_max_background_workers,
							0,
							1000, /* no reasonable way to have more than
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
ts_bgw_counter_shmem_alloc(void)
{
	RequestAddinShmemSpace(sizeof(CounterState));
}

extern void
ts_bgw_counter_shmem_startup(void)
{
	bgw_counter_state_init();
}

extern void
ts_bgw_counter_reinit(void)
{
	/* set counter back to zero on startup */
	SpinLockAcquire(&ct->mutex);
	ct->total_workers = 0;
	SpinLockRelease(&ct->mutex);
}

extern bool
ts_bgw_total_workers_increment_by(int increment_by)
{
	bool incremented = false;
	int max_workers = ts_guc_max_background_workers;

	SpinLockAcquire(&ct->mutex);
	if (ct->total_workers + increment_by <= max_workers)
	{
		ct->total_workers += increment_by;
		incremented = true;
	}
	SpinLockRelease(&ct->mutex);
	return incremented;
}

extern bool
ts_bgw_total_workers_increment()
{
	return ts_bgw_total_workers_increment_by(1);
}

extern void
ts_bgw_total_workers_decrement_by(int decrement_by)
{
	/*
	 * Launcher is 1 worker, and when it dies we reinitialize, so we should
	 * never be below 1
	 */
	SpinLockAcquire(&ct->mutex);
	if (ct->total_workers - decrement_by >= 1)
	{
		ct->total_workers -= decrement_by;
		SpinLockRelease(&ct->mutex);
	}
	else
	{
		SpinLockRelease(&ct->mutex);
		ereport(FATAL,
				(errmsg("TimescaleDB background worker cannot decrement workers below 1"),
				 errhint("The background worker scheduler is in an invalid state and may not be "
						 "keeping track of workers allocated to TimescaleDB properly, please "
						 "submit a bug report.")));
	}
}

extern void
ts_bgw_total_workers_decrement()
{
	ts_bgw_total_workers_decrement_by(1);
}

extern int
ts_bgw_total_workers_get()
{
	int nworkers;

	SpinLockAcquire(&ct->mutex);
	nworkers = ct->total_workers;
	SpinLockRelease(&ct->mutex);
	return nworkers;
}
