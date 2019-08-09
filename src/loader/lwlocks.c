/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <storage/lwlock.h>
#include <storage/shmem.h>

#include "loader/lwlocks.h"

#define TS_LWLOCKS_SHMEM_NAME "ts_lwlocks_shmem"
#define CHUNK_APPEND_LWLOCK_TRANCHE_NAME "ts_chunk_append_lwlock_tranche"

/*
 * since shared memory can only be setup in a library loaded as
 * shared_preload_libraries we have to setup this struct here
 */
typedef struct TSLWLocks
{
	LWLock *chunk_append;
} TSLWLocks;

static TSLWLocks *ts_lwlocks = NULL;

void
ts_lwlocks_shmem_startup()
{
	bool found;
	LWLock **lock_pointer;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	ts_lwlocks = ShmemInitStruct(TS_LWLOCKS_SHMEM_NAME, sizeof(TSLWLocks), &found);
	if (!found)
	{
		memset(ts_lwlocks, 0, sizeof(TSLWLocks));
		ts_lwlocks->chunk_append = &(GetNamedLWLockTranche(CHUNK_APPEND_LWLOCK_TRANCHE_NAME))->lock;
	}
	LWLockRelease(AddinShmemInitLock);

	/*
	 * We use a lock specific rendezvous variable to decouple the struct
	 * from the individual lock users to have no constraints on the struct
	 * across timescaledb versions.
	 */
	lock_pointer = (LWLock **) find_rendezvous_variable(RENDEZVOUS_CHUNK_APPEND_LWLOCK);
	*lock_pointer = ts_lwlocks->chunk_append;
}

void
ts_lwlocks_shmem_alloc()
{
	RequestNamedLWLockTranche(CHUNK_APPEND_LWLOCK_TRANCHE_NAME, 1);
	RequestAddinShmemSpace(sizeof(TSLWLocks));
}
