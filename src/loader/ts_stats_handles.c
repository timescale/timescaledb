/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "ts_stats_handles.h"

#include <postgres.h>
#include <miscadmin.h>
#include <storage/ipc.h>
#include <storage/shmem.h>
#include <utils/memutils.h>

/*
 * Registry of the per-database shared memory segments for chunk observability,
 * keyed by database OID. We cannot use the DSM registry (GetNamedDSMSegment)
 * for this, because it pins the named segments forever and offers no way to
 * remove them, so the segments of dropped databases would leak.
 */

static Size
ts_stats_handle_table_size(void)
{
	return MAXALIGN(sizeof(TsObservHandleTable));
}

void
ts_stats_shmem_request(void)
{
	RequestAddinShmemSpace(ts_stats_handle_table_size());
}

void
ts_stats_shmem_startup(void)
{
	bool found;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	TsObservHandleTable *tbl =
		ShmemInitStruct("ts_stats_handles", ts_stats_handle_table_size(), &found);
	if (!found)
	{
		memset(tbl, 0, ts_stats_handle_table_size());
#if PG19_GE
		/* PG19 merged tranche registration into LWLockNewTrancheId(name). */
		LWLockInitialize(&tbl->lock, LWLockNewTrancheId("ts_stats_handles"));
#else
		LWLockInitialize(&tbl->lock, LWLockNewTrancheId());
		LWLockRegisterTranche(tbl->lock.tranche, "ts_stats_handles");
#endif
		tbl->max_entries = TS_STATS_MAX_DATABASES;
		for (int i = 0; i < TS_STATS_MAX_DATABASES; i++)
		{
			tbl->entries[i].dboid = InvalidOid;
		}
	}

	LWLockRelease(AddinShmemInitLock);

	/* Publish via rendezvous for the versioned extension to find. */
	TsObservHandleTable **rv =
		(TsObservHandleTable **) find_rendezvous_variable("ts_stats_handles");
	*rv = tbl;
}
