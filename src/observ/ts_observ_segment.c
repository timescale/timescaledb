/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "ts_observ_segment.h"
#include "guc.h" /* for ts_guc_observ_buffer_capacity */

/*
 * The shared memory segment that holds the observability log for a database. The complication
 * is that the convenience function GetNamedDSMSegment exists only in PG 17+ so we need a fallback
 * implementation for PG 15-16 that timescaledb supports at the moment. The fallback implementation
 * is coming from ts_observ_handles.[ch], which provides a simple rendezvous-based registry of DSM
 * segments keyed by database OID.
 */
#include <postgres.h>
#include <miscadmin.h>
#include <storage/lwlock.h>

#if PG17_GE
#include <storage/dsm_registry.h>
#else
#include "../loader/ts_observ_handles.h"
#include <storage/dsm.h>
#include <storage/shmem.h>
#endif

/* Shared by all versions: */

#define IS_OBSERV_ENABLED() (ts_guc_observ_buffer_capacity > 0)

Size
ts_observ_segment_size(void)
{
	Assert(IS_OBSERV_ENABLED());
	Size size = MAXALIGN(sizeof(TsObservSegment));
	size = add_size(size, mul_size((uint32) ts_guc_observ_buffer_capacity, sizeof(TsObservTuple)));
	return size;
}

/*
 * Initialize a freshly-allocated segment. Called exactly once per DSM segment, under
 * serialization provided by the caller (DSM registry lock on PG 17+, or before the handle
 * is published on PG 15-16). Never called on an existing, published segment — doing so
 * would race against writers holding the lock.
 */
static void
ts_observ_init_segment(void *ptr)
{
	TsObservSegment *seg = (TsObservSegment *) ptr;

	memset(seg, 0, ts_observ_segment_size());

	seg->magic = TS_OBSERV_SHMEM_MAGIC;
	seg->dboid = MyDatabaseId;

	LWLockInitialize(&seg->lock, LWLockNewTrancheId());
	LWLockRegisterTranche(seg->lock.tranche, "ts_observ");

	pg_atomic_init_u64(&seg->next_seqno, 0);
	pg_atomic_init_u64(&seg->last_event_ts, 0);
	seg->base_timestamp = GetCurrentTimestamp();

	seg->log.capacity = (uint32) ts_guc_observ_buffer_capacity;
	seg->log.capacity_mask = seg->log.capacity - 1;
}

/*
 * The segment pointer is cached in backend-local memory.
 */
static TsObservSegment *cached_segment = NULL;

/* PG 17+: per-database DSM via the DSM registry: */

#if PG17_GE

TsObservSegment *
ts_observ_get_segment(void)
{
	if (!IS_OBSERV_ENABLED())
		return NULL;

	if (likely(cached_segment != NULL))
		return cached_segment;

	char name[64];
	snprintf(name, sizeof(name), "ts_observ_db_%u", MyDatabaseId);

	bool found;
	cached_segment =
		GetNamedDSMSegment(name, ts_observ_segment_size(), ts_observ_init_segment, &found);

	if (cached_segment != NULL && cached_segment->magic != TS_OBSERV_SHMEM_MAGIC)
	{
		elog(WARNING,
			 "ts_observ: segment for database %u has unexpected magic, disabling",
			 MyDatabaseId);
		cached_segment = NULL;
	}

	return cached_segment;
}

#else /* PG17_LT */

/* PG 15-16: per-database DSM via the handle table */

static TsObservHandleEntry *
ts_observ_find_entry(TsObservHandleTable *tbl, Oid dboid)
{
	for (uint32 i = 0; i < tbl->max_entries; i++)
	{
		if (tbl->entries[i].dboid == dboid)
			return &tbl->entries[i];
	}
	return NULL;
}

static TsObservSegment *
ts_observ_create_segment(TsObservHandleTable *tbl)
{
	dsm_segment *seg = dsm_create(ts_observ_segment_size(), 0);
	if (seg == NULL)
		return NULL;

	dsm_pin_segment(seg);
	dsm_pin_mapping(seg);

	TsObservSegment *stats = dsm_segment_address(seg);
	ts_observ_init_segment(stats);

	/* Register in the handle table */
	LWLockAcquire(&tbl->lock, LW_EXCLUSIVE);

	TsObservHandleEntry *entry = ts_observ_find_entry(tbl, MyDatabaseId);
	if (entry == NULL)
	{
		entry = ts_observ_find_entry(tbl, InvalidOid);
		if (entry == NULL)
		{
			LWLockRelease(&tbl->lock);
			elog(WARNING,
				 "ts_observ: handle table full, observability disabled "
				 "for database %u",
				 MyDatabaseId);
			return stats; /* usable for this backend's lifetime */
		}
		tbl->num_entries++;
	}

	entry->dboid = MyDatabaseId;
	entry->handle = dsm_segment_handle(seg);

	LWLockRelease(&tbl->lock);
	return stats;
}

TsObservSegment *
ts_observ_get_segment(void)
{
	static bool rendezvous_checked = false;

	if (!IS_OBSERV_ENABLED())
		return NULL;

	if (likely(cached_segment != NULL))
		return cached_segment;

	if (rendezvous_checked)
		return NULL;
	rendezvous_checked = true;

	/* Find the handle table via rendezvous. */
	TsObservHandleTable **rv =
		(TsObservHandleTable **) find_rendezvous_variable("ts_observ_handles");
	TsObservHandleTable *tbl = *rv;

	if (tbl == NULL)
		return NULL;

	/* Look up our database. */
	LWLockAcquire(&tbl->lock, LW_SHARED);
	TsObservHandleEntry *entry = ts_observ_find_entry(tbl, MyDatabaseId);

	if (entry != NULL)
	{
		dsm_segment *seg = dsm_find_mapping(entry->handle);
		if (seg == NULL)
		{
			seg = dsm_attach(entry->handle);
			if (seg != NULL)
				dsm_pin_mapping(seg);
		}
		LWLockRelease(&tbl->lock);

		if (seg != NULL)
		{
			cached_segment = dsm_segment_address(seg);

			/*
			 * An existing segment whose magic is wrong indicates corruption.
			 * Disable rather than reinitialize (would race with writers).
			 */
			if (cached_segment->magic != TS_OBSERV_SHMEM_MAGIC)
			{
				elog(WARNING,
					 "ts_observ: segment for database %u has unexpected magic, disabling",
					 MyDatabaseId);
				cached_segment = NULL;
			}
			return cached_segment;
		}
	}
	else
	{
		LWLockRelease(&tbl->lock);
	}

	/* No segment yet for this database — create one. */
	cached_segment = ts_observ_create_segment(tbl);
	return cached_segment;
}

#endif /* PG17_GE */
