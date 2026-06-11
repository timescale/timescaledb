/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * ===========================================================================
 * Per-tenant invalidation tracker -- shared-memory design
 * ===========================================================================
 *
 * PURPOSE
 *   An aggregator (shared by all backends) that records per-tenant
 *   invalidated time ranges [min_ts, max_ts], and on continuous-aggregate refresh
 *   drains them into  _timescaledb_catalog.continuous_aggs_tenant_tracking.
 *   It is an optimization layered over the invalidation log: any overflow or
 *   failure degrades the refresh falling back to using the invalidation log
 *   instead of tenant based granular refresh
 *
 * ALLOCATION & LIFECYCLE (DSA-backed, allocated on demand -- no fixed cap)
 *   DSA areas, dshash tables and LWLock tranches must be set up by a
 *   shared_preload_libraries library, so the loader (src/loader/tenant_tracker_shmem.c)
 *   creates them and publishes a tiny fixed CONTROL block via a rendezvous
 *   variable (RENDEZVOUS_TENANT_TRACKER).  The control block holds the dshash
 *   handle, two LWLock tranche ids (map machinery / tracker partition locks), and
 *   a pointer to the in-place DSA region.
 *
 *   TenantTracking per hypertable is dsa_allocate'd on first use and found
 *   through a dshash keyed by (database, hypertable id).  The map is
 *   process-global but hypertable ids are per-database, so the database
 *   qualifier keeps two databases' equal ids from colliding on one tracker:
 *       map: (Oid database_id, int32 hypertable_id) -> dsa_pointer(TenantTracking)
 *   This tsl library attaches to the DSA + dshash lazily per backend (cached);
 *   ts_tenant_tracker_lookup() reads the dsa_pointer (NULL if absent),
 *   ts_tenant_tracker_get_or_attach() find-or-inserts and, on first insert,
 *   dsa_allocate0 + init_tracker while holding the dshash exclusive lock.  The
 *   dshash lock protects only the MAPPING; the tracker has its own concurrency
 *   and is never moved or freed, so a dsa_pointer stays valid after the
 *   dshash lock is released.
 *
 * LAYOUT (one tracker per hypertable; per-tracker sizes are compile-time
 *   constants: TENANT_TRACKER_CAPACITY=4096, TENANT_TRACKER_NUM_PARTITIONS=16,
 *   TENANT_TRACKER_KEY_MAXLEN=64, tracking is set to INVALID at 3/4 capacity to
 *   prevent look-up degradation. We might tune capacity or partitions later.)
 *
 *     TenantTracking  (dsa_allocate'd, ~705 KB)
 *      | hypertable_id (int32)
 *      | active_gen   (atomic u32)  0|1: generation writers use now
 *      | generations[2]             double buffer
 *          TenantGeneration
 *           | num_writers (atomic u32) writers currently pinning this gen
 *           | nentries    (atomic u32) occupied slots (full detection)
 *           | status      (atomic u32) TENANT_TRACKER_VALID | TENANT_TRACKER_INVALID
 *           | seqnum      (atomic u32) epoch; bound to the generation
 *           | partitions[16] LWLock   inline insert locks (disjoint slot sets),
 *           |                          LWLockInitialize'd with the partition
 *           |                          tranche id at tracker allocation
 *           | entries[4096]
 *               TenantEntry { u16 key_len(0=empty); u32 key_hash;
 *                             char key[64]; atomic u64 min_ts, max_ts }
 *
 *   - Open-addressed hash table, linear probing strided by TENANT_TRACKER_NUM_PARTITIONS so
 *     each partition lock owns a disjoint slot set.  key_hash is a cheap
 *     pre-memcmp reject; the entry stores the exact key bytes.
 *   - Merge operator is min/max re-applying or reordering updates is harmless as this
 *     is commutative and idempotent operation.
 *   - TODO: entries[] is a fixed TENANT_TRACKER_CAPACITY per generation, so it caps distinct
 *     late tenants per refresh interval and over-allocates for sparse hypertables;
 *     a growable dsa_allocate'd entries array (resize at flush quiescence) is the
 *     planned scaling follow-up.
 *
 * GENERATION MODEL (double buffer)
 *   active_gen selects the buffer writers use.  A flush flips it and drains the
 *   other buffer IN PLACE.  Within an active generation slots only go
 *   empty->occupied (never deleted), so a probe that hits an empty slot proves the
 *   tenant is absent.  The flush pre-clears the TARGET buffer (not the old one), so
 *   the drained buffer stays readable until the next flush reuses it.
 *
 * ACCESS PATHS
 *   Write ts_tenant_tracker_update (DML commit path):
 *     1. Pin the active gen: read active_gen -> fetch_add(num_writers) -> RE-READ active_gen;
 *        if it changed, fetch_sub and retry (pin-then-verify, so a concurrent flush's
 *        drain-wait cannot miss this writer).
 *     2. Bail to false if the generation is TENANT_TRACKER_INVALID, or mark as INVALID if the key
 *        is unstorable (len==0 or >TENANT_TRACKER_KEY_MAXLEN).
 *     3. Apply: lock-free probe to update an existing tenant via atomic CAS
 *        min/max; else take the partition lock and insert the new tenant.
 *        Insert trips TENANT_TRACKER_INVALID at 3/4 load or probe exhaustion.
 *     4. fetch_sub(num_writers).
 *
 *   Flush via ts_tenant_tracker_flush (refresh txn 2, under the per-cagg
 *   lock, active snapshot pushed):
 *     1. Pre-clear target buffer; bind its seqnum (old's + 1); write_barrier;
 *        flip active_gen. Writers can use this as the new active gen.
 *     2. Busy-wait for existing writers to finish up i.e. old.num_writers == 0 (interruptible).
 *     3. read_barrier.
 *     4. If old gen TENANT_TRACKER_INVALID -> persist <null,null,null,seqnum> marker.  Else
 *        batch-insert one row per occupied slot, stamped with old's seqnum, reading
 *        keys in place.
 *
 * CONCURRENCY PROTOCOL
 *   active_gen flip + writer pin/re-verify  flush and writers agree on the live
 *                                           buffer; drain never races live writers
 *   partition LWLock (exclusive)            inserter vs inserter, same partition
 *   atomic CAS min/max updates              concurrent updaters of an existing
 *                                           tenant (lock-free, convergent)
 *   num_writers counter                     flush waits for old-gen writers to
 *                                           quiesce before draining
 *   status = TENANT_TRACKER_INVALID                     overflow/unstorable key -> fallback
 *   memory barriers (below)                 cross-process visibility ordering
 *
 *   Three barrier pairings, all of the form
 *       producer: payload -> write-fence -> FLAG
 *       consumer: FLAG    -> read-fence  -> payload
 *
 *     FLAG          producer fence           consumer fence           protects
 *     ----------    ----------------------   ----------------------   ----------------------------
 *     key_len       pg_write_barrier         pg_read_barrier          entry payload published
 *                                                                     before slot marked live
 *     active_gen    pg_write_barrier         fetch_add (full fence)   target-buffer reset visible
 *                                                                     before the flip
 *     num_writers   fetch_sub (full fence)   pg_read_barrier          departed writers' stores
 *                                                                     visible before drain reads
 *
 *   Plus one store-load (StoreBuffer) fence, not a producer/consumer pairing:
 *
 *     FLAG/load     flush fence                         writer fence             protects
 *     ----------    ---------------------------------   ----------------------
 * ---------------------------- active_gen /  pg_memory_barrier (between          fetch_add (full
 * fence)   flush sees a live writer OR num_writers  active_gen flip and flush of        (between
 * bump & re-read) writer sees the flip; never old gen) both stale (store-load order)
 *
 *   PostgreSQL pg_atomic_read/write carry NO barrier; the ReadModifyWrite ops (fetch_add/sub,
 *   compare_exchange) are FULL fences -- hence some sides use a bare barrier and
 *   others get the fence for free from an Read-Modify-Write already on the path.  The
 *   FLAG-linking edge is reads-from when the consumer reads the new value
 *   (key_len), and single-location coherence when it reasons about the old value
 *   or a later write (active_gen, num_writers).
 *
 * INVARIANTS
 *   1. A writer only mutates the generation it has pinned AND re-verified active.
 *   2. Drain reads a generation only after num_writers == 0 on it.
 *   3. Slots are append-only within a generation; cleared only when a buffer is
 *      re-activated as the flush target.
 *   4. seqnum is monotonic; every drain produces either per-tenant rows or exactly
 *      one marker row for that seqnum.
 *
 * DURABILITY & FAILURE
 *   The shared-memory flip and seqnum bump are NON-transactional; the catalog
 *   inserts are part of refresh txn 2 and durable only on its commit.  Drained
 *   data is NOT re-merged into the live generation, and the old buffer is wiped by
 *   the next flush -- so if txn 2 rolls back, that seqnum leaves no rows and the
 *   data is gone from shmem.  Correctness then depends on the consumer treating a
 *   missing seqnum the same as an INVALID marker (fall back to the full log); see
 *   the restart-recovery TODO below.
 *
 * TODO (deferred to a later version):
 *   - Recovery on restart: on startup set seqnum = max(seqnum) from the
 *     hypertable invalidation log and write a <null,null,null,seqnum> marker so
 *     refreshes for that seqnum fall back (design doc 5.4.4 #1).
 *   - Per-cagg instances keyed by hypertable_id (V1 has a single instance).
 *   - num_writers is a per-generation counter touched by every writer; if it
 *     shows up in profiling, shard it per-partition and sum across partitions.
 *   - Replace the busy-wait in the flush with a ConditionVariable + timeout.
 *   - heap_multi_insert for very large drains.
 */

#include <postgres.h>

#include <common/hashfn.h>
#include <fmgr.h>
#include <lib/dshash.h>
#include <miscadmin.h>
#include <port/atomics.h>
#include <storage/lwlock.h>
#include <utils/dsa.h>
#include <utils/memutils.h>
#include <utils/timestamp.h>

#include "debug_point.h"
#include "loader/tenant_tracker_shmem.h"
#include "tenant_tracker.h"
#include "ts_catalog/continuous_aggs_tenant_tracking.h"

/* ------------------------------------------------------------------------- */
/* Private per-tracker struct layout.                            */
/* ------------------------------------------------------------------------- */
#define TENANT_TRACKER_CAPACITY 4096	 /* slots per generation (power of 2) */
#define TENANT_TRACKER_NUM_PARTITIONS 16 /* partition (insert) locks per generation */
#define TENANT_TRACKER_SLOTS_PER_PARTITION (TENANT_TRACKER_CAPACITY / TENANT_TRACKER_NUM_PARTITIONS)
#define TENANT_TRACKER_LOAD_FACTOR_NUM 3 /* trip INVALID at 3/4 full */
#define TENANT_TRACKER_LOAD_FACTOR_DEN 4

typedef enum
{
	TENANT_TRACKER_VALID = 0,
	TENANT_TRACKER_INVALID,
} TenantTrackStatus;

/*
 * One hash slot.  key_len == 0 marks an empty slot.  Within an active
 * generation slots only ever go empty -> occupied (never deleted), so a probe
 * that reaches an empty slot proves the tenant is absent.
 */
typedef struct TenantEntry
{
	uint16 key_len;						 /* 0 == empty */
	uint32 key_hash;					 /* cached: cheap reject before memcmp */
	char key[TENANT_TRACKER_KEY_MAXLEN]; /* exact tenant bytes */
	pg_atomic_uint64 min_ts;			 /* TimestampTz held as uint64 */
	pg_atomic_uint64 max_ts;
} TenantEntry;

struct TenantGeneration
{
	pg_atomic_uint32 num_writers; /* in-flight writers pinning this gen */
	pg_atomic_uint32 nentries;	  /* occupied slots (for full detection) */
	pg_atomic_uint32 status;	  /* TenantTrackStatus for this generation */
	/*
	 * Epoch stamped on this generation's writers and drained rows.  Bound to the
	 * generation (not the tracker) and published with the active_gen flip, so a
	 * writer reads (generation, epoch) as one consistent pair.
	 */
	pg_atomic_uint32 seqnum;
	/* Partition (insert) locks, inline so they live in the tracker's DSA
	 * allocation; LWLockInitialize'd with the partition tranche id at creation. */
	LWLock partitions[TENANT_TRACKER_NUM_PARTITIONS];
	TenantEntry entries[TENANT_TRACKER_CAPACITY];
};

/* One per-hypertable tracker, dsa_allocate'd on first use. */
struct TenantTracking
{
	int32 hypertable_id;		 /* TS raw hypertable id */
	pg_atomic_uint32 active_gen; /* 0 or 1: generation writers use now */
	TenantGeneration generations[2];
};

StaticAssertDecl((TENANT_TRACKER_CAPACITY & (TENANT_TRACKER_CAPACITY - 1)) == 0,
				 "TENANT_TRACKER_CAPACITY must be a power of 2");
StaticAssertDecl((TENANT_TRACKER_NUM_PARTITIONS & (TENANT_TRACKER_NUM_PARTITIONS - 1)) == 0,
				 "TENANT_TRACKER_NUM_PARTITIONS must be a power of 2");

/* ------------------------------------------------------------------------- */
/* Lock-free convergent min/max.                                             */
/*                                                                           */
/* TimestampTz is a signed int64; we keep the bit pattern in a uint64 atomic */
/* and compare as signed.  The CAS reloads `current` on failure and the loop */
/* exits once the candidate no longer extends the bound -- so once a tenant  */
/* has converged, callers do a plain read and no write.                      */
/* ------------------------------------------------------------------------- */
static inline void
atomic_min_ts(pg_atomic_uint64 *slot, TimestampTz candidate)
{
	int64 current = (int64) pg_atomic_read_u64(slot);

	while (candidate < current &&
		   !pg_atomic_compare_exchange_u64(slot, (uint64 *) &current, (uint64) candidate))
		; /* retry: `current` now holds the installed value */
}

static inline void
atomic_max_ts(pg_atomic_uint64 *slot, TimestampTz candidate)
{
	int64 current = (int64) pg_atomic_read_u64(slot);

	while (candidate > current &&
		   !pg_atomic_compare_exchange_u64(slot, (uint64 *) &current, (uint64) candidate))
		;
}

static inline bool
entry_matches(const TenantEntry *entry, uint32 hash, const char *key, uint16 key_len)
{
	return entry->key_hash == hash && entry->key_len == key_len &&
		   memcmp(entry->key, key, key_len) == 0;
}

/*
 * Merge one tenant's range into a specific generation.  Caller has already
 * pinned the generation (num_writers).  Returns false if the generation had to
 * be marked INVALID (full / probe exhausted).
 */
static bool
tenant_tracker_apply(TenantGeneration *generation, uint32 hash, const char *key, uint16 key_len,
					 TimestampTz min_ts, TimestampTz max_ts)
{
	/* the generation buffer is used as a hash table that is split into partitions,
	 * so that we don't have to lock the entire hash table to add a new hash key.
	 * This is a simple hash table with open addressing.
	 * Only inserts to the hash table need a lock. Updates to an entry are cheap as
	 * we modify min/max using a CAS operation (and don't use a lock like the built in dshash)
	 */
	int partition_idx = hash & (TENANT_TRACKER_NUM_PARTITIONS - 1);
	int start_index = (hash / TENANT_TRACKER_NUM_PARTITIONS) % TENANT_TRACKER_SLOTS_PER_PARTITION;
	LWLock *partition_lock = &generation->partitions[partition_idx];

	/*
	 * Each partition owns a CONTIGUOUS block of TENANT_TRACKER_SLOTS_PER_PARTITION entries:
	 * partition p owns entries[p*S .. p*S + S-1].  The blocks are disjoint (one
	 * lock per block) and probing stays within the block, so a key only ever
	 * needs its own partition lock.
	 * partition selection uses the low hash bits, so keys spread
	 * evenly across the blocks.
	 */
	int partition_base = partition_idx * TENANT_TRACKER_SLOTS_PER_PARTITION;

	/* ---- fast path: lock-free probe for an existing tenant ---- */
	for (int i = 0, slot_index = start_index; i < TENANT_TRACKER_SLOTS_PER_PARTITION;
		 i++, slot_index = (slot_index + 1) % TENANT_TRACKER_SLOTS_PER_PARTITION)
	{
		TenantEntry *entry = &generation->entries[partition_base + slot_index];
		uint16 found_len = entry->key_len;

		if (found_len == 0) /* empty slot -> tenant not present yet */
		{
			break;
		}

		/* See the published payload before reading the rest of the entry. */
		pg_read_barrier(); /* for key_len */

		if (entry_matches(entry, hash, key, found_len))
		{
			atomic_min_ts(&entry->min_ts, min_ts);
			atomic_max_ts(&entry->max_ts, max_ts);
			return true;
		}
	}

	/* ---- slow path: insert under this partition's lock ---- */
	LWLockAcquire(partition_lock, LW_EXCLUSIVE);

	for (int i = 0, slot_index = start_index; i < TENANT_TRACKER_SLOTS_PER_PARTITION;
		 i++, slot_index = (slot_index + 1) % TENANT_TRACKER_SLOTS_PER_PARTITION)
	{
		TenantEntry *entry = &generation->entries[partition_base + slot_index];

		/* A racer may have inserted this tenant while we waited for the lock. */
		if (entry->key_len != 0 && entry_matches(entry, hash, key, key_len))
		{
			atomic_min_ts(&entry->min_ts, min_ts);
			atomic_max_ts(&entry->max_ts, max_ts);
			LWLockRelease(partition_lock);
			return true;
		}

		if (entry->key_len == 0) /* claim this slot */
		{
			if (pg_atomic_read_u32(&generation->nentries) >= TENANT_TRACKER_CAPACITY *
																 TENANT_TRACKER_LOAD_FACTOR_NUM /
																 TENANT_TRACKER_LOAD_FACTOR_DEN)
			{
				break; /* too full -> fall through to INVALID */
			}

			/* Publish the payload before key_len so a lock-free reader that
			 * sees key_len != 0 also sees a fully initialized entry. */
			pg_atomic_write_u64(&entry->min_ts, (uint64) min_ts);
			pg_atomic_write_u64(&entry->max_ts, (uint64) max_ts);
			entry->key_hash = hash;
			memcpy(entry->key, key, key_len);
			pg_write_barrier(); /* for key_len */
			entry->key_len = key_len;

			pg_atomic_fetch_add_u32(&generation->nentries, 1);
			LWLockRelease(partition_lock);
			return true;
		}
	}

	/* No room in this partition: stop tracking and let the caller fall back. */
	pg_atomic_write_u32(&generation->status, TENANT_TRACKER_INVALID);
	LWLockRelease(partition_lock);
	return false;
}

bool
ts_tenant_tracker_update(TenantTracking *tracking, const char *key, uint16 key_len,
						 TimestampTz min_ts, TimestampTz max_ts)
{
	uint32 hash = 0;
	uint32 gen;
	TenantGeneration *generation;
	bool result;

	if (key_len > 0 && key_len <= TENANT_TRACKER_KEY_MAXLEN)
	{
		hash = hash_bytes((const unsigned char *) key, key_len);
	}

	/*
	 * Pin the active generation: bump num_writers, then re-read active_gen.  If
	 * a flush swapped generations between the two, back the count out and retry
	 * so the flush's drain wait cannot miss us.
	 */
retry_generation:
	/* we read the active_gen here. Need a read barrier (see tenant_tracker_flush)
	 * pg_atomic_fetch_add_u32 is a read/write barrier.
	 * so we don't an explicit read barrier
	 */
	gen = pg_atomic_read_u32(&tracking->active_gen);
	generation = &tracking->generations[gen];
	pg_atomic_fetch_add_u32(&generation->num_writers, 1);

	/* we could have a situation where
	 * 1. Writer W1: reads active_gen == 1.
	 * 2. (writer stalls)
	 * 3. Flush flips active_gen =0
	 * 4. Flush: wait loop reads A.num_writers --> sees 0 writers --> proceeds.
	 * 5. Flush: drains A and returns.
	 * 6. Writer resumes and updates num_writers for generation =1 . But flush has moved on to
	 * gen=0. So we need a check here to verify that we are still at the correct generation
	 */

	if (pg_atomic_read_u32(&tracking->active_gen) != gen)
	{
		pg_atomic_fetch_sub_u32(&generation->num_writers, 1);
		goto retry_generation;
	}

	if (pg_atomic_read_u32(&generation->status) == TENANT_TRACKER_INVALID)
	{
		result = false;
	}
	else if (key_len == 0 || key_len > TENANT_TRACKER_KEY_MAXLEN)
	{
		/* Cannot store this tenant exactly -> stop tracking this generation. */
		pg_atomic_write_u32(&generation->status, TENANT_TRACKER_INVALID);
		result = false;
	}
	else
	{
		result = tenant_tracker_apply(generation, hash, key, key_len, min_ts, max_ts);
	}

	pg_atomic_fetch_sub_u32(&generation->num_writers, 1); /* write barrier for num_writers */
	return result;
}

TenantGeneration *
ts_tenant_tracker_begin_batch(TenantTracking *tracking, int32 *seqnum)
{
	uint32 gen;
	TenantGeneration *generation;

	Assert(seqnum != NULL);

	/* Pin the active generation ONCE for the whole batch (see
	 * ts_tenant_tracker_update for the pin-then-verify rationale). */
retry_generation:
	gen = pg_atomic_read_u32(&tracking->active_gen);
	generation = &tracking->generations[gen];
	pg_atomic_fetch_add_u32(&generation->num_writers, 1);
	if (pg_atomic_read_u32(&tracking->active_gen) != gen)
	{
		pg_atomic_fetch_sub_u32(&generation->num_writers, 1);
		goto retry_generation;
	}

	*seqnum = (int32) pg_atomic_read_u32(&generation->seqnum);

	return generation;
}

bool
ts_tenant_tracker_apply_one(TenantGeneration *generation, const char *key, uint16 key_len,
							TimestampTz min_ts, TimestampTz max_ts)
{
	uint32 hash;

	/* Once the generation is INVALID nothing more can be recorded. */
	if (pg_atomic_read_u32(&generation->status) == TENANT_TRACKER_INVALID)
	{
		return false;
	}

	if (key_len == 0 || key_len > TENANT_TRACKER_KEY_MAXLEN)
	{
		pg_atomic_write_u32(&generation->status, TENANT_TRACKER_INVALID);
		return false;
	}

	hash = hash_bytes((const unsigned char *) key, key_len);

	/* tenant_tracker_apply returns false if it just tripped INVALID (full). */
	return tenant_tracker_apply(generation, hash, key, key_len, min_ts, max_ts);
}

void
ts_tenant_tracker_end_batch(TenantGeneration *generation)
{
	pg_atomic_fetch_sub_u32(&generation->num_writers, 1);
}

void
ts_tenant_tracker_mark_invalid(TenantTracking *tracking)
{
	uint32 gen;
	TenantGeneration *generation;

	/* Pin the active generation the same way ts_tenant_tracker_update does. */
retry_generation:
	gen = pg_atomic_read_u32(&tracking->active_gen);
	generation = &tracking->generations[gen];
	pg_atomic_fetch_add_u32(&generation->num_writers, 1);
	if (pg_atomic_read_u32(&tracking->active_gen) != gen)
	{
		pg_atomic_fetch_sub_u32(&generation->num_writers, 1);
		goto retry_generation;
	}

	pg_atomic_write_u32(&generation->status, TENANT_TRACKER_INVALID);

	pg_atomic_fetch_sub_u32(&generation->num_writers, 1);
}

/*
 * Refresh Transaction 2 flush path.  Drains the current generation, starts a
 * fresh one, and persists the drained tenants directly into
 * _timescaledb_catalog.continuous_aggs_tenant_tracking
 *
 * If the generation was INVALID (full / overflowed), no per-tenant rows are
 * written; instead the <null,null,null,seqnum> marker is persisted so the
 * refresh falls back to the full invalidation log.
 *
 * We flush shared mem contents to disk by swapping between the 2 buffers
 * (old_gen and new_gen) and then copying the contents to disk.
 * It should be atomic so that any new writer writes to the correct
 * gen of the buffer.
 * We use write/read barriers to achieve this:
 *  Flush (the writer of active_gen):
 *      P1: reset new buffer  (key_len = 0 for every slot)
 *      P2: pg_write_barrier()
 *      P3: active_gen = new  -------------> this is the flag protected by the barrier
 *  active_gen will be written out only after the buffer for active_gen is ready for use.
 *  the write barrier ensues that there is no write reordering.
 *
 * Writer i.e DML paths (the reader of active_gen):
 *      C1: gen = read(active_gen)
 *      C2: --------> need read barrier here. ensures that everything that happened before write to
 *          ----> active_gen is visible here. (in this case the reset values for entries)
 *      C3: read entries of generations[gen]
 *
 *
 */
void
ts_tenant_tracker_flush(TenantTracking *tracking, int32 hypertable_id)
{
	uint32 old_gen = pg_atomic_read_u32(&tracking->active_gen);
	uint32 new_gen = 1 - old_gen;
	TenantGeneration *old = &tracking->generations[old_gen];
	TenantGeneration *target = &tracking->generations[new_gen];
	bool was_valid;
	int32 seqnum;

	/*
	 * `old`'s epoch: drained rows are stamped with it, and the generation we
	 * activate below gets the next one.  No barrier needed -- only the flush
	 * writes seqnum, and flushes are serialized by the caller's per-cagg lock.
	 */
	seqnum = (int32) pg_atomic_read_u32(&old->seqnum);

	/*
	 * Reset the old generation we are about to activate so writers find it empty.
	 * It is currently inactive, so no writer pins it (writers only ever pin the
	 * active generation).
	 * Resetting the new gen just before flip allows us to read the
	 * read the old generation in place during persistence without copying its keys,
	 * and needs no error-time cleanup (the old generation is cleared by the next flush,
	 * when it becomes the active_gen )
	 */
	for (int i = 0; i < TENANT_TRACKER_CAPACITY; i++)
	{
		target->entries[i].key_len = 0;
	}
	pg_atomic_write_u32(&target->nentries, 0);
	pg_atomic_write_u32(&target->status, TENANT_TRACKER_VALID);
	/* Bind the next epoch to the generation we are about to activate. */
	pg_atomic_write_u32(&target->seqnum, (uint32) (seqnum + 1));

	/* Publish the reset AND the next epoch before the flip so a writer that pins
	 * the newly-active generation sees re-inited entries and the matching epoch.
	 */
	pg_write_barrier(); /* for active_gen */
	pg_atomic_write_u32(&tracking->active_gen, new_gen);

	/*
	 * Make the active_gen flip above visible before we read num_writers below.
	 * A writer increments num_writers and then re-reads active_gen; here we do
	 * the opposite order (write active_gen, then read num_writers).  Without a
	 * full barrier on both sides, a writer could still see the old active_gen
	 * and keep updating this generation while we read num_writers == 0 and
	 * start reading its entries.  The write/read barriers above and below do
	 * not help: neither one is between the flip and this read.
		 * This is the problematic scenario:
		 *    DML  Writer pin
		 *       - S: fetch_add(num_writers) — store
		 *       - L: re-read active_gen
		 *       - fetch_add is a full fence, so there is a StoreLoad fence between S and L.

		 *   Flush :
		 *      - S: write(active_gen = new)
		 *      - L: read(num_writers)
		 *      - pg_memory_barrier is the full fence between S and L
	 */
	pg_memory_barrier();

	/*
	 * Wait for in-flight writers on the old generation to finish.  We hold no
	 * LWLock here, so it is safe to loop and accept interrupts.
	 *
	 * The pin is held only for one tenant_tracker_apply (a few atomics + a <=64B
	 * memcpy, no I/O), so num_writers normally drops to 0 in well under the 1ms
	 * sleep granularity.  The busy-wait is fine for that profile and keeps the
	 * hot DML writer path free of any flush-side synchronization.
	 *
	 * POTENTIAL IMPROVEMENT (only if a real slowdown shows up here): replace the
	 * spin with a *gated* ConditionVariable -- flush sets a flush_waiting flag and
	 * sleeps on the CV; writers broadcast on their num_writers decrement only when
	 * that flag is set.  An UNCONDITIONAL CV signal would tax every INSERT to speed
	 * up a rare flush (wrong trade for a lock-free writer); gating keeps the writer
	 * cost to one atomic read.
	 * Cost: one more StoreLoad ordering case (flush_waiting <->
	 * num_writers), analogous to the active_gen <-> num_writers fence above
	 */
	while (pg_atomic_read_u32(&old->num_writers) > 0)
	{
		CHECK_FOR_INTERRUPTS();
		pg_usleep(1000L); /* 1ms */
	}

	/* See the quiesced writers' entry stores before reading the old generation. */
	pg_read_barrier(); /* read barrier for num_writers */

	was_valid = (pg_atomic_read_u32(&old->status) == TENANT_TRACKER_VALID);

	/* persisted rows are stamped with `old`'s epoch (`seqnum`, read above). */

	/*
	 * Persist directly from the quiesced old generation: read each entry's key
	 * bytes in place and hand them to the catalog inserter, which copies them
	 * into the tuple.  No intermediate snapshot array, no per-row copy here.  The
	 * old generation stays readable until the next flush resets it.
	 */
	if (!was_valid)
	{
		/* INVALID: write the marker; refresh falls back to the full log. */
		ts_cagg_tenant_tracking_insert_invalid_marker(hypertable_id, seqnum);
		return;
	}

	int occupied = (int) pg_atomic_read_u32(&old->nentries);

	if (occupied > 0)
	{
		CaggTenantTrackingInserter *inserter =
			ts_cagg_tenant_tracking_insert_begin(hypertable_id, seqnum);
		int count = 0;

		for (int i = 0; i < TENANT_TRACKER_CAPACITY && count < occupied; i++)
		{
			TenantEntry *entry = &old->entries[i];

			if (entry->key_len == 0)
			{
				continue;
			}

			ts_cagg_tenant_tracking_insert_row(inserter,
											   entry->key,
											   entry->key_len,
											   (int64) pg_atomic_read_u64(&entry->min_ts),
											   (int64) pg_atomic_read_u64(&entry->max_ts));
			count++;
		}

		ts_cagg_tenant_tracking_insert_end(inserter);
	}
}

/*
 * Per-backend cached attachment.  The loader publishes the control block; this
 * backend attaches to the DSA area + dshash lazily on first use and caches them
 * (DSM maps at different addresses per backend, so each attaches its own).
 * Returns false if the loader is absent.
 */
static TenantTrackerControl *control = NULL;
static dsa_area *tracker_area = NULL;
static dshash_table *tracker_map = NULL;

static dshash_parameters
tenant_map_params(void)
{
	return tenant_tracker_map_params(control->map_tranche_id);
}

static bool
tenant_tracker_attach(void)
{
	if (tracker_map != NULL)
	{
		return true;
	}

	if (control == NULL)
	{
		void **rendezvous = find_rendezvous_variable(RENDEZVOUS_TENANT_TRACKER);

		control = (TenantTrackerControl *) *rendezvous;
		if (control == NULL)
		{
			return false; /* loader not present */
		}
	}

	{
		dshash_parameters params = tenant_map_params();
		/* Attach in TopMemoryContext so the cached area/map (and their bookkeeping)
		 * survive across transactions -- they are reused for the backend's life. */
		MemoryContext oldcxt = MemoryContextSwitchTo(TopMemoryContext);

		tracker_area = dsa_attach_in_place(control->dsa_place, NULL);
		dsa_pin_mapping(tracker_area); /* keep segment mappings across transactions */
		tracker_map = dshash_attach(tracker_area, &params, control->map_handle, NULL);

		MemoryContextSwitchTo(oldcxt);
	}
	return true;
}

/* Initialize a freshly dsa_allocate0'd tracker (atomics + inline partition locks). */
static void
init_tracker(TenantTracking *tracker, int32 hypertable_id)
{
	tracker->hypertable_id = hypertable_id;
	pg_atomic_init_u32(&tracker->active_gen, 0);

	for (int gen = 0; gen < 2; gen++)
	{
		TenantGeneration *generation = &tracker->generations[gen];

		pg_atomic_init_u32(&generation->num_writers, 0);
		pg_atomic_init_u32(&generation->nentries, 0);
		pg_atomic_init_u32(&generation->status, TENANT_TRACKER_VALID);
		/* Gen 0 is initially active -> epoch 1 (seqnum 0 means "untracked");
		 * gen 1's epoch is assigned by the first flush before it activates. */
		pg_atomic_init_u32(&generation->seqnum, gen == 0 ? 1 : 0);

		for (int i = 0; i < TENANT_TRACKER_NUM_PARTITIONS; i++)
		{
			LWLockInitialize(&generation->partitions[i], control->tracker_tranche_id);
		}

		for (int i = 0; i < TENANT_TRACKER_CAPACITY; i++)
		{
			TenantEntry *entry = &generation->entries[i];

			entry->key_len = 0; /* empty slot */
			pg_atomic_init_u64(&entry->min_ts, 0);
			pg_atomic_init_u64(&entry->max_ts, 0);
		}
	}
}

TenantTracking *
ts_tenant_tracker_lookup(int32 hypertable_id)
{
	TenantMapEntry *entry;
	dsa_pointer dp;

	if (!tenant_tracker_attach())
	{
		return NULL;
	}

	TenantMapKey key = { .database_id = MyDatabaseId, .hypertable_id = hypertable_id };

	/* Hold the dshash lock only to read the dsa_pointer; the tracker itself is
	 * never moved/freed, so it stays valid after we release. */
	entry = dshash_find(tracker_map, &key, false /* shared */);
	if (entry == NULL)
	{
		return NULL;
	}
	dp = entry->tracker;
	dshash_release_lock(tracker_map, entry);

	/* InvalidDsaPointer is the negative-cache marker set by get_or_attach when a
	 * tracker allocation failed; treat it as "not tracked" (dsa_get_address would
	 * also map it to NULL, but be explicit). */
	if (dp == InvalidDsaPointer)
	{
		return NULL;
	}

	return (TenantTracking *) dsa_get_address(tracker_area, dp);
}

TenantTracking *
ts_tenant_tracker_get_or_attach(int32 hypertable_id)
{
	TenantMapEntry *entry;
	bool found;
	dsa_pointer dp;

	if (!tenant_tracker_attach())
	{
		return NULL;
	}

	TenantMapKey key = { .database_id = MyDatabaseId, .hypertable_id = hypertable_id };

	entry = dshash_find_or_insert(tracker_map, &key, &found);

	/* Debug-only: simulate the OOM ERROR that dshash_find_or_insert can throw
	 * (its bucket-item dsa_allocate).  dshash_find_or_insert returns with its
	 * partition lock held, so throwing here reproduces the real lock state that
	 * the drain's PG_CATCH must release.  See caller for PG_TRY/CATCH block
	 * that releases locks.
	 */
	DEBUG_ERROR_INJECTION("tenant_tracker_map_dshash_insert_oom");

	if (found)
	{
		/* InvalidDsaPointer is the negative-cache marker we
		 * leave behind when a previous allocation failed (see below): skip
		 * tracking for this hypertable without re-attempting the allocation on
		 * every commit. */
		dp = entry->tracker;
		dshash_release_lock(tracker_map, entry);

		if (dp == InvalidDsaPointer)
		{
			return NULL;
		}

		return (TenantTracking *) dsa_get_address(tracker_area, dp);
	}

	/* New hypertable: allocate + initialize the tracker while holding the dshash
	 * exclusive lock, so a concurrent lookup blocks until it is fully built and
	 * published.
	 *
	 * Allocate with DSA_ALLOC_NO_OOM so a depleted DSA returns InvalidDsaPointer
	 * instead of throwing.  Throwing here would be doubly bad: this runs in the
	 * pre-commit drain of a user's INSERT, so it would fail an otherwise valid
	 * commit; and we would leave behind a half-built dshash entry (entry->tracker
	 * still unset) that a later lookup would dereference as a garbage
	 * dsa_pointer. */
	if (DEBUG_INJECTION_ENABLED("tenant_tracker_area_oom"))
	{
		dp = InvalidDsaPointer;
	}
	else
	{
		dp = dsa_allocate_extended(tracker_area,
								   sizeof(TenantTracking),
								   DSA_ALLOC_ZERO | DSA_ALLOC_NO_OOM);
	}

	if (dp == InvalidDsaPointer)
	{
		/* Out of shared memory. Record the failure as a negative-cache marker
		 * (tracker = InvalidDsaPointer) rather
		 * than deleting it: a deleted entry would be re-inserted and re-attempt
		 * this large allocation on every subsequent commit (a retry/log storm
		 * that also keeps re-exercising the throwing dshash insert path).  With
		 * the marker, later inserts find the entry, see the marker, and skip
		 * tracking.  The marker persists until restart.
		 * matches the current design where trackers are never freed; tracking for
		 * this hypertable stays off and the refresh falls back to the full log.
		 */

		entry->tracker = InvalidDsaPointer;
		dshash_release_lock(tracker_map, entry);
		ereport(LOG,
				(errmsg("per-tenant invalidation tracker out of shared memory "
						"for hypertable %d",
						hypertable_id),
				 errdetail("Disabling per-tenant tracking for this hypertable until "
						   "restart; continuous aggregate refreshes cannot use granular refresh")));
		ereport(NOTICE,
				(errmsg("per-tenant invalidation tracker out of shared memory "
						"no granular tracking for hypertable %d",
						hypertable_id)));
		return NULL;
	}

	init_tracker((TenantTracking *) dsa_get_address(tracker_area, dp), hypertable_id);
	entry->tracker = dp;
	dshash_release_lock(tracker_map, entry);

	return (TenantTracking *) dsa_get_address(tracker_area, dp);
}
