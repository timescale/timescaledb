/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "ts_stats_segment.h"
#include "guc.h" /* ts_guc_stats_max_chunks */

#include <postgres.h>
#include "utils/syscache.h"
#include <miscadmin.h>
#include <storage/lwlock.h>
#include <utils/timestamp.h>

#if PG17_GE
#include <storage/dsm_registry.h>
#else
#include "../loader/ts_stats_handles.h"
#include <storage/dsm.h>
#include <storage/shmem.h>
#endif

/*
 * Please also check ts_stats_defs.h for related definitions and comments.
 *
 * The NUM_BUCKETS represents the number of hash buckets in the segment's
 * hash table. NUM_SLOTS represents the number of slots available for storing
 * chunk information. These two don't need to be the same but in our current
 * implementation they are, and both are powers of two.
 *
 * The hash buckets are used to quickly find the slot index for a given chunk
 * compressed_relid, while the slots store the actual chunk information and are linked
 * together in an LRU list.
 */

/*
 * Caller must hold seg->lock EXCLUSIVE if the segment is published.  When
 * called from init, the segment isn't published yet so no lock is needed.
 */
static void
ts_stats_chunk_reset_dynamic_state(TsStatsChunkSegment *seg)
{
	slot_idx_t *buckets = ts_stats_chunk_buckets(seg);
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);

	seg->lru_head = seg->lru_tail = INVALID_IDX;

	for (uint32 b = 0; b < seg->num_buckets; b++)
	{
		buckets[b] = INVALID_IDX;
	}

	/* Mark all slots free, thread them onto the free list via lru_next. */
	for (uint32 s = 0; s < seg->num_slots; s++)
	{
		slots[s].compressed_relid = InvalidOid;
		slots[s].lru_next = (slot_idx_t) (s + 1);
	}
	slots[seg->num_slots - 1].lru_next = INVALID_IDX;
	seg->free_head = 0;
}

/*
 * One-time init.
 */
static void
ts_stats_chunk_init_segment(void *ptr)
{
	TsStatsChunkSegment *seg = (TsStatsChunkSegment *) ptr;
	uint32 num_slots = (uint32) ts_guc_stats_max_chunks;
	uint32 num_buckets = num_slots;

	/* both must be powers of two */
	Assert((num_slots & (num_slots - 1)) == 0);
	Assert((num_buckets & (num_buckets - 1)) == 0);

	memset(seg, 0, ts_stats_chunk_segment_size(num_slots, num_buckets));

	seg->magic = TS_STATS_SHMEM_MAGIC;
	seg->dboid = MyDatabaseId;
	LWLockInitialize(&seg->lock, LWLockNewTrancheId());
	LWLockRegisterTranche(seg->lock.tranche, "ts_stats_chunk");
	seg->base_timestamp = GetCurrentTimestamp();
	seg->num_slots = num_slots;
	seg->num_buckets = num_buckets;

	ts_stats_chunk_reset_dynamic_state(seg);
}

void
ts_stats_chunk_segment_reset(TsStatsChunkSegment *seg)
{
	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	ts_stats_chunk_reset_dynamic_state(seg);
	LWLockRelease(&seg->lock);
}

/*
 * Backend-local cache.  MyDatabaseId is fixed for a backend's lifetime, so
 * cached_segment is safe to reuse across calls.
 */
static TsStatsChunkSegment *cached_segment = NULL;

/* GetNamedDSMSegment was introduced in PG 17, so we need separate
 * logic for older versions. */
#if PG17_GE

TsStatsChunkSegment *
ts_get_stats_chunk_segment(void)
{
	if (!IS_STATS_CHUNKS_ENABLED())
	{
		return NULL;
	}

	if (likely(cached_segment != NULL))
	{
		return cached_segment;
	}

	Size seg_size =
		ts_stats_chunk_segment_size((uint32) ts_guc_stats_max_chunks,
									(uint32) ts_guc_stats_max_chunks); /* num_buckets = num_slots */

	char name[64];
	snprintf(name, sizeof(name), "ts_stats_chunk_db_%u", MyDatabaseId);

	bool found;
	cached_segment = GetNamedDSMSegment(name, seg_size, ts_stats_chunk_init_segment, &found);

	if (cached_segment != NULL && cached_segment->magic != TS_STATS_SHMEM_MAGIC)
	{
		elog(WARNING,
			 "ts_stats_chunk: segment for database %u has unexpected magic, disabling",
			 MyDatabaseId);
		cached_segment = NULL;
	}
	return cached_segment;
}

#else /* PG17_LT */

static TsObservHandleEntry *
ts_stats_chunk_find_free_entry(TsObservHandleTable *tbl)
{
	for (uint32 i = 0; i < tbl->max_entries; i++)
	{
		if (!OidIsValid(tbl->entries[i].dboid))
		{
			return &tbl->entries[i];
		}
	}
	return NULL;
}

static TsObservHandleEntry *
ts_stats_chunk_find_entry(TsObservHandleTable *tbl, Oid dboid, int segment_type)
{
	for (uint32 i = 0; i < tbl->max_entries; i++)
	{
		if (tbl->entries[i].dboid == dboid && tbl->entries[i].segment_type == segment_type)
		{
			return &tbl->entries[i];
		}
	}
	return NULL;
}

static void
ts_stats_chunk_sweep_stale(TsObservHandleTable *tbl)
{
	Assert(IsTransactionState());

	typedef struct
	{
		uint32 idx;
		Oid dboid;
	} SnapEntry;
	SnapEntry snap[TS_STATS_MAX_DATABASES];
	SnapEntry gone[TS_STATS_MAX_DATABASES];
	dsm_handle unpin[TS_STATS_MAX_DATABASES];
	uint32 snap_n = 0, gone_n = 0, unpin_n = 0;

	/* Sweep through the handle table and take a snapshot of valid entries */
	LWLockAcquire(&tbl->lock, LW_EXCLUSIVE);
	for (uint32 i = 0; i < TS_STATS_MAX_DATABASES; i++)
	{
		if (OidIsValid(tbl->entries[i].dboid))
		{
			snap[snap_n++] = (SnapEntry){ i, tbl->entries[i].dboid };
		}
	}
	LWLockRelease(&tbl->lock);

	/* Check which entries are no longer valid */
	for (uint32 j = 0; j < snap_n; j++)
	{
		HeapTuple tup = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(snap[j].dboid));
		if (!HeapTupleIsValid(tup))
		{
			gone[gone_n++] = snap[j];
		}
		else
		{
			ReleaseSysCache(tup);
		}
	}

	/* Remove stale entries from the handle table */
	LWLockAcquire(&tbl->lock, LW_EXCLUSIVE);
	for (uint32 k = 0; k < gone_n; k++)
	{
		TsObservHandleEntry *e = &tbl->entries[gone[k].idx];
		if (e->dboid == gone[k].dboid)
		{
			unpin[unpin_n++] = e->handle;
			e->dboid = InvalidOid;
			e->handle = 0;
		}
	}
	LWLockRelease(&tbl->lock);

	/* Unpin the stale segments */
	for (uint32 m = 0; m < unpin_n; m++)
	{
		dsm_unpin_segment(unpin[m]);
	}
}

static TsStatsChunkSegment *
ts_stats_chunk_create_segment(TsObservHandleTable *tbl)
{
	Size seg_size = ts_stats_chunk_segment_size((uint32) ts_guc_stats_max_chunks,
												(uint32) ts_guc_stats_max_chunks);

	dsm_segment *local_seg = dsm_create(seg_size, 0);
	if (local_seg == NULL)
	{
		return NULL;
	}

	dsm_pin_segment(local_seg);
	dsm_pin_mapping(local_seg);
	dsm_handle local_handle = dsm_segment_handle(local_seg);

	TsStatsChunkSegment *obs = dsm_segment_address(local_seg);
	ts_stats_chunk_init_segment(obs);

	LWLockAcquire(&tbl->lock, LW_EXCLUSIVE);

	TsObservHandleEntry *entry =
		ts_stats_chunk_find_entry(tbl, MyDatabaseId, TS_STATS_TYPE_CHUNK_SEGMENT);
	if (entry != NULL)
	{
		dsm_handle winner_handle = entry->handle;
		LWLockRelease(&tbl->lock);

		dsm_unpin_segment(local_handle);
		dsm_detach(local_seg);

		dsm_segment *winner_seg = dsm_attach(winner_handle);
		if (winner_seg == NULL)
		{
			return NULL;
		}
		dsm_pin_mapping(winner_seg);
		return dsm_segment_address(winner_seg);
	}

	/* Find a free slot, with one sweep retry. */
	entry = ts_stats_chunk_find_free_entry(tbl);
	if (entry == NULL)
	{
		LWLockRelease(&tbl->lock);
		ts_stats_chunk_sweep_stale(tbl);
		LWLockAcquire(&tbl->lock, LW_EXCLUSIVE);
		entry = ts_stats_chunk_find_free_entry(tbl);
	}

	/* Table full. */
	if (entry == NULL)
	{
		LWLockRelease(&tbl->lock);
		dsm_unpin_segment(local_handle);
		dsm_detach(local_seg);
		elog(LOG,
			 "ts_stats_chunk: handle table full after sweep, chunk statistics disabled "
			 "for database %u",
			 MyDatabaseId);
		return NULL;
	}

	/* Publish. */
	entry->dboid = MyDatabaseId;
	entry->handle = local_handle;
	entry->segment_type = TS_STATS_TYPE_CHUNK_SEGMENT;
	LWLockRelease(&tbl->lock);
	return obs;
}

TsStatsChunkSegment *
ts_get_stats_chunk_segment(void)
{
	static bool rendezvous_checked = false;

	if (!IS_STATS_CHUNKS_ENABLED())
	{
		return NULL;
	}

	if (likely(cached_segment != NULL))
	{
		return cached_segment;
	}

	if (rendezvous_checked)
	{
		return NULL;
	}
	rendezvous_checked = true;

	TsObservHandleTable **rv =
		(TsObservHandleTable **) find_rendezvous_variable("ts_stats_handles");
	TsObservHandleTable *tbl = *rv;
	if (tbl == NULL)
	{
		return NULL;
	}

	LWLockAcquire(&tbl->lock, LW_SHARED);
	TsObservHandleEntry *entry =
		ts_stats_chunk_find_entry(tbl, MyDatabaseId, TS_STATS_TYPE_CHUNK_SEGMENT);

	if (entry != NULL)
	{
		dsm_segment *seg = dsm_find_mapping(entry->handle);
		if (seg == NULL)
		{
			seg = dsm_attach(entry->handle);
			if (seg != NULL)
			{
				dsm_pin_mapping(seg);
			}
		}
		LWLockRelease(&tbl->lock);

		if (seg != NULL)
		{
			cached_segment = dsm_segment_address(seg);
			if (cached_segment->magic != TS_STATS_SHMEM_MAGIC)
			{
				elog(WARNING,
					 "ts_stats_chunk: segment for database %u has unexpected magic, disabling",
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

	cached_segment = ts_stats_chunk_create_segment(tbl);
	return cached_segment;
}

#endif /* PG17_GE */

/* LRU list helpers.
 *
 * Reminder: the list chaining fields are lru_prev and lru_next, and they are indices into
 * the slots array, not pointers.  INVALID_IDX means end of list / null.
 *
 * See ts_stats_defs.h for more explanation and the definition of INVALID_IDX and the slot struct
 * fields.
 */

static inline void
lru_unlink(TsStatsChunkSegment *seg, slot_idx_t s)
{
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);

	if (s == seg->lru_head)
	{
		seg->lru_head = slots[s].lru_next;
	}
	else
	{
		slots[slots[s].lru_prev].lru_next = slots[s].lru_next;
	}

	if (s == seg->lru_tail)
	{
		seg->lru_tail = slots[s].lru_prev;
	}
	else
	{
		slots[slots[s].lru_next].lru_prev = slots[s].lru_prev;
	}
}

static inline void
lru_push_front(TsStatsChunkSegment *seg, slot_idx_t s)
{
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);

	slots[s].lru_prev = INVALID_IDX;
	slots[s].lru_next = seg->lru_head;

	if (seg->lru_head != INVALID_IDX)
	{
		slots[seg->lru_head].lru_prev = s;
	}
	else
	{
		seg->lru_tail = s;
	}

	seg->lru_head = s;
}

/* Hash chain helpers.
 *
 * Reminder: the hash chain field is hash_next, and it is an index into the slots array, not a
 * pointer. INVALID_IDX means end of chain / null. The starting point of the hash chain is the
 * bucket entry, which is also an index into the slots array.
 *
 * See ts_stats_defs.h for more explanation and the definition of INVALID_IDX and the slot struct
 * fields.
 */

/*
 * Knuth's multiplicative hash. The constant 0x9E3779B9 = floor(2^32 / phi).
 * Multiplying scrambles input structure across the 32-bit space; the AND mask
 * picks the high bits.
 *
 * This method is chosen for speed, such that we reduce lock contention.
 * The hash quality is not critical since collisions are handled via chaining.
 *
 * Combined cost: one IMUL + one AND.  No modulo, no division.
 */
static inline uint32
bucket_of(const TsStatsChunkSegment *seg, Oid compressed_relid)
{
	return ((uint32) compressed_relid * 0x9E3779B9U) & (seg->num_buckets - 1);
}

/*
 * The hash lookup first finds the bucket for the given compressed_relid, then traverses
 * the chain of slots in that bucket.  It returns the slot index if found, or
 * INVALID_IDX if not found.
 */
static inline slot_idx_t
hash_lookup(const TsStatsChunkSegment *seg, Oid compressed_relid, uint32 *bucket_out)
{
	const slot_idx_t *buckets = ts_stats_chunk_buckets_const(seg);
	const TsStatsChunk *slots = ts_stats_chunk_slots_const(seg);

	/* get the hash chain first */
	uint32 b = bucket_of(seg, compressed_relid);
	if (bucket_out)
	{
		/* return the bucket index to the caller */
		*bucket_out = b;
	}

	/* first entry in the chain */
	slot_idx_t s = buckets[b];

	while (s != INVALID_IDX)
	{
		if (slots[s].compressed_relid == compressed_relid)
		{
			return s;
		}
		/* move to the next slot in the chain */
		s = slots[s].hash_next;
	}
	return INVALID_IDX;
}

/* Insert s at the head of bucket b's chain. */
static inline void
hash_push_front(TsStatsChunkSegment *seg, slot_idx_t s, uint32 b)
{
	slot_idx_t *buckets = ts_stats_chunk_buckets(seg);
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);

	slots[s].hash_next = buckets[b];
	buckets[b] = s;
}

/* Unlink s from its bucket's chain. s must currently be in the chain. */
static inline void
hash_unlink(TsStatsChunkSegment *seg, slot_idx_t s)
{
	slot_idx_t *buckets = ts_stats_chunk_buckets(seg);
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);

	uint32 b = bucket_of(seg, slots[s].compressed_relid);
	/* the first entry in the chain */
	slot_idx_t cur = buckets[b];

	if (cur == s)
	{
		/* s is the first entry in the chain */
		buckets[b] = slots[s].hash_next;
		return;
	}

	while (cur != INVALID_IDX && slots[cur].hash_next != s)
	{
		cur = slots[cur].hash_next;
	}

	Assert(cur != INVALID_IDX);
	if (cur != INVALID_IDX)
	{
		/* do the unlink */
		slots[cur].hash_next = slots[s].hash_next;
	}
}

/*
 * Eviction needs to take care of both the LRU list and the hash chain.
 * It also needs to mark the slot free, which we do by setting compressed_relid to
 * InvalidOid and pushing it onto the free list.
 */
static void
evict_lru(TsStatsChunkSegment *seg)
{
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);
	slot_idx_t s = seg->lru_tail;

	Assert(s != INVALID_IDX);

	hash_unlink(seg, s);
	lru_unlink(seg, s);

	slots[s].compressed_relid = InvalidOid; /* marks slot free */
	slots[s].lru_next = seg->free_head;
	seg->free_head = s;
}

/* Slot initialization */
static void
init_fresh_slot(TsStatsChunk *slot, Oid compressed_relid, Oid uncompressed_relid)
{
	memset(slot, 0, sizeof(TsStatsChunk));
	slot->compressed_relid = compressed_relid;
	slot->uncompressed_relid = uncompressed_relid;
	slot->lru_next = INVALID_IDX;
	slot->lru_prev = INVALID_IDX;
	slot->hash_next = INVALID_IDX;
	init_min_max_sum(&slot->compressed_batch_rows);
	init_min_max_sum(&slot->compressed_batch_bytes);
	init_min_max_sum(&slot->compressed_block_bytes);
}

/* Public API */
slot_idx_t
ts_stats_chunk_segment_find_or_insert(TsStatsChunkSegment *seg, Oid compressed_relid,
									  Oid uncompressed_relid)
{
	if (!OidIsValid(compressed_relid) || !OidIsValid(uncompressed_relid))
	{
		return INVALID_IDX;
	}

	/* First lookup the slot via the hash table */
	uint32 b;
	slot_idx_t s = hash_lookup(seg, compressed_relid, &b);
	if (s != INVALID_IDX)
	{
		if (s != seg->lru_head)
		{
			lru_unlink(seg, s);
			lru_push_front(seg, s);
		}
		return s;
	}

	/* If the free list is empty, evict the LRU tail */
	if (seg->free_head == INVALID_IDX)
	{
		/* The end of the LRU list must be a valid, occupied slot */
		Assert(seg->lru_tail != INVALID_IDX);
		if (seg->lru_tail == INVALID_IDX)
		{
			return INVALID_IDX;
		}

		/* Kick out the LRU tail to make room */
		evict_lru(seg);
		b = bucket_of(seg, compressed_relid);
	}

	/* Pop from free list. */
	Assert(seg->free_head != INVALID_IDX);
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);
	s = seg->free_head;
	seg->free_head = slots[s].lru_next;

	/* Initialize fields, link into bucket and LRU. */
	init_fresh_slot(&slots[s], compressed_relid, uncompressed_relid);
	hash_push_front(seg, s, b);
	lru_push_front(seg, s);

	return s;
}

slot_idx_t
ts_stats_chunk_segment_lookup(const TsStatsChunkSegment *seg, Oid compressed_relid)
{
	if (!OidIsValid(compressed_relid))
	{
		return INVALID_IDX;
	}
	return hash_lookup(seg, compressed_relid, NULL);
}
