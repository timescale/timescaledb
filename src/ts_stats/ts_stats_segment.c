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
 * One-time init.
 */
static void
#if PG19_GE
/* PG19 added an extra argument to the GetNamedDSMSegment init callback. */
ts_stats_chunk_init_segment(void *ptr, void *arg)
#else
ts_stats_chunk_init_segment(void *ptr)
#endif
{
	TsStatsChunkSegment *seg = (TsStatsChunkSegment *) ptr;
	uint32 num_slots = (uint32) ts_guc_stats_max_chunks;

	/* must be powers of two */
	Assert((num_slots & (num_slots - 1)) == 0);

	/* zeros are valid for all variables, in particular this clears
	 * the validity flag part of the TsStatsChunkMetadata.state field */
	memset(seg, 0, ts_stats_chunk_segment_size(num_slots));

	seg->magic = TS_STATS_SHMEM_MAGIC;
	seg->dboid = MyDatabaseId;
	LWLockInitialize(&seg->lock, LWLockNewTrancheId());
	LWLockRegisterTranche(seg->lock.tranche, "ts_stats_chunk");
	seg->base_timestamp = GetCurrentTimestamp();
	seg->num_slots = num_slots;

	/* sequence number starts at 1 */
	pg_atomic_write_u64(&seg->update_seqno, 1);
}

void
ts_stats_chunk_segment_reset(TsStatsChunkSegment *seg)
{
	/* blindly overwrite all dynamic state, no locking and no checks */
	void *meta_base = ts_stats_chunk_metadata(seg);
	Size data_size = ts_stats_chunk_data_size(seg->num_slots);
	memset(meta_base, 0, data_size);
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

	Size seg_size = ts_stats_chunk_segment_size((uint32) ts_guc_stats_max_chunks);

	char name[64];
	snprintf(name, sizeof(name), "ts_stats_chunk_db_%u", MyDatabaseId);

	bool found;
#if PG19_GE
	cached_segment = GetNamedDSMSegment(name, seg_size, ts_stats_chunk_init_segment, &found, NULL);
#else
	cached_segment = GetNamedDSMSegment(name, seg_size, ts_stats_chunk_init_segment, &found);
#endif

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

/* The below section is about handling older PostgreSQL version, before PG 17.
 * The memory segments for each database are registered in a global handle table.
 * When the handle table is full, we sweep through the entries and remove the
 * segments for the databases that have been dropped. If there are more than
 * TS_STATS_MAX_DATABASES databases, or if the segment creation fails for any
 * reason, we disable stats tracking for the current database.
 */

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
	Size seg_size = ts_stats_chunk_segment_size((uint32) ts_guc_stats_max_chunks);

	dsm_segment *local_seg = dsm_create(seg_size, 0);
	if (local_seg == NULL)
	{
		return NULL;
	}

	dsm_pin_segment(local_seg);
	dsm_pin_mapping(local_seg);
	dsm_handle local_handle = dsm_segment_handle(local_seg);

	TsStatsChunkSegment *obs = dsm_segment_address(local_seg);
#if PG19_GE
	ts_stats_chunk_init_segment(obs, NULL);
#else
	ts_stats_chunk_init_segment(obs);
#endif

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

/*
 * Knuth's multiplicative hash. The constant 0x9E3779B9 = floor(2^32 / phi).
 * Multiplying scrambles input structure across the 32-bit space. We don't
 * need perfect hashing here, just a good enough distribution that puts
 * consecutive relids at least window width apart to avoid clustering in
 * the linear probing.
 */
static inline uint32
hash_relid(const TsStatsChunkSegment *seg, Oid compressed_relid)
{
	uint32 bucket_size = seg->num_slots / TS_STATS_BUCKETS;
	uint32 product = (uint32) compressed_relid * 0x9E3779B9U;
	return (uint32) (((uint64_t) product * (uint64_t) bucket_size) >> 32);
}

/* Return the sorted bucket order as an array of uint8 indices */
static inline void
get_sorted_bucket_in_desc_order(TsStatsChunkSegment *seg, uint8 *out)
{
	Assert(out != NULL && seg != NULL);
	uint64 seqnos[TS_STATS_BUCKETS];
	TsStatsChunkBucketMetadata *bucket_base = ts_stats_chunk_bucket_metadata(seg);
	for (int i = 0; i < TS_STATS_BUCKETS; i++)
	{
		out[i] = (uint8) i;
		seqnos[i] = pg_atomic_read_u64(&bucket_base[i].last_update_seqno);
	}

	/* sort the bucket index by the last update sequence number in descending order */
	for (int j = 0; j < TS_STATS_BUCKETS - 1; j++)
	{
		for (int k = 0; k < TS_STATS_BUCKETS - j - 1; k++)
		{
			if (seqnos[out[k]] < seqnos[out[k + 1]])
			{
				uint8 temp = out[k];
				out[k] = out[k + 1];
				out[k + 1] = temp;
			}
		}
	}
}

/* This function evicts/updates a chunk at the specified index and also sets the
 * in-progress flag, plus updated the bucket's last update sequence number.
 * This is a utility function to simplify 'ts_stats_chunk_segment_prepare_upsert'
 */
static inline int32
ts_stats_chunk_update_at(TsStatsChunkSegment *seg, TsStatsRelids relids, int32 idx,
						 uint64 old_state)
{
	Assert(seg != NULL || OidIsValid(relids.compressed_relid) ||
		   OidIsValid(relids.uncompressed_relid));
	if (seg == NULL || !OidIsValid(relids.compressed_relid) ||
		!OidIsValid(relids.uncompressed_relid))
	{
		return TS_STATS_INVALID_IDX;
	}

	uint32 bucket_size = seg->num_slots / TS_STATS_BUCKETS;
	uint32 bucket_idx = idx / bucket_size;
	uint64 new_seqno = pg_atomic_add_fetch_u64(&seg->update_seqno, 1);
	uint64 new_state = (new_seqno << TS_STATS_CHUNK_METADATA_SEQNO_SHIFT) |
					   TS_STATS_CHUNK_METADATA_VALID_FLAG |
					   TS_STATS_CHUNK_METADATA_IN_PROGRESS_FLAG;

	TsStatsChunkMetadata *meta_base = ts_stats_chunk_metadata(seg);
	TsStatsChunkBucketMetadata *bucket_base = ts_stats_chunk_bucket_metadata(seg);

	TsStatsChunkMetadata *meta = &meta_base[idx];

	/* only evict when the state is the same as the old state, which means
	 * no other process has updated this slot since we read it last time */
	if (!pg_atomic_compare_exchange_u64(&meta->state, &old_state, new_state))
	{
		return TS_STATS_INVALID_IDX;
	}

	/* update the metadata */
	meta->relids = relids;
	pg_atomic_write_u64(&bucket_base[bucket_idx].last_update_seqno, new_seqno);

	return (int32) idx;
}

/* Public API */
int32
ts_stats_chunk_segment_prepare_upsert(TsStatsChunkSegment *seg, TsStatsRelids relids)
{
	Assert(seg != NULL);
	if (seg == NULL || !OidIsValid(relids.compressed_relid) ||
		!OidIsValid(relids.uncompressed_relid))
	{
		return TS_STATS_INVALID_IDX;
	}

	/* get the base pointers */
	TsStatsChunkMetadata *meta_base = ts_stats_chunk_metadata(seg);
	TsStatsChunk *slot_base = ts_stats_chunk_slots(seg);

	/* sort the buckets by the last update sequence number in descending order */
	uint8 bucket_order[TS_STATS_BUCKETS];
	get_sorted_bucket_in_desc_order(seg, bucket_order);

	/* get the home location within the buckets */
	uint32 home = hash_relid(seg, relids.compressed_relid);
	uint32 bucket_size = seg->num_slots / TS_STATS_BUCKETS;

	/* keep the empty positions as we scan forward. these will
	 * be tried if the chunk was not found. */
	uint32 empty_positions[TS_STATS_PROBE_WIDTH * TS_STATS_BUCKETS];
	uint32 empty_count = 0;

	/* iterate over the buckets in most-recently-updated order */
	for (uint8 i = 0; i < TS_STATS_BUCKETS; i++)
	{
		uint8 bucket_idx = bucket_order[i];
		uint32 start = bucket_idx * bucket_size;

		/* linear probe within the bucket */
		for (uint8 j = 0; j < TS_STATS_PROBE_WIDTH; j++)
		{
			uint32 idx = start + ((home + j) % bucket_size);
			TsStatsChunkMetadata *meta = &meta_base[idx];

			uint64 state = pg_atomic_read_u64(&meta->state);
			if (!TS_STATS_CHUNK_METADATA_IS_VALID(state))
			{
				/* record the empty position for later use, but we still need
				 * to continue scanning the remaining slots */
				empty_positions[empty_count++] = idx;
				continue;
			}
			if (meta->relids.compressed_relid == relids.compressed_relid)
			{
				/* update the chunk metadata state to in-progress and the bucket's
				 * last update sequence number.
				 */
				if (ts_stats_chunk_update_at(seg, relids, idx, state) != TS_STATS_INVALID_IDX)
				{
					return (int32) idx;
				}

				/* if the update failed, then another process has updated this slot.
				 * if the slot still belongs to the same compressed_relid, we can try
				 * to update it again, so the update sequence numbers get updated.
				 */
				state = pg_atomic_read_u64(&meta->state);
				if (TS_STATS_CHUNK_METADATA_IS_VALID(state) &&
					meta->relids.compressed_relid == relids.compressed_relid)
				{
					if (ts_stats_chunk_update_at(seg, relids, idx, state) != TS_STATS_INVALID_IDX)
					{
						return (int32) idx;
					}
				}

				/* if this failed at the second time, we better leave it alone and proceed
				 * with the next slot.
				 */
			}
		}
	}

	/* at this point we haven't found the chunk so we first check the empty slots.
	 * they may be taken conncurrently by another process, so they need to be checked
	 * again. the order of the empty slots are somwewhat ordered as we gathered them
	 * in the bucket recency order. best to traverse them in this recency order so
	 * the buckets keep the recency property as much as possible.
	 */
	for (uint32 i = 0; i < empty_count; i++)
	{
		uint32 idx = empty_positions[i];
		TsStatsChunkMetadata *meta = &meta_base[idx];

		uint64 state = pg_atomic_read_u64(&meta->state);
		if (TS_STATS_CHUNK_METADATA_IS_VALID(state))
		{
			/* this slot is no longer empty, skip it */
			continue;
		}

		/* try to claim this slot, and it will update its state to in-progress
		 * and the bucket's last update sequence number.
		 */
		if (ts_stats_chunk_update_at(seg, relids, idx, state) != TS_STATS_INVALID_IDX)
		{
			TsStatsChunk *slot = &slot_base[idx];
			ts_stats_chunk_init_slot(slot);
			return (int32) idx;
		}
	}

	/* at this point we have not found the slot and neither empty ones to use
	 * we will need to evict (overwrite) one. the eviction only checks the
	 * oldest bucket's window and uses the slot that is the oldest and not
	 * in-progress. if every slot is in progress we try the next bucket in
	 * recency order. if none has an evictable slot, we give up and return
	 * invalid index.
	 *
	 * the worst case is that we checked the windows of all buckets and we
	 * tried to evict from all slots, so that is
	 *
	 *   2 x TS_STATS_BUCKETS x TS_STATS_PROBE_WIDTH checks, which is
	 *
	 * still acceptable, but very unlikely to happen.
	 */

	uint8 oldest_bucket_idx = bucket_order[TS_STATS_BUCKETS - 1];
	uint32 start = oldest_bucket_idx * bucket_size;
	int32 candidate_idx = TS_STATS_INVALID_IDX;
	uint64 candidate_seqno = UINT64_MAX;

	for (uint8 j = 0; j < TS_STATS_PROBE_WIDTH; j++)
	{
		uint32 idx = start + ((home + j) % bucket_size);
		TsStatsChunkMetadata *meta = &meta_base[idx];

		uint64 state = pg_atomic_read_u64(&meta->state);
		if (!TS_STATS_CHUNK_METADATA_IS_VALID(state))
		{
			/* this slot is empty, although at this point it should not happen
			 * as the previous loops should have handled empty slots. */
			if (ts_stats_chunk_update_at(seg, relids, idx, state) != TS_STATS_INVALID_IDX)
			{
				TsStatsChunk *slot = &slot_base[idx];
				ts_stats_chunk_init_slot(slot);
				return (int32) idx;
			}
		}
		if (!TS_STATS_CHUNK_METADATA_IS_IN_PROGRESS(state))
		{
			/* this slot is not in progress, mark as a candidate */
			uint64 seqno = TS_STATS_CHUNK_METADATA_GET_SEQNO(state);
			if (seqno < candidate_seqno)
			{
				candidate_seqno = seqno;
				candidate_idx = (int32) idx;
			}
		}
	}

	/* if we found a candidate, try to evict it */
	if (candidate_idx != TS_STATS_INVALID_IDX)
	{
		TsStatsChunkMetadata *meta = &meta_base[candidate_idx];
		uint64 state = pg_atomic_read_u64(&meta->state);

		/* try to evict this slot, and it will update its state to in-progress
		 * and the bucket's last update sequence number.
		 */
		int32 updated_idx = ts_stats_chunk_update_at(seg, relids, candidate_idx, state);

		if (updated_idx != TS_STATS_INVALID_IDX)
		{
			TsStatsChunk *slot = &slot_base[updated_idx];
			ts_stats_chunk_init_slot(slot);
			return updated_idx;
		}
	}

	return TS_STATS_INVALID_IDX;
}

void
ts_stats_chunk_segment_finish_upsert(TsStatsChunkSegment *seg, int32 slot_idx, Oid compressed_relid)
{
	Assert(seg != NULL);
	if (seg == NULL || slot_idx == TS_STATS_INVALID_IDX || !OidIsValid(compressed_relid))
	{
		return;
	}

	/* get the metadata pointer */
	TsStatsChunkMetadata *meta = ts_stats_chunk_metadata(seg) + slot_idx;

	/* don't do anything if the compressed_relid doesn't match */
	if (meta->relids.compressed_relid != compressed_relid)
	{
		return;
	}

	/* get the current flag */
	uint64 state = pg_atomic_read_u64(&meta->state);

	/* only update if the slot is in the in-progress state and still valid */
	if (TS_STATS_CHUNK_METADATA_IS_IN_PROGRESS(state) && TS_STATS_CHUNK_METADATA_IS_VALID(state))
	{
		/* clear the in-progress flag */
		uint64 new_state = (state & ~TS_STATS_CHUNK_METADATA_IN_PROGRESS_FLAG);

		/* we could do compare-exchange here, but we have no means to handle failure
		 * and we don't want to wait or retry, so better to simply overwrite it */
		pg_atomic_write_u64(&meta->state, new_state);
	}
}

int32
ts_stats_chunk_segment_lookup(TsStatsChunkSegment *seg, Oid compressed_relid)
{
	Assert(seg != NULL);
	if (seg == NULL || !OidIsValid(compressed_relid))
	{
		return TS_STATS_INVALID_IDX;
	}

	/* get the base pointers */
	TsStatsChunkMetadata *meta_base = ts_stats_chunk_metadata(seg);

	/* get the home location within the buckets */
	uint32 home = hash_relid(seg, compressed_relid);
	uint32 bucket_size = seg->num_slots / TS_STATS_BUCKETS;

	/* sort the buckets by the last update sequence number in descending order */
	uint8 bucket_order[TS_STATS_BUCKETS];
	get_sorted_bucket_in_desc_order(seg, bucket_order);

	/* iterate over the buckets in most-recently-updated order */
	for (uint8 i = 0; i < TS_STATS_BUCKETS; i++)
	{
		uint8 bucket_idx = bucket_order[i];
		uint32 start = bucket_idx * bucket_size;

		/* linear probe within the bucket */
		for (uint32 j = 0; j < TS_STATS_PROBE_WIDTH; j++)
		{
			uint32 idx = start + ((home + j) % bucket_size);
			TsStatsChunkMetadata *meta = &meta_base[idx];

			uint64 state = pg_atomic_read_u64(&meta->state);
			if (!TS_STATS_CHUNK_METADATA_IS_VALID(state))
			{
				/* even if the current slot was evicted, we still need to check
				 * the all remaining slots in the bucket and all buckets until we
				 * exhausted the all, because the eviction may have removed old
				 * entried in the middle of the search window
				 */
				continue;
			}
			if (meta->relids.compressed_relid == compressed_relid)
			{
				return (int32) idx;
			}
		}
	}
	return TS_STATS_INVALID_IDX;
}
