/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "ts_stats_defs.h"

/*
 * Returns the chunk statistics segment for the current database, creating it
 * lazily on first call.  Returns NULL if statistics collection is disabled
 * (max_chunks == 0), if DSM allocation fails, or if a published segment is
 * found with an unexpected magic, i.e. not initialized.
 *
 * The returned pointer is cached per-backend.
 */
TsStatsChunkSegment *ts_get_stats_chunk_segment(void);

/*
 * Logical reset — clears all cached chunks WITHOUT touching the segment's
 * structural fields (magic, dboid, lock, base_timestamp, num_slots,
 * num_buckets).  Caller need not hold the lock; the function takes
 * seg->lock EXCLUSIVE itself.
 *
 * After this call, all slots are on the free list and all buckets are
 * empty; readers see an empty cache.  See doc 016 §8.2.
 */
void ts_stats_chunk_segment_reset(TsStatsChunkSegment *seg);

/*
 * Both functions assume seg->lock is already held EXCLUSIVE by the caller.
 * They operate purely on the segment's internal data structure (hash chains,
 * LRU list, free list).
 */

/*
 * Find the slot for compressed_relid, or allocate a new one (evicting LRU tail if the
 * cache is full).  The returned slot is moved to the LRU head.
 *
 * On failure (compressed_relid is InvalidOid): returns INVALID_IDX.
 */
slot_idx_t ts_stats_chunk_segment_find_or_insert(TsStatsChunkSegment *seg, Oid compressed_relid,
												 Oid uncompressed_relid);

/* Lookup — find an existing slot WITHOUT promoting it in the LRU list.
 * Pure read; safe under SHARED lock.  Returns INVALID_IDX on miss.  Does not allocate. */
slot_idx_t ts_stats_chunk_segment_lookup(const TsStatsChunkSegment *seg, Oid compressed_relid);
