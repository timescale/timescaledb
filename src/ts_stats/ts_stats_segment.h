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
 * structural fields.
 */
void ts_stats_chunk_segment_reset(TsStatsChunkSegment *seg);

/* Try to find the location of the next available slot for upsert.
 *  - if the chunk already exists, return its slot index, and sets the
 *    in-progress flag for the slot
 *  - if the chunk doesn't exists, find try to find an empty slot for it and
 *    sets the in-progress flag for the slot
 *  - if there are no empty slots, find the oldest candidate in the oldest
 *    bucket and evict it by updating the slot's compressed_relid and setting
 *    the in-progress flag for the slot
 *  - the search is limited to the search window (TS_STATS_PROBE_WIDTH slots) in each
 *    bucket, and the buckets are traversed in recency order. if all slots are
 *    in-progress in the window, the function returns TS_STATS_INVALID_IDX
 *
 * Note that this call updates:
 *  - the in-progress flag of the slot, as mentioned above
 *  - the sequence number of the slot, which by itself should prevent eviction
 *  - the last update sequence for the bucket, which affects the recency order of the buckets
 */
int32 ts_stats_chunk_segment_prepare_upsert(TsStatsChunkSegment *seg, TsStatsRelids relids);

/* Once the caller prepared the slot for upsert and updated the stats, it should call this
 * function to clear the in-progress flag and thus make the slot eligible for eviction and
 * communicate to the readers that the slot can be read consistently.
 */
void ts_stats_chunk_segment_finish_upsert(TsStatsChunkSegment *seg, int32 slot_idx,
										  Oid compressed_relid);

/* Lookup — find an existing slot without allocating a new one. Return the global position of the
 * slot within all slots, ignoring buckets. Returns TS_STATS_INVALID_IDX on miss.
 */
int32 ts_stats_chunk_segment_lookup(TsStatsChunkSegment *seg, Oid compressed_relid);
