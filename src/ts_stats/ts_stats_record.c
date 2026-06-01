/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "ts_stats_record.h"
#include "ts_stats/ts_stats_defs.h"
#include "ts_stats_segment.h"

#include <postgres.h>
#include <miscadmin.h>
#include <storage/lwlock.h>
#include <utils/dsa.h>
#include <utils/timestamp.h>

static inline uint64
now_us_offset(TsStatsChunkSegment *seg)
{
	return (uint64) (GetCurrentTimestamp() - seg->base_timestamp);
}

static inline void
slot_touch_timestamps(TsStatsChunk *slot, uint64 now_us)
{
	/* There is very slight chance that this function is being called, just in the
	 * same microsecond as the base timestamp. */
	if (now_us == 0)
	{
		now_us = 1;
	}

	if (slot->first_update_us == 0)
	{
		slot->first_update_us = now_us;
	}
	slot->last_update_us = now_us;
}

void
ts_stats_chunk_record_compression(TsStatsRelids relids, const CompressionStatsAccumulator *a)
{
	if (!OidIsValid(relids.compressed_relid) || !OidIsValid(relids.uncompressed_relid) ||
		a->batch_count == 0)
	{
		return;
	}

	TsStatsChunkSegment *seg = ts_get_stats_chunk_segment();
	if (seg == NULL)
	{
		return;
	}

	/* get the slot_index for the upsert. this sets the in-progress flag and updates the bucket's
	 * metadata if successful.
	 */
	int32 slot_idx = ts_stats_chunk_segment_prepare_upsert(seg, relids);
	if (slot_idx == TS_STATS_INVALID_IDX)
	{
		/* if we failed to find or insert a slot for this compressed_relid, we just give up this
		 * time */
		return;
	}
	Assert(slot_idx >= 0 && slot_idx < (int32) seg->num_slots);

	/* get the timestamp outside the lock scope */
	uint64 now_us = now_us_offset(seg);

	/* get the base pointers for the chunk */
	TsStatsChunk *slot_base = ts_stats_chunk_slots(seg);

	/* since we have a valid slot_idx, it means the admin stuff is done with the slot, and we only
	 * need to update the actual statistics. the in-progress flag is set for the slot which we
	 * will need to clear once the update is done.
	 */
	TsStatsChunk *slot = &slot_base[slot_idx];
	pg_atomic_add_fetch_u32(&slot->compressed_batch_count, a->batch_count);
	pg_atomic_add_fetch_u32(&slot->compressed_block_count, a->block_count);

	/* Fold the three accumulator triples into the matching slot triples. */
	mms_merge(&slot->compressed_block_bytes, &a->block_size);
	mms_merge(&slot->compressed_batch_bytes, &a->batch_size);
	mms_merge(&slot->compressed_batch_rows, &a->batch_rows);

	slot_touch_timestamps(slot, now_us);

	/* clear the in-progress flag */
	ts_stats_chunk_segment_finish_upsert(seg, slot_idx, relids.compressed_relid);
}

void
ts_stats_chunk_record_cmd(TsStatsRelids relids, CmdType cmd, const SharedCounters *c)
{
	if (!OidIsValid(relids.compressed_relid) || !OidIsValid(relids.uncompressed_relid) || c == NULL)
	{
		return;
	}
	if (cmd != CMD_SELECT && cmd != CMD_INSERT && cmd != CMD_UPDATE && cmd != CMD_DELETE)
	{
		return;
	}

	TsStatsChunkSegment *seg = ts_get_stats_chunk_segment();
	if (seg == NULL)
	{
		return;
	}

	/* get the slot_index for the upsert. this sets the in-progress flag and updates the bucket's
	 * metadata if successful.
	 */
	int32 slot_idx = ts_stats_chunk_segment_prepare_upsert(seg, relids);
	if (slot_idx == TS_STATS_INVALID_IDX)
	{
		/* if we failed to find or insert a slot for this compressed_relid, we just give up this
		 * time */
		return;
	}
	Assert(slot_idx >= 0 && slot_idx < (int32) seg->num_slots);

	/* get the timestamp outside the lock scope */
	uint64 now_us = now_us_offset(seg);

	/* get the base pointers for the chunk */
	TsStatsChunk *slot_base = ts_stats_chunk_slots(seg);

	/* since we have a valid slot_idx, it means the admin stuff is done with the slot, and we only
	 * need to update the actual statistics. the in-progress flag is set for the slot which we
	 * will need to clear once the update is done.
	 */
	TsStatsChunk *slot = &slot_base[slot_idx];

	switch (cmd)
	{
		case CMD_SELECT:
			pg_atomic_add_fetch_u32(&slot->n_selects, 1);
			break;
		case CMD_INSERT:
			pg_atomic_add_fetch_u32(&slot->n_inserts, 1);
			break;
		case CMD_UPDATE:
			pg_atomic_add_fetch_u32(&slot->n_updates, 1);
			break;
		case CMD_DELETE:
			pg_atomic_add_fetch_u32(&slot->n_deletes, 1);
			break;
		default:
			break;
	}

	/* update totals */
	pg_atomic_add_fetch_u64(&slot->cmd_totals.batches_deleted, c->batches_deleted);
	pg_atomic_add_fetch_u64(&slot->cmd_totals.batches_decompressed, c->batches_decompressed);
	pg_atomic_add_fetch_u64(&slot->cmd_totals.tuples_decompressed, c->tuples_decompressed);
	pg_atomic_add_fetch_u64(&slot->cmd_totals.batches_scanned, c->batches_scanned);
	pg_atomic_add_fetch_u64(&slot->cmd_totals.batches_checked_by_bloom,
							c->batches_checked_by_bloom);
	pg_atomic_add_fetch_u64(&slot->cmd_totals.batches_pruned_by_bloom, c->batches_pruned_by_bloom);
	pg_atomic_add_fetch_u64(&slot->cmd_totals.batches_without_bloom, c->batches_without_bloom);
	pg_atomic_add_fetch_u64(&slot->cmd_totals.batches_bloom_false_positives,
							c->batches_bloom_false_positives);
	pg_atomic_add_fetch_u64(&slot->cmd_totals.batches_filtered_compressed,
							c->batches_filtered_compressed);
	pg_atomic_add_fetch_u64(&slot->cmd_totals.batches_filtered_decompressed,
							c->batches_filtered_decompressed);

	/* update last op */
	pg_atomic_write_u64(&slot->cmd_last_op.batches_deleted, c->batches_deleted);
	pg_atomic_write_u64(&slot->cmd_last_op.batches_decompressed, c->batches_decompressed);
	pg_atomic_write_u64(&slot->cmd_last_op.tuples_decompressed, c->tuples_decompressed);
	pg_atomic_write_u64(&slot->cmd_last_op.batches_scanned, c->batches_scanned);
	pg_atomic_write_u64(&slot->cmd_last_op.batches_checked_by_bloom, c->batches_checked_by_bloom);
	pg_atomic_write_u64(&slot->cmd_last_op.batches_pruned_by_bloom, c->batches_pruned_by_bloom);
	pg_atomic_write_u64(&slot->cmd_last_op.batches_without_bloom, c->batches_without_bloom);
	pg_atomic_write_u64(&slot->cmd_last_op.batches_bloom_false_positives,
						c->batches_bloom_false_positives);
	pg_atomic_write_u64(&slot->cmd_last_op.batches_filtered_compressed,
						c->batches_filtered_compressed);
	pg_atomic_write_u64(&slot->cmd_last_op.batches_filtered_decompressed,
						c->batches_filtered_decompressed);

	/* update timestamps */
	slot_touch_timestamps(slot, now_us);

	/* clear the in-progress flag */
	ts_stats_chunk_segment_finish_upsert(seg, slot_idx, relids.compressed_relid);
}

void
ts_stats_chunk_record_decompression(TsStatsRelids relids, const CompressionStatsAccumulator *a)
{
	if (!OidIsValid(relids.compressed_relid) || !OidIsValid(relids.uncompressed_relid))
	{
		return;
	}
	if (a->batch_count == 0 && a->block_count == 0)
	{
		return;
	}

	TsStatsChunkSegment *seg = ts_get_stats_chunk_segment();
	if (seg == NULL)
	{
		return;
	}

	/* get the slot_index for the upsert. this sets the in-progress flag and updates the bucket's
	 * metadata if successful.
	 */
	int32 slot_idx = ts_stats_chunk_segment_prepare_upsert(seg, relids);
	if (slot_idx == TS_STATS_INVALID_IDX)
	{
		/* if we failed to find or insert a slot for this compressed_relid, we just give up this
		 * time */
		return;
	}
	Assert(slot_idx >= 0 && slot_idx < (int32) seg->num_slots);

	/* get the timestamp outside the lock scope */
	uint64 now_us = now_us_offset(seg);

	/* get the base pointers for the chunk */
	TsStatsChunk *slot_base = ts_stats_chunk_slots(seg);

	/* since we have a valid slot_idx, it means the admin stuff is done with the slot, and we only
	 * need to update the actual statistics. the in-progress flag is set for the slot which we
	 * will need to clear once the update is done.
	 */
	TsStatsChunk *slot = &slot_base[slot_idx];

	/* Batch group being updated if the current information is more complete. */
	if (a->batch_count > pg_atomic_read_u32(&slot->compressed_batch_count) &&
		a->batch_rows.sum_val > pg_atomic_read_u64(&slot->compressed_batch_rows.sum_val) &&
		a->batch_rows.sqsum_val > mms_sqsum(&slot->compressed_batch_rows) &&
		a->batch_size.sum_val > pg_atomic_read_u64(&slot->compressed_batch_bytes.sum_val) &&
		a->batch_size.sqsum_val > mms_sqsum(&slot->compressed_batch_bytes))
	{
		pg_atomic_write_u32(&slot->compressed_batch_count, a->batch_count);
		mms_update(&slot->compressed_batch_rows, &a->batch_rows);
		mms_update(&slot->compressed_batch_bytes, &a->batch_size);
	}

	/* Column group being updated if the current information is more complete. */
	if (a->block_count > pg_atomic_read_u32(&slot->compressed_block_count) &&
		a->block_size.sum_val > pg_atomic_read_u64(&slot->compressed_block_bytes.sum_val) &&
		a->block_size.sqsum_val > mms_sqsum(&slot->compressed_block_bytes))
	{
		pg_atomic_write_u32(&slot->compressed_block_count, a->block_count);
		mms_update(&slot->compressed_block_bytes, &a->block_size);
	}

	/* update timestamps */
	slot_touch_timestamps(slot, now_us);

	/* clear the in-progress flag */
	ts_stats_chunk_segment_finish_upsert(seg, slot_idx, relids.compressed_relid);
}

bool
ts_stats_chunk_lookup(Oid compressed_relid, TsStatsChunk *out)
{
	if (!OidIsValid(compressed_relid) || out == NULL)
	{
		return false;
	}

	TsStatsChunkSegment *seg = ts_get_stats_chunk_segment();
	if (seg == NULL)
	{
		return false;
	}

	int32 slot_idx = ts_stats_chunk_segment_lookup(seg, compressed_relid);

	if (slot_idx == TS_STATS_INVALID_IDX)
	{
		return false;
	}
	Assert(slot_idx >= 0 && slot_idx < (int32) seg->num_slots);

	/* get the base pointers for the chunk */
	TsStatsChunk *slot_base = ts_stats_chunk_slots(seg);
	TsStatsChunk *slot = &slot_base[slot_idx];

	/* dumb memcpy, hoping the data is still consistent */
	memcpy(out, slot, sizeof(TsStatsChunk));
	return true;
}

void
ts_stats_chunk_evict(Oid compressed_relid)
{
	if (!OidIsValid(compressed_relid))
	{
		return;
	}
	TsStatsChunkSegment *seg = ts_get_stats_chunk_segment();
	if (seg == NULL)
	{
		return;
	}

	int32 slot_idx = ts_stats_chunk_segment_lookup(seg, compressed_relid);

	if (slot_idx == TS_STATS_INVALID_IDX)
	{
		return;
	}
	Assert(slot_idx >= 0 && slot_idx < (int32) seg->num_slots);

	TsStatsChunkMetadata *meta_base = ts_stats_chunk_metadata(seg);
	TsStatsChunkMetadata *meta = &meta_base[slot_idx];

	/* limited CAS loop to ensure we don't evict a chunk that is currently being updated, the danger
	 * is that the one that we are evicting is already being overwritten by a newer/other chunk.
	 */
	uint64 state = pg_atomic_read_u64(&meta->state);
	uint64 new_seqno = pg_atomic_add_fetch_u64(&seg->update_seqno, 1);
	uint64 new_state = (new_seqno << TS_STATS_CHUNK_METADATA_SEQNO_SHIFT);
	for (int i = 0;
		 i < 5 &&
		 /* the slot still matches the compressed_relid */
		 meta->relids.compressed_relid == compressed_relid &&
		 /* the slot is still valid and not in progress */
		 TS_STATS_CHUNK_METADATA_IS_VALID(state) &&
		 !(TS_STATS_CHUNK_METADATA_IS_IN_PROGRESS(state)) &&
		 /* the slot being updated is older than the sequence number of the current update */
		 TS_STATS_CHUNK_METADATA_GET_SEQNO(state) < new_seqno;
		 ++i)
	{
		if (pg_atomic_compare_exchange_u64(&meta->state, &state, new_state))
		{
			meta->relids.compressed_relid = InvalidOid;
			break;
		}
	}
}
