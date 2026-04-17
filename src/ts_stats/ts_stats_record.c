/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "ts_stats_record.h"
#include "ts_stats_segment.h"

#include <postgres.h>
#include <miscadmin.h>
#include <storage/lwlock.h>
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
ts_stats_chunk_record_compression(Oid compressed_relid, Oid uncompressed_relid,
								  const CompressionStatsAccumulator *a)
{
	if (!OidIsValid(compressed_relid) || !OidIsValid(uncompressed_relid) || a->batch_count == 0)
	{
		return;
	}

	TsStatsChunkSegment *seg = ts_get_stats_chunk_segment();
	if (seg == NULL)
	{
		return;
	}

	/* get the timestamp outside the lock scope */
	uint64 now_us = now_us_offset(seg);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);

	slot_idx_t s = ts_stats_chunk_segment_find_or_insert(seg, compressed_relid, uncompressed_relid);
	if (s != INVALID_IDX)
	{
		TsStatsChunk *slot = &ts_stats_chunk_slots(seg)[s];

		slot->compressed_batch_count += a->batch_count;
		slot->compressed_block_count += a->block_count;

		/* Fold the three accumulator triples into the matching slot triples. */
		mms_merge(&slot->compressed_block_bytes, &a->block_size);
		mms_merge(&slot->compressed_batch_bytes, &a->batch_size);
		mms_merge(&slot->compressed_batch_rows, &a->batch_rows);

		slot_touch_timestamps(slot, now_us);
	}

	LWLockRelease(&seg->lock);
}

void
ts_stats_chunk_record_cmd(Oid compressed_relid, Oid uncompressed_relid, CmdType cmd,
						  const SharedCounters *c)
{
	if (!OidIsValid(compressed_relid) || !OidIsValid(uncompressed_relid) || c == NULL)
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

	/* get the timestamp outside the lock scope */
	uint64 now_us = now_us_offset(seg);

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);

	slot_idx_t s = ts_stats_chunk_segment_find_or_insert(seg, compressed_relid, uncompressed_relid);
	if (s != INVALID_IDX)
	{
		TsStatsChunk *slot = &ts_stats_chunk_slots(seg)[s];

		switch (cmd)
		{
			case CMD_SELECT:
				slot->n_selects++;
				break;
			case CMD_INSERT:
				slot->n_inserts++;
				break;
			case CMD_UPDATE:
				slot->n_updates++;
				break;
			case CMD_DELETE:
				slot->n_deletes++;
				break;
			default:
				break;
		}

		/* update totals */
		slot->cmd_totals.batches_deleted += c->batches_deleted;
		slot->cmd_totals.batches_decompressed += c->batches_decompressed;
		slot->cmd_totals.tuples_decompressed += c->tuples_decompressed;
		slot->cmd_totals.batches_scanned += c->batches_scanned;
		slot->cmd_totals.batches_checked_by_bloom += c->batches_checked_by_bloom;
		slot->cmd_totals.batches_pruned_by_bloom += c->batches_pruned_by_bloom;
		slot->cmd_totals.batches_without_bloom += c->batches_without_bloom;
		slot->cmd_totals.batches_bloom_false_positives += c->batches_bloom_false_positives;
		slot->cmd_totals.batches_filtered_compressed += c->batches_filtered_compressed;
		slot->cmd_totals.batches_filtered_decompressed += c->batches_filtered_decompressed;

		/* update last op */
		slot->cmd_last_op = *c;

		slot_touch_timestamps(slot, now_us);
	}

	LWLockRelease(&seg->lock);
}

void
ts_stats_chunk_record_decompression(Oid compressed_relid, Oid uncompressed_relid,
									const CompressionStatsAccumulator *a)
{
	if (!OidIsValid(compressed_relid) || !OidIsValid(uncompressed_relid))
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

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);

	slot_idx_t s = ts_stats_chunk_segment_find_or_insert(seg, compressed_relid, uncompressed_relid);
	if (s != INVALID_IDX)
	{
		TsStatsChunk *slot = &ts_stats_chunk_slots(seg)[s];

		/* Batch group, being updated if the current information is more complete. */
		if (a->batch_count > slot->compressed_batch_count &&
			a->batch_rows.sum_val > slot->compressed_batch_rows.sum_val &&
			a->batch_rows.sqsum > slot->compressed_batch_rows.sqsum &&
			a->batch_size.sum_val > slot->compressed_batch_bytes.sum_val &&
			a->batch_size.sqsum > slot->compressed_batch_bytes.sqsum)
		{
			slot->compressed_batch_count = a->batch_count;
			slot->compressed_batch_rows = a->batch_rows;
			slot->compressed_batch_bytes = a->batch_size;
		}

		/* Column group, being updated if the current information is more complete. */
		if (a->block_count > slot->compressed_block_count &&
			a->block_size.sum_val > slot->compressed_block_bytes.sum_val &&
			a->block_size.sqsum > slot->compressed_block_bytes.sqsum)
		{
			slot->compressed_block_count = a->block_count;
			slot->compressed_block_bytes = a->block_size;
		}

		slot_touch_timestamps(slot, now_us_offset(seg));
	}

	LWLockRelease(&seg->lock);
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

	LWLockAcquire(&seg->lock, LW_SHARED);

	slot_idx_t s = ts_stats_chunk_segment_lookup(seg, compressed_relid);
	if (s != INVALID_IDX)
	{
		*out = ts_stats_chunk_slots(seg)[s];
	}

	LWLockRelease(&seg->lock);

	return s != INVALID_IDX;
}
