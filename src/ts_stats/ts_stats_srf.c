/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "ts_stats_defs.h"
#include "ts_stats_segment.h"

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>
#include <utils/tuplestore.h>

/*
 * SRF Pushdown args:
 *   $1 compressed_relid   regclass    — point lookup by hash key; emits 0 or 1 row.
 *   $2 uncompressed_relid regclass    — linear scan, or cross-checks $1.
 *   $3 since              timestamptz — recency walk; LRU short-circuit, O(matches).
 * All NULL → full scan. If both relid args are set, both must agree.
 */

TS_FUNCTION_INFO_V1(ts_stats_chunks);
TS_FUNCTION_INFO_V1(ts_stats_reset);

/*
 * 2 relids + 2 counts (batch, column) + 9 compression min/max/sum
 * (batch_rows, batch_bytes, column_bytes) + 3 sqsum (one per MinMaxSumData)
 * + 10 cmd_totals + 10 cmd_last_op + 4 n_* + 2 timestamps = 42.
 */
#define STATS_CHUNKS_NCOLS 42
#define ARG_COMPRESSED_RELID 0
#define ARG_UNCOMPRESSED_RELID 1
#define ARG_SINCE 2

Datum
ts_stats_chunks(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);
	TupleDesc tupdesc = rsinfo->setDesc;
	Tuplestorestate *tupstore = rsinfo->setResult;

	TsStatsChunkSegment *seg = ts_get_stats_chunk_segment();
	if (seg == NULL)
	{
		PG_RETURN_VOID();
	}

	bool comp_filter = !PG_ARGISNULL(ARG_COMPRESSED_RELID);
	bool uncomp_filter = !PG_ARGISNULL(ARG_UNCOMPRESSED_RELID);
	Oid comp_oid = comp_filter ? PG_GETARG_OID(ARG_COMPRESSED_RELID) : InvalidOid;
	Oid uncomp_oid = uncomp_filter ? PG_GETARG_OID(ARG_UNCOMPRESSED_RELID) : InvalidOid;

	if (comp_filter && !OidIsValid(comp_oid))
	{
		/* don't return any rows for invalid Oid */
		PG_RETURN_VOID();
	}

	if (uncomp_filter && !OidIsValid(uncomp_oid))
	{
		/* don't return any rows for invalid Oid */
		PG_RETURN_VOID();
	}

	bool since_filter = !PG_ARGISNULL(ARG_SINCE);
	uint64 since_offset = 0;

	/* base_timestamp is set once at init */
	TimestampTz since_ts = since_filter ? PG_GETARG_TIMESTAMPTZ(ARG_SINCE) : 0;
	if (since_filter)
	{
		if (since_ts <= seg->base_timestamp)
		{
			/* if threshold is earlier than the base, it means all record matches, so we can disable
			 * the filter */
			since_filter = false;
		}
		else
		{
			since_offset = (uint64) (since_ts - seg->base_timestamp);
		}
	}

	/* First snapshot the values */
	TsStatsChunk *slot_snap = NULL;
	TsStatsChunkMetadata *meta_snap = NULL;
	bool *retry_map = NULL;
	uint32 n_retry_entries = 0;
	uint32 first_to_try = seg->num_slots - 1;
	uint32 last_to_try = 0;
	uint32 snap_count = 0;
	TimestampTz base = 0;

	base = seg->base_timestamp;
	TsStatsChunk *slot_base = ts_stats_chunk_slots(seg);
	TsStatsChunkMetadata *meta_base = ts_stats_chunk_metadata(seg);

	if (comp_filter)
	{
		/* Point lookup based on compressed_relid */
		int32 slot_idx = ts_stats_chunk_segment_lookup(seg, comp_oid);
		if (slot_idx == TS_STATS_INVALID_IDX)
		{
			/* no matching record found, return empty set */
			PG_RETURN_VOID();
		}

		TsStatsChunkMetadata *meta = &meta_base[slot_idx];
		TsStatsChunk *slot = &slot_base[slot_idx];

		if (uncomp_filter)
		{
			/* if uncompressed_relid is also provided, check the uncompressed_relid matches as well
			 */
			if (meta->relids.uncompressed_relid != uncomp_oid)
			{
				/* no matching record found, return empty set */
				PG_RETURN_VOID();
			}
		}

		if (since_filter)
		{
			uint64 last_update_us = slot->last_update_us;
			if (last_update_us == 0 || last_update_us < since_offset)
			{
				/* record is older than the threshold, return empty set */
				PG_RETURN_VOID();
			}
		}

		uint64 state = pg_atomic_read_u64(&meta->state);
		if (TS_STATS_CHUNK_METADATA_IS_VALID(state) == 0)
		{
			/* slot is not valid anymore, return empty set */
			PG_RETURN_VOID();
		}

		if (TS_STATS_CHUNK_METADATA_IS_IN_PROGRESS(state))
		{
			/* slot is being updated, mark it for retry */
			retry_map = palloc0(sizeof(bool) * seg->num_slots);
			retry_map[slot_idx] = true;
			n_retry_entries = 1;
			first_to_try = last_to_try = slot_idx;
		}
		else
		{
			/* safe to read, take a snapshot */
			slot_snap = palloc(sizeof(TsStatsChunk));
			meta_snap = palloc(sizeof(TsStatsChunkMetadata));
			memcpy(slot_snap, slot, sizeof(TsStatsChunk));
			memcpy(meta_snap, meta, sizeof(TsStatsChunkMetadata));
			snap_count = 1;
		}
	}
	else if (uncomp_filter)
	{
		/* Linear scan of the metadata matching uncompressed_relid, but only one  */
		slot_snap = palloc(sizeof(TsStatsChunk));
		meta_snap = palloc(sizeof(TsStatsChunkMetadata));
		retry_map = palloc0(sizeof(bool) * seg->num_slots);

		for (uint32 i = 0; i < seg->num_slots; i++)
		{
			TsStatsChunk *slot = &slot_base[i];
			TsStatsChunkMetadata *meta = &meta_base[i];

			uint64 state = pg_atomic_read_u64(&meta->state);
			if (!TS_STATS_CHUNK_METADATA_IS_VALID(state))
			{
				/* don't deal with evicted/empty entries */
				continue;
			}
			if (meta->relids.uncompressed_relid != uncomp_oid)
			{
				continue;
			}
			/* the update timestamp is read without checking the in-progress flag
			 * because its meaning doesn't depend on other fields and it is consistent
			 * as it is. the worst case is that we might miss an update here, but
			 * the in-progress flag is about the overall state of the chunk slot
			 */
			if (since_filter && slot->last_update_us < since_offset)
			{
				continue;
			}

			if (TS_STATS_CHUNK_METADATA_IS_IN_PROGRESS(state))
			{
				/* slot is being updated, mark it for retry */
				retry_map[i] = true;
				++n_retry_entries;
				if (i < first_to_try)
				{
					first_to_try = i;
				}
				if (i > last_to_try)
				{
					last_to_try = i;
				}
			}
			else
			{
				/* safe to read, take a snapshot */
				memcpy(slot_snap, slot, sizeof(TsStatsChunk));
				memcpy(meta_snap, meta, sizeof(TsStatsChunkMetadata));
				snap_count = 1;
			}
			/* either we did the snapshot or marked it for retry, in both
			 * cases we can stop the loop. */
			break;
		}
	}
	else
	{
		/* full scan in slot order. check since timestamp if needed. */
		slot_snap = palloc(sizeof(TsStatsChunk) * seg->num_slots);
		meta_snap = palloc(sizeof(TsStatsChunkMetadata) * seg->num_slots);
		retry_map = palloc0(sizeof(bool) * seg->num_slots);

		for (uint32 i = 0; i < seg->num_slots; i++)
		{
			TsStatsChunk *slot = &slot_base[i];
			TsStatsChunkMetadata *meta = &meta_base[i];

			uint64 state = pg_atomic_read_u64(&meta->state);
			if (!TS_STATS_CHUNK_METADATA_IS_VALID(state))
			{
				/* don't deal with evicted/empty entries */
				continue;
			}
			/* the update timestamp is read without checking the in-progress flag
			 * because its meaning doesn't depend on other fields and it is consistent
			 * as it is. the worst case is that we might miss an update here, but
			 * the in-progress flag is about the overall state of the chunk slot
			 */
			if (since_filter && slot->last_update_us < since_offset)
			{
				continue;
			}

			if (TS_STATS_CHUNK_METADATA_IS_IN_PROGRESS(state))
			{
				/* slot is being updated, mark it for retry */
				retry_map[i] = true;
				++n_retry_entries;
				if (i < first_to_try)
				{
					first_to_try = i;
				}
				if (i > last_to_try)
				{
					last_to_try = i;
				}
			}
			else
			{
				/* safe to read, take a snapshot */
				memcpy(&slot_snap[snap_count], slot, sizeof(TsStatsChunk));
				memcpy(&meta_snap[snap_count], meta, sizeof(TsStatsChunkMetadata));
				snap_count++;
			}
		}
	}

	/* give three attempts to retry the in-progress entries, use
	 * first_to_try and last_to_try to limit the range
	 */
	for (uint8 r = 0; r < 3 && n_retry_entries > 0; r++)
	{
		for (uint32 i = first_to_try; i <= last_to_try; i++)
		{
			if (!retry_map[i])
			{
				/* this entry is not marked for retry, skip it */
				continue;
			}

			TsStatsChunk *slot = &slot_base[i];
			TsStatsChunkMetadata *meta = &meta_base[i];

			uint64 state = pg_atomic_read_u64(&meta->state);
			if (TS_STATS_CHUNK_METADATA_IS_IN_PROGRESS(state))
			{
				/* still in progress, keep it in the retry map for the next round */
				continue;
			}

			/* no longer in progress, take a snapshot and remove it from the retry map */
			memcpy(&slot_snap[snap_count], slot, sizeof(TsStatsChunk));
			memcpy(&meta_snap[snap_count], meta, sizeof(TsStatsChunkMetadata));
			snap_count++;
			retry_map[i] = false;
			n_retry_entries--;
		}
	}

	/* Build the result tuples */
	Datum values[STATS_CHUNKS_NCOLS];
	bool nulls[STATS_CHUNKS_NCOLS] = { false };

	for (uint32 i = 0; i < snap_count; i++)
	{
		TsStatsChunk *s = &slot_snap[i];
		TsStatsChunkMetadata *m = &meta_snap[i];
		int c = 0;

		values[c++] = ObjectIdGetDatum(m->relids.compressed_relid);
		values[c++] = ObjectIdGetDatum(m->relids.uncompressed_relid);

		/* Compression aggregates */
		values[c++] = Int64GetDatum((int64) pg_atomic_read_u32(&s->compressed_batch_count));
		values[c++] = Int64GetDatum((int64) pg_atomic_read_u32(&s->compressed_block_count));

		if (pg_atomic_read_u32(&s->compressed_batch_count) == 0)
		{
			for (int j = 0; j < 8; j++)
			{
				nulls[c + j] = true;
			}
			c += 8;
		}
		else
		{
			values[c++] =
				Int64GetDatum((int64) pg_atomic_read_u32(&s->compressed_batch_rows.min_val));
			values[c++] =
				Int64GetDatum((int64) pg_atomic_read_u32(&s->compressed_batch_rows.max_val));
			values[c++] =
				Int64GetDatum((int64) pg_atomic_read_u64(&s->compressed_batch_rows.sum_val));
			values[c++] = Float8GetDatum(mms_sqsum(&s->compressed_batch_rows));
			values[c++] =
				Int64GetDatum((int64) pg_atomic_read_u32(&s->compressed_batch_bytes.min_val));
			values[c++] =
				Int64GetDatum((int64) pg_atomic_read_u32(&s->compressed_batch_bytes.max_val));
			values[c++] =
				Int64GetDatum((int64) pg_atomic_read_u64(&s->compressed_batch_bytes.sum_val));
			values[c++] = Float8GetDatum(mms_sqsum(&s->compressed_batch_bytes));
		}

		if (pg_atomic_read_u32(&s->compressed_block_count) == 0)
		{
			for (int j = 0; j < 4; j++)
			{
				nulls[c + j] = true;
			}
			c += 4;
		}
		else
		{
			values[c++] =
				Int64GetDatum((int64) pg_atomic_read_u32(&s->compressed_block_bytes.min_val));
			values[c++] =
				Int64GetDatum((int64) pg_atomic_read_u32(&s->compressed_block_bytes.max_val));
			values[c++] =
				Int64GetDatum((int64) pg_atomic_read_u64(&s->compressed_block_bytes.sum_val));
			values[c++] = Float8GetDatum(mms_sqsum(&s->compressed_block_bytes));
		}

		/* Cmd totals */
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.batches_deleted));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.batches_decompressed));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.tuples_decompressed));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.batches_scanned));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.batches_checked_by_bloom));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.batches_pruned_by_bloom));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.batches_without_bloom));
		values[c++] =
			Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.batches_bloom_false_positives));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.batches_filtered_compressed));
		values[c++] =
			Int64GetDatum(pg_atomic_read_u64(&s->cmd_totals.batches_filtered_decompressed));

		/* Last cmd stats */
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.batches_deleted));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.batches_decompressed));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.tuples_decompressed));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.batches_scanned));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.batches_checked_by_bloom));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.batches_pruned_by_bloom));
		values[c++] = Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.batches_without_bloom));
		values[c++] =
			Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.batches_bloom_false_positives));
		values[c++] =
			Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.batches_filtered_compressed));
		values[c++] =
			Int64GetDatum(pg_atomic_read_u64(&s->cmd_last_op.batches_filtered_decompressed));

		/* Operation counts */
		values[c++] = Int64GetDatum((int64) pg_atomic_read_u32(&s->n_selects));
		values[c++] = Int64GetDatum((int64) pg_atomic_read_u32(&s->n_inserts));
		values[c++] = Int64GetDatum((int64) pg_atomic_read_u32(&s->n_updates));
		values[c++] = Int64GetDatum((int64) pg_atomic_read_u32(&s->n_deletes));

		/* Timestamps */
		values[c++] = TimestampTzGetDatum(base + (TimestampTz) s->first_update_us);
		values[c++] = TimestampTzGetDatum(base + (TimestampTz) s->last_update_us);

		Assert(c == STATS_CHUNKS_NCOLS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);

		/* Reset nulls for the next iteration. */
		for (int j = 0; j < STATS_CHUNKS_NCOLS; j++)
		{
			nulls[j] = false;
		}
	}

	/* free snapshots */
	if (slot_snap != NULL)
	{
		pfree(slot_snap);
	}
	if (meta_snap != NULL)
	{
		pfree(meta_snap);
	}
	PG_RETURN_VOID();
}

Datum
ts_stats_reset(PG_FUNCTION_ARGS)
{
	TsStatsChunkSegment *seg = ts_get_stats_chunk_segment();
	if (seg != NULL)
	{
		ts_stats_chunk_segment_reset(seg);
	}
	PG_RETURN_VOID();
}
