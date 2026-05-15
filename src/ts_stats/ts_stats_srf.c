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
#include <storage/lwlock.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>

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

	/* base_timestamp is set once at init and never written; reading it before the lock is safe.*/
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

	/* First snapshot the values under a SHARED lock */
	TsStatsChunk *snap = NULL;
	uint32 snap_count = 0;
	TimestampTz base = 0;

	base = seg->base_timestamp;
	TsStatsChunk *slots = ts_stats_chunk_slots(seg);

	if (comp_filter)
	{
		/* Point lookup: at most one slot. */
		snap = palloc(sizeof(TsStatsChunk));
		LWLockAcquire(&seg->lock, LW_SHARED);
		slot_idx_t s = ts_stats_chunk_segment_lookup(seg, comp_oid);
		bool match = (s != INVALID_IDX);
		if (match && uncomp_filter && slots[s].uncompressed_relid != uncomp_oid)
		{
			match = false;
		}
		if (match && since_filter && slots[s].last_update_us < since_offset)
		{
			match = false;
		}
		if (match)
		{
			snap[snap_count++] = slots[s];
		}
		LWLockRelease(&seg->lock);
	}
	else if (uncomp_filter)
	{
		/* Linear scan matching uncompressed_relid. */
		snap = palloc(seg->num_slots * sizeof(TsStatsChunk));
		LWLockAcquire(&seg->lock, LW_SHARED);
		for (uint32 i = 0; i < seg->num_slots; i++)
		{
			if (!OidIsValid(slots[i].compressed_relid))
			{
				continue;
			}
			if (slots[i].uncompressed_relid != uncomp_oid)
			{
				continue;
			}
			if (since_filter && slots[i].last_update_us < since_offset)
			{
				continue;
			}
			snap[snap_count++] = slots[i];
		}
		LWLockRelease(&seg->lock);
	}
	else if (since_filter)
	{
		/* Walk through the list in LRU order. */
		snap = palloc(seg->num_slots * sizeof(TsStatsChunk));
		LWLockAcquire(&seg->lock, LW_SHARED);
		slot_idx_t idx = seg->lru_head;
		while (idx != INVALID_IDX)
		{
			if (slots[idx].last_update_us < since_offset)
			{
				break;
			}
			snap[snap_count++] = slots[idx];
			idx = slots[idx].lru_next;
		}
		LWLockRelease(&seg->lock);
	}
	else
	{
		/* Full scan in slot order. */
		snap = palloc(seg->num_slots * sizeof(TsStatsChunk));
		LWLockAcquire(&seg->lock, LW_SHARED);
		for (uint32 i = 0; i < seg->num_slots; i++)
		{
			if (OidIsValid(slots[i].compressed_relid))
			{
				snap[snap_count++] = slots[i];
			}
		}
		LWLockRelease(&seg->lock);
	}

	/* Build the result tuples outside the lock */
	Datum values[STATS_CHUNKS_NCOLS];
	bool nulls[STATS_CHUNKS_NCOLS] = { false };

	for (uint32 i = 0; i < snap_count; i++)
	{
		TsStatsChunk *s = &snap[i];
		int c = 0;

		values[c++] = ObjectIdGetDatum(s->compressed_relid);
		values[c++] = ObjectIdGetDatum(s->uncompressed_relid);

		/* Compression aggregates */
		values[c++] = Int64GetDatum((int64) s->compressed_batch_count);
		values[c++] = Int64GetDatum((int64) s->compressed_block_count);

		if (s->compressed_batch_count == 0)
		{
			for (int j = 0; j < 8; j++)
			{
				nulls[c + j] = true;
			}
			c += 8;
		}
		else
		{
			values[c++] = Int64GetDatum((int64) s->compressed_batch_rows.min_val);
			values[c++] = Int64GetDatum((int64) s->compressed_batch_rows.max_val);
			values[c++] = Int64GetDatum((int64) s->compressed_batch_rows.sum_val);
			values[c++] = Float8GetDatum(s->compressed_batch_rows.sqsum);
			values[c++] = Int64GetDatum((int64) s->compressed_batch_bytes.min_val);
			values[c++] = Int64GetDatum((int64) s->compressed_batch_bytes.max_val);
			values[c++] = Int64GetDatum((int64) s->compressed_batch_bytes.sum_val);
			values[c++] = Float8GetDatum(s->compressed_batch_bytes.sqsum);
		}

		if (s->compressed_block_count == 0)
		{
			for (int j = 0; j < 4; j++)
			{
				nulls[c + j] = true;
			}
			c += 4;
		}
		else
		{
			values[c++] = Int64GetDatum((int64) s->compressed_block_bytes.min_val);
			values[c++] = Int64GetDatum((int64) s->compressed_block_bytes.max_val);
			values[c++] = Int64GetDatum((int64) s->compressed_block_bytes.sum_val);
			values[c++] = Float8GetDatum(s->compressed_block_bytes.sqsum);
		}

		/* Cmd totals */
		values[c++] = Int64GetDatum(s->cmd_totals.batches_deleted);
		values[c++] = Int64GetDatum(s->cmd_totals.batches_decompressed);
		values[c++] = Int64GetDatum(s->cmd_totals.tuples_decompressed);
		values[c++] = Int64GetDatum(s->cmd_totals.batches_scanned);
		values[c++] = Int64GetDatum(s->cmd_totals.batches_checked_by_bloom);
		values[c++] = Int64GetDatum(s->cmd_totals.batches_pruned_by_bloom);
		values[c++] = Int64GetDatum(s->cmd_totals.batches_without_bloom);
		values[c++] = Int64GetDatum(s->cmd_totals.batches_bloom_false_positives);
		values[c++] = Int64GetDatum(s->cmd_totals.batches_filtered_compressed);
		values[c++] = Int64GetDatum(s->cmd_totals.batches_filtered_decompressed);

		/* Last cmd stats */
		values[c++] = Int64GetDatum(s->cmd_last_op.batches_deleted);
		values[c++] = Int64GetDatum(s->cmd_last_op.batches_decompressed);
		values[c++] = Int64GetDatum(s->cmd_last_op.tuples_decompressed);
		values[c++] = Int64GetDatum(s->cmd_last_op.batches_scanned);
		values[c++] = Int64GetDatum(s->cmd_last_op.batches_checked_by_bloom);
		values[c++] = Int64GetDatum(s->cmd_last_op.batches_pruned_by_bloom);
		values[c++] = Int64GetDatum(s->cmd_last_op.batches_without_bloom);
		values[c++] = Int64GetDatum(s->cmd_last_op.batches_bloom_false_positives);
		values[c++] = Int64GetDatum(s->cmd_last_op.batches_filtered_compressed);
		values[c++] = Int64GetDatum(s->cmd_last_op.batches_filtered_decompressed);

		/* Operation counts */
		values[c++] = Int64GetDatum((int64) s->n_selects);
		values[c++] = Int64GetDatum((int64) s->n_inserts);
		values[c++] = Int64GetDatum((int64) s->n_updates);
		values[c++] = Int64GetDatum((int64) s->n_deletes);

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

	if (snap)
	{
		pfree(snap);
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
