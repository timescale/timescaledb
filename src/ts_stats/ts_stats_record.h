/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "ts_stats_defs.h"
#include <nodes/parsenodes.h> /* CmdType */

typedef struct CompressionStatsAccumulator
{
	/* Per-column: updated on every column write. */
	uint32 block_count;
	MinMaxSumData block_size;

	/* Per-compressed-row: updated on every batch flush. */
	uint32 batch_count;
	MinMaxSumData batch_size;
	MinMaxSumData batch_rows;

	/* Reused between the per-column and per-comp-row. */
	uint32 pending_batch_bytes;

} CompressionStatsAccumulator;

static inline void
ts_stats_compression_acc_init(CompressionStatsAccumulator *a)
{
	memset(a, 0, sizeof(*a));
	init_min_max_sum(&a->block_size);
	init_min_max_sum(&a->batch_size);
	init_min_max_sum(&a->batch_rows);
}

static inline void
ts_stats_compression_acc_column(CompressionStatsAccumulator *a, uint32 block_bytes)
{
	a->block_count++;
	mms_observe(&a->block_size, block_bytes);
	a->pending_batch_bytes += block_bytes;
}

static inline void
ts_stats_compression_acc_batch(CompressionStatsAccumulator *a, uint32 batch_rows)
{
	a->batch_count++;
	mms_observe(&a->batch_size, a->pending_batch_bytes);
	mms_observe(&a->batch_rows, batch_rows);
	a->pending_batch_bytes = 0;
}

/* Update shared memory with new statistics information. */

/* Compression — commits the accumulator into the chunk's slot. */
extern TSDLLEXPORT void ts_stats_chunk_record_compression(Oid compressed_relid,
														  Oid uncompressed_relid,
														  const CompressionStatsAccumulator *a);

/* Cmd stats on compressed data. cmd is one of CMD_INSERT / CMD_UPDATE / CMD_DELETE / CMD_SELECT */
extern TSDLLEXPORT void ts_stats_chunk_record_cmd(Oid compressed_relid, Oid uncompressed_relid,
												  CmdType cmd, const SharedCounters *c);

/* For backfilling compressed data during decompression.
 * This function will update the compression stats for the chunk when it finds more complete
 * information about the compressed data than the one we have in memory. The reason we
 * need this is because the in-memory entries may miss the compression event, either because
 * of a restart or because the chunk information has been evicted from memory due to lack of use.
 */
extern TSDLLEXPORT void ts_stats_chunk_record_decompression(Oid compressed_relid,
															Oid uncompressed_relid,
															const CompressionStatsAccumulator *a);

/* Lookup TsStatsChunk based on the compressed relid. It returns true if the chunk is found, false
 * otherwise. */
extern TSDLLEXPORT bool ts_stats_chunk_lookup(Oid compressed_relid, TsStatsChunk *out);
