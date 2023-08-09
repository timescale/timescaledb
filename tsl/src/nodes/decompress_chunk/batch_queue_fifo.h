/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include "compression/compression.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/batch_array.h"

inline static void
batch_queue_fifo_create(DecompressChunkState *chunk_state)
{
	batch_array_create(chunk_state, 1);
}

inline static void
batch_queue_fifo_free(DecompressChunkState *chunk_state)
{
	batch_array_destroy(chunk_state);
}

inline static bool
batch_queue_fifo_needs_next_batch(DecompressChunkState *chunk_state)
{
	return TupIsNull(batch_array_get_at(chunk_state, 0)->decompressed_scan_slot);
}

inline static void
batch_queue_fifo_pop(DecompressChunkState *chunk_state)
{
	DecompressBatchState *batch_state = batch_array_get_at(chunk_state, 0);
	if (TupIsNull(batch_state->decompressed_scan_slot))
	{
		/* Allow this function to be called on the initial empty queue. */
		return;
	}

	compressed_batch_advance(chunk_state, batch_state);
}

inline static void
batch_queue_fifo_push_batch(DecompressChunkState *chunk_state, TupleTableSlot *compressed_slot)
{
	DecompressBatchState *batch_state = batch_array_get_at(chunk_state, 0);
	Assert(TupIsNull(batch_array_get_at(chunk_state, 0)->decompressed_scan_slot));
	compressed_batch_set_compressed_tuple(chunk_state, batch_state, compressed_slot);
	compressed_batch_advance(chunk_state, batch_state);
}

inline static void
batch_queue_fifo_reset(DecompressChunkState *chunk_state)
{
	batch_array_free_at(chunk_state, 0);
}

inline static TupleTableSlot *
batch_queue_fifo_top_tuple(DecompressChunkState *chunk_state)
{
	return batch_array_get_at(chunk_state, 0)->decompressed_scan_slot;
}
