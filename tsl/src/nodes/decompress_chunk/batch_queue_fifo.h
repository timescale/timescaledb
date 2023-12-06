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
	DecompressContext *dcontext = &chunk_state->decompress_context;

	batch_array_init(&dcontext->batch_array,
					 1,
					 chunk_state->num_compressed_columns,
					 dcontext->batch_memory_context_bytes);
}

inline static void
batch_queue_fifo_free(DecompressChunkState *chunk_state)
{
	batch_array_destroy(&chunk_state->decompress_context.batch_array);
}

inline static bool
batch_queue_fifo_needs_next_batch(DecompressChunkState *chunk_state)
{
	return TupIsNull(batch_array_get_at(&chunk_state->decompress_context.batch_array, 0)
						 ->decompressed_scan_slot);
}

inline static void
batch_queue_fifo_pop(DecompressChunkState *chunk_state)
{
	DecompressBatchState *batch_state =
		batch_array_get_at(&chunk_state->decompress_context.batch_array, 0);
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
	BatchArray *batch_array = &chunk_state->decompress_context.batch_array;
	DecompressBatchState *batch_state = batch_array_get_at(batch_array, 0);
	Assert(TupIsNull(batch_array_get_at(batch_array, 0)->decompressed_scan_slot));
	compressed_batch_set_compressed_tuple(chunk_state, batch_state, compressed_slot);
	compressed_batch_advance(chunk_state, batch_state);
}

inline static void
batch_queue_fifo_reset(DecompressChunkState *chunk_state)
{
	batch_array_clear_at(&chunk_state->decompress_context.batch_array, 0);
}

inline static TupleTableSlot *
batch_queue_fifo_top_tuple(DecompressChunkState *chunk_state)
{
	return batch_array_get_at(&chunk_state->decompress_context.batch_array, 0)
		->decompressed_scan_slot;
}
