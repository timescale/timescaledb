/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_DECOMPRESS_SORTED_MERGE_H
#define TIMESCALEDB_DECOMPRESS_SORTED_MERGE_H

#include "compression/compression.h"
#include "nodes/decompress_chunk/exec.h"

extern void batch_queue_heap_free(DecompressChunkState *chunk_state);

extern bool batch_queue_heap_needs_next_batch(DecompressChunkState *chunk_state);

extern void batch_queue_heap_push_batch(DecompressChunkState *chunk_state,
										TupleTableSlot *compressed_slot);

extern TupleTableSlot *batch_queue_heap_top_tuple(DecompressChunkState *chunk_state);

void batch_queue_heap_pop(DecompressChunkState *chunk_state);

void batch_queue_heap_create(DecompressChunkState *chunk_state);

void batch_queue_heap_reset(DecompressChunkState *chunk_state);

#endif
