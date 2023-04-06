/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_DECOMPRESS_SORTED_MERGE_H
#define TIMESCALEDB_DECOMPRESS_SORTED_MERGE_H

#include "compression/compression.h"
#include "nodes/decompress_chunk/exec.h"

/* We have to decompress the compressed batches in parallel. Therefore, we need a high
 * amount of memory. Set the tuple cost for this algorithm a very high value to prevent
 * that this algorithm is chosen when a lot of batches needs to be merged. For more details,
 * see the discussion in cost_decompress_sorted_merge_append(). */
#define DECOMPRESS_CHUNK_HEAP_MERGE_CPU_TUPLE_COST 0.8

/* The value for an invalid batch id */
#define INVALID_BATCH_ID -1

extern void decompress_sorted_merge_init(DecompressChunkState *chunk_state);

extern void decompress_sorted_merge_free(DecompressChunkState *chunk_state);

extern void
decompress_sorted_merge_remove_top_tuple_and_decompress_next(DecompressChunkState *chunk_state);

extern TupleTableSlot *decompress_sorted_merge_get_next_tuple(DecompressChunkState *chunk_state);

#endif
