/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include "compression/compression.h"
#include "nodes/decompress_chunk/exec.h"
#include "nodes/decompress_chunk/compressed_batch.h"

/* The value for an invalid batch id */
#define INVALID_BATCH_ID -1

/*
 * Create states to hold information for up to n batches
 */
void batch_array_create(DecompressChunkState *chunk_state, int nbatches);

void batch_array_destroy(DecompressChunkState *chunk_state);

extern int batch_array_get_free_slot(DecompressChunkState *chunk_state);

inline static DecompressBatchState *
batch_array_get_at(DecompressChunkState *chunk_state, int batch_id)
{
	return (DecompressBatchState *) ((char *) chunk_state->batch_states +
									 chunk_state->n_batch_state_bytes * batch_id);
}

extern void batch_array_free_at(DecompressChunkState *chunk_state, int batch_id);
