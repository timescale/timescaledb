/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/bitmapset.h>

#include "compression/compression.h"
#include "nodes/decompress_chunk/batch_array.h"
#include "nodes/decompress_chunk/exec.h"

/*
 * Create states to hold information for up to n batches.
 */
void
batch_array_create(DecompressChunkState *chunk_state, int nbatches)
{
	Assert(nbatches >= 0);

	chunk_state->n_batch_states = nbatches;
	chunk_state->batch_states = palloc0(chunk_state->n_batch_state_bytes * nbatches);

	chunk_state->unused_batch_states = bms_add_range(NULL, 0, nbatches - 1);

	Assert(bms_num_members(chunk_state->unused_batch_states) == chunk_state->n_batch_states);
}

/*
 * Destroy batch states.
 */
void
batch_array_destroy(DecompressChunkState *chunk_state)
{
	for (int i = 0; i < chunk_state->n_batch_states; i++)
	{
		DecompressBatchState *batch_state = batch_array_get_at(chunk_state, i);
		Assert(batch_state != NULL);

		if (batch_state->compressed_slot != NULL)
			ExecDropSingleTupleTableSlot(batch_state->compressed_slot);

		if (batch_state->decompressed_scan_slot != NULL)
			ExecDropSingleTupleTableSlot(batch_state->decompressed_scan_slot);
	}

	pfree(chunk_state->batch_states);
	chunk_state->batch_states = NULL;
}

/*
 * Enhance the capacity of existing batch states.
 */
static void
batch_array_enlarge(DecompressChunkState *chunk_state, int new_number)
{
	Assert(new_number > chunk_state->n_batch_states);

	/* Request additional memory */
	chunk_state->batch_states =
		repalloc(chunk_state->batch_states, chunk_state->n_batch_state_bytes * new_number);

	/* Zero out the tail. The batch states are initialized on first use. */
	memset(((char *) chunk_state->batch_states) +
			   chunk_state->n_batch_state_bytes * chunk_state->n_batch_states,
		   0x0,
		   chunk_state->n_batch_state_bytes * (new_number - chunk_state->n_batch_states));

	/* Register the new states as unused */
	chunk_state->unused_batch_states = bms_add_range(chunk_state->unused_batch_states,
													 chunk_state->n_batch_states,
													 new_number - 1);

	Assert(bms_num_members(chunk_state->unused_batch_states) ==
		   new_number - chunk_state->n_batch_states);

	/* Update number of available batch states */
	chunk_state->n_batch_states = new_number;
}

/*
 * Mark a DecompressBatchState as unused
 */
void
batch_array_free_at(DecompressChunkState *chunk_state, int batch_index)
{
	Assert(batch_index >= 0);
	Assert(batch_index < chunk_state->n_batch_states);

	DecompressBatchState *batch_state = batch_array_get_at(chunk_state, batch_index);

	/* Reset batch state */
	batch_state->total_batch_rows = 0;
	batch_state->next_batch_row = 0;

	if (batch_state->per_batch_context != NULL)
	{
		ExecClearTuple(batch_state->compressed_slot);
		ExecClearTuple(batch_state->decompressed_scan_slot);
		MemoryContextReset(batch_state->per_batch_context);
	}

	chunk_state->unused_batch_states =
		bms_add_member(chunk_state->unused_batch_states, batch_index);
}

/*
 * Get the next free and unused batch state and mark as used
 */
int
batch_array_get_free_slot(DecompressChunkState *chunk_state)
{
	if (bms_is_empty(chunk_state->unused_batch_states))
		batch_array_enlarge(chunk_state, chunk_state->n_batch_states * 2);

	Assert(!bms_is_empty(chunk_state->unused_batch_states));

	int next_free_batch = bms_next_member(chunk_state->unused_batch_states, -1);

	Assert(next_free_batch >= 0);
	Assert(next_free_batch < chunk_state->n_batch_states);
	Assert(TupIsNull(batch_array_get_at(chunk_state, next_free_batch)->decompressed_scan_slot));

	bms_del_member(chunk_state->unused_batch_states, next_free_batch);

	return next_free_batch;
}
