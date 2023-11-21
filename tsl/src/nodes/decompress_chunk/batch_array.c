/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "compression/compression.h"
#include "nodes/decompress_chunk/batch_array.h"
#include "nodes/decompress_chunk/compressed_batch.h"
/*
 * Create states to hold information for up to n batches.
 */
void
batch_array_init(BatchArray *array, int nbatches, int ncolumns_per_batch,
				 Size memory_context_block_size_bytes)
{
	Assert(nbatches >= 0);

	array->n_batch_states = nbatches;
	array->n_columns_per_batch = ncolumns_per_batch;
	array->unused_batch_states = bms_add_range(NULL, 0, nbatches - 1);
	array->batch_memory_context_bytes = memory_context_block_size_bytes;
	array->n_batch_state_bytes =
		sizeof(DecompressBatchState) + sizeof(CompressedColumnValues) * ncolumns_per_batch;
	array->batch_states = palloc0(array->n_batch_state_bytes * nbatches);
	Assert(bms_num_members(array->unused_batch_states) == array->n_batch_states);
}

/*
 * Destroy batch states.
 */
void
batch_array_destroy(BatchArray *array)
{
	for (int i = 0; i < array->n_batch_states; i++)
	{
		DecompressBatchState *batch_state = batch_array_get_at(array, i);
		Assert(batch_state != NULL);

		if (batch_state->compressed_slot != NULL)
			ExecDropSingleTupleTableSlot(batch_state->compressed_slot);

		if (batch_state->decompressed_scan_slot != NULL)
			ExecDropSingleTupleTableSlot(batch_state->decompressed_scan_slot);
	}

	pfree(array->batch_states);
	array->batch_states = NULL;
}

/*
 * Enhance the capacity of existing batch states.
 */
static void
batch_array_enlarge(BatchArray *array, int new_number)
{
	Assert(new_number > array->n_batch_states);

	/* Request additional memory */
	array->batch_states = repalloc(array->batch_states, array->n_batch_state_bytes * new_number);

	/* Zero out the tail. The batch states are initialized on first use. */
	memset(((char *) array->batch_states) + array->n_batch_state_bytes * array->n_batch_states,
		   0x0,
		   array->n_batch_state_bytes * (new_number - array->n_batch_states));

	/* Register the new states as unused */
	array->unused_batch_states =
		bms_add_range(array->unused_batch_states, array->n_batch_states, new_number - 1);

	Assert(bms_num_members(array->unused_batch_states) == new_number - array->n_batch_states);

	/* Update number of available batch states */
	array->n_batch_states = new_number;
}

/*
 * Mark a DecompressBatchState as unused
 */
void
batch_array_clear_at(BatchArray *array, int batch_index)
{
	Assert(batch_index >= 0);
	Assert(batch_index < array->n_batch_states);

	DecompressBatchState *batch_state = batch_array_get_at(array, batch_index);

	/* Reset batch state */
	batch_state->total_batch_rows = 0;
	batch_state->next_batch_row = 0;
	batch_state->vector_qual_result = NULL;

	if (batch_state->per_batch_context != NULL)
	{
		ExecClearTuple(batch_state->compressed_slot);
		ExecClearTuple(batch_state->decompressed_scan_slot);
		MemoryContextReset(batch_state->per_batch_context);
	}

	array->unused_batch_states = bms_add_member(array->unused_batch_states, batch_index);
}

void
batch_array_clear_all(BatchArray *array)
{
	for (int i = 0; i < array->n_batch_states; i++)
		batch_array_clear_at(array, i);

	Assert(bms_num_members(array->unused_batch_states) == array->n_batch_states);
}

/*
 * Get the next free and unused batch state and mark as used
 */
int
batch_array_get_unused_slot(BatchArray *array)
{
	if (bms_is_empty(array->unused_batch_states))
		batch_array_enlarge(array, array->n_batch_states * 2);

	Assert(!bms_is_empty(array->unused_batch_states));

	int next_unused_batch = bms_next_member(array->unused_batch_states, -1);

	Assert(next_unused_batch >= 0);
	Assert(next_unused_batch < array->n_batch_states);
	Assert(TupIsNull(batch_array_get_at(array, next_unused_batch)->decompressed_scan_slot));

	array->unused_batch_states = bms_del_member(array->unused_batch_states, next_unused_batch);

	return next_unused_batch;
}
