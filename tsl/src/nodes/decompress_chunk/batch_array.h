/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <stdbool.h>

/* The value for an invalid batch id */
#define INVALID_BATCH_ID -1

typedef struct BatchArray
{
	/* Batch states */
	int n_batch_states; /* Number of batch states */
	/*
	 * The batch states. It's void* because they have a variable length
	 * column array, so normal indexing can't be used. Use the batch_array_get_at
	 * accessor instead.
	 */
	void *batch_states;
	int n_batch_state_bytes;
	int n_columns_per_batch;
	Bitmapset *unused_batch_states; /* The unused batch states */
	int batch_memory_context_bytes;
} BatchArray;
/*
 * Create states to hold information for up to n batches
 */
void batch_array_init(BatchArray *array, int nbatches, int ncolumns_per_batch,
					  Size memory_context_block_size_bytes);

void batch_array_destroy(BatchArray *array);

extern int batch_array_get_unused_slot(BatchArray *array);

inline static struct DecompressBatchState *
batch_array_get_at(const BatchArray *array, int batch_index)
{
	/*
	 * Since we're accessing batch states through a "char" pointer, use
	 * "restrict" to tell the compiler that it doesn't alias with anything.
	 * Might be important in hot loops.
	 */
	return (struct DecompressBatchState *) ((char *restrict) array->batch_states +
											array->n_batch_state_bytes * batch_index);
}

extern void batch_array_clear_at(BatchArray *array, int batch_index);
extern void batch_array_clear_all(BatchArray *array);

inline static bool
batch_array_has_active_batches(const BatchArray *array)
{
	return bms_num_members(array->unused_batch_states) != array->n_batch_states;
}
