/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include "batch_array.h"
#include "batch_queue_fifo.h"

/*
 * Create a FIFO batch queue.
 *
 * Pass the function struct as argument to ensure it is a pointer to the
 * struct that is inlined at the place this function is called. This is mostly
 * to be able to later Assert() that the struct pointer is pointing to the
 * expected function struct.
 */
BatchQueue *
batch_queue_fifo_create(int num_compressed_cols, const BatchQueueFunctions *funcs)
{
	BatchQueue *bq = palloc0(sizeof(BatchQueue));

	batch_array_init(&bq->batch_array, INITIAL_BATCH_CAPACITY, num_compressed_cols);
	bq->funcs = funcs;

	return bq;
}
