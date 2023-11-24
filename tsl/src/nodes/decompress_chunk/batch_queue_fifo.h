/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_BATCH_QUEUE_FIFO_H
#define TIMESCALEDB_BATCH_QUEUE_FIFO_H

#include "batch_queue.h"
#include "compressed_batch.h"

static inline void
batch_queue_fifo_free(BatchQueue *bq)
{
	batch_array_destroy(&bq->batch_array);
	pfree(bq);
}

static inline bool
batch_queue_fifo_needs_next_batch(BatchQueue *bq)
{
	return TupIsNull(batch_array_get_at(&bq->batch_array, 0)->decompressed_scan_slot);
}

static inline void
batch_queue_fifo_pop(BatchQueue *bq, DecompressContext *dcontext)
{
	DecompressBatchState *batch_state = batch_array_get_at(&bq->batch_array, 0);
	if (TupIsNull(batch_state->decompressed_scan_slot))
	{
		/* Allow this function to be called on the initial empty queue. */
		return;
	}

	compressed_batch_advance(dcontext, batch_state);
}

static inline void
batch_queue_fifo_push_batch(BatchQueue *bq, DecompressContext *dcontext,
							TupleTableSlot *compressed_slot)
{
	BatchArray *batch_array = &bq->batch_array;
	DecompressBatchState *batch_state = batch_array_get_at(batch_array, 0);
	Assert(TupIsNull(batch_array_get_at(batch_array, 0)->decompressed_scan_slot));
	compressed_batch_set_compressed_tuple(dcontext, batch_state, compressed_slot);
	compressed_batch_advance(dcontext, batch_state);
}

static inline void
batch_queue_fifo_reset(BatchQueue *bq)
{
	batch_array_clear_all(&bq->batch_array);
}

static inline TupleTableSlot *
batch_queue_fifo_top_tuple(BatchQueue *bq)
{
	return batch_array_get_at(&bq->batch_array, 0)->decompressed_scan_slot;
}

static const struct BatchQueueFunctions BatchQueueFunctionsFifo = {
	.free = batch_queue_fifo_free,
	.needs_next_batch = batch_queue_fifo_needs_next_batch,
	.pop = batch_queue_fifo_pop,
	.push_batch = batch_queue_fifo_push_batch,
	.reset = batch_queue_fifo_reset,
	.top_tuple = batch_queue_fifo_top_tuple,
};

extern BatchQueue *batch_queue_fifo_create(int num_compressed_cols, Size batch_memory_context_bytes,
										   const BatchQueueFunctions *funcs);

#endif /* TIMESCALEDB_BATCH_QUEUE_FIFO_H */
