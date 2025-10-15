/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_BATCH_QUEUE_H
#define TIMESCALEDB_BATCH_QUEUE_H

#include <postgres.h>
#include <executor/tuptable.h>

#include "decompress_context.h"

/* Initial amount of batch states */
#define INITIAL_BATCH_CAPACITY 16

struct BatchQueue;

typedef struct BatchQueueFunctions
{
	void (*free)(struct BatchQueue *);
	bool (*needs_next_batch)(struct BatchQueue *);
	void (*pop)(struct BatchQueue *, DecompressContext *);
	void (*push_batch)(struct BatchQueue *, DecompressContext *, TupleTableSlot *);
	void (*reset)(struct BatchQueue *);
	TupleTableSlot *(*top_tuple)(struct BatchQueue *);
} BatchQueueFunctions;

typedef struct BatchQueue
{
	BatchArray batch_array;
	const BatchQueueFunctions *funcs;
} BatchQueue;

#include "batch_queue_fifo.h"
#include "batch_queue_heap.h"

#endif /* TIMESCALEDB_BATCH_QUEUE_H */
