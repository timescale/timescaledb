/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/bitmapset.h>
#include <lib/binaryheap.h>

#include "compression/compression.h"
#include "nodes/decompress_chunk/batch_array.h"
#include "nodes/decompress_chunk/batch_queue.h"
#include "nodes/decompress_chunk/compressed_batch.h"

typedef struct BatchQueueHeap
{
	BatchQueue bq;
	binaryheap *merge_heap; /* Binary heap of slot indices */
	TupleTableSlot *last_batch_first_tuple;
	SortSupport sortkeys;
	int nkeys;
} BatchQueueHeap;

/*
 * Compare the tuples of two given slots.
 */
static int32
decompress_binaryheap_compare_slots(TupleTableSlot *tupleA, TupleTableSlot *tupleB,
									const SortSupport sortkeys, int nkeys)
{
	Assert(!TupIsNull(tupleA));
	Assert(!TupIsNull(tupleB));

	for (int nkey = 0; nkey < nkeys; nkey++)
	{
		SortSupport sortKey = &sortkeys[nkey];
		Assert(sortKey != NULL);
		AttrNumber attno = sortKey->ssup_attno;

		bool isNullA, isNullB;

		Datum datumA = slot_getattr(tupleA, attno, &isNullA);
		Datum datumB = slot_getattr(tupleB, attno, &isNullB);

		int compare = ApplySortComparator(datumA, isNullA, datumB, isNullB, sortKey);

		if (compare != 0)
		{
			INVERT_COMPARE_RESULT(compare);
			return compare;
		}
	}

	return 0;
}

/*
 * Compare top tuples of two given batch array slots.
 */
static int32
decompress_binaryheap_compare_heap_pos(Datum a, Datum b, void *arg)
{
	BatchQueueHeap *bqh = (BatchQueueHeap *) arg;
	BatchArray *batch_array = &bqh->bq.batch_array;
	int batchA = DatumGetInt32(a);
	Assert(batchA <= batch_array->n_batch_states);

	int batchB = DatumGetInt32(b);
	Assert(batchB <= batch_array->n_batch_states);

	TupleTableSlot *tupleA = batch_array_get_at(batch_array, batchA)->decompressed_scan_slot;
	TupleTableSlot *tupleB = batch_array_get_at(batch_array, batchB)->decompressed_scan_slot;

	return decompress_binaryheap_compare_slots(tupleA, tupleB, bqh->sortkeys, bqh->nkeys);
}

/* Add a new datum to the heap and perform an automatic resizing if needed. In contrast to
 * the binaryheap_add_unordered() function, the capacity of the heap is automatically
 * increased if needed.
 */
static pg_nodiscard binaryheap *
binaryheap_add_unordered_autoresize(binaryheap *heap, Datum d)
{
	/* Resize heap if needed */
	if (heap->bh_size >= heap->bh_space)
	{
		heap->bh_space = heap->bh_space * 2;
		Size new_size = offsetof(binaryheap, bh_nodes) + sizeof(Datum) * heap->bh_space;
		heap = (binaryheap *) repalloc(heap, new_size);
	}

	/* Insert new element */
	binaryheap_add(heap, d);

	return heap;
}

static void
batch_queue_heap_pop(BatchQueue *bq, DecompressContext *dcontext)
{
	BatchQueueHeap *bqh = (BatchQueueHeap *) bq;
	BatchArray *batch_array = &bq->batch_array;

	if (binaryheap_empty(bqh->merge_heap))
	{
		/* Allow this function to be called on the initial empty heap. */
		return;
	}

	const int top_batch_index = DatumGetInt32(binaryheap_first(bqh->merge_heap));
	DecompressBatchState *top_batch = batch_array_get_at(batch_array, top_batch_index);

	compressed_batch_advance(dcontext, top_batch);

	if (TupIsNull(top_batch->decompressed_scan_slot))
	{
		/* Batch is exhausted, recycle batch_state */
		(void) binaryheap_remove_first(bqh->merge_heap);
		batch_array_clear_at(batch_array, top_batch_index);
	}
	else
	{
		/* Put the next tuple from this batch on the heap */
		binaryheap_replace_first(bqh->merge_heap, Int32GetDatum(top_batch_index));
	}
}

static bool
batch_queue_heap_needs_next_batch(BatchQueue *bq)
{
	BatchQueueHeap *bqh = (BatchQueueHeap *) bq;
	BatchArray *batch_array = &bq->batch_array;

	if (binaryheap_empty(bqh->merge_heap))
	{
		return true;
	}

	const int top_batch_index = DatumGetInt32(binaryheap_first(bqh->merge_heap));
	DecompressBatchState *top_batch = batch_array_get_at(batch_array, top_batch_index);

	const int comparison_result =
		decompress_binaryheap_compare_slots(top_batch->decompressed_scan_slot,
											bqh->last_batch_first_tuple,
											bqh->sortkeys,
											bqh->nkeys);

	/*
	 * The invariant we have to preserve is that either:
	 * 1) the current top tuple sorts before the first tuple of the last
	 *    added batch,
	 * 2) the input has ended.
	 * Since the incoming batches arrive in the order of their first tuple,
	 * if this invariant holds, then the current top tuple is found inside the
	 * heap.
	 * If it doesn't hold, the top tuple might be in the next incoming batches,
	 * and we have to continue adding them.
	 */
	return comparison_result <= 0;
}

static void
batch_queue_heap_push_batch(BatchQueue *bq, DecompressContext *dcontext,
							TupleTableSlot *compressed_slot)
{
	BatchQueueHeap *bqh = (BatchQueueHeap *) bq;
	BatchArray *batch_array = &bq->batch_array;

	Assert(!TupIsNull(compressed_slot));

	const int new_batch_index = batch_array_get_unused_slot(batch_array);
	DecompressBatchState *batch_state = batch_array_get_at(batch_array, new_batch_index);

	compressed_batch_set_compressed_tuple(dcontext, batch_state, compressed_slot);
	compressed_batch_save_first_tuple(dcontext, batch_state, bqh->last_batch_first_tuple);

	if (TupIsNull(batch_state->decompressed_scan_slot))
	{
		/* Might happen if there are no tuples in the batch that pass the quals. */
		batch_array_clear_at(batch_array, new_batch_index);
		return;
	}

	bqh->merge_heap = binaryheap_add_unordered_autoresize(bqh->merge_heap, new_batch_index);
}

static TupleTableSlot *
batch_queue_heap_top_tuple(BatchQueue *bq)
{
	BatchQueueHeap *bqh = (BatchQueueHeap *) bq;
	BatchArray *batch_array = &bq->batch_array;

	if (binaryheap_empty(bqh->merge_heap))
	{
		return NULL;
	}

	const int top_batch_index = DatumGetInt32(binaryheap_first(bqh->merge_heap));
	DecompressBatchState *top_batch = batch_array_get_at(batch_array, top_batch_index);
	Assert(!TupIsNull(top_batch->decompressed_scan_slot));
	return top_batch->decompressed_scan_slot;
}

static void
batch_queue_heap_reset(BatchQueue *bq)
{
	BatchQueueHeap *bqh = (BatchQueueHeap *) bq;
	binaryheap_reset(bqh->merge_heap);
}

/*
 * Free the binary heap.
 */
static void
batch_queue_heap_free(BatchQueue *bq)
{
	BatchQueueHeap *bqh = (BatchQueueHeap *) bq;
	BatchArray *batch_array = &bq->batch_array;

	elog(DEBUG3, "heap has capacity of %d", bqh->merge_heap->bh_space);
	elog(DEBUG3, "created batch states %d", batch_array->n_batch_states);
	batch_array_clear_all(&bq->batch_array);
	binaryheap_free(bqh->merge_heap);
	bqh->merge_heap = NULL;
	pfree(bqh->sortkeys);
	ExecDropSingleTupleTableSlot(bqh->last_batch_first_tuple);
	batch_array_destroy(batch_array);
	pfree(bq);
}

const struct BatchQueueFunctions BatchQueueFunctionsHeap = {
	.free = batch_queue_heap_free,
	.needs_next_batch = batch_queue_heap_needs_next_batch,
	.pop = batch_queue_heap_pop,
	.push_batch = batch_queue_heap_push_batch,
	.reset = batch_queue_heap_reset,
	.top_tuple = batch_queue_heap_top_tuple,
};

static SortSupport
build_batch_sorted_merge_info(const List *sortinfo, int *nkeys)
{
	Assert(sortinfo != NULL);

	List *sort_col_idx = linitial(sortinfo);
	List *sort_ops = lsecond(sortinfo);
	List *sort_collations = lthird(sortinfo);
	List *sort_nulls = lfourth(sortinfo);

	*nkeys = list_length(linitial((sortinfo)));

	Assert(list_length(sort_col_idx) == list_length(sort_ops));
	Assert(list_length(sort_ops) == list_length(sort_collations));
	Assert(list_length(sort_collations) == list_length(sort_nulls));
	Assert(*nkeys > 0);

	SortSupportData *sortkeys = palloc0(sizeof(SortSupportData) * *nkeys);

	/* Inspired by nodeMergeAppend.c */
	for (int i = 0; i < *nkeys; i++)
	{
		SortSupportData *sortkey = &sortkeys[i];

		sortkey->ssup_cxt = CurrentMemoryContext;
		sortkey->ssup_collation = list_nth_oid(sort_collations, i);
		sortkey->ssup_nulls_first = list_nth_oid(sort_nulls, i);
		sortkey->ssup_attno = list_nth_oid(sort_col_idx, i);

		/*
		 * It isn't feasible to perform abbreviated key conversion, since
		 * tuples are pulled into mergestate's binary heap as needed.  It
		 * would likely be counter-productive to convert tuples into an
		 * abbreviated representation as they're pulled up, so opt out of that
		 * additional optimization entirely.
		 */
		sortkey->abbreviate = false;
		PrepareSortSupportFromOrderingOp(list_nth_oid(sort_ops, i), sortkey);
	}

	return sortkeys;
}

BatchQueue *
batch_queue_heap_create(int num_compressed_cols, Size batch_memory_context_bytes,
						const List *sortinfo, const TupleDesc result_tupdesc,
						const BatchQueueFunctions *funcs)
{
	BatchQueueHeap *bqh = palloc0(sizeof(BatchQueueHeap));

	batch_array_init(&bqh->bq.batch_array,
					 INITIAL_BATCH_CAPACITY,
					 num_compressed_cols,
					 batch_memory_context_bytes);

	bqh->sortkeys = build_batch_sorted_merge_info(sortinfo, &bqh->nkeys);
	bqh->merge_heap =
		binaryheap_allocate(INITIAL_BATCH_CAPACITY, decompress_binaryheap_compare_heap_pos, bqh);
	bqh->last_batch_first_tuple = MakeSingleTupleTableSlot(result_tupdesc, &TTSOpsVirtual);
	bqh->bq.funcs = funcs;

	return &bqh->bq;
}
