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

typedef struct
{
	Datum value;
	bool null;
} HeapEntryColumn;

typedef struct BatchQueueHeap
{
	BatchQueue queue;
	binaryheap *merge_heap; /* Binary heap of slot indices */

	/*
	 * Requested sort order of the heap.
	 */
	int nkeys;
	SortSupport sortkeys;

	/*
	 * This is the actual entries of the heap we're going to compare. We're using
	 * these minimal structures for better memory locality instead of addressing
	 * the entire compressed batches. Would be even better to put them into the
	 * heap inline, but unfortunately the Postgres binary heap doesn't support
	 * this.
	 *
	 * For each batch, we have nkeys of HeapEntryColumn values, which contain
	 * the latest decompressed values.
	 */
	HeapEntryColumn *heap_entries;

	/*
	 * We use this to check when we have to ask for the next input batch.
	 */
	TupleTableSlot *last_batch_first_tuple_slot;
	HeapEntryColumn *last_batch_first_tuple_entry;
} BatchQueueHeap;

/*
 * Compare heap entries for two batches. This function is used for comparing the
 * first tuple of the last batch to the current top tuple, so it is only called
 * once per input tuple, and optimized specializations for it are less important.
 */
static int32
compare_entries(HeapEntryColumn *entryA, HeapEntryColumn *entryB, const SortSupport sortkeys,
				int nkeys)
{
	for (int key = 0; key < nkeys; key++)
	{
		int compare = ApplySortComparator(entryA[key].value,
										  entryA[key].null,
										  entryB[key].value,
										  entryB[key].null,
										  &sortkeys[key]);

		if (compare != 0)
		{
			INVERT_COMPARE_RESULT(compare);
			return compare;
		}
	}

	return 0;
}

/*
 * Compare top tuples of two given batch array slots. We support specializations
 * for comparison of the first tuple, like tuplesort.
 */
static pg_attribute_always_inline int32
compare_heap_pos_impl(Datum a, Datum b, void *arg,
					  int32 (*apply_first_datum_comparator)(Datum, bool, Datum, bool, SortSupport))
{
	BatchQueueHeap *queue = (BatchQueueHeap *) arg;
	PG_USED_FOR_ASSERTS_ONLY BatchArray *batch_array = &queue->queue.batch_array;
	int batchA = DatumGetInt32(a);
	Assert(batchA <= batch_array->n_batch_states);

	int batchB = DatumGetInt32(b);
	Assert(batchB <= batch_array->n_batch_states);

	const int nkeys = queue->nkeys;
	SortSupport sortkeys = queue->sortkeys;

	HeapEntryColumn *entryA = &queue->heap_entries[batchA * nkeys];
	HeapEntryColumn *entryB = &queue->heap_entries[batchB * nkeys];

	int compare = apply_first_datum_comparator(entryA[0].value,
											   entryA[0].null,
											   entryB[0].value,
											   entryB[0].null,
											   &sortkeys[0]);
	if (compare != 0)
	{
		INVERT_COMPARE_RESULT(compare);
		return compare;
	}

	for (int key = 1; key < nkeys; key++)
	{
		int compare = ApplySortComparator(entryA[key].value,
										  entryA[key].null,
										  entryB[key].value,
										  entryB[key].null,
										  &sortkeys[key]);

		if (compare != 0)
		{
			INVERT_COMPARE_RESULT(compare);
			return compare;
		}
	}

	return 0;
}

static int32
compare_heap_pos_generic(Datum a, Datum b, void *arg)
{
	return compare_heap_pos_impl(a, b, arg, ApplySortComparator);
}

#if PG15_GE
static int32
compare_heap_pos_int32(Datum a, Datum b, void *arg)
{
	return compare_heap_pos_impl(a, b, arg, ApplyInt32SortComparator);
}

#if SIZEOF_DATUM >= 8
static int32
compare_heap_pos_signed(Datum a, Datum b, void *arg)
{
	return compare_heap_pos_impl(a, b, arg, ApplySignedSortComparator);
}
#endif
#endif

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
	BatchQueueHeap *queue = (BatchQueueHeap *) bq;
	BatchArray *batch_array = &bq->batch_array;

	if (binaryheap_empty(queue->merge_heap))
	{
		/* Allow this function to be called on the initial empty heap. */
		return;
	}

	const int top_batch_index = DatumGetInt32(binaryheap_first(queue->merge_heap));
	DecompressBatchState *top_batch = batch_array_get_at(batch_array, top_batch_index);

	compressed_batch_advance(dcontext, top_batch);

	TupleTableSlot *top_tuple = compressed_batch_current_tuple(top_batch);
	if (TupIsNull(top_tuple))
	{
		/* Batch is exhausted, recycle batch_state */
		(void) binaryheap_remove_first(queue->merge_heap);
		batch_array_clear_at(batch_array, top_batch_index);
	}
	else
	{
		/*
		 * Update the heap entries for this batch with the current decompressed
		 * tuple values.
		 */
		for (int key = 0; key < queue->nkeys; key++)
		{
			SortSupport sortKey = &queue->sortkeys[key];
			queue->heap_entries[top_batch_index * queue->nkeys + key].value =
				slot_getattr(top_tuple,
							 sortKey->ssup_attno,
							 &queue->heap_entries[top_batch_index * queue->nkeys + key].null);
		}

		/* Place this batch on the heap according to its new decompressed tuple. */
		binaryheap_replace_first(queue->merge_heap, Int32GetDatum(top_batch_index));
	}
}

static bool
batch_queue_heap_needs_next_batch(BatchQueue *_queue)
{
	BatchQueueHeap *queue = (BatchQueueHeap *) _queue;

	if (binaryheap_empty(queue->merge_heap))
	{
		return true;
	}

	const int top_batch_index = DatumGetInt32(binaryheap_first(queue->merge_heap));
	const int comparison_result =
		compare_entries(&queue->heap_entries[queue->nkeys * top_batch_index],
						queue->last_batch_first_tuple_entry,
						queue->sortkeys,
						queue->nkeys);

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
batch_queue_heap_push_batch(BatchQueue *_queue, DecompressContext *dcontext,
							TupleTableSlot *compressed_slot)
{
	BatchQueueHeap *queue = (BatchQueueHeap *) _queue;
	BatchArray *batch_array = &queue->queue.batch_array;

	Assert(!TupIsNull(compressed_slot));

	const int old_size = batch_array->n_batch_states;
	const int new_batch_index = batch_array_get_unused_slot(batch_array);
	if (batch_array->n_batch_states != old_size)
	{
		queue->heap_entries =
			repalloc(queue->heap_entries,
					 sizeof(HeapEntryColumn) * queue->nkeys * batch_array->n_batch_states);
	}
	DecompressBatchState *batch_state = batch_array_get_at(batch_array, new_batch_index);

	compressed_batch_set_compressed_tuple(dcontext, batch_state, compressed_slot);
	compressed_batch_save_first_tuple(dcontext, batch_state, queue->last_batch_first_tuple_slot);

	/*
	 * Update the heap entries for the first tuple of the last batch.
	 */
	for (int key = 0; key < queue->nkeys; key++)
	{
		SortSupport sortKey = &queue->sortkeys[key];
		queue->last_batch_first_tuple_entry[key].value =
			slot_getattr(queue->last_batch_first_tuple_slot,
						 sortKey->ssup_attno,
						 &queue->last_batch_first_tuple_entry[key].null);
	}

	TupleTableSlot *current_tuple = compressed_batch_current_tuple(batch_state);
	if (TupIsNull(current_tuple))
	{
		/* Might happen if there are no tuples in the batch that pass the quals. */
		batch_array_clear_at(batch_array, new_batch_index);
		return;
	}

	/*
	 * Update the heap entries for this batch with the first decompressed tuple
	 * values.
	 */
	for (int key = 0; key < queue->nkeys; key++)
	{
		SortSupport sortKey = &queue->sortkeys[key];
		queue->heap_entries[new_batch_index * queue->nkeys + key].value =
			slot_getattr(current_tuple,
						 sortKey->ssup_attno,
						 &queue->heap_entries[new_batch_index * queue->nkeys + key].null);
	}

	/*
	 * Put the batch on the heap.
	 */
	queue->merge_heap = binaryheap_add_unordered_autoresize(queue->merge_heap, new_batch_index);
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
	TupleTableSlot *top_tuple = compressed_batch_current_tuple(top_batch);
	Assert(!TupIsNull(top_tuple));
	return top_tuple;
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
batch_queue_heap_free(BatchQueue *_queue)
{
	BatchQueueHeap *queue = (BatchQueueHeap *) _queue;
	BatchArray *batch_array = &queue->queue.batch_array;

	elog(DEBUG3, "heap has capacity of %d", queue->merge_heap->bh_space);
	elog(DEBUG3, "created batch states %d", batch_array->n_batch_states);
	batch_array_clear_all(batch_array);
	pfree(queue->heap_entries);
	binaryheap_free(queue->merge_heap);
	queue->merge_heap = NULL;
	pfree(queue->sortkeys);
	ExecDropSingleTupleTableSlot(queue->last_batch_first_tuple_slot);
	pfree(queue->last_batch_first_tuple_entry);
	batch_array_destroy(batch_array);
	pfree(queue);
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
batch_queue_heap_create(int num_compressed_cols, const List *sortinfo,
						const TupleDesc result_tupdesc, const BatchQueueFunctions *funcs)
{
	BatchQueueHeap *queue = palloc0(sizeof(BatchQueueHeap));

	batch_array_init(&queue->queue.batch_array, INITIAL_BATCH_CAPACITY, num_compressed_cols);

	queue->sortkeys = build_batch_sorted_merge_info(sortinfo, &queue->nkeys);

	queue->heap_entries = palloc(sizeof(HeapEntryColumn) * queue->nkeys * INITIAL_BATCH_CAPACITY);

	/*
	 * Choose a specialization for faster comparison of the first column. This is
	 * the approach that tuplesort uses, see e.g. qsort_tuple_signed().
	 * The ssup_datum_unsigned_cmp is used only for abbreviated keys which the
	 * batch sorted merge doesn't use, so we use a generic comparator in this
	 * case.
	 */
	binaryheap_comparator comparator = compare_heap_pos_generic;
#if PG15_GE
	if (queue->sortkeys[0].comparator == ssup_datum_int32_cmp)
	{
		comparator = compare_heap_pos_int32;
	}
#if SIZEOF_DATUM >= 8
	else if (queue->sortkeys[0].comparator == ssup_datum_signed_cmp)
	{
		comparator = compare_heap_pos_signed;
	}
#endif
#endif

	queue->merge_heap = binaryheap_allocate(INITIAL_BATCH_CAPACITY, comparator, queue);
	queue->last_batch_first_tuple_slot = MakeSingleTupleTableSlot(result_tupdesc, &TTSOpsVirtual);
	queue->last_batch_first_tuple_entry = palloc(sizeof(HeapEntryColumn) * queue->nkeys);
	queue->queue.funcs = funcs;

	return &queue->queue;
}
