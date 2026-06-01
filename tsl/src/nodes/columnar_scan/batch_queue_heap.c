/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <lib/binaryheap.h>
#include <nodes/bitmapset.h>
#include <utils/datum.h>
#include <utils/typcache.h>

#include "compression/compression.h"
#include "nodes/columnar_scan/batch_array.h"
#include "nodes/columnar_scan/batch_queue_heap.h"
#include "nodes/columnar_scan/compressed_batch.h"

typedef struct
{
	Datum value;
	bool null;
} HeapEntryColumn;

typedef struct SegmentEntryColumn
{
	Datum value;
	bool null;
	bool typbyval;
	int16 typlen;
} SegmentEntryColumn;

typedef struct BatchQueueHeap
{
	BatchQueue queue;
	binaryheap *merge_heap; /* Binary heap of slot indices */

	/*
	 * Requested sort order of the heap.
	 */
	int nsegkeys;  /* segmentby keys only */
	int nsortkeys; /* heap sort keys only */
	SortSupport sortkeys;

	/*
	 * This is the actual entries of the heap we're going to compare. We're using
	 * these minimal structures for better memory locality instead of addressing
	 * the entire compressed batches. Would be even better to put them into the
	 * heap inline, but unfortunately the Postgres binary heap doesn't support
	 * this.
	 *
	 * For each batch, we have nsortkeys of HeapEntryColumn values, which contain
	 * the latest decompressed values.
	 */
	HeapEntryColumn *heap_entries;

	/*
	 * We use this to check when we have to ask for the next input batch.
	 */
	TupleTableSlot *last_batch_first_tuple_slot;
	HeapEntryColumn *last_batch_first_tuple_entry;

	/* Comparing segmentby entries:
	 * we keep current segmentby entry and compare it
	 * with segmentby of each new batch.
	 */
	SegmentEntryColumn *current_segmentby_entry;
	bool segmentby_entries_initialized;
	/* Index of a batch with new segmentby entry */
	int new_segment_batch_index;

} BatchQueueHeap;

static void
reset_segment(BatchQueueHeap *queue, TupleTableSlot *slot)
{
	TypeCacheEntry *tce;
	int tc_flags = TYPECACHE_EQ_OPR;
	TupleDesc tupledesc = slot->tts_tupleDescriptor;

	Datum value;
	bool isnull;
	for (int key = 0; key < queue->nsegkeys; key++)
	{
		SortSupport sortKey = &queue->sortkeys[key];
		const AttrNumber attr = AttrNumberGetAttrOffset(sortKey->ssup_attno);

		tce = lookup_type_cache(TupleDescAttr(tupledesc, attr)->atttypid, tc_flags);
		queue->current_segmentby_entry[key].typbyval = tce->typbyval;
		queue->current_segmentby_entry[key].typlen = tce->typlen;

		value = slot_getattr(slot, AttrOffsetGetAttrNumber(attr), &isnull);
		queue->current_segmentby_entry[key].null = isnull;
		if (!isnull)
		{
			queue->current_segmentby_entry[key].value =
				datumCopy(value,
						  queue->current_segmentby_entry[key].typbyval,
						  queue->current_segmentby_entry[key].typlen);
		}
	}
}

static bool
is_new_segment(BatchQueueHeap *queue, TupleTableSlot *slot)
{
	/* groups not initialized yet */
	if (!queue->segmentby_entries_initialized)
	{
		queue->segmentby_entries_initialized = true;
		reset_segment(queue, slot);
		return false;
	}

	Datum value;
	bool isnull;
	for (int key = 0; key < queue->nsegkeys; key++)
	{
		SortSupport sortKey = &queue->sortkeys[key];
		const AttrNumber attr = AttrNumberGetAttrOffset(sortKey->ssup_attno);
		value = slot_getattr(slot, AttrOffsetGetAttrNumber(attr), &isnull);
		if (isnull && queue->current_segmentby_entry[key].null)
		{
			continue;
		}
		if (isnull != queue->current_segmentby_entry[key].null)
		{
			return true;
		}
		int compare = ApplySortComparator(queue->current_segmentby_entry[key].value,
										  queue->current_segmentby_entry[key].null,
										  value,
										  isnull,
										  sortKey);

		if (compare)
		{
			return true;
		}
	}

	return false;
}

/*
 * Compare heap entries for two batches. This function is used for comparing the
 * first tuple of the last batch to the current top tuple, so it is only called
 * once per input tuple, and optimized specializations for it are less important.
 */
static int32
compare_entries(HeapEntryColumn *entryA, HeapEntryColumn *entryB, const SortSupport sortkeys,
				int nsortkeys, int offset)
{
	for (int key = 0; key < nsortkeys; key++)
	{
		int compare = ApplySortComparator(entryA[key].value,
										  entryA[key].null,
										  entryB[key].value,
										  entryB[key].null,
										  &sortkeys[key + offset]);

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
compare_heap_pos(Datum a, Datum b, void *arg)
{
	BatchQueueHeap *queue = (BatchQueueHeap *) arg;
	PG_USED_FOR_ASSERTS_ONLY BatchArray *batch_array = &queue->queue.batch_array;
	int batchA = DatumGetInt32(a);
	Assert(batchA <= batch_array->n_batch_states);

	int batchB = DatumGetInt32(b);
	Assert(batchB <= batch_array->n_batch_states);

	/* B > A always */
	if (batchB == queue->new_segment_batch_index)
	{
		return 1;
	}
	/* A > B always */
	else if (batchA == queue->new_segment_batch_index)
	{
		return -1;
	}

	const int nsortkeys = queue->nsortkeys;
	SortSupport sortkeys = queue->sortkeys;

	HeapEntryColumn *entryA = &queue->heap_entries[batchA * nsortkeys];
	HeapEntryColumn *entryB = &queue->heap_entries[batchB * nsortkeys];

	int compare = ApplySortComparator(entryA[0].value,
									  entryA[0].null,
									  entryB[0].value,
									  entryB[0].null,
									  &sortkeys[queue->nsegkeys]);
	if (compare != 0)
	{
		INVERT_COMPARE_RESULT(compare);
		return compare;
	}

	for (int key = 1; key < nsortkeys; key++)
	{
		int compare = ApplySortComparator(entryA[key].value,
										  entryA[key].null,
										  entryB[key].value,
										  entryB[key].null,
										  &sortkeys[key + queue->nsegkeys]);

		if (compare != 0)
		{
			INVERT_COMPARE_RESULT(compare);
			return compare;
		}
	}

	return 0;
}

/* Add a new datum to the heap and perform an automatic resizing if needed. In contrast to
 * the binaryheap_add_unordered() function, the capacity of the heap is automatically
 * increased if needed.
 */
pg_nodiscard static binaryheap *
binaryheap_add_unordered_autoresize(binaryheap *heap, Datum d)
{
	/* Resize heap if needed */
	if (heap->bh_size >= heap->bh_space)
	{
		heap->bh_space = heap->bh_space * 2;
		Size new_size = offsetof(binaryheap, bh_nodes) + (sizeof(Datum) * heap->bh_space);
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
		queue->new_segment_batch_index = -1;
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

		/* the only batch left is the new segment batch: reset to current segment batch.*/
		if (queue->new_segment_batch_index >= 0 && binaryheap_size(queue->merge_heap) <= 1)
		{
			queue->new_segment_batch_index = -1;
		}
	}
	else
	{
		/*
		 * Update the heap entries for this batch with the current decompressed
		 * tuple values.
		 */
		for (int key = 0; key < queue->nsortkeys; key++)
		{
			SortSupport sortKey = &queue->sortkeys[key + queue->nsegkeys];
			const AttrNumber attr = AttrNumberGetAttrOffset(sortKey->ssup_attno);
			/*
			 * We're working with virtual tuple slots so no need for slot_getattr().
			 */
			Assert(TTS_IS_VIRTUAL(top_tuple));
			queue->heap_entries[(top_batch_index * queue->nsortkeys) + key].value =
				top_tuple->tts_values[attr];
			queue->heap_entries[(top_batch_index * queue->nsortkeys) + key].null =
				top_tuple->tts_isnull[attr];
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
	if (queue->new_segment_batch_index >= 0)
	{
		return false;
	}

	const int top_batch_index = DatumGetInt32(binaryheap_first(queue->merge_heap));
	const int comparison_result =
		compare_entries(&queue->heap_entries[queue->nsortkeys * top_batch_index],
						queue->last_batch_first_tuple_entry,
						queue->sortkeys,
						queue->nsortkeys,
						/* offset = */ queue->nsegkeys);

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
					 sizeof(HeapEntryColumn) * queue->nsortkeys * batch_array->n_batch_states);
	}
	DecompressBatchState *batch_state = batch_array_get_at(batch_array, new_batch_index);

	compressed_batch_set_compressed_tuple(dcontext, batch_state, compressed_slot);
	compressed_batch_save_first_tuple(dcontext, batch_state, queue->last_batch_first_tuple_slot);
	/*
	 * We're working with virtual tuple slots so no need for slot_getattr().
	 */
	Assert(TTS_IS_VIRTUAL(queue->last_batch_first_tuple_slot));

	/* Check if we need to save or change the current segment */
	if (queue->nsegkeys > 0)
	{
		if (is_new_segment(queue, queue->last_batch_first_tuple_slot))
		{
			queue->new_segment_batch_index = new_batch_index;
			reset_segment(queue, queue->last_batch_first_tuple_slot);
		}
	}

	/*
	 * Update the heap entries for the first tuple of the last batch.
	 */
	for (int key = 0; key < queue->nsortkeys; key++)
	{
		SortSupport sortKey = &queue->sortkeys[key + queue->nsegkeys];
		const AttrNumber attr = AttrNumberGetAttrOffset(sortKey->ssup_attno);
		queue->last_batch_first_tuple_entry[key].value =
			queue->last_batch_first_tuple_slot->tts_values[attr];
		queue->last_batch_first_tuple_entry[key].null =
			queue->last_batch_first_tuple_slot->tts_isnull[attr];
	}

	TupleTableSlot *current_tuple = compressed_batch_current_tuple(batch_state);
	if (TupIsNull(current_tuple))
	{
		/* Might happen if there are no tuples in the batch that pass the quals. */
		batch_array_clear_at(batch_array, new_batch_index);
		queue->new_segment_batch_index = -1;
		return;
	}

	/*
	 * Update the heap entries for this batch with the first decompressed tuple
	 * values.
	 */
	for (int key = 0; key < queue->nsortkeys; key++)
	{
		SortSupport sortKey = &queue->sortkeys[key + queue->nsegkeys];
		const AttrNumber attr = AttrNumberGetAttrOffset(sortKey->ssup_attno);
		/*
		 * We're working with virtual tuple slots so no need for slot_getattr().
		 */
		Assert(TTS_IS_VIRTUAL(current_tuple));
		queue->heap_entries[(new_batch_index * queue->nsortkeys) + key].value =
			current_tuple->tts_values[attr];
		queue->heap_entries[(new_batch_index * queue->nsortkeys) + key].null =
			current_tuple->tts_isnull[attr];
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
batch_queue_heap_segmentby_cleanup(BatchQueueHeap *queue)
{
	queue->new_segment_batch_index = -1;
	if (queue->current_segmentby_entry)
	{
		pfree(queue->current_segmentby_entry);
		queue->current_segmentby_entry = NULL;
	}
}

static void
batch_queue_heap_reset(BatchQueue *bq)
{
	BatchQueueHeap *bqh = (BatchQueueHeap *) bq;
	bqh->new_segment_batch_index = -1;
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
	batch_queue_heap_segmentby_cleanup(queue);
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
build_batch_sorted_merge_info(const List *sortinfo, int *nsegkeys, int *nsortkeys)
{
	Assert(sortinfo != NULL);

	List *sort_col_idx = linitial(sortinfo);
	List *sort_ops = lsecond(sortinfo);
	List *sort_collations = lthird(sortinfo);
	List *sort_nulls = lfourth(sortinfo);
	List *sort_nsegkeys = lfifth(sortinfo);

	int nkeys = list_length(linitial((sortinfo)));
	*nsegkeys = linitial_int(sort_nsegkeys);
	*nsortkeys = nkeys - *nsegkeys;

	Assert(list_length(sort_col_idx) == list_length(sort_ops));
	Assert(list_length(sort_ops) == list_length(sort_collations));
	Assert(list_length(sort_collations) == list_length(sort_nulls));
	Assert(nkeys > 0);

	SortSupportData *sortkeys = palloc0(sizeof(SortSupportData) * nkeys);

	/* Inspired by nodeMergeAppend.c */
	for (int i = 0; i < nkeys; i++)
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

	queue->sortkeys = build_batch_sorted_merge_info(sortinfo, &queue->nsegkeys, &queue->nsortkeys);

	queue->heap_entries =
		palloc(sizeof(HeapEntryColumn) * queue->nsortkeys * INITIAL_BATCH_CAPACITY);

	queue->merge_heap = binaryheap_allocate(INITIAL_BATCH_CAPACITY, compare_heap_pos, queue);
	queue->last_batch_first_tuple_slot = MakeSingleTupleTableSlot(result_tupdesc, &TTSOpsVirtual);
	queue->last_batch_first_tuple_entry = palloc(sizeof(HeapEntryColumn) * queue->nsortkeys);

	/* Allocate segmentby entries for comparison if needed */
	if (queue->nsegkeys > 0)
	{
		queue->current_segmentby_entry = palloc(sizeof(SegmentEntryColumn) * queue->nsegkeys);
	}
	else
	{
		queue->current_segmentby_entry = NULL;
	}
	queue->new_segment_batch_index = -1;
	queue->segmentby_entries_initialized = false;

	queue->queue.funcs = funcs;

	return &queue->queue;
}
