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
#include "nodes/decompress_chunk/batch_queue_heap.h"
#include "nodes/decompress_chunk/exec.h"

/* Initial amount of batch states */
#define INITIAL_BATCH_CAPACITY 16

/*
 * Compare the tuples of two given slots.
 */
static int32
decompress_binaryheap_compare_slots(TupleTableSlot *tupleA, TupleTableSlot *tupleB,
									DecompressChunkState *chunk_state)
{
	Assert(!TupIsNull(tupleA));
	Assert(!TupIsNull(tupleB));
	Assert(chunk_state != NULL);

	for (int nkey = 0; nkey < chunk_state->n_sortkeys; nkey++)
	{
		SortSupportData *sortKey = &chunk_state->sortkeys[nkey];
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
	DecompressChunkState *chunk_state = (DecompressChunkState *) arg;
	int batchA = DatumGetInt32(a);
	Assert(batchA <= chunk_state->n_batch_states);

	int batchB = DatumGetInt32(b);
	Assert(batchB <= chunk_state->n_batch_states);

	TupleTableSlot *tupleA = batch_array_get_at(chunk_state, batchA)->decompressed_scan_slot;
	TupleTableSlot *tupleB = batch_array_get_at(chunk_state, batchB)->decompressed_scan_slot;

	return decompress_binaryheap_compare_slots(tupleA, tupleB, chunk_state);
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

void
batch_queue_heap_pop(DecompressChunkState *chunk_state)
{
	if (binaryheap_empty(chunk_state->merge_heap))
	{
		/* Allow this function to be called on the initial empty heap. */
		return;
	}

	const int top_batch_index = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));
	DecompressBatchState *top_batch = batch_array_get_at(chunk_state, top_batch_index);

	compressed_batch_advance(chunk_state, top_batch);

	if (TupIsNull(top_batch->decompressed_scan_slot))
	{
		/* Batch is exhausted, recycle batch_state */
		(void) binaryheap_remove_first(chunk_state->merge_heap);
		batch_array_free_at(chunk_state, top_batch_index);
	}
	else
	{
		/* Put the next tuple from this batch on the heap */
		binaryheap_replace_first(chunk_state->merge_heap, Int32GetDatum(top_batch_index));
	}
}

bool
batch_queue_heap_needs_next_batch(DecompressChunkState *chunk_state)
{
	if (binaryheap_empty(chunk_state->merge_heap))
	{
		return true;
	}

	const int top_batch_index = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));
	DecompressBatchState *top_batch = batch_array_get_at(chunk_state, top_batch_index);

	const int comparison_result =
		decompress_binaryheap_compare_slots(top_batch->decompressed_scan_slot,
											chunk_state->last_batch_first_tuple,
											chunk_state);

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

void
batch_queue_heap_push_batch(DecompressChunkState *chunk_state, TupleTableSlot *compressed_slot)
{
	Assert(!TupIsNull(compressed_slot));

	const int new_batch_index = batch_array_get_free_slot(chunk_state);
	DecompressBatchState *batch_state = batch_array_get_at(chunk_state, new_batch_index);

	compressed_batch_set_compressed_tuple(chunk_state, batch_state, compressed_slot);
	compressed_batch_save_first_tuple(chunk_state,
									  batch_state,
									  chunk_state->last_batch_first_tuple);

	if (TupIsNull(batch_state->decompressed_scan_slot))
	{
		/* Might happen if there are no tuples in the batch that pass the quals. */
		batch_array_free_at(chunk_state, new_batch_index);
		return;
	}

	chunk_state->merge_heap =
		binaryheap_add_unordered_autoresize(chunk_state->merge_heap, new_batch_index);
}

TupleTableSlot *
batch_queue_heap_top_tuple(DecompressChunkState *chunk_state)
{
	if (binaryheap_empty(chunk_state->merge_heap))
	{
		return NULL;
	}

	const int top_batch_index = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));
	DecompressBatchState *top_batch = batch_array_get_at(chunk_state, top_batch_index);
	Assert(!TupIsNull(top_batch->decompressed_scan_slot));
	return top_batch->decompressed_scan_slot;
}

void
batch_queue_heap_create(DecompressChunkState *chunk_state)
{
	batch_array_create(chunk_state, INITIAL_BATCH_CAPACITY);

	chunk_state->merge_heap = binaryheap_allocate(INITIAL_BATCH_CAPACITY,
												  decompress_binaryheap_compare_heap_pos,
												  chunk_state);

	Assert(chunk_state->last_batch_first_tuple == NULL);
	chunk_state->last_batch_first_tuple =
		MakeSingleTupleTableSlot(chunk_state->csstate.ss.ss_ScanTupleSlot->tts_tupleDescriptor,
								 &TTSOpsVirtual);
}

void
batch_queue_heap_reset(DecompressChunkState *chunk_state)
{
	binaryheap_reset(chunk_state->merge_heap);
}

/*
 * Free the binary heap.
 */
void
batch_queue_heap_free(DecompressChunkState *chunk_state)
{
	elog(DEBUG3, "Heap has capacity of %d", chunk_state->merge_heap->bh_space);
	elog(DEBUG3, "Created batch states %d", chunk_state->n_batch_states);
	binaryheap_free(chunk_state->merge_heap);
	chunk_state->merge_heap = NULL;
	ExecDropSingleTupleTableSlot(chunk_state->last_batch_first_tuple);

	batch_array_destroy(chunk_state);
}
