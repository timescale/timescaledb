/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <lib/binaryheap.h>

#include "compression/compression.h"
#include "nodes/decompress_chunk/sorted_merge.h"
#include "nodes/decompress_chunk/exec.h"

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
 * Compare the tuples of the datum of two given DecompressSlotNumbers.
 */
static int32
decompress_binaryheap_compare_heap_pos(Datum a, Datum b, void *arg)
{
	DecompressChunkState *chunk_state = (DecompressChunkState *) arg;
	DecompressSlotNumber batchA = DatumGetInt32(a);
	Assert(batchA <= chunk_state->n_batch_states);

	DecompressSlotNumber batchB = DatumGetInt32(b);
	Assert(batchB <= chunk_state->n_batch_states);

	TupleTableSlot *tupleA = chunk_state->batch_states[batchA].decompressed_slot_projected;
	TupleTableSlot *tupleB = chunk_state->batch_states[batchB].decompressed_slot_projected;

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

/*
 * Open the next batch and add the tuple to the heap
 */
static void
decompress_batch_open_next_batch(DecompressChunkState *chunk_state)
{
	while (true)
	{
		TupleTableSlot *subslot = ExecProcNode(linitial(chunk_state->csstate.custom_ps));

		/* All batches are consumed */
		if (TupIsNull(subslot))
		{
			chunk_state->most_recent_batch = INVALID_BATCH_ID;
			return;
		}

		DecompressSlotNumber batch_state_id = decompress_get_free_batch_state_id(chunk_state);
		DecompressBatchState *batch_state = &chunk_state->batch_states[batch_state_id];

		decompress_initialize_batch(chunk_state, batch_state, subslot);

		bool first_tuple_returned = decompress_get_next_tuple_from_batch(chunk_state, batch_state);

		if (!TupIsNull(batch_state->decompressed_slot_projected))
		{
			chunk_state->merge_heap =
				binaryheap_add_unordered_autoresize(chunk_state->merge_heap,
													Int32GetDatum(batch_state_id));

			chunk_state->most_recent_batch = batch_state_id;

			/* In decompress_sorted_merge_get_next_tuple it is determined how many batches
			 * need to be opened currently to perform a sorted merge. This is done by
			 * checking if the first tuple from the last opened batch is larger than the
			 * last returned tuple.
			 *
			 * If a filter removes the first tuple, the first into the heap inserted
			 * tuple from this batch can no longer be used to perform the check.
			 * Therefore, we must continue opening additional batches until the condition
			 * is met.
			 */
			if (first_tuple_returned)
				return;
		}
	}
}

/*
 * Remove the top tuple from the heap (i.e., the tuple we have returned last time) and decompress
 * the next tuple from the batch.
 */
void
decompress_sorted_merge_remove_top_tuple_and_decompress_next(DecompressChunkState *chunk_state)
{
	DecompressSlotNumber i = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));

	DecompressBatchState *batch_state = &chunk_state->batch_states[i];
	Assert(batch_state != NULL);

#ifdef USE_ASSERT_CHECKING
	/* Prepare an assert on the tuple sort between the last returned tuple and the intended next
	 * tuple. The last returned tuple will be changed during this function. So, store a copy for
	 * later comparison. */
	TupleTableSlot *last_returned_tuple =
		MakeSingleTupleTableSlot(batch_state->decompressed_slot_projected->tts_tupleDescriptor,
								 batch_state->decompressed_slot_projected->tts_ops);
	ExecCopySlot(last_returned_tuple, batch_state->decompressed_slot_projected);
#endif

	/* Decompress the next tuple from segment */
	decompress_get_next_tuple_from_batch(chunk_state, batch_state);

	if (TupIsNull(batch_state->decompressed_slot_projected))
	{
		/* Batch is exhausted, recycle batch_state */
		(void) binaryheap_remove_first(chunk_state->merge_heap);
		decompress_set_batch_state_to_unused(chunk_state, i);
	}
	else
	{
		/* Put the next tuple from this batch on the heap */
		binaryheap_replace_first(chunk_state->merge_heap, Int32GetDatum(i));
	}

#ifdef USE_ASSERT_CHECKING
	if (!binaryheap_empty(chunk_state->merge_heap))
	{
		DecompressSlotNumber next_tuple = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));
		DecompressBatchState *next_batch_state = &chunk_state->batch_states[next_tuple];

		/* Assert that the intended sorting is produced. */
		Assert(decompress_binaryheap_compare_slots(last_returned_tuple,
												   next_batch_state->decompressed_slot_projected,
												   chunk_state) >= 0);
	}
	ExecDropSingleTupleTableSlot(last_returned_tuple);
	last_returned_tuple = NULL;
#endif
}

/*
 * Init the binary heap and open the first compressed batch.
 */
void
decompress_sorted_merge_init(DecompressChunkState *chunk_state)
{
	/* Prepare the heap and the batch states */
	chunk_state->merge_heap = binaryheap_allocate(INITIAL_BATCH_CAPACITY,
												  decompress_binaryheap_compare_heap_pos,
												  chunk_state);

	/* Open the first batch */
	decompress_batch_open_next_batch(chunk_state);
}

/*
 * Free the binary heap.
 */
void
decompress_sorted_merge_free(DecompressChunkState *chunk_state)
{
	elog(DEBUG3, "Heap has capacity of %d", chunk_state->merge_heap->bh_space);
	elog(DEBUG3, "Created batch states %d", chunk_state->n_batch_states);
	binaryheap_free(chunk_state->merge_heap);
	chunk_state->merge_heap = NULL;
}

/*
 * Get the next tuple from the binary heap. In addition, further batches are opened
 * and placed on the heep if needed (i.e., the top tuple is from the top batch).
 * This function returns NULL if all tuples from the batches are consumed.
 */
TupleTableSlot *
decompress_sorted_merge_get_next_tuple(DecompressChunkState *chunk_state)
{
	/* All tuples are decompressed and consumed */
	if (binaryheap_empty(chunk_state->merge_heap))
		return NULL;

	/* If the next tuple is from the top batch, open the next batches until
	 * the next batch contains a tuple that is larger than the top tuple from the
	 * heap (i.e., the batch is not the top element of the heap). */
	while (DatumGetInt32(binaryheap_first(chunk_state->merge_heap)) ==
		   chunk_state->most_recent_batch)
	{
		decompress_batch_open_next_batch(chunk_state);
	}

	/* Fetch tuple the top tuple from the heap */
	DecompressSlotNumber slot_number = DatumGetInt32(binaryheap_first(chunk_state->merge_heap));
	TupleTableSlot *decompressed_slot_projected =
		chunk_state->batch_states[slot_number].decompressed_slot_projected;

	Assert(decompressed_slot_projected != NULL);
	Assert(!TupIsNull(decompressed_slot_projected));

	return decompressed_slot_projected;
}
