/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include "compression/compression.h"
#include "nodes/decompress_chunk/decompress_context.h"

typedef struct ArrowArray ArrowArray;

/* How to obtain the decompressed datum for individual row. */
typedef enum
{
	DT_ArrowTextDict = -4,
	DT_ArrowText = -3,
	DT_Default = -2,
	DT_Iterator = -1,
	DT_Invalid = 0,
	/*
	 * Any positive number is also valid for the decompression type. It means
	 * arrow array of a fixed-size by-value type, with size given by the number.
	 */
} DecompressionType;

typedef struct CompressedColumnValues
{
	/* How to obtain the decompressed datum for individual row. */
	DecompressionType decompression_type;

	/* Where to put the decompressed datum. */
	Datum *output_value;
	bool *output_isnull;

	/*
	 * The flattened source buffers for getting the decompressed datum.
	 * Depending on decompression type, they are as follows:
	 * iterator:        iterator
	 * arrow fixed:     validity, value
	 * arrow text:      validity, uint32* offsets, void* bodies
	 * arrow dict text: validity, uint32* dict offsets, void* dict bodies, int16* indices
	 */
	const void *restrict buffers[4];

	/*
	 * The source arrow array, if any. We don't use it for building the
	 * individual rows, and use the flattened buffers instead to lessen the
	 * amount of indirections. However, it is used for vectorized filters.
	 */
	ArrowArray *arrow;
} CompressedColumnValues;

/*
 * All the information needed to decompress a batch.
 */
typedef struct DecompressBatchState
{
	VirtualTupleTableSlot decompressed_scan_slot_data; /* A slot for the decompressed data */
	/*
	 * Compressed target slot. We have to keep a local copy when doing batch
	 * sorted merge, because the segmentby column values might reference the
	 * original tuple, and a batch outlives its source tuple.
	 */
	TupleTableSlot *compressed_slot;
	uint16 total_batch_rows;
	uint16 next_batch_row;
	Size block_size_bytes; /* Block size to use for memory context */
	MemoryContext per_batch_context;

	/*
	 * Arrow-style bitmap that says whether the vector quals passed for a given
	 * row. Indexed same as arrow arrays, w/o accounting for the reverse scan
	 * direction. Initialized to all ones, i.e. all rows pass.
	 */
	uint64 *restrict vector_qual_result;

	CompressedColumnValues compressed_columns[FLEXIBLE_ARRAY_MEMBER];
} DecompressBatchState;

extern void compressed_batch_set_compressed_tuple(DecompressContext *dcontext,
												  DecompressBatchState *batch_state,
												  TupleTableSlot *subslot);

extern void compressed_batch_advance(DecompressContext *dcontext,
									 DecompressBatchState *batch_state);

extern void compressed_batch_save_first_tuple(DecompressContext *dcontext,
											  DecompressBatchState *batch_state,
											  TupleTableSlot *first_tuple_slot);

#define create_bulk_decompression_mctx(parent_mctx)                                                \
	AllocSetContextCreate(parent_mctx,                                                             \
						  "DecompressBatchState bulk decompression",                               \
						  /* minContextSize = */ 0,                                                \
						  /* initBlockSize = */ 64 * 1024,                                         \
						  /* maxBlockSize = */ 64 * 1024);
/*
 * Initialize the batch memory context
 *
 * We use custom size for the batch memory context page, calculated to
 * fit the typical result of bulk decompression (if we use it).
 * This allows us to save on expensive malloc/free calls, because the
 * Postgres memory contexts reallocate all pages except the first one
 * after each reset.
 */
#define create_per_batch_mctx(block_size_bytes)                                                    \
	AllocSetContextCreate(CurrentMemoryContext,                                                    \
						  "DecompressBatchState per-batch",                                        \
						  0,                                                                       \
						  block_size_bytes,                                                        \
						  block_size_bytes);

extern void compressed_batch_destroy(DecompressBatchState *batch_state);

extern void compressed_batch_discard_tuples(DecompressBatchState *batch_state);

inline static TupleTableSlot *
compressed_batch_current_tuple(DecompressBatchState *batch_state)
{
	if (IsA(&batch_state->decompressed_scan_slot_data, Invalid))
	{
		/*
		 * For convenience, we want a zero-initialized batch to be a valid
		 * "empty" state, but unfortunately a zero-initialized TupleTableSlotData
		 * is not a valid tuple slot, so here we have to work around this mismatch.
		 */
		Assert(batch_state->decompressed_scan_slot_data.base.tts_ops == NULL);
		Assert(batch_state->per_batch_context == NULL);
		return NULL;
	}

	Assert(batch_state->per_batch_context != NULL);
	return &batch_state->decompressed_scan_slot_data.base;
}
