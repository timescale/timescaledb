/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include "compression/compression.h"
#include "nodes/decompress_chunk/decompress_context.h"
#include <executor/tuptable.h>

typedef struct ArrowArray ArrowArray;

/* How to obtain the decompressed datum for individual row. */
typedef enum
{
	DT_ArrowTextDict = -4,

	DT_ArrowText = -3,

	/*
	 * The decompressed value is already in the decompressed slot. This is used
	 * for segmentby and compressed columns with default value in batch.
	 */
	DT_Scalar = -2,

	DT_Iterator = -1,

	DT_Invalid = 0,

	/*
	 * Any positive number is also valid for the decompression type. It means
	 * arrow array of a fixed-size by-value type, with size in bytes given by
	 * the number.
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
	/*
	 * The slot for the decompressed tuple.
	 *
	 * We embed it into the batch state as the first member (data inheritance),
	 * so that it's easier to pass out to parent nodes, while following the usual
	 * Postgres interface of passing the tuple table slots.
	 * We use &batch_state->decompressed_scan_slot_data.base everywhere where we
	 * need the TupleTableSlot*, and some parent nodes can cast this pointer to
	 * DecompressBatchState* to use our custom interfaces.
	 *
	 * The slot itself follows the TTSVirtualOps tuple slot protocol, because Postgres
	 * expression executor has special fast path for virtual tuples, and we don't
	 * really need the custom tuple slot protocol for anything. One potential use
	 * case for it would be late decompression by implementing custom slot_getattr().
	 * It was actually implemented and didn't show any benefits in the preliminary
	 * testing, compared to what we already achieve with lazy decompression after
	 * vectorized filters. One reason is that the Postgres expression compiler
	 * can be eager in requesting materialization. For example, it would call
	 * slot_getattr up to the last attribute used by every filter in a qualifier,
	 * before running any qualifiers. This might be possible to configure, but
	 * the area needs more research.
	 *
	 * See the PR #6628 for context.
	 */
	VirtualTupleTableSlot decompressed_scan_slot_data;

	uint16 total_batch_rows;
	uint16 next_batch_row;
	MemoryContext per_batch_context;

	/*
	 * Arrow-style bitmap that says whether the vector quals passed for a given
	 * row. Indexed same as arrow arrays, w/o accounting for the reverse scan
	 * direction. Initialized to all ones, i.e. all rows pass.
	 */
	uint64 *restrict vector_qual_result;

	/*
	 * This follows DecompressContext.compressed_chunk_columns, but does not
	 * include the trailing metadata columns, but only the leading data columns.
	 * These columns are compressed and segmentby columns, their total number is
	 * given by DecompressContext.num_data_columns.
	 */
	CompressedColumnValues compressed_columns[FLEXIBLE_ARRAY_MEMBER];
} DecompressBatchState;

extern void compressed_batch_set_compressed_tuple(DecompressContext *dcontext,
												  DecompressBatchState *batch_state,
												  TupleTableSlot *compressed_slot);

extern void compressed_batch_advance(DecompressContext *dcontext,
									 DecompressBatchState *batch_state);

extern void compressed_batch_save_first_tuple(DecompressContext *dcontext,
											  DecompressBatchState *batch_state,
											  TupleTableSlot *first_tuple_slot);

/*
 * Initialize the batch memory context and bulk decompression context.
 *
 * We use Generation context here because the AllocSet has a hardcoded threshold
 * of 8kB per allocation, after which it allocates directly through malloc. We
 * want to make the blocks as big as possible, but below the malloc's mmap
 * threshold. For small queries, these contexts are basically single-shot and
 * the page faults after an mmap slow them down significantly. The threshold
 * should be 128 kiB according to the docs, but I'm seeing 64 kiB in testing.
 *
 * If bulk decompression is not used, use the default size for batch context.
 * This reduces memory usage and improves performance with batch sorted merge.
 */
#define create_bulk_decompression_mctx(parent_mctx)                                                \
	GenerationContextCreateCompat(parent_mctx,                                                     \
								  "DecompressBatchState bulk decompression",                       \
								  64 * 1024);

#define create_per_batch_mctx(dcontext)                                                            \
	GenerationContextCreateCompat(CurrentMemoryContext,                                            \
								  "DecompressBatchState per-batch",                                \
								  dcontext->enable_bulk_decompression ? 64 * 1024 : 8 * 1024);

extern void compressed_batch_destroy(DecompressBatchState *batch_state);

extern void compressed_batch_discard_tuples(DecompressBatchState *batch_state);

/*
 * Returns the current decompressed tuple in the compressed batch.
 */
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
