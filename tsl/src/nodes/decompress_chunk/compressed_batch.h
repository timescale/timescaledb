/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include "compression/compression.h"
#include "nodes/decompress_chunk/exec.h"

typedef struct ArrowArray ArrowArray;

typedef struct CompressedColumnValues
{
	/* For row-by-row decompression. */
	DecompressionIterator *iterator;

	/*
	 * For bulk decompression and vectorized filters, mutually exclusive
	 * with the above.
	 */
	ArrowArray *arrow;

	/*
	 * These are the arrow buffers cached here to reduce the amount of
	 * indirections (we have about three there, so it matters).
	 */
	const void *arrow_validity;
	const void *arrow_values;

	/*
	 * The following fields are copied here for better data locality.
	 */
	AttrNumber output_attno;
	int8 value_bytes;
} CompressedColumnValues;

/*
 * All the information needed to decompress a batch.
 */
typedef struct DecompressBatchState
{
	TupleTableSlot *decompressed_scan_slot; /* A slot for the decompressed data */
	/*
	 * Compressed target slot. We have to keep a local copy when doing batch
	 * sorted merge, because the segmentby column values might reference the
	 * original tuple, and a batch outlives its source tuple.
	 */
	TupleTableSlot *compressed_slot;
	int total_batch_rows;
	int next_batch_row;
	Size block_size_bytes; /* Block size to use for memory context */
	MemoryContext per_batch_context;

	/*
	 * Arrow-style bitmap that says whether the vector quals passed for a given
	 * row. Indexed same as arrow arrays, w/o accounting for the reverse scan
	 * direction. Initialized to all ones, i.e. all rows pass.
	 */
	uint64 *vector_qual_result;

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
						  "Bulk decompression",                                                    \
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
						  "Per-batch decompression",                                               \
						  0,                                                                       \
						  block_size_bytes,                                                        \
						  block_size_bytes);
