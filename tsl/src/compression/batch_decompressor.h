/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/memutils.h>

#include "compression/column_values.h"
#include "nodes/columnar_scan/detoaster.h"

/*
 * Shared batch decompressor: build CompressedColumnValues for the columns of
 * one compressed batch. Consumers: the columnar scan, the RowDecompressor
 * bulk path, and the DML single-pass decompression state.
 *
 * The core borrows its resources from the caller: the wrapper array, the
 * output pointers inside the wrappers, the batch memory context, the bulk
 * scratch context handle, and the Detoaster are all owned by the caller and
 * must outlive the decompressor state. The core only fills wrappers and decompresses
 * columns; it never resets or deletes caller memory.
 */

typedef struct BatchDecompressorColumn
{
	/* Attno of the compressed datum in the caller's input (informational). */
	AttrNumber input_attno;

	/*
	 * Attno of the column in the uncompressed chunk tuple descriptor, used
	 * for getmissingattr() batch defaults.
	 */
	AttrNumber uncompressed_attno;

	/* The decompressed (output) type, never compressed_data. */
	Oid typid;

	/* Whether bulk decompression is allowed for this column. */
	bool bulk_decompression_supported;
} BatchDecompressorColumn;

typedef struct BatchDecompressorMetrics
{
	/* False for cache hits, scalar defaults, and all-NULL batches. */
	bool decompressed;

	/* Bytes of the compressed datum, for caller observability accounting. */
	Size compressed_bytes;
} BatchDecompressorMetrics;

typedef struct BatchDecompressor
{
	int num_columns;
	const BatchDecompressorColumn *columns;

	/* Alias of the caller's persistent wrapper array. */
	CompressedColumnValues *values;

	/* Borrowed: uncompressed chunk descriptor, for batch defaults. */
	TupleDesc uncompressed_tdesc;

	/* Borrowed: caller initializes and closes. */
	Detoaster *detoaster;

	/* Borrowed: caller resets and deletes; owns all batch-lifetime data. */
	MemoryContext per_batch_context;

	/* Caller-owned handle, lazily created sibling of per_batch_context. */
	MemoryContext *bulk_scratch;

	bool enable_bulk_decompression;
	bool reverse;

	/* Validated row count of the current batch; 0 means no batch. */
	uint16 total_batch_rows;
} BatchDecompressor;

extern void batch_decompressor_init(BatchDecompressor *state, int num_columns,
							  const BatchDecompressorColumn *columns, CompressedColumnValues *values,
							  TupleDesc uncompressed_tdesc, Detoaster *detoaster,
							  MemoryContext per_batch_context, MemoryContext *bulk_scratch,
							  bool enable_bulk_decompression, bool reverse);

/*
 * Start a new batch: validate the row count before narrowing it to uint16,
 * and mark every column DT_Invalid with cleared Arrow/buffer pointers. The
 * output pointers wired by the caller into the wrappers are preserved.
 */
extern void batch_decompressor_begin(BatchDecompressor *state, int rows, int max_rows);

/*
 * Decompress one column of the current batch on demand from the caller-supplied
 * compressed datum. Idempotent per batch: DT_Invalid -> decompress, else no-op.
 */
extern BatchDecompressorMetrics batch_decompressor_column(BatchDecompressor *state, int column_index,
											  Datum compressed, bool input_isnull);

/*
 * Invalidate every wrapper (DT_Invalid, cleared Arrow/buffer pointers). The
 * validated row count is left in place; the caller owns the context reset.
 */
extern void batch_decompressor_discard(BatchDecompressor *state);
