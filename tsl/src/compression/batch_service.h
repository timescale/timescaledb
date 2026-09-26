/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/memutils.h>

#include "compression/column_values.h"
#include "compression/compression.h"
#include "nodes/columnar_scan/detoaster.h"

/*
 * Shared owned batch decompression service. One BatchDecompressOwner per
 * executor or utility operation owns the normalized column layout, a
 * descriptor copy carrying default values, the Detoaster, and a shared bulk
 * scratch context. Each DecompressedBatch owns the existing
 * CompressedColumnValues wrappers, a resettable value context, the retained
 * compressed inputs, and iterator progress. Callers bind their own output
 * Datum/bool arrays; inputs arrive as compressed Datum/isnull pairs.
 *
 * Ownership rules: the service never owns caller output arrays or slots and
 * never resets caller memory. Callers must not reset or delete service-owned
 * contexts through aliases. Internal Arrow arrays use PostgreSQL memory
 * contexts; there is no Arrow release-callback protocol here.
 */

/* Memory policy for a batch's value context. */
typedef enum BatchDecompressMemoryPolicy
{
	BATCH_DECOMPRESS_MEMORY_SCAN,
	BATCH_DECOMPRESS_MEMORY_UTILITY
} BatchDecompressMemoryPolicy;

/* Normalized description of one decompressed column. */
typedef struct BatchDecompressColumnSpec
{
	/* Attno of the compressed datum in the caller's input. */
	AttrNumber input_attno;

	/* Attno in the real uncompressed descriptor, for defaults and keys. */
	AttrNumber source_attno;

	/* Position in the caller's output Datum/bool arrays. */
	AttrNumber output_attno;

	/* The decompressed (output) type, never compressed_data. */
	Oid typid;

	/* Layout metadata; the runtime value is DT_Scalar either way. */
	bool segmentby;
	bool bulk_supported;
} BatchDecompressColumnSpec;

typedef struct BatchDecompressLoadMetrics
{
	/* False for cache hits and scalar setup. */
	bool new_compressed_column;

	/* Bytes of the compressed datum, for caller observability accounting. */
	Size compressed_bytes;
} BatchDecompressLoadMetrics;

/* Factory-created, immutable column selection; indexes are dense and unique. */
typedef struct BatchDecompressColumnSet
{
	const BatchDecompressOwner *owner;
	int ncolumns;
	const int *indexes;
} BatchDecompressColumnSet;

extern BatchDecompressOwner *batch_decompress_owner_create(MemoryContext parent,
														   TupleDesc source_desc,
														   const BatchDecompressColumnSpec *specs,
														   int ncols, int output_natts,
														   int row_limit, bool bulk_enabled,
														   bool reverse,
														   BatchDecompressMemoryPolicy policy);
extern void batch_decompress_owner_destroy(BatchDecompressOwner *owner);
extern int batch_decompress_owner_num_columns(const BatchDecompressOwner *owner);
extern const BatchDecompressColumnSpec *batch_decompress_owner_spec(const BatchDecompressOwner *owner,
																	int column);
extern const BatchDecompressColumnSet *batch_decompress_owner_columns(BatchDecompressOwner *owner,
																	  const AttrNumber *source_attnos,
																	  int nattnos);

extern DecompressedBatch *decompressed_batch_create(BatchDecompressOwner *owner);
extern void decompressed_batch_destroy(DecompressedBatch *batch);
extern bool decompressed_batch_active(const DecompressedBatch *batch);

/*
 * Start a new batch: validate the row count as an integer before narrowing
 * it, bind the caller's output arrays, and mark every column DT_Invalid.
 * begin on an active batch is an error.
 */
extern void decompressed_batch_begin(DecompressedBatch *batch, int rows, bool count_isnull,
									 Datum *output_values, bool *output_isnull, int output_natts);

/* Decompress one column of the current batch on demand. Idempotent. */
extern BatchDecompressLoadMetrics decompressed_batch_load(DecompressedBatch *batch, int column,
														  Datum input, bool input_isnull);

extern const CompressedColumnValues *decompressed_batch_columns(const DecompressedBatch *batch);
extern MemoryContext decompressed_batch_context(const DecompressedBatch *batch);
extern int decompressed_batch_rows(const DecompressedBatch *batch);

/* NULL set means all columns. Sets from another owner are rejected. */
extern void decompressed_batch_read(DecompressedBatch *batch, const BatchDecompressColumnSet *set,
									int arrow_row);
extern void decompressed_batch_skip(DecompressedBatch *batch, const BatchDecompressColumnSet *set);
extern void decompressed_batch_finish(DecompressedBatch *batch,
									  const BatchDecompressColumnSet *set);

/* Restart consumed iterators from the retained inputs (D7). */
extern void decompressed_batch_rewind_consumed(DecompressedBatch *batch);

/* Invalidate all columns and reset the value context; safe on idle batches. */
extern void decompressed_batch_discard(DecompressedBatch *batch);
