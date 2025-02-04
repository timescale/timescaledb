/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_DECOMPRESS_CONTEXT_H
#define TIMESCALEDB_DECOMPRESS_CONTEXT_H

#include <postgres.h>
#include <access/attnum.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/pg_list.h>

#include "batch_array.h"
#include "detoaster.h"

typedef enum CompressionColumnType
{
	SEGMENTBY_COLUMN,
	COMPRESSED_COLUMN,
	COUNT_COLUMN,
	SEQUENCE_NUM_COLUMN,
} CompressionColumnType;

typedef struct CompressionColumnDescription
{
	CompressionColumnType type;
	Oid typid;
	int16 value_bytes;
	bool by_value;

	/*
	 * Attno of the decompressed column in the scan tuple of DecompressChunk node.
	 * Negative values are special columns that do not have a representation in
	 * the decompressed chunk, but are still used for decompression. The `type`
	 * field is set accordingly for these columns.
	 */
	AttrNumber custom_scan_attno;

	/*
	 * Attno of this column in the uncompressed chunks. We use it to fetch the
	 * default value from the uncompressed chunk tuple descriptor.
	 */
	AttrNumber uncompressed_chunk_attno;

	/*
	 * Attno of the compressed column in the input compressed chunk scan.
	 */
	AttrNumber compressed_scan_attno;

	bool bulk_decompression_supported;
} CompressionColumnDescription;

typedef struct DecompressContext
{
	/*
	 * Note that this array contains only those columns that are decompressed
	 * (output_attno != 0), and the order is different from the compressed chunk
	 * tuple order: first go the actual data columns, and after that the metadata
	 * columns.
	 */
	CompressionColumnDescription *compressed_chunk_columns;

	/*
	 * This includes all decompressed columns (output_attno != 0), including the
	 * metadata columns.
	 */
	int num_columns_with_metadata;

	/* This excludes the metadata columns. */
	int num_data_columns;

	List *vectorized_quals_constified;
	bool reverse;
	bool batch_sorted_merge; /* Merge append optimization enabled */
	bool enable_bulk_decompression;

	/*
	 * Scratch space for bulk decompression which might need a lot of temporary
	 * data.
	 */
	MemoryContext bulk_decompression_context;

	TupleTableSlot *custom_scan_slot;

	/*
	 * The scan tuple descriptor might be different from the uncompressed chunk
	 * one, and it doesn't have the default column values in that case, so we
	 * have to fetch the default values from the uncompressed chunk tuple
	 * descriptor which we store here.
	 */
	TupleDesc uncompressed_chunk_tdesc;

	PlanState *ps; /* Set for filtering and instrumentation */

	Detoaster detoaster;
} DecompressContext;

#endif /* TIMESCALEDB_DECOMPRESS_CONTEXT_H */
