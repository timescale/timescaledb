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
	int value_bytes;

	/*
	 * Attno of the decompressed column in the output of DecompressChunk node.
	 * Negative values are special columns that do not have a representation in
	 * the decompressed chunk, but are still used for decompression. They should
	 * have the respective `type` field.
	 */
	AttrNumber output_attno;

	/*
	 * Attno of the compressed column in the input compressed chunk scan.
	 */
	AttrNumber compressed_scan_attno;

	bool bulk_decompression_supported;
} CompressionColumnDescription;

typedef struct DecompressContext
{
	CompressionColumnDescription *template_columns;
	int num_total_columns;
	int num_compressed_columns;
	List *vectorized_quals_constified;
	bool reverse;
	bool batch_sorted_merge; /* Merge append optimization enabled */
	bool enable_bulk_decompression;

	/*
	 * Scratch space for bulk decompression which might need a lot of temporary
	 * data.
	 */
	MemoryContext bulk_decompression_context;

	TupleTableSlot *decompressed_slot;

	/*
	 * Make non-refcounted copies of the tupdesc for reuse across all batch states
	 * and avoid spending CPU in ResourceOwner when creating a big number of table
	 * slots. This happens because each new slot pins its tuple descriptor using
	 * PinTupleDesc, and for reference-counting tuples this involves adding a new
	 * reference to ResourceOwner, which is not very efficient for a large number of
	 * references.
	 *
	 * We don't have to do this for the decompressed slot tuple descriptor,
	 * because there we use custom tuple slot (de)initialization functions, which
	 * don't use reference counting and just use a raw pointer to the tuple
	 * descriptor.
	 */
	TupleDesc compressed_slot_tdesc;

	PlanState *ps; /* Set for filtering and instrumentation */

	Detoaster detoaster;

	TupleTableSlotOps tts_ops;
} DecompressContext;

#endif /* TIMESCALEDB_DECOMPRESS_CONTEXT_H */
