/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H
#define TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H

#include <postgres.h>

#include <nodes/extensible.h>

#define DECOMPRESS_CHUNK_COUNT_ID -9
#define DECOMPRESS_CHUNK_SEQUENCE_NUM_ID -10

typedef enum DecompressChunkColumnType
{
	SEGMENTBY_COLUMN,
	COMPRESSED_COLUMN,
	COUNT_COLUMN,
	SEQUENCE_NUM_COLUMN,
} DecompressChunkColumnType;

typedef struct DecompressChunkColumnDescription
{
	DecompressChunkColumnType type;
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
} DecompressChunkColumnDescription;

typedef struct DecompressChunkState
{
	CustomScanState csstate;
	List *decompression_map;
	List *is_segmentby_column;
	List *bulk_decompression_column;
	int num_total_columns;
	int num_compressed_columns;

	DecompressChunkColumnDescription *template_columns;

	bool reverse;
	int hypertable_id;
	Oid chunk_relid;

	/* Batch states */
	int n_batch_states; /* Number of batch states */
	/*
	 * The batch states. It's void* because they have a variable length
	 * column array, so normal indexing can't be used. Use the batch_array_get_at
	 * accessor instead.
	 */
	void *batch_states;
	int n_batch_state_bytes;
	Bitmapset *unused_batch_states; /* The unused batch states */
	int batch_memory_context_bytes;

	const struct BatchQueueFunctions *batch_queue;
	CustomExecMethods exec_methods;

	bool batch_sorted_merge; /* Merge append optimization enabled */
	List *sortinfo;
	struct binaryheap *merge_heap; /* Binary heap of slot indices */
	int n_sortkeys;				   /* Number of sort keys for heap compare function */
	SortSupportData *sortkeys;	   /* Sort keys for binary heap compare function */
	TupleTableSlot *last_batch_first_tuple;

	bool enable_bulk_decompression;

	/*
	 * Scratch space for bulk decompression which might need a lot of temporary
	 * data.
	 */
	MemoryContext bulk_decompression_context;

	/*
	 * Make non-refcounted copies of the tupdesc for reuse across all batch states
	 * and avoid spending CPU in ResourceOwner when creating a big number of table
	 * slots. This happens because each new slot pins its tuple descriptor using
	 * PinTupleDesc, and for reference-counting tuples this involves adding a new
	 * reference to ResourceOwner, which is not very efficient for a large number of
	 * references.
	 */
	TupleDesc decompressed_slot_scan_tdesc;
	TupleDesc compressed_slot_tdesc;
} DecompressChunkState;

extern Node *decompress_chunk_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H */
