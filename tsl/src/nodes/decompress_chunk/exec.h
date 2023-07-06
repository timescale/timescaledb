/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H
#define TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H

#include <postgres.h>

#define DECOMPRESS_CHUNK_COUNT_ID -9
#define DECOMPRESS_CHUNK_SEQUENCE_NUM_ID -10

/* Initial amount of batch states */
#define INITIAL_BATCH_CAPACITY 16

/*
 * From nodeMergeAppend.c
 *
 * We have one slot for each item in the heap array.  We use DecompressSlotNumber
 * to store slot indexes.  This doesn't actually provide any formal
 * type-safety, but it makes the code more self-documenting.
 */
typedef int32 DecompressSlotNumber;

typedef enum DecompressChunkColumnType
{
	SEGMENTBY_COLUMN,
	COMPRESSED_COLUMN,
	COUNT_COLUMN,
	SEQUENCE_NUM_COLUMN,
} DecompressChunkColumnType;

typedef struct DecompressChunkColumnState
{
	DecompressChunkColumnType type;
	Oid typid;

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

	union
	{
		struct
		{
			Datum value;
			bool isnull;
			int count;
		} segmentby;

		struct
		{
			/* For row-by-row decompression. */
			DecompressionIterator *iterator;

			/* For entire batch decompression, mutually exclusive with the above. */
			ArrowArray *arrow;
			int value_bytes;
		} compressed;
	};
} DecompressChunkColumnState;

/*
 * All the needed information to decompress a batch
 */
typedef struct DecompressBatchState
{
	bool initialized;
	TupleTableSlot *decompressed_slot_projected; /* The result slot with the final tuples */
	TupleTableSlot *decompressed_slot_scan;		 /* A slot for the decompressed data */
	TupleTableSlot *compressed_slot;			 /* A slot for compressed data */
	DecompressChunkColumnState *columns;
	int total_batch_rows;
	int current_batch_row;
	MemoryContext per_batch_context;
} DecompressBatchState;

typedef struct DecompressChunkState
{
	CustomScanState csstate;
	List *decompression_map;
	List *is_segmentby_column;
	int num_columns;

	bool reverse;
	int hypertable_id;
	Oid chunk_relid;

	/* Batch states */
	int n_batch_states;					/* Number of batch states */
	DecompressBatchState *batch_states; /* The batch states */
	Bitmapset *unused_batch_states;		/* The unused batch states */

	bool sorted_merge_append;	   /* Merge append optimization enabled */
	int most_recent_batch;		   /* The batch state with the most recent value */
	struct binaryheap *merge_heap; /* Binary heap of slot indices */
	int n_sortkeys;				   /* Number of sort keys for heap compare function */
	SortSupportData *sortkeys;	   /* Sort keys for binary heap compare function */

	bool using_bulk_decompression; /* For EXPLAIN ANALYZE. */

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
	TupleDesc decompressed_slot_projected_tdesc;
	TupleDesc decompressed_slot_scan_tdesc;
	TupleDesc compressed_slot_tdesc;
} DecompressChunkState;

extern Node *decompress_chunk_state_create(CustomScan *cscan);

extern DecompressSlotNumber decompress_get_free_batch_state_id(DecompressChunkState *chunk_state);

extern void decompress_initialize_batch(DecompressChunkState *chunk_state,
										DecompressBatchState *batch_state, TupleTableSlot *subslot);

extern bool decompress_get_next_tuple_from_batch(DecompressChunkState *chunk_state,
												 DecompressBatchState *batch_state);

extern void decompress_set_batch_state_to_unused(DecompressChunkState *chunk_state, int batch_id);

#endif /* TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H */
