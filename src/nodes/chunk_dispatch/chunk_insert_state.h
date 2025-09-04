/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/tupconvert.h>
#include <funcapi.h>

#include "cache.h"
#include "chunk.h"
#include "cross_module_fn.h"

typedef struct ChunkDispatchState ChunkDispatchState;
typedef struct CompressionSettings CompressionSettings;
typedef struct tuple_filtering_constraints tuple_filtering_constraints;

/*
 * Bundle the ScanKey and the attribute numbers together
 * to be able to update the scankey by replacing the
 * `sk_argument` field with the value from the actual slot.
 */
typedef struct ScanKeyWithAttnos
{
	int num_scankeys;
	ScanKeyData *scankeys;
	AttrNumber *attnos;
} ScanKeyWithAttnos;

/*
 * Holds information to cache scan keys and other
 * information needed for repeated calls of
 * `decompress_batches_for_insert` on the same chunk.
 */
typedef struct CachedDecompressionState
{
	bool has_primary_or_unique_index;
	CompressionSettings *compression_settings;
	tuple_filtering_constraints *constraints;
	/* Columns that needs to be checked manually because
	 * heap scan doesn't support SK_SEARCHNULL:
	 */
	Bitmapset *columns_with_null_check;
	ScanKeyWithAttnos heap_scankeys;
	ScanKeyWithAttnos index_scankeys;
	ScanKeyWithAttnos mem_scankeys;
	Oid index_relid;
} CachedDecompressionState;

typedef struct SharedCounters
{
	/* Number of batches deleted */
	int64 batches_deleted;
	/* Number of batches filtered */
	int64 batches_filtered;
	/* Number of batches decompressed */
	int64 batches_decompressed;
	/* Number of tuples decompressed */
	int64 tuples_decompressed;
} SharedCounters;

typedef struct ChunkInsertState
{
	Relation rel;
	ResultRelInfo *result_relation_info;
	ChunkDispatchState *cds;

	/* When the tuple descriptors for the main hypertable (root) and a chunk
	 * differs, it is necessary to convert tuples to chunk format before
	 * insertion, ON CONFLICT, or RETURNING handling. The table AM (storage format)
	 * can also differ between the hypertable root and each chunk (as well as
	 * between each chunk, in theory).
	 *
	 * The ResultRelInfo keeps per-relation slots for these purposes. The slots
	 * here simply points to the per-relation slots in the ResultRelInfo.
	 */

	/* Pointer to slot for projected tuple in ON CONFLICT handling */
	TupleTableSlot *conflproj_slot;
	/* Pointer to slot for tuple replaced in ON CONFLICT DO UPDATE
	 * handling. Note that this slot's tuple descriptor is always the same as
	 * the chunk rel's. */
	TupleTableSlot *existing_slot;

	/* Slot for inserted/new tuples going into the chunk */
	TupleTableSlot *slot;
	/* Map for converting tuple from hypertable (root table) format to chunk format */
	TupleConversionMap *hyper_to_chunk_map;
	MemoryContext mctx;
	EState *estate;
	Oid hypertable_relid;
	int32 chunk_id;
	Oid user_id;

	/* for tracking compressed chunks */
	bool chunk_compressed;
	bool chunk_partial;
	bool columnstore_insert;
	bool needs_partial;

	/* To speedup repeated calls of `decompress_batches_for_insert` */
	CachedDecompressionState *cached_decompression_state;

	OnConflictAction onConflictAction;
	/* Should this INSERT be skipped due to ON CONFLICT DO NOTHING */
	bool skip_current_tuple;
	SharedCounters *counters;

	/* for tracking generated column computations */
	bool skip_generated_column_computations;
} ChunkInsertState;

typedef struct ChunkDispatch ChunkDispatch;

extern ChunkInsertState *ts_chunk_insert_state_create(Oid chunk_relid,
													  const ChunkDispatch *dispatch);
extern void ts_chunk_insert_state_destroy(ChunkInsertState *state);
ResultRelInfo *create_chunk_result_relation_info(ResultRelInfo *ht_rri, Relation rel,
												 EState *estate);

void ts_set_compression_status(ChunkInsertState *state, const Chunk *chunk);
