/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/tupconvert.h>
#include <funcapi.h>

#include "bmslist_utils.h"
#include "cache.h"
#include "chunk.h"

typedef struct ChunkTupleRouting ChunkTupleRouting;
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

	/*
	 * Bloom information for UPSERT bloom optimization.
	 * They refer to (potentially) multiple bloom filters ordered
	 * by the number of columns in the bloom filter.
	 *
	 * The below collections are parallel, each have the same length
	 * and the items are related to each other, holding data
	 * for a given bloom filter.
	 *
	 * bloom_column_names: List of char* - compressed chunk column names for blooms
	 *
	 * bloom_insert_attnums: TsBmsList (List of Bitmapset*) - the INSERT tuple
	 *   columns to extract for each bloom (hypertable attnums)
	 *
	 * upsert_bloom_attnums: AttrNumber[] - compressed chunk attnums for each bloom
	 *   column (array parallel to bloom_column_names, all entries valid)
	 *
	 * bloom_builders: List of BatchMetadataBuilder* - cached builders for hash
	 *   computation (parallel with bloom_column_names). Each builder caches the
	 *   hash functions for its column types. Reusable across INSERT tuples.
	 */
	List *bloom_column_names;
	TsBmsList bloom_insert_attnums;
	AttrNumber *upsert_bloom_attnums;
	List *bloom_builders;
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
	/* Number of batches pruned by bloom */
	int64 batches_pruned_by_bloom;
} SharedCounters;

typedef struct ChunkInsertState
{
	Relation rel;
	ResultRelInfo *result_relation_info;

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

extern ChunkInsertState *ts_chunk_insert_state_create(Oid chunk_relid,
													  const ChunkTupleRouting *ctr);
extern void ts_chunk_insert_state_destroy(ChunkInsertState *state, bool single_chunk_insert);
ResultRelInfo *create_chunk_result_relation_info(ResultRelInfo *ht_rri, Relation rel,
												 EState *estate);

void ts_set_compression_status(ChunkInsertState *state, const Chunk *chunk);
