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

typedef struct TSCopyMultiInsertBuffer TSCopyMultiInsertBuffer;
typedef struct ChunkDispatchState ChunkDispatchState;

typedef struct ChunkInsertState
{
	Relation rel;
	ResultRelInfo *result_relation_info;
	/* Per-chunk arbiter indexes for ON CONFLICT handling */
	List *arbiter_indexes;
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
	int32 compressed_chunk_id;
	Oid user_id;

	/* for tracking compressed chunks */
	bool chunk_compressed;
	bool chunk_partial;

	/* Chunk uses our own table access method */
	bool use_tam;

	Oid compressed_chunk_table_id;
} ChunkInsertState;

typedef struct ChunkDispatch ChunkDispatch;

extern ChunkInsertState *ts_chunk_insert_state_create(Oid chunk_relid,
													  const ChunkDispatch *dispatch);
extern void ts_chunk_insert_state_destroy(ChunkInsertState *state);

TSDLLEXPORT OnConflictAction
ts_chunk_dispatch_get_on_conflict_action(const ChunkDispatch *dispatch);
void ts_set_compression_status(ChunkInsertState *state, const Chunk *chunk);
