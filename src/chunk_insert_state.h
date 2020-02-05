/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_INSERT_STATE_H
#define TIMESCALEDB_CHUNK_INSERT_STATE_H

#include <nodes/execnodes.h>
#include <postgres.h>
#include <funcapi.h>
#include <access/tupconvert.h>

#include "chunk.h"
#include "cache.h"
#include "compat.h"

typedef struct ChunkInsertState
{
	Relation rel;
	ResultRelInfo *result_relation_info;
	/* Per-chunk arbiter indexes for ON CONFLICT handling */
	List *arbiter_indexes;

	/* When the tuple descriptors for the main hypertable (root) and a chunk
	 * differs, it is necessary to convert tuples to chunk format before
	 * insertion, ON CONFLICT, or RETURNING handling. In particular, with
	 * PG12, the table AM (storage format) can also differ between the
	 * hypertable root and each chunk (as well as between each chunk, in
	 * theory).
	 *
	 * Thus, for PG12, which has more mature partitioning support, the
	 * ResultRelInfo keeps per-relation slots for these purposes. In that
	 * case, the slots here simply points to the per-relation slots in the
	 * ResultRelInfo. However, earlier PostgreSQL versions are hardcoded to a
	 * single tuple table slot (for the root), and in that case each per-chunk
	 * slot here points to a common shared slot and it necessary to set the
	 * tuple descriptor on the slots when switching between chunks.
	 */

	/* Tuple descriptor for projected tuple in ON CONFLICT handling */
	TupleDesc conflproj_tupdesc;
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
} ChunkInsertState;

typedef struct ChunkDispatch ChunkDispatch;

extern ChunkInsertState *ts_chunk_insert_state_create(Chunk *chunk, ChunkDispatch *dispatch);
extern void ts_chunk_insert_state_destroy(ChunkInsertState *state);

#endif /* TIMESCALEDB_CHUNK_INSERT_STATE_H */
