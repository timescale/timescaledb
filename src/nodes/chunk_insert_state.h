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
#include "cross_module_fn.h"

/* Info related to compressed chunk
 * Continuous aggregate triggers are called explicitly on
 * compressed chunks after INSERTS as AFTER ROW insert triggers
 * do now work with the PG infrastructure.
 * Note: the 2nd trigger arg is required for distributed hypertables.
 */
typedef struct CompressChunkInsertState
{
	Relation compress_rel;					  /*compressed chunk */
	ResultRelInfo *orig_result_relation_info; /*original chunk */
	CompressSingleRowState *compress_state;
	int32 cagg_trig_args[2]; /* cagg trigger args are hypertable ids */
	bool has_cagg_trigger;
	int cagg_trig_nargs;
} CompressChunkInsertState;

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
	List *chunk_data_nodes; /* List of data nodes for the chunk (ChunkDataNode objects) */
	int32 chunk_id;
	Oid user_id;

	/* for tracking compressed chunks */
	CompressChunkInsertState *compress_info;
} ChunkInsertState;

typedef struct ChunkDispatch ChunkDispatch;

extern ChunkInsertState *ts_chunk_insert_state_create(const Chunk *chunk, ChunkDispatch *dispatch);
extern void ts_chunk_insert_state_destroy(ChunkInsertState *state);
extern void ts_compress_chunk_invoke_cagg_trigger(CompressChunkInsertState *compress_info,
												  Relation chunk_rel, HeapTuple tuple);

#endif /* TIMESCALEDB_CHUNK_INSERT_STATE_H */
