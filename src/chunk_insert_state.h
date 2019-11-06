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

#include "hypertable.h"
#include "chunk.h"
#include "cache.h"
#include "chunk_dispatch_state.h"
#include "compat.h"

typedef struct ChunkInsertState
{
	Relation rel;
	ResultRelInfo *result_relation_info;
	List *arbiter_indexes;
	TupleConversionMap *tup_conv_map;
	TupleTableSlot *slot;
	MemoryContext mctx;

	EState *estate;
} ChunkInsertState;

typedef struct ChunkDispatch ChunkDispatch;

extern HeapTuple ts_chunk_insert_state_convert_tuple(ChunkInsertState *state, HeapTuple tuple,
													 TupleTableSlot **existing_slot);
extern ChunkInsertState *ts_chunk_insert_state_create(Chunk *chunk, ChunkDispatch *dispatch,
													  const TupleTableSlotOps *const ops);
extern void ts_chunk_insert_state_switch(ChunkInsertState *state);

extern void ts_chunk_insert_state_destroy(ChunkInsertState *state);

#endif /* TIMESCALEDB_CHUNK_INSERT_STATE_H */
