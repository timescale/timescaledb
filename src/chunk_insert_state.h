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

typedef struct ChunkInsertState
{
	Relation	rel;
	ResultRelInfo *result_relation_info;
	List	   *arbiter_indexes;
	TupleConversionMap *tup_conv_map;
	TupleTableSlot *slot;
	MemoryContext mctx;

	EState	   *estate;
} ChunkInsertState;

typedef struct ChunkDispatch ChunkDispatch;

extern HeapTuple chunk_insert_state_convert_tuple(ChunkInsertState *state, HeapTuple tuple, TupleTableSlot **existing_slot);
extern ChunkInsertState *chunk_insert_state_create(Chunk *chunk, ChunkDispatch *dispatch);
extern void chunk_insert_state_destroy(ChunkInsertState *state);

#endif							/* TIMESCALEDB_CHUNK_INSERT_STATE_H */
