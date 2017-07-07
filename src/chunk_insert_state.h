#ifndef TIMESCALEDB_CHUNK_INSERT_STATE_H
#define TIMESCALEDB_CHUNK_INSERT_STATE_H

#include <postgres.h>
#include <funcapi.h>
#include "hypertable.h"
#include "chunk.h"
#include "cache.h"

typedef struct ChunkInsertState
{
	Chunk	   *chunk;
	Relation	rel;
	ResultRelInfo *result_relation_info;
} ChunkInsertState;

typedef struct ChunkDispatch ChunkDispatch;

extern ChunkInsertState *chunk_insert_state_create(Chunk *chunk, ChunkDispatch *dispatch);
extern void chunk_insert_state_destroy(ChunkInsertState *state);

#endif   /* TIMESCALEDB_CHUNK_INSERT_STATE_H */
