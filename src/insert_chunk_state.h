#ifndef TIMESCALEDB_CHUNK_INSERT_STATE_H
#define TIMESCALEDB_CHUNK_INSERT_STATE_H

#include <postgres.h>
#include <funcapi.h>
#include "chunk.h"
#include "cache.h"

typedef struct InsertChunkState
{
	Chunk	   *chunk;
	List	   *replica_states;
}	InsertChunkState;

extern InsertChunkState *insert_chunk_state_new(Chunk *chunk);

extern void insert_chunk_state_destroy(InsertChunkState *state);

extern void insert_chunk_state_insert_tuple(InsertChunkState *state, HeapTuple tup);

#endif   /* TIMESCALEDB_CHUNK_INSERT_STATE_H */
