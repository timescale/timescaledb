#ifndef TIMESCALEDB_CHUNK_CONSTRAINT_H
#define TIMESCALEDB_CHUNK_CONSTRAINT_H


#include <postgres.h>
#include <nodes/pg_list.h>

#include "catalog.h"

typedef struct ChunkConstraint
{
	FormData_chunk_constraint fd;
} ChunkConstraint;

typedef struct Chunk Chunk;

extern Chunk *chunk_constraint_scan(Chunk *chunk);

#endif /* TIMESCALEDB_CHUNK_CONSTRAINT_H */

