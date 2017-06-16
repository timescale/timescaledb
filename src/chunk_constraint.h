#ifndef TIMESCALEDB_CHUNK_CONSTRAINT_H
#define TIMESCALEDB_CHUNK_CONSTRAINT_H


#include <postgres.h>
#include <nodes/pg_list.h>

#include "catalog.h"

typedef struct ChunkConstraint{
	FormData_chunk_constraint fd;
} ChunkConstraint;

List *
chunk_constraint_scan(int32 chunk_id);

#endif /* TIMESCALEDB_CHUNK_CONSTRAINT_H */

