#ifndef TIMESCALEDB_CHUNK_CONSTRAINT_H
#define TIMESCALEDB_CHUNK_CONSTRAINT_H


#include <postgres.h>
#include <nodes/pg_list.h>

#include "catalog.h"
#include "hypertable.h"

typedef struct ChunkConstraint
{
	FormData_chunk_constraint fd;
} ChunkConstraint;


typedef struct ChunkConstraintVec
{
	int16		num_constraints;
	ChunkConstraint constraints[FLEXIBLE_ARRAY_MEMBER];
}	ChunkConstraintVec;

typedef struct Chunk Chunk;
typedef struct DimensionSlice DimensionSlice;
typedef struct ChunkScanCtx ChunkScanCtx;

extern Chunk *chunk_constraint_scan_by_chunk_id(Chunk *chunk);
extern int	chunk_constraint_scan_by_dimension_slice_id(DimensionSlice *slice, ChunkScanCtx *ctx);
extern void chunk_constraint_insert_multi(ChunkConstraint *constraints, Size num_constraints);
extern void chunk_constraint_create_all_on_chunk(Hypertable *ht, Chunk *chunk);
extern void chunk_constraint_create_on_chunk(Hypertable *ht, Chunk *chunk, Oid constraintOid);
#endif   /* TIMESCALEDB_CHUNK_CONSTRAINT_H */
