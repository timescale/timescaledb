#ifndef TIMESCALEDB_CHUNK_H
#define TIMESCALEDB_CHUNK_H

#include <postgres.h>
#include <access/htup.h>
#include <access/tupdesc.h>

#include "catalog.h"
#include "chunk_constraint.h"

typedef struct Hypercube Hypercube;
typedef struct ChunkConstraint ChunkConstraint;
typedef struct Point Point;
typedef struct Hyperspace Hyperspace;

/*
 * A chunk represents a table that stores data, part of a partitioned
 * table. 
 *
 * Conceptually, a chunk is a hypercube in an N-dimensional space. The
 * boundaries of the cube is represented by a collection of slices from the N
 * distinct dimensions.
 */
typedef struct Chunk
{
	FormData_chunk fd;
	Oid			table_id;

	/* 
	 * The hypercube defines the chunks position in the N-dimensional space.
	 * Each of the N slices in the cube corresponds to a constraint on the chunk
	 * table.
	 */
	Hypercube   *cube;
	int16 num_constraints;
	ChunkConstraint constraints[0];
} Chunk;

#define CHUNK_SIZE(num_constraints)				\
	(sizeof(Chunk) + sizeof(ChunkConstraint) * num_constraints)

extern Chunk *chunk_create(HeapTuple tuple, int16 num_constraints);
extern Chunk *chunk_get_or_create(Hyperspace *hs, Point *p);

#endif   /* TIMESCALEDB_CHUNK_H */
