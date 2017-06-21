#ifndef TIMESCALEDB_CHUNK_H
#define TIMESCALEDB_CHUNK_H

#include <postgres.h>
#include <access/htup.h>
#include <access/tupdesc.h>

#include "catalog.h"
#include "chunk_constraint.h"

typedef struct Hypercube Hypercube;
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
	int16 capacity;
	int16 num_constraints;
	ChunkConstraint constraints[0];
} Chunk;

#define CHUNK_SIZE(num_constraints)								\
	(sizeof(Chunk) + sizeof(ChunkConstraint) * (num_constraints))

/*
 * ChunkScanCtx is used to scan for a chunk matching a specific point in a
 * hypertable's N-dimensional hyperspace.
 *
 * For every matching constraint, a corresponding chunk will be created in the
 * context's hash table, keyed on the chunk ID. At the end of the scan, there
 * will be only one chunk in the hash table that has N number of matching
 * constraints, and this is the chunk that encloses the point.
 */
typedef struct ChunkScanCtx
{
	HTAB *htab;
	DimensionSlice *slice;
	int16 num_dimensions;
} ChunkScanCtx;

/* The hash table entry for the ChunkScanCtx */
typedef struct ChunkScanEntry
{
	int32 chunk_id;
	Chunk *chunk;
} ChunkScanEntry;

extern Chunk *chunk_create_from_tuple(HeapTuple tuple, int16 num_constraints);
extern Chunk *chunk_create_new(Hyperspace *hs, Point *p);
extern Chunk *chunk_get_or_create_new(Hyperspace *hs, Point *p);
extern bool chunk_add_constraint(Chunk *chunk, ChunkConstraint *constraint);
extern bool chunk_add_constraint_from_tuple(Chunk *chunk, HeapTuple constraint_tuple);
extern Chunk *chunk_find(Hyperspace *hs, Point *p);
extern Chunk *chunk_copy(Chunk *chunk);

#endif   /* TIMESCALEDB_CHUNK_H */
