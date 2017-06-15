#ifndef TIMESCALEDB_CHUNK_H
#define TIMESCALEDB_CHUNK_H

#include <postgres.h>
#include <access/htup.h>
#include <access/tupdesc.h>

#include "catalog.h"

typedef struct DimensionSlice DimensionSlice;
typedef struct Hypercube Hypercube;

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
	int32		id;
	int32		partition_id;
	int64		start_time;
	int64		end_time;
	char		schema_name[NAMEDATALEN];
	char		table_name[NAMEDATALEN];
	Oid			table_id;

	/* 
	 * The hypercube defines the chunks position in the N-dimensional space.
	 * Each of the N slices in the cube corresponds to a constraint on the chunk
	 * table.
	 */
	Hypercube   *cube;
} Chunk;

extern bool chunk_timepoint_is_member(const Chunk *row, const int64 time_pt);
extern Chunk *chunk_create(HeapTuple tuple, TupleDesc tupdesc, MemoryContext ctx);

#endif   /* TIMESCALEDB_CHUNK_H */
