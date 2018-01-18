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

typedef struct ChunkConstraints
{
	int16		capacity;
	int16		num_constraints;
	int16		num_dimension_constraints;
	ChunkConstraint *constraints;
} ChunkConstraints;

#define chunk_constraints_get(cc, i)			\
	&((cc)->constraints[i])

#define is_dimension_constraint(cc)				\
	((cc)->fd.dimension_slice_id > 0)


typedef struct Chunk Chunk;
typedef struct DimensionSlice DimensionSlice;
typedef struct Hypercube Hypercube;
typedef struct ChunkScanCtx ChunkScanCtx;

extern ChunkConstraints *chunk_constraints_alloc(int size_hint);
extern ChunkConstraints *chunk_constraint_scan_by_chunk_id(int32 chunk_id, Size count_hint);
extern ChunkConstraints *chunk_constraints_copy(ChunkConstraints *constraints);
extern int	chunk_constraints_add_dimension_constraints(ChunkConstraints *ccs, int32 chunk_id, Hypercube *cube);
extern int	chunk_constraints_add_inheritable_constraints(ChunkConstraints *ccs, int32 chunk_id, Oid hypertable_oid);
extern void chunk_constraints_create(ChunkConstraints *ccs, Oid chunk_oid, int32 chunk_id, Oid hypertable_oid, int32 hypertable_id);

extern int	chunk_constraint_scan_by_dimension_slice_id(DimensionSlice *slice, ChunkScanCtx *ctx);
extern void chunk_constraint_create_on_chunk(Chunk *chunk, Oid constraint_oid);
extern int	chunk_constraint_delete_by_hypertable_constraint_name(int32 chunk_id, Oid chunk_oid, char *hypertable_constraint_name);
extern int	chunk_constraint_delete_by_chunk_id(int32 chunk_id, Oid chunk_oid);
extern void chunk_constraint_recreate(ChunkConstraint *cc, Oid chunk_oid);
extern int	chunk_constraint_rename_hypertable_constraint(int32 chunk_id, const char *oldname, const char *newname);


#endif							/* TIMESCALEDB_CHUNK_CONSTRAINT_H */
