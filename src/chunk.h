#ifndef TIMESCALEDB_CHUNK_H
#define TIMESCALEDB_CHUNK_H

#include <postgres.h>
#include <access/htup.h>
#include <access/tupdesc.h>
#include <utils/hsearch.h>

#include "catalog.h"
#include "chunk_constraint.h"
#include "hypertable.h"

typedef struct Hypercube Hypercube;
typedef struct Point Point;
typedef struct Hyperspace Hyperspace;
typedef struct Hypertable Hypertable;

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
	Oid			hypertable_relid;

	/*
	 * The hypercube defines the chunks position in the N-dimensional space.
	 * Each of the N slices in the cube corresponds to a constraint on the
	 * chunk table.
	 */
	Hypercube  *cube;
	ChunkConstraints *constraints;
} Chunk;

/*
 * ChunkScanCtx is used to scan for chunks in a hypertable's N-dimensional
 * hyperspace.
 *
 * For every matching constraint, a corresponding chunk will be created in the
 * context's hash table, keyed on the chunk ID.
 */
typedef struct ChunkScanCtx
{
	HTAB	   *htab;
	Hyperspace *space;
	Point	   *point;
	bool		early_abort;
	LOCKMODE	lockmode;
	void	   *data;
} ChunkScanCtx;

/* The hash table entry for the ChunkScanCtx */
typedef struct ChunkScanEntry
{
	int32		chunk_id;
	Chunk	   *chunk;
} ChunkScanEntry;

extern Chunk *chunk_create(Hypertable *ht, Point *p, const char *schema, const char *prefix);
extern Chunk *chunk_create_stub(int32 id, int16 num_constraints);
extern void chunk_free(Chunk *chunk);
extern Chunk *chunk_find(Hyperspace *hs, Point *p);
extern List *chunk_find_all_oids(Hyperspace *hs, List *dimension_vecs, LOCKMODE lockmode);
extern Chunk *chunk_copy(Chunk *chunk);
extern Chunk *chunk_get_by_name_with_memory_context(const char *schema_name, const char *table_name, int16 num_constraints, MemoryContext mctx, bool fail_if_not_found);
extern Chunk *chunk_get_by_relid(Oid relid, int16 num_constraints, bool fail_if_not_found);
extern Chunk *chunk_get_by_id(int32 id, int16 num_constraints, bool fail_if_not_found);
extern bool chunk_exists(const char *schema_name, const char *table_name);
extern bool chunk_exists_relid(Oid relid);
extern void chunk_recreate_all_constraints_for_dimension(Hyperspace *hs, int32 dimension_id);
extern int	chunk_delete_by_relid(Oid chunk_oid);
extern int	chunk_delete_by_hypertable_id(int32 hypertable_id);
extern int	chunk_delete_by_name(const char *schema, const char *table);
extern bool chunk_set_name(Chunk *chunk, const char *newname);
extern bool chunk_set_schema(Chunk *chunk, const char *newschema);
extern List *chunk_get_window(int32 dimension_id, int64 point, int count, MemoryContext mctx);

#define chunk_get_by_name(schema_name, table_name, num_constraints, fail_if_not_found) \
	chunk_get_by_name_with_memory_context(schema_name, table_name, num_constraints, \
										  CurrentMemoryContext, fail_if_not_found)

#endif							/* TIMESCALEDB_CHUNK_H */
