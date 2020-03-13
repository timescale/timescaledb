/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_H
#define TIMESCALEDB_CHUNK_H

#include <postgres.h>
#include <access/htup.h>
#include <access/tupdesc.h>
#include <utils/hsearch.h>

#include "export.h"
#include "catalog.h"
#include "chunk_constraint.h"
#include "hypertable.h"

#define INVALID_CHUNK_ID 0

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
	Oid table_id;
	Oid hypertable_relid;

	/*
	 * The hypercube defines the chunks position in the N-dimensional space.
	 * Each of the N slices in the cube corresponds to a constraint on the
	 * chunk table.
	 */
	Hypercube *cube;
	ChunkConstraints *constraints;
} Chunk;

/* This structure is used during the join of the chunk constraints to find
 * chunks that match all constraints. It is a stripped down version of the chunk
 * since we don't want to fill in all the fields until we find a match. */
typedef struct ChunkStub
{
	int32 id;
	Hypercube *cube;
	ChunkConstraints *constraints;
} ChunkStub;

/*
 * ChunkScanCtx is used to scan for chunks in a hypertable's N-dimensional
 * hyperspace.
 *
 * For every matching constraint, a corresponding chunk will be created in the
 * context's hash table, keyed on the chunk ID.
 */
typedef struct ChunkScanCtx
{
	HTAB *htab;
	Hyperspace *space;
	Point *point;
	unsigned int num_complete_chunks;
	int num_processed;
	bool early_abort;
	LOCKMODE lockmode;
	void *data;
} ChunkScanCtx;

/* Returns true if the stub has a full set of constraints, otherwise
 * false. Used to find a stub matching a point in an N-dimensional
 * hyperspace. */
static inline bool
chunk_stub_is_complete(ChunkStub *chunk, Hyperspace *space)
{
	return space->num_dimensions == chunk->constraints->num_dimension_constraints;
}

/* The hash table entry for the ChunkScanCtx */
typedef struct ChunkScanEntry
{
	int32 chunk_id;
	ChunkStub *stub;
} ChunkScanEntry;

typedef enum CascadeToMaterializationOption
{
	CASCADE_TO_MATERIALIZATION_UNKNOWN = -1,
	CASCADE_TO_MATERIALIZATION_FALSE = 0,
	CASCADE_TO_MATERIALIZATION_TRUE = 1
} CascadeToMaterializationOption;

extern Chunk *ts_chunk_create(Hypertable *ht, Point *p, const char *schema, const char *prefix);
extern TSDLLEXPORT Chunk *ts_chunk_create_base(int32 id, int16 num_constraints);
extern TSDLLEXPORT ChunkStub *ts_chunk_stub_create(int32 id, int16 num_constraints);
extern Chunk *ts_chunk_find(Hyperspace *hs, Point *p, bool include_chunks_marked_as_dropped);
extern Chunk **ts_chunk_find_all(Hyperspace *hs, List *dimension_vecs, LOCKMODE lockmode,
								 unsigned int *num_chunks);
extern List *ts_chunk_find_all_oids(Hyperspace *hs, List *dimension_vecs, LOCKMODE lockmode);
extern TSDLLEXPORT int ts_chunk_add_constraints(Chunk *chunk);

extern Chunk *ts_chunk_copy(Chunk *chunk);
extern Chunk *ts_chunk_get_by_name_with_memory_context(const char *schema_name,
													   const char *table_name,
													   int16 num_constraints, MemoryContext mctx,
													   bool fail_if_not_found);
extern TSDLLEXPORT void ts_chunk_insert_lock(Chunk *chunk, LOCKMODE lock);

extern TSDLLEXPORT Oid ts_chunk_create_table(Chunk *chunk, Hypertable *ht, char *tablespacename);
extern TSDLLEXPORT Chunk *ts_chunk_get_by_id(int32 id, int16 num_constraints,
											 bool fail_if_not_found);
extern TSDLLEXPORT Chunk *ts_chunk_get_by_relid(Oid relid, int16 num_constraints,
												bool fail_if_not_found);
extern bool ts_chunk_exists(const char *schema_name, const char *table_name);
extern bool ts_chunk_exists_relid(Oid relid);

extern TSDLLEXPORT bool ts_chunk_exists_with_compression(int32 hypertable_id);
extern void ts_chunk_recreate_all_constraints_for_dimension(Hyperspace *hs, int32 dimension_id);
extern TSDLLEXPORT void ts_chunk_drop_fks(Chunk *chunk);
extern TSDLLEXPORT void ts_chunk_create_fks(Chunk *chunk);
extern int ts_chunk_delete_by_hypertable_id(int32 hypertable_id);
extern int ts_chunk_delete_by_name(const char *schema, const char *table, DropBehavior behavior);
extern bool ts_chunk_set_name(Chunk *chunk, const char *newname);
extern bool ts_chunk_set_schema(Chunk *chunk, const char *newschema);
extern List *ts_chunk_get_window(int32 dimension_id, int64 point, int count, MemoryContext mctx);
extern void ts_chunks_rename_schema_name(char *old_schema, char *new_schema);
extern TSDLLEXPORT bool ts_chunk_set_compressed_chunk(Chunk *chunk, int32 compressed_chunk_id,
													  bool isnull);
extern TSDLLEXPORT void ts_chunk_drop(Chunk *chunk, DropBehavior behavior, int32 log_level);
extern TSDLLEXPORT void ts_chunk_drop_preserve_catalog_row(Chunk *chunk, DropBehavior behavior,
														   int32 log_level);
extern TSDLLEXPORT List *
ts_chunk_do_drop_chunks(Oid table_relid, Datum older_than_datum, Datum newer_than_datum,
						Oid older_than_type, Oid newer_than_type, bool cascade,
						CascadeToMaterializationOption cascades_to_materializations,
						int32 log_level, bool user_supplied_table_name);
extern TSDLLEXPORT Chunk *
ts_chunk_get_chunks_in_time_range(Oid table_relid, Datum older_than_datum, Datum newer_than_datum,
								  Oid older_than_type, Oid newer_than_type, char *caller_name,
								  MemoryContext mctx, uint64 *num_chunks_returned,
								  bool include_chunks_marked_as_dropped);

extern bool TSDLLEXPORT ts_chunk_contains_compressed_data(Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_can_be_compressed(int32 chunk_id);

#define chunk_get_by_name(schema_name, table_name, num_constraints, fail_if_not_found)             \
	ts_chunk_get_by_name_with_memory_context(schema_name,                                          \
											 table_name,                                           \
											 num_constraints,                                      \
											 CurrentMemoryContext,                                 \
											 fail_if_not_found)

#endif /* TIMESCALEDB_CHUNK_H */
