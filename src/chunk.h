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
#include <foreign/foreign.h>

#include "export.h"
#include "ts_catalog/catalog.h"
#include "chunk_constraint.h"
#include "hypertable.h"
#include "export.h"

#define INVALID_CHUNK_ID 0

/* Should match definitions in ddl_api.sql */
#define DROP_CHUNKS_FUNCNAME "drop_chunks"
#define DROP_CHUNKS_NARGS 4
#define COMPRESS_CHUNK_FUNCNAME "compress_chunk"
#define COMPRESS_CHUNK_NARGS 2
#define DECOMPRESS_CHUNK_FUNCNAME "decompress_chunk"
#define RECOMPRESS_CHUNK_FUNCNAME "recompress_chunk"
#define RECOMPRESS_CHUNK_NARGS 2

typedef enum ChunkCompressionStatus
{
	CHUNK_COMPRESS_NONE = 0,
	CHUNK_COMPRESS_UNORDERED,
	CHUNK_COMPRESS_ORDERED,
	CHUNK_DROPPED
} ChunkCompressionStatus;

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
	char relkind;
	Oid table_id;
	Oid hypertable_relid;

	/*
	 * The hypercube defines the chunks position in the N-dimensional space.
	 * Each of the N slices in the cube corresponds to a constraint on the
	 * chunk table.
	 */
	Hypercube *cube;
	ChunkConstraints *constraints;

	/*
	 * The data nodes that hold a copy of the chunk. NIL for non-distributed
	 * hypertables.
	 */
	List *data_nodes;
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
	char relkind; /* Create chunks of this relkind */
	const Hyperspace *space;
	const Point *point;
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
chunk_stub_is_complete(const ChunkStub *stub, const Hyperspace *space)
{
	return space->num_dimensions == stub->constraints->num_dimension_constraints;
}

/* The hash table entry for the ChunkScanCtx */
typedef struct ChunkScanEntry
{
	int32 chunk_id;
	ChunkStub *stub;
} ChunkScanEntry;

/*
 * Information to be able to display a scan key details for error messages.
 */
typedef struct DisplayKeyData
{
	const char *name;
	const char *(*as_string)(Datum);
} DisplayKeyData;

extern void ts_chunk_formdata_fill(FormData_chunk *fd, const TupleInfo *ti);
extern Chunk *ts_chunk_create_from_point(const Hypertable *ht, const Point *p, const char *schema,
										 const char *prefix);

extern TSDLLEXPORT Chunk *ts_chunk_create_base(int32 id, int16 num_constraints, const char relkind);
extern TSDLLEXPORT ChunkStub *ts_chunk_stub_create(int32 id, int16 num_constraints);
extern Chunk *ts_chunk_find(const Hypertable *ht, const Point *p, bool lock_slices);
extern TSDLLEXPORT Chunk *ts_chunk_copy(const Chunk *chunk);
extern TSDLLEXPORT Chunk *ts_chunk_get_by_name_with_memory_context(const char *schema_name,
																   const char *table_name,
																   MemoryContext mctx,
																   bool fail_if_not_found);
extern TSDLLEXPORT void ts_chunk_insert_lock(const Chunk *chunk, LOCKMODE lock);

extern TSDLLEXPORT Oid ts_chunk_create_table(const Chunk *chunk, const Hypertable *ht,
											 const char *tablespacename);
extern TSDLLEXPORT Chunk *ts_chunk_get_by_id(int32 id, bool fail_if_not_found);
extern TSDLLEXPORT Chunk *ts_chunk_get_by_relid(Oid relid, bool fail_if_not_found);
extern TSDLLEXPORT void ts_chunk_free(Chunk *chunk);
extern bool ts_chunk_exists(const char *schema_name, const char *table_name);
extern Oid ts_chunk_get_relid(int32 chunk_id, bool missing_ok);
extern Oid ts_chunk_get_schema_id(int32 chunk_id, bool missing_ok);
extern bool ts_chunk_get_id(const char *schema, const char *table, int32 *chunk_id,
							bool missing_ok);
extern bool ts_chunk_exists_relid(Oid relid);
extern TSDLLEXPORT int ts_chunk_num_of_chunks_created_after(const Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_exists_with_compression(int32 hypertable_id);
extern void ts_chunk_recreate_all_constraints_for_dimension(Hyperspace *hs, int32 dimension_id);
extern TSDLLEXPORT void ts_chunk_drop_fks(const Chunk *const chunk);
extern TSDLLEXPORT void ts_chunk_create_fks(const Chunk *const chunk);
extern int ts_chunk_delete_by_hypertable_id(int32 hypertable_id);
extern int ts_chunk_delete_by_name(const char *schema, const char *table, DropBehavior behavior);
extern TSDLLEXPORT bool ts_chunk_add_status(Chunk *chunk, int32 status);
extern TSDLLEXPORT bool ts_chunk_set_status(Chunk *chunk, int32 status);
extern bool ts_chunk_set_name(Chunk *chunk, const char *newname);
extern bool ts_chunk_set_schema(Chunk *chunk, const char *newschema);
extern TSDLLEXPORT List *ts_chunk_get_window(int32 dimension_id, int64 point, int count,
											 MemoryContext mctx);
extern void ts_chunks_rename_schema_name(char *old_schema, char *new_schema);

extern TSDLLEXPORT bool ts_chunk_set_unordered(Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_set_compressed_chunk(Chunk *chunk, int32 compressed_chunk_id);
extern TSDLLEXPORT bool ts_chunk_clear_compressed_chunk(Chunk *chunk);
extern TSDLLEXPORT void ts_chunk_drop(const Chunk *chunk, DropBehavior behavior, int32 log_level);
extern TSDLLEXPORT void ts_chunk_drop_preserve_catalog_row(const Chunk *chunk,
														   DropBehavior behavior, int32 log_level);
extern TSDLLEXPORT List *ts_chunk_do_drop_chunks(Hypertable *ht, int64 older_than, int64 newer_than,
												 int32 log_level, List **affected_data_nodes);
extern TSDLLEXPORT Chunk *
ts_chunk_find_or_create_without_cuts(const Hypertable *ht, Hypercube *hc, const char *schema_name,
									 const char *table_name, Oid chunk_table_relid, bool *created);
extern TSDLLEXPORT Chunk *ts_chunk_get_compressed_chunk_parent(const Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_is_unordered(const Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_is_compressed(const Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_is_uncompressed_or_unordered(const Chunk *chunk);

extern TSDLLEXPORT bool ts_chunk_contains_compressed_data(const Chunk *chunk);
extern TSDLLEXPORT ChunkCompressionStatus ts_chunk_get_compression_status(int32 chunk_id);
extern TSDLLEXPORT Datum ts_chunk_id_from_relid(PG_FUNCTION_ARGS);
extern TSDLLEXPORT List *ts_chunk_get_chunk_ids_by_hypertable_id(int32 hypertable_id);
extern TSDLLEXPORT List *ts_chunk_get_data_node_name_list(const Chunk *chunk);
extern bool TSDLLEXPORT ts_chunk_has_data_node(const Chunk *chunk, const char *node_name);
extern List *ts_chunk_data_nodes_copy(const Chunk *chunk);
extern TSDLLEXPORT Chunk *ts_chunk_create_only_table(Hypertable *ht, Hypercube *cube,
													 const char *schema_name,
													 const char *table_name);

extern TSDLLEXPORT int64 ts_chunk_primary_dimension_start(const Chunk *chunk);

extern TSDLLEXPORT int64 ts_chunk_primary_dimension_end(const Chunk *chunk);
extern Chunk *ts_chunk_build_from_tuple_and_stub(Chunk **chunkptr, TupleInfo *ti,
												 const ChunkStub *stub);

extern ScanIterator ts_chunk_scan_iterator_create(MemoryContext result_mcxt);
extern void ts_chunk_scan_iterator_set_chunk_id(ScanIterator *it, int32 chunk_id);
extern bool ts_chunk_lock_if_exists(Oid chunk_oid, LOCKMODE chunk_lockmode);
extern int ts_chunk_oid_cmp(const void *p1, const void *p2);

#define chunk_get_by_name(schema_name, table_name, fail_if_not_found)                              \
	ts_chunk_get_by_name_with_memory_context(schema_name,                                          \
											 table_name,                                           \
											 CurrentMemoryContext,                                 \
											 fail_if_not_found)

#define IS_VALID_CHUNK(chunk)                                                                      \
	((chunk) && !(chunk)->fd.dropped && (chunk)->fd.id > 0 && (chunk)->fd.hypertable_id > 0 &&     \
	 OidIsValid((chunk)->table_id) && OidIsValid((chunk)->hypertable_relid) &&                     \
	 (chunk)->constraints && (chunk)->cube &&                                                      \
	 (chunk)->cube->num_slices == (chunk)->constraints->num_dimension_constraints &&               \
	 ((chunk)->relkind == RELKIND_RELATION ||                                                      \
	  ((chunk)->relkind == RELKIND_FOREIGN_TABLE && (chunk)->data_nodes != NIL)))

#define ASSERT_IS_VALID_CHUNK(chunk) Assert(IS_VALID_CHUNK(chunk))

#define ASSERT_IS_NULL_OR_VALID_CHUNK(chunk) Assert(chunk == NULL || IS_VALID_CHUNK(chunk))

#endif /* TIMESCALEDB_CHUNK_H */
