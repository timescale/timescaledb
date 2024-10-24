/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <access/htup.h>
#include <access/tupdesc.h>
#include <foreign/foreign.h>
#include <utils/hsearch.h>

#include "chunk_constraint.h"
#include "export.h"
#include "hypertable.h"
#include "ts_catalog/catalog.h"

#define INVALID_CHUNK_ID 0
#define IS_OSM_CHUNK(chunk) ((chunk)->fd.osm_chunk == true)

/* Should match definitions in ddl_api.sql */
#define DROP_CHUNKS_FUNCNAME "drop_chunks"
#define DROP_CHUNKS_NARGS 6
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

typedef enum ChunkOperation
{
	CHUNK_DROP = 0,
	CHUNK_INSERT,
	CHUNK_UPDATE,
	CHUNK_DELETE,
	CHUNK_SELECT,
	CHUNK_COMPRESS,
	CHUNK_DECOMPRESS,
} ChunkOperation;

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
	Oid amoid; /* Table access method used by chunk */

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
	char relkind; /* Create chunks of this relkind */
	const Hypertable *ht;
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

	/* Used for fast chunk search where we don't want to build chunk stubs. */
	int32 num_dimension_constraints;
} ChunkScanEntry;

/*
 * Information to be able to display a scan key details for error messages.
 */
typedef struct DisplayKeyData
{
	const char *name;
	const char *(*as_string)(Datum);
} DisplayKeyData;

/*
 * Chunk vector is collection of chunks for a given hypertable_id.
 */
typedef struct ChunkVec
{
	uint32 capacity;
	uint32 num_chunks;

	Chunk chunks[FLEXIBLE_ARRAY_MEMBER];
} ChunkVec;

extern ChunkVec *ts_chunk_vec_create(int32 capacity);
extern ChunkVec *ts_chunk_vec_sort(ChunkVec **chunks);
extern ChunkVec *ts_chunk_vec_add_from_tuple(ChunkVec **chunks, TupleInfo *ti);

#define CHUNK_VEC_SIZE(num_chunks) (sizeof(ChunkVec) + sizeof(Chunk) * num_chunks)
#define DEFAULT_CHUNK_VEC_SIZE 10

extern void ts_chunk_formdata_fill(FormData_chunk *fd, const TupleInfo *ti);
extern Chunk *ts_chunk_find_for_point(const Hypertable *ht, const Point *p);
extern Chunk *ts_chunk_create_for_point(const Hypertable *ht, const Point *p, bool *found,
										const char *schema, const char *prefix);
List *ts_chunk_id_find_in_subspace(Hypertable *ht, List *dimension_vecs);

extern TSDLLEXPORT Chunk *ts_chunk_create_base(int32 id, int16 num_constraints, const char relkind);
extern TSDLLEXPORT ChunkStub *ts_chunk_stub_create(int32 id, int16 num_constraints);
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
extern TSDLLEXPORT int32 ts_chunk_get_hypertable_id_by_reloid(Oid reloid);
extern TSDLLEXPORT FormData_chunk ts_chunk_get_formdata(int32 chunk_id);
extern TSDLLEXPORT bool ts_chunk_simple_scan_by_reloid(Oid reloid, FormData_chunk *form,
													   bool missing_ok);
extern TSDLLEXPORT Oid ts_chunk_get_relid(int32 chunk_id, bool missing_ok);
extern Oid ts_chunk_get_schema_id(int32 chunk_id, bool missing_ok);
extern TSDLLEXPORT bool ts_chunk_get_id(const char *schema, const char *table, int32 *chunk_id,
										bool missing_ok);
extern bool ts_chunk_exists_relid(Oid relid);
extern TSDLLEXPORT bool ts_chunk_exists_with_compression(int32 hypertable_id);
extern void ts_chunk_recreate_all_constraints_for_dimension(Hypertable *ht, int32 dimension_id);
extern int ts_chunk_delete_by_hypertable_id(int32 hypertable_id);
extern int ts_chunk_delete_by_name(const char *schema, const char *table, DropBehavior behavior);
extern bool ts_chunk_set_name(Chunk *chunk, const char *newname);
extern bool ts_chunk_set_schema(Chunk *chunk, const char *newschema);
extern TSDLLEXPORT List *ts_chunk_get_window(int32 dimension_id, int64 point, int count,
											 MemoryContext mctx);
extern void ts_chunks_rename_schema_name(char *old_schema, char *new_schema);

extern TSDLLEXPORT bool ts_chunk_set_partial(Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_set_unordered(Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_set_frozen(Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_unset_frozen(Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_is_frozen(Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_set_compressed_chunk(Chunk *chunk, int32 compressed_chunk_id);
extern TSDLLEXPORT bool ts_chunk_clear_compressed_chunk(Chunk *chunk);
extern TSDLLEXPORT void ts_chunk_drop(const Chunk *chunk, DropBehavior behavior, int32 log_level);
extern TSDLLEXPORT void ts_chunk_drop_preserve_catalog_row(const Chunk *chunk,
														   DropBehavior behavior, int32 log_level);
extern TSDLLEXPORT List *ts_chunk_do_drop_chunks(Hypertable *ht, int64 older_than, int64 newer_than,
												 int32 log_level, Oid time_type, Oid arg_type,
												 bool older_newer);
extern TSDLLEXPORT Chunk *
ts_chunk_find_or_create_without_cuts(const Hypertable *ht, Hypercube *hc, const char *schema_name,
									 const char *table_name, Oid chunk_table_relid, bool *created);
extern TSDLLEXPORT Chunk *ts_chunk_get_compressed_chunk_parent(const Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_is_unordered(const Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_is_partial(const Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_is_compressed(const Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_needs_recompression(const Chunk *chunk);
extern TSDLLEXPORT bool ts_chunk_validate_chunk_status_for_operation(const Chunk *chunk,
																	 ChunkOperation cmd,
																	 bool throw_error);

extern TSDLLEXPORT bool ts_chunk_contains_compressed_data(const Chunk *chunk);
extern TSDLLEXPORT ChunkCompressionStatus ts_chunk_get_compression_status(int32 chunk_id);
extern TSDLLEXPORT Datum ts_chunk_id_from_relid(PG_FUNCTION_ARGS);
extern TSDLLEXPORT List *ts_chunk_get_chunk_ids_by_hypertable_id(int32 hypertable_id);
extern TSDLLEXPORT List *ts_chunk_get_by_hypertable_id(int32 hypertable_id);

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
int ts_chunk_get_osm_chunk_id(int hypertable_id);
extern TSDLLEXPORT void ts_chunk_merge_on_dimension(const Hypertable *ht, Chunk *chunk,
													const Chunk *merge_chunk, int32 dimension_id);

#define chunk_get_by_name(schema_name, table_name, fail_if_not_found)                              \
	ts_chunk_get_by_name_with_memory_context(schema_name,                                          \
											 table_name,                                           \
											 CurrentMemoryContext,                                 \
											 fail_if_not_found)

/*
 * Sanity checks for chunk.
 *
 * The individual checks are split into separate Asserts so it's
 * easier to tell from a stacktrace which one failed.
 */
#define ASSERT_IS_VALID_CHUNK(chunk)                                                               \
	do                                                                                             \
	{                                                                                              \
		Assert(chunk);                                                                             \
		Assert(!(chunk)->fd.dropped);                                                              \
		Assert((chunk)->fd.id > 0);                                                                \
		Assert((chunk)->fd.hypertable_id > 0);                                                     \
		Assert(OidIsValid((chunk)->table_id));                                                     \
		Assert(OidIsValid((chunk)->hypertable_relid));                                             \
		Assert((chunk)->constraints);                                                              \
		Assert((chunk)->cube);                                                                     \
		Assert((chunk)->cube->num_slices == (chunk)->constraints->num_dimension_constraints);      \
		Assert((chunk)->relkind == RELKIND_RELATION || (chunk)->relkind == RELKIND_FOREIGN_TABLE); \
	} while (0)

/*
 * The chunk status field values are persisted in the database and must never be changed.
 * Those values are used as flags and must always be powers of 2 to allow bitwise operations.
 * When adding new status values we must make sure to add special handling for these values
 * to the downgrade script as previous versions will not know how to deal with those.
 */
#define CHUNK_STATUS_DEFAULT 0
/*
 * Setting a Data-Node chunk as CHUNK_STATUS_COMPRESSED means that the corresponding
 * compressed_chunk_id field points to a chunk that holds the compressed data. Otherwise,
 * the corresponding compressed_chunk_id is NULL.
 *
 * However, for Access-Nodes compressed_chunk_id is always NULL. CHUNK_STATUS_COMPRESSED being set
 * means that a remote compress_chunk() operation has taken place for this distributed
 * meta-chunk. On the other hand, if CHUNK_STATUS_COMPRESSED is cleared, then it is probable
 * that a remote compress_chunk() has not taken place, but not certain.
 *
 * For the above reason, this flag should not be assumed to be consistent (when it is cleared)
 * for Access-Nodes. When used in distributed hypertables one should take advantage of the
 * idempotent properties of remote compress_chunk() and distributed compression policy to
 * make progress.
 */
#define CHUNK_STATUS_COMPRESSED 1
/*
 * When inserting into a compressed chunk the configured compress_orderby is not retained.
 * Any such chunks need an explicit Sort step to produce ordered output until the chunk
 * ordering has been restored by recompress_chunk. This flag can only exist on compressed
 * chunks.
 */
#define CHUNK_STATUS_COMPRESSED_UNORDERED 2
/*
 * A chunk is in frozen state (i.e no inserts/updates/deletes into this chunk are
 * permitted. Other chunk level operations like dropping chunk etc. are also blocked.
 *
 */
#define CHUNK_STATUS_FROZEN 4
/*
 * A chunk is in this state when it is compressed but also has uncompressed tuples
 * in the uncompressed chunk.
 */
#define CHUNK_STATUS_COMPRESSED_PARTIAL 8

extern TSDLLEXPORT bool ts_chunk_clear_status(Chunk *chunk, int32 status);
extern bool ts_osm_chunk_range_is_invalid(int64 range_start, int64 range_end);
extern int32 ts_chunk_get_osm_slice_id(int32 chunk_id, int32 time_dim_id);
