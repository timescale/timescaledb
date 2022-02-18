/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_class.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>
#include <catalog/indexing.h>
#include <catalog/pg_inherits.h>
#include <catalog/toasting.h>
#include <commands/trigger.h>
#include <commands/tablecmds.h>
#include <commands/defrem.h>
#include <tcop/tcopprot.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <access/reloptions.h>
#include <nodes/makefuncs.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/hsearch.h>
#include <storage/lmgr.h>
#include <miscadmin.h>
#include <funcapi.h>
#include <fmgr.h>
#include <utils/datum.h>
#include <catalog/pg_type.h>
#include <utils/acl.h>
#include <utils/timestamp.h>
#include <nodes/execnodes.h>
#include <executor/executor.h>
#include <access/tupdesc.h>

#include "export.h"
#include "debug_point.h"
#include "chunk.h"
#include "chunk_index.h"
#include "ts_catalog/chunk_data_node.h"
#include "cross_module_fn.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_agg.h"
#include "cross_module_fn.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "errors.h"
#include "partitioning.h"
#include "hypertable.h"
#include "ts_catalog/hypertable_data_node.h"
#include "hypercube.h"
#include "scanner.h"
#include "process_utility.h"
#include "time_utils.h"
#include "trigger.h"
#include "compat/compat.h"
#include "utils.h"
#include "hypertable_cache.h"
#include "cache.h"
#include "bgw_policy/chunk_stats.h"
#include "scan_iterator.h"
#include "ts_catalog/compression_chunk_size.h"
#include "extension.h"
#include "chunk_scan.h"

TS_FUNCTION_INFO_V1(ts_chunk_show_chunks);
TS_FUNCTION_INFO_V1(ts_chunk_drop_chunks);
TS_FUNCTION_INFO_V1(ts_chunks_in);
TS_FUNCTION_INFO_V1(ts_chunk_id_from_relid);
TS_FUNCTION_INFO_V1(ts_chunk_show);
TS_FUNCTION_INFO_V1(ts_chunk_create);
TS_FUNCTION_INFO_V1(ts_chunk_status);

static const char *
DatumGetNameString(Datum datum)
{
	Name name = DatumGetName(datum);
	return pstrdup(NameStr(*name));
}

/* Used when processing scanned chunks */
typedef enum ChunkResult
{
	CHUNK_DONE,
	CHUNK_IGNORED,
	CHUNK_PROCESSED
} ChunkResult;

/*
 * Context for scanning and building a chunk from a stub.
 *
 * If found, the chunk will be created and the chunk pointer member is set in
 * the result. Optionally, a caller can pre-allocate the chunk member's memory,
 * which is useful if one, e.g., wants to fill in an memory-aligned array of
 * chunks.
 *
 * If the chunk is a tombstone (dropped flag set), then the Chunk will not be
 * created and instead is_dropped will be TRUE.
 */
typedef struct ChunkStubScanCtx
{
	ChunkStub *stub;
	Chunk *chunk;
	bool is_dropped;
} ChunkStubScanCtx;

static bool
chunk_stub_is_valid(const ChunkStub *stub, unsigned int expected_slices)
{
	return stub && stub->id > 0 && stub->constraints && expected_slices == stub->cube->num_slices &&
		   stub->cube->num_slices == stub->constraints->num_dimension_constraints;
}

typedef ChunkResult (*on_chunk_stub_func)(ChunkScanCtx *ctx, ChunkStub *stub);
static void chunk_scan_ctx_init(ChunkScanCtx *ctx, const Hyperspace *hs, const Point *p);
static void chunk_scan_ctx_destroy(ChunkScanCtx *ctx);
static void chunk_collision_scan(ChunkScanCtx *scanctx, const Hypercube *cube);
static int chunk_scan_ctx_foreach_chunk_stub(ChunkScanCtx *ctx, on_chunk_stub_func on_chunk,
											 uint16 limit);
static Datum chunks_return_srf(FunctionCallInfo fcinfo);
static int chunk_cmp(const void *ch1, const void *ch2);
static Chunk *chunk_find(const Hypertable *ht, const Point *p, bool resurrect, bool lock_slices);
static void init_scan_by_qualified_table_name(ScanIterator *iterator, const char *schema_name,
											  const char *table_name);
static Hypertable *find_hypertable_from_table_or_cagg(Cache *hcache, Oid relid, bool allow_matht);
static Chunk *get_chunks_in_time_range(Hypertable *ht, int64 older_than, int64 newer_than,
									   const char *caller_name, MemoryContext mctx,
									   uint64 *num_chunks_returned, ScanTupLock *tuplock);

/*
 * The chunk status field values are persisted in the database and must never be changed.
 * Those values are used as flags and must always be powers of 2 to allow bitwise operations.
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

static HeapTuple
chunk_formdata_make_tuple(const FormData_chunk *fd, TupleDesc desc)
{
	Datum values[Natts_chunk];
	bool nulls[Natts_chunk] = { false };

	memset(values, 0, sizeof(Datum) * Natts_chunk);

	values[AttrNumberGetAttrOffset(Anum_chunk_id)] = Int32GetDatum(fd->id);
	values[AttrNumberGetAttrOffset(Anum_chunk_hypertable_id)] = Int32GetDatum(fd->hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_schema_name)] = NameGetDatum(&fd->schema_name);
	values[AttrNumberGetAttrOffset(Anum_chunk_table_name)] = NameGetDatum(&fd->table_name);
	/*when we insert a chunk the compressed chunk id is always NULL */
	if (fd->compressed_chunk_id == INVALID_CHUNK_ID)
		nulls[AttrNumberGetAttrOffset(Anum_chunk_compressed_chunk_id)] = true;
	else
	{
		values[AttrNumberGetAttrOffset(Anum_chunk_compressed_chunk_id)] =
			Int32GetDatum(fd->compressed_chunk_id);
	}
	values[AttrNumberGetAttrOffset(Anum_chunk_dropped)] = BoolGetDatum(fd->dropped);
	values[AttrNumberGetAttrOffset(Anum_chunk_status)] = Int32GetDatum(fd->status);

	return heap_form_tuple(desc, values, nulls);
}

void
ts_chunk_formdata_fill(FormData_chunk *fd, const TupleInfo *ti)
{
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	bool nulls[Natts_chunk];
	Datum values[Natts_chunk];

	memset(fd, 0, sizeof(FormData_chunk));
	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	Assert(!nulls[AttrNumberGetAttrOffset(Anum_chunk_id)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_chunk_hypertable_id)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_chunk_schema_name)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_chunk_table_name)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_chunk_dropped)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_chunk_status)]);

	fd->id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_id)]);
	fd->hypertable_id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_hypertable_id)]);
	memcpy(&fd->schema_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_chunk_schema_name)]),
		   NAMEDATALEN);
	memcpy(&fd->table_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_chunk_table_name)]),
		   NAMEDATALEN);

	if (nulls[AttrNumberGetAttrOffset(Anum_chunk_compressed_chunk_id)])
		fd->compressed_chunk_id = INVALID_CHUNK_ID;
	else
		fd->compressed_chunk_id =
			DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_compressed_chunk_id)]);

	fd->dropped = DatumGetBool(values[AttrNumberGetAttrOffset(Anum_chunk_dropped)]);
	fd->status = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_status)]);

	if (should_free)
		heap_freetuple(tuple);
}

int64
ts_chunk_primary_dimension_start(const Chunk *chunk)
{
	return chunk->cube->slices[0]->fd.range_start;
}

int64
ts_chunk_primary_dimension_end(const Chunk *chunk)
{
	return chunk->cube->slices[0]->fd.range_end;
}

static void
chunk_insert_relation(Relation rel, const Chunk *chunk)
{
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;

	new_tuple = chunk_formdata_make_tuple(&chunk->fd, RelationGetDescr(rel));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert(rel, new_tuple);
	ts_catalog_restore_user(&sec_ctx);

	heap_freetuple(new_tuple);
}

void
ts_chunk_insert_lock(const Chunk *chunk, LOCKMODE lock)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;

	rel = table_open(catalog_get_table_id(catalog, CHUNK), lock);
	chunk_insert_relation(rel, chunk);
	table_close(rel, lock);
}

typedef struct CollisionInfo
{
	Hypercube *cube;
	ChunkStub *colliding_chunk;
} CollisionInfo;

/*-
 * Align a chunk's hypercube in 'aligned' dimensions.
 *
 * Alignment ensures that chunks line up in a particular dimension, i.e., their
 * ranges should either be identical or not overlap at all.
 *
 * Non-aligned:
 *
 * ' [---------]      <- existing slice
 * '      [---------] <- calculated (new) slice
 *
 * To align the slices above there are two cases depending on where the
 * insertion point happens:
 *
 * Case 1 (reuse slice):
 *
 * ' [---------]
 * '      [--x------]
 *
 * The insertion point x falls within the range of the existing slice. We should
 * reuse the existing slice rather than creating a new one.
 *
 * Case 2 (cut to align):
 *
 * ' [---------]
 * '      [-------x-]
 *
 * The insertion point falls outside the range of the existing slice and we need
 * to cut the new slice to line up.
 *
 * ' [---------]
 * '        cut [---]
 * '
 *
 * Note that slice reuse (case 1) happens already when calculating the tentative
 * hypercube for the chunk, and is thus already performed once reaching this
 * function. Thus, we deal only with case 2 here. Also note that a new slice
 * might overlap in complicated ways, requiring multiple cuts. For instance,
 * consider the following situation:
 *
 * ' [------]   [-] [---]
 * '      [---x-------]  <- calculated slice
 *
 * This should but cut-to-align as follows:
 *
 * ' [------]   [-] [---]
 * '         [x]
 *
 * After a chunk collision scan, this function is called for each chunk in the
 * chunk scan context. Chunks in the scan context may have only a partial set of
 * slices if they only overlap in some, but not all, dimensions (see
 * illustrations below). Still, partial chunks may still be of interest for
 * alignment in a particular dimension. Thus, if a chunk has an overlapping
 * slice in an aligned dimension, we cut to not overlap with that slice.
 */
static ChunkResult
do_dimension_alignment(ChunkScanCtx *scanctx, ChunkStub *stub)
{
	CollisionInfo *info = scanctx->data;
	Hypercube *cube = info->cube;
	const Hyperspace *space = scanctx->space;
	ChunkResult res = CHUNK_IGNORED;
	int i;

	for (i = 0; i < space->num_dimensions; i++)
	{
		const Dimension *dim = &space->dimensions[i];
		const DimensionSlice *chunk_slice;
		DimensionSlice *cube_slice;
		int64 coord = scanctx->point->coordinates[i];

		if (!dim->fd.aligned)
			continue;

		/*
		 * The stub might not have a slice for each dimension, so we cannot
		 * use array indexing. Fetch slice by dimension ID instead.
		 */
		chunk_slice = ts_hypercube_get_slice_by_dimension_id(stub->cube, dim->fd.id);

		if (NULL == chunk_slice)
			continue;

		cube_slice = cube->slices[i];

		/*
		 * Only cut-to-align if the slices collide and are not identical
		 * (i.e., if we are reusing an existing slice we should not cut it)
		 */
		if (!ts_dimension_slices_equal(cube_slice, chunk_slice) &&
			ts_dimension_slices_collide(cube_slice, chunk_slice))
		{
			ts_dimension_slice_cut(cube_slice, chunk_slice, coord);
			res = CHUNK_PROCESSED;
		}
	}

	return res;
}

/*
 * Calculate, and potentially set, a new chunk interval for an open dimension.
 */
static bool
calculate_and_set_new_chunk_interval(const Hypertable *ht, const Point *p)
{
	Hyperspace *hs = ht->space;
	Dimension *dim = NULL;
	Datum datum;
	int64 chunk_interval, coord;
	int i;

	if (!OidIsValid(ht->chunk_sizing_func) || ht->fd.chunk_target_size <= 0)
		return false;

	/* Find first open dimension */
	for (i = 0; i < hs->num_dimensions; i++)
	{
		dim = &hs->dimensions[i];

		if (IS_OPEN_DIMENSION(dim))
			break;

		dim = NULL;
	}

	/* Nothing to do if no open dimension */
	if (NULL == dim)
	{
		elog(WARNING,
			 "adaptive chunking enabled on hypertable \"%s\" without an open (time) dimension",
			 get_rel_name(ht->main_table_relid));

		return false;
	}

	coord = p->coordinates[i];
	datum = OidFunctionCall3(ht->chunk_sizing_func,
							 Int32GetDatum(dim->fd.id),
							 Int64GetDatum(coord),
							 Int64GetDatum(ht->fd.chunk_target_size));
	chunk_interval = DatumGetInt64(datum);

	/* Check if the function didn't set and interval or nothing changed */
	if (chunk_interval <= 0 || chunk_interval == dim->fd.interval_length)
		return false;

	/* Update the dimension */
	ts_dimension_set_chunk_interval(dim, chunk_interval);

	return true;
}

/*
 * Resolve chunk collisions.
 *
 * After a chunk collision scan, this function is called for each chunk in the
 * chunk scan context. We only care about chunks that have a full set of
 * slices/constraints that overlap with our tentative hypercube, i.e., they
 * fully collide. We resolve those collisions by cutting the hypercube.
 */
static ChunkResult
do_collision_resolution(ChunkScanCtx *scanctx, ChunkStub *stub)
{
	CollisionInfo *info = scanctx->data;
	Hypercube *cube = info->cube;
	const Hyperspace *space = scanctx->space;
	ChunkResult res = CHUNK_IGNORED;
	int i;

	if (stub->cube->num_slices != space->num_dimensions || !ts_hypercubes_collide(cube, stub->cube))
		return CHUNK_IGNORED;

	for (i = 0; i < space->num_dimensions; i++)
	{
		DimensionSlice *cube_slice = cube->slices[i];
		DimensionSlice *chunk_slice = stub->cube->slices[i];
		int64 coord = scanctx->point->coordinates[i];

		/*
		 * Only cut if we aren't reusing an existing slice and there is a
		 * collision
		 */
		if (!ts_dimension_slices_equal(cube_slice, chunk_slice) &&
			ts_dimension_slices_collide(cube_slice, chunk_slice))
		{
			ts_dimension_slice_cut(cube_slice, chunk_slice, coord);
			res = CHUNK_PROCESSED;

			/*
			 * Redo the collision check after each cut since cutting in one
			 * dimension might have resolved the collision in another
			 */
			if (!ts_hypercubes_collide(cube, stub->cube))
				return res;
		}
	}

	Assert(!ts_hypercubes_collide(cube, stub->cube));

	return res;
}

static ChunkResult
check_for_collisions(ChunkScanCtx *scanctx, ChunkStub *stub)
{
	CollisionInfo *info = scanctx->data;
	Hypercube *cube = info->cube;
	const Hyperspace *space = scanctx->space;

	/* Check if this chunk collides with our hypercube */
	if (stub->cube->num_slices == space->num_dimensions && ts_hypercubes_collide(cube, stub->cube))
	{
		info->colliding_chunk = stub;
		return CHUNK_DONE;
	}

	return CHUNK_IGNORED;
}

/*
 * Check if a (tentative) chunk collides with existing chunks.
 *
 * Return the colliding chunk. Note that the chunk is a stub and not a full
 * chunk.
 */
static ChunkStub *
chunk_collides(const Hypertable *ht, const Hypercube *hc)
{
	ChunkScanCtx scanctx;
	CollisionInfo info = {
		.cube = (Hypercube *) hc,
		.colliding_chunk = NULL,
	};

	chunk_scan_ctx_init(&scanctx, ht->space, NULL);

	/* Scan for all chunks that collide with the hypercube of the new chunk */
	chunk_collision_scan(&scanctx, hc);
	scanctx.data = &info;

	/* Find chunks that collide */
	chunk_scan_ctx_foreach_chunk_stub(&scanctx, check_for_collisions, 0);

	chunk_scan_ctx_destroy(&scanctx);

	return info.colliding_chunk;
}

/*-
 * Resolve collisions and perform alignmment.
 *
 * Chunks collide only if their hypercubes overlap in all dimensions. For
 * instance, the 2D chunks below collide because they overlap in both the X and
 * Y dimensions:
 *
 * ' _____
 * ' |    |
 * ' | ___|__
 * ' |_|__|  |
 * '   |     |
 * '   |_____|
 *
 * While the following chunks do not collide, although they still overlap in the
 * X dimension:
 *
 * ' _____
 * ' |    |
 * ' |    |
 * ' |____|
 * '   ______
 * '   |     |
 * '   |    *|
 * '   |_____|
 *
 * For the collision case above we obviously want to cut our hypercube to no
 * longer collide with existing chunks. However, the second case might still be
 * of interest for alignment in case X is an 'aligned' dimension. If '*' is the
 * insertion point, then we still want to cut the hypercube to ensure that the
 * dimension remains aligned, like so:
 *
 * ' _____
 * ' |    |
 * ' |    |
 * ' |____|
 * '       ___
 * '       | |
 * '       |*|
 * '       |_|
 *
 *
 * We perform alignment first as that might actually resolve chunk
 * collisions. After alignment we check for any remaining collisions.
 */
static void
chunk_collision_resolve(const Hypertable *ht, Hypercube *cube, const Point *p)
{
	ChunkScanCtx scanctx;
	CollisionInfo info = {
		.cube = cube,
		.colliding_chunk = NULL,
	};

	chunk_scan_ctx_init(&scanctx, ht->space, p);

	/* Scan for all chunks that collide with the hypercube of the new chunk */
	chunk_collision_scan(&scanctx, cube);
	scanctx.data = &info;

	/* Cut the hypercube in any aligned dimensions */
	chunk_scan_ctx_foreach_chunk_stub(&scanctx, do_dimension_alignment, 0);

	/*
	 * If there are any remaining collisions with chunks, then cut-to-fit to
	 * resolve those collisions
	 */
	chunk_scan_ctx_foreach_chunk_stub(&scanctx, do_collision_resolution, 0);

	chunk_scan_ctx_destroy(&scanctx);
}

static int
chunk_add_constraints(const Chunk *chunk)
{
	int num_added;

	num_added = ts_chunk_constraints_add_dimension_constraints(chunk->constraints,
															   chunk->fd.id,
															   chunk->cube);
	num_added += ts_chunk_constraints_add_inheritable_constraints(chunk->constraints,
																  chunk->fd.id,
																  chunk->relkind,
																  chunk->hypertable_relid);

	return num_added;
}

/* applies the attributes and statistics target for columns on the hypertable
   to columns on the chunk */
static void
set_attoptions(Relation ht_rel, Oid chunk_oid)
{
	TupleDesc tupleDesc = RelationGetDescr(ht_rel);
	int natts = tupleDesc->natts;
	int attno;

	for (attno = 1; attno <= natts; attno++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDesc, attno - 1);
		char *attributeName = NameStr(attribute->attname);
		HeapTuple tuple;
		Datum options;
		bool isnull;

		/* Ignore dropped */
		if (attribute->attisdropped)
			continue;

		tuple = SearchSysCacheAttName(RelationGetRelid(ht_rel), attributeName);

		Assert(tuple != NULL);

		/*
		 * Pass down the attribute options (ALTER TABLE ALTER COLUMN SET
		 * attribute_option)
		 */
		options = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attoptions, &isnull);

		if (!isnull)
		{
			AlterTableCmd *cmd = makeNode(AlterTableCmd);

			cmd->subtype = AT_SetOptions;
			cmd->name = attributeName;
			cmd->def = (Node *) untransformRelOptions(options);
			AlterTableInternal(chunk_oid, list_make1(cmd), false);
		}

		/*
		 * Pass down the attribute options (ALTER TABLE ALTER COLUMN SET
		 * STATISTICS)
		 */
		options = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attstattarget, &isnull);
		if (!isnull)
		{
			int32 target = DatumGetInt32(options);

			/* Don't do anything if it's set to the default */
			if (target != -1)
			{
				AlterTableCmd *cmd = makeNode(AlterTableCmd);

				cmd->subtype = AT_SetStatistics;
				cmd->name = attributeName;
				cmd->def = (Node *) makeInteger(target);
				AlterTableInternal(chunk_oid, list_make1(cmd), false);
			}
		}

		ReleaseSysCache(tuple);
	}
}

static void
create_toast_table(CreateStmt *stmt, Oid chunk_oid)
{
	/* similar to tcop/utility.c */
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	Datum toast_options =
		transformRelOptions((Datum) 0, stmt->options, "toast", validnsps, true, false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	NewRelationCreateToastTable(chunk_oid, toast_options);
}

/*
 * Get the access method name for a relation.
 */
static char *
get_am_name_for_rel(Oid relid)
{
	HeapTuple tuple;
	Form_pg_class cform;
	Oid amoid;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	cform = (Form_pg_class) GETSTRUCT(tuple);
	amoid = cform->relam;
	ReleaseSysCache(tuple);

	return get_am_name(amoid);
}

static void
copy_hypertable_acl_to_relid(const Hypertable *ht, Oid owner_id, Oid relid)
{
	HeapTuple ht_tuple;
	bool is_null;
	Datum acl_datum;
	Relation class_rel;

	/* We open it here since there is no point in trying to update the tuples
	 * if we cannot open the Relation catalog table */
	class_rel = table_open(RelationRelationId, RowExclusiveLock);

	ht_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(ht->main_table_relid));
	Assert(HeapTupleIsValid(ht_tuple));

	/* We only bother about setting the chunk ACL if the hypertable ACL is
	 * non-null */
	acl_datum = SysCacheGetAttr(RELOID, ht_tuple, Anum_pg_class_relacl, &is_null);
	if (!is_null)
	{
		HeapTuple chunk_tuple, newtuple;
		Datum new_val[Natts_pg_class] = { 0 };
		bool new_null[Natts_pg_class] = { false };
		bool new_repl[Natts_pg_class] = { false };
		Acl *acl = DatumGetAclP(acl_datum);

		new_repl[Anum_pg_class_relacl - 1] = true;
		new_val[Anum_pg_class_relacl - 1] = PointerGetDatum(acl);

		/* Find the tuple for the chunk in `pg_class` */
		chunk_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
		Assert(HeapTupleIsValid(chunk_tuple));

		/* Update the relacl for the chunk tuple to use the acl from the hypertable */
		newtuple = heap_modify_tuple(chunk_tuple,
									 RelationGetDescr(class_rel),
									 new_val,
									 new_null,
									 new_repl);
		CatalogTupleUpdate(class_rel, &newtuple->t_self, newtuple);

		/* We need to update the shared dependencies as well to indicate that
		 * the chunk is dependent on any roles that the hypertable is
		 * dependent on. */
		Oid *newmembers;
		int nnewmembers = aclmembers(acl, &newmembers);

		/* The list of old members is intentionally empty since we are using
		 * updateAclDependencies to set the ACL for the chunk. We can use NULL
		 * because getOidListDiff, which is called from updateAclDependencies,
		 * can handle that. */
		updateAclDependencies(RelationRelationId,
							  relid,
							  0,
							  owner_id,
							  0,
							  NULL,
							  nnewmembers,
							  newmembers);

		heap_freetuple(newtuple);
		ReleaseSysCache(chunk_tuple);
	}

	ReleaseSysCache(ht_tuple);
	table_close(class_rel, RowExclusiveLock);
}

/*
 * Create a chunk's table.
 *
 * A chunk inherits from the main hypertable and will have the same owner. Since
 * chunks can be created either in the TimescaleDB internal schema or in a
 * user-specified schema, some care has to be taken to use the right
 * permissions, depending on the case:
 *
 * 1. if the chunk is created in the internal schema, we create it as the
 * catalog/schema owner (i.e., anyone can create chunks there via inserting into
 * a hypertable, but can not do it via CREATE TABLE).
 *
 * 2. if the chunk is created in a user-specified "associated schema", then we
 * shouldn't use the catalog owner to create the table since that typically
 * implies super-user permissions. If we would allow that, anyone can specify
 * someone else's schema in create_hypertable() and create chunks in it without
 * having the proper permissions to do so. With this logic, the hypertable owner
 * must have permissions to create tables in the associated schema, or else
 * table creation will fail. If the schema doesn't yet exist, the table owner
 * instead needs the proper permissions on the database to create the schema.
 */
Oid
ts_chunk_create_table(const Chunk *chunk, const Hypertable *ht, const char *tablespacename)
{
	Relation rel;
	ObjectAddress objaddr;
	int sec_ctx;

	/*
	 * The CreateForeignTableStmt embeds a regular CreateStmt, so we can use
	 * it to create both regular and foreign tables
	 */
	CreateForeignTableStmt stmt = {
		.base.type = T_CreateStmt,
		.base.relation = makeRangeVar((char *) NameStr(chunk->fd.schema_name),
									  (char *) NameStr(chunk->fd.table_name),
									  0),
		.base.inhRelations = list_make1(makeRangeVar((char *) NameStr(ht->fd.schema_name),
													 (char *) NameStr(ht->fd.table_name),
													 0)),
		.base.tablespacename = tablespacename ? (char *) tablespacename : NULL,
		/* Propagate storage options of the main table to a regular chunk
		 * table, but avoid using it for a foreign chunk table. */
		.base.options =
			(chunk->relkind == RELKIND_RELATION) ? ts_get_reloptions(ht->main_table_relid) : NIL,
		.base.accessMethod = (chunk->relkind == RELKIND_RELATION) ?
								 get_am_name_for_rel(chunk->hypertable_relid) :
								 NULL,
	};
	Oid uid, saved_uid;

	Assert(chunk->hypertable_relid == ht->main_table_relid);

	rel = table_open(ht->main_table_relid, AccessShareLock);

	/*
	 * If the chunk is created in the internal schema, become the catalog
	 * owner, otherwise become the hypertable owner
	 */
	if (namestrcmp((Name) &chunk->fd.schema_name, INTERNAL_SCHEMA_NAME) == 0)
		uid = ts_catalog_database_info_get()->owner_uid;
	else
		uid = rel->rd_rel->relowner;

	GetUserIdAndSecContext(&saved_uid, &sec_ctx);

	if (uid != saved_uid)
		SetUserIdAndSecContext(uid, sec_ctx | SECURITY_LOCAL_USERID_CHANGE);

	objaddr = DefineRelation(&stmt.base, chunk->relkind, rel->rd_rel->relowner, NULL, NULL);

	/* Make the newly defined relation visible so that we can update the
	 * ACL. */
	CommandCounterIncrement();

	/* Copy acl from hypertable to chunk relation record */
	copy_hypertable_acl_to_relid(ht, rel->rd_rel->relowner, objaddr.objectId);

	if (chunk->relkind == RELKIND_RELATION)
	{
		/*
		 * need to create a toast table explicitly for some of the option
		 * setting to work
		 */
		create_toast_table(&stmt.base, objaddr.objectId);

		if (uid != saved_uid)
			SetUserIdAndSecContext(saved_uid, sec_ctx);
	}
	else if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		ChunkDataNode *cdn;

		if (list_length(chunk->data_nodes) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
					 (errmsg("no data nodes associated with chunk \"%s\"",
							 get_rel_name(chunk->table_id)))));

		/*
		 * Use the first chunk data node as the "primary" to put in the foreign
		 * table
		 */
		cdn = linitial(chunk->data_nodes);
		stmt.base.type = T_CreateForeignServerStmt;
		stmt.servername = NameStr(cdn->fd.node_name);

		/* Create the foreign table catalog information */
		CreateForeignTable(&stmt, objaddr.objectId);

		/*
		 * Need to restore security context to execute remote commands as the
		 * original user
		 */
		if (uid != saved_uid)
			SetUserIdAndSecContext(saved_uid, sec_ctx);

		/* Create the corresponding chunk replicas on the remote data nodes */
		ts_cm_functions->create_chunk_on_data_nodes(chunk, ht, NULL, NIL);

		/* Record the remote data node chunk ID mappings */
		ts_chunk_data_node_insert_multi(chunk->data_nodes);
	}
	else
		elog(ERROR, "invalid relkind \"%c\" when creating chunk", chunk->relkind);

	set_attoptions(rel, objaddr.objectId);

	table_close(rel, AccessShareLock);

	return objaddr.objectId;
}

static List *
chunk_assign_data_nodes(const Chunk *chunk, const Hypertable *ht)
{
	List *htnodes;
	List *chunk_data_nodes = NIL;
	ListCell *lc;

	if (chunk->relkind != RELKIND_FOREIGN_TABLE)
		return NIL;

	if (ht->data_nodes == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_INSUFFICIENT_NUM_DATA_NODES),
				 (errmsg("no data nodes associated with hypertable \"%s\"",
						 get_rel_name(ht->main_table_relid)))));

	Assert(chunk->cube != NULL);

	htnodes = ts_hypertable_assign_chunk_data_nodes(ht, chunk->cube);
	Assert(htnodes != NIL);

	foreach (lc, htnodes)
	{
		HypertableDataNode *htnode = lfirst(lc);
		ForeignServer *foreign_server =
			GetForeignServerByName(NameStr(htnode->fd.node_name), false);
		ChunkDataNode *chunk_data_node = palloc0(sizeof(ChunkDataNode));

		/*
		 * Create a stub data node (partially filled in entry). This will be
		 * fully filled in and persisted to metadata tables once we create the
		 * remote tables during insert
		 */
		chunk_data_node->fd.chunk_id = chunk->fd.id;
		chunk_data_node->fd.node_chunk_id = -1;
		namestrcpy(&chunk_data_node->fd.node_name, foreign_server->servername);
		chunk_data_node->foreign_server_oid = foreign_server->serverid;
		chunk_data_nodes = lappend(chunk_data_nodes, chunk_data_node);
	}

	return chunk_data_nodes;
}

List *
ts_chunk_get_data_node_name_list(const Chunk *chunk)
{
	List *datanodes = NULL;
	ListCell *lc;

	foreach (lc, chunk->data_nodes)
	{
		ChunkDataNode *cdn = lfirst(lc);

		datanodes = lappend(datanodes, NameStr(cdn->fd.node_name));
	}

	return datanodes;
}

bool
ts_chunk_has_data_node(const Chunk *chunk, const char *node_name)
{
	ListCell *lc;
	ChunkDataNode *cdn;
	bool found = false;

	if (chunk == NULL || node_name == NULL)
		return false;

	/* check that the chunk is indeed present on the specified data node */
	foreach (lc, chunk->data_nodes)
	{
		cdn = lfirst(lc);
		if (namestrcmp(&cdn->fd.node_name, node_name) == 0)
		{
			found = true;
			break;
		}
	}

	return found;
}

static int32
get_next_chunk_id()
{
	int32 chunk_id;
	CatalogSecurityContext sec_ctx;
	const Catalog *catalog = ts_catalog_get();

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	chunk_id = ts_catalog_table_next_seq_id(catalog, CHUNK);
	ts_catalog_restore_user(&sec_ctx);

	return chunk_id;
}

/*
 * Create a chunk object from the dimensional constraints in the given hypercube.
 *
 * The chunk object is then used to create the actual chunk table and update the
 * metadata separately.
 *
 * The table name for the chunk can be given explicitly, or generated if
 * table_name is NULL. If the table name is generated, it will use the given
 * prefix or, if NULL, use the hypertable's associated table prefix. Similarly,
 * if schema_name is NULL it will use the hypertable's associated schema for
 * the chunk.
 */
static Chunk *
chunk_create_object(const Hypertable *ht, Hypercube *cube, const char *schema_name,
					const char *table_name, const char *prefix, int32 chunk_id)
{
	const Hyperspace *hs = ht->space;
	Chunk *chunk;
	const char relkind = hypertable_chunk_relkind(ht);

	if (NULL == schema_name || schema_name[0] == '\0')
		schema_name = NameStr(ht->fd.associated_schema_name);

	/* Create a new chunk based on the hypercube */
	chunk = ts_chunk_create_base(chunk_id, hs->num_dimensions, relkind);

	chunk->fd.hypertable_id = hs->hypertable_id;
	chunk->cube = cube;
	chunk->hypertable_relid = ht->main_table_relid;
	namestrcpy(&chunk->fd.schema_name, schema_name);

	if (NULL == table_name || table_name[0] == '\0')
	{
		int len;

		if (NULL == prefix)
			prefix = NameStr(ht->fd.associated_table_prefix);

		len = snprintf(chunk->fd.table_name.data, NAMEDATALEN, "%s_%d_chunk", prefix, chunk->fd.id);

		if (len >= NAMEDATALEN)
			elog(ERROR, "chunk table name too long");
	}
	else
		namestrcpy(&chunk->fd.table_name, table_name);

	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
		chunk->data_nodes = chunk_assign_data_nodes(chunk, ht);

	return chunk;
}

static void
chunk_insert_into_metadata_after_lock(const Chunk *chunk)
{
	/* Insert chunk */
	ts_chunk_insert_lock(chunk, RowExclusiveLock);

	/* Add metadata for dimensional and inheritable constraints */
	ts_chunk_constraints_insert_metadata(chunk->constraints);
}

static void
chunk_create_table_constraints(const Chunk *chunk)
{
	/* Create the chunk's constraints, triggers, and indexes */
	ts_chunk_constraints_create(chunk->constraints,
								chunk->table_id,
								chunk->fd.id,
								chunk->hypertable_relid,
								chunk->fd.hypertable_id);

	if (chunk->relkind == RELKIND_RELATION)
	{
		ts_trigger_create_all_on_chunk(chunk);
		ts_chunk_index_create_all(chunk->fd.hypertable_id,
								  chunk->hypertable_relid,
								  chunk->fd.id,
								  chunk->table_id,
								  InvalidOid);
	}
}

static Oid
chunk_create_table(Chunk *chunk, const Hypertable *ht)
{
	/* Create the actual table relation for the chunk */
	const char *tablespace = ts_hypertable_select_tablespace_name(ht, chunk);

	chunk->table_id = ts_chunk_create_table(chunk, ht, tablespace);

	Assert(OidIsValid(chunk->table_id));

	return chunk->table_id;
}

/*
 * Creates only a table for a chunk.
 * Either table name or chunk id needs to be provided.
 */
static Chunk *
chunk_create_only_table_after_lock(const Hypertable *ht, Hypercube *cube, const char *schema_name,
								   const char *table_name, const char *prefix, int32 chunk_id)
{
	Chunk *chunk;

	Assert(table_name != NULL || chunk_id != INVALID_CHUNK_ID);

	chunk = chunk_create_object(ht, cube, schema_name, table_name, prefix, chunk_id);
	Assert(chunk != NULL);

	chunk_create_table(chunk, ht);

	return chunk;
}

static void
chunk_table_drop_inherit(const Chunk *chunk, Hypertable *ht)
{
	AlterTableCmd drop_inh_cmd = {
		.type = T_AlterTableCmd,
		.subtype = AT_DropInherit,
		.def = (Node *) makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), -1),
		.missing_ok = false
	};

	AlterTableInternal(chunk->table_id, list_make1(&drop_inh_cmd), false);
}

/*
 * Checks that given hypercube does not collide with existing chunks and
 * creates an empty table for a chunk without any metadata modifications.
 */
Chunk *
ts_chunk_create_only_table(Hypertable *ht, Hypercube *cube, const char *schema_name,
						   const char *table_name)
{
	ChunkStub *stub;
	Chunk *chunk;
	ScanTupLock tuplock = {
		.lockmode = LockTupleKeyShare,
		.waitpolicy = LockWaitBlock,
	};

	/*
	 * Chunk table can be created if no chunk collides with the dimension slices.
	 */
	stub = chunk_collides(ht, cube);
	if (stub != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_CHUNK_COLLISION),
				 errmsg("chunk table creation failed due to dimension slice collision")));

	/*
	 * Serialize chunk creation around a lock on the "main table" to avoid
	 * multiple processes trying to create the same chunk. We use a
	 * ShareUpdateExclusiveLock, which is the weakest lock possible that
	 * conflicts with itself. The lock needs to be held until transaction end.
	 */
	LockRelationOid(ht->main_table_relid, ShareUpdateExclusiveLock);

	ts_hypercube_find_existing_slices(cube, &tuplock);

	chunk = chunk_create_only_table_after_lock(ht,
											   cube,
											   schema_name,
											   table_name,
											   NULL,
											   INVALID_CHUNK_ID);
	chunk_table_drop_inherit(chunk, ht);

	return chunk;
}

static Chunk *
chunk_create_from_hypercube_after_lock(const Hypertable *ht, Hypercube *cube,
									   const char *schema_name, const char *table_name,
									   const char *prefix)
{
	/* Insert any new dimension slices into metadata */
	ts_dimension_slice_insert_multi(cube->slices, cube->num_slices);

	Chunk *chunk = chunk_create_only_table_after_lock(ht,
													  cube,
													  schema_name,
													  table_name,
													  prefix,
													  get_next_chunk_id());

	chunk_add_constraints(chunk);
	chunk_insert_into_metadata_after_lock(chunk);
	chunk_create_table_constraints(chunk);

	return chunk;
}

/*
 * Make a chunk table inherit a hypertable.
 *
 * Execution happens via high-level ALTER TABLE statement. This includes
 * numerous checks to ensure that the chunk table has all the prerequisites to
 * properly inherit the hypertable.
 */
static void
chunk_add_inheritance(Chunk *chunk, const Hypertable *ht)
{
	AlterTableCmd altercmd = {
		.type = T_AlterTableCmd,
		.subtype = AT_AddInherit,
		.def = (Node *) makeRangeVar((char *) NameStr(ht->fd.schema_name),
									 (char *) NameStr(ht->fd.table_name),
									 0),
		.missing_ok = false,
	};
	AlterTableStmt alterstmt = {
		.type = T_AlterTableStmt,
		.cmds = list_make1(&altercmd),
		.missing_ok = false,
#if PG14_GE
		.objtype = OBJECT_TABLE,
#else
		.relkind = OBJECT_TABLE,
#endif
		.relation = makeRangeVar((char *) NameStr(chunk->fd.schema_name),
								 (char *) NameStr(chunk->fd.table_name),
								 0),
	};
	LOCKMODE lockmode = AlterTableGetLockLevel(alterstmt.cmds);
#if PG13_GE
	AlterTableUtilityContext atcontext = {
		.relid = AlterTableLookupRelation(&alterstmt, lockmode),
	};

	AlterTable(&alterstmt, lockmode, &atcontext);
#else
	AlterTable(AlterTableLookupRelation(&alterstmt, lockmode), lockmode, &alterstmt);
#endif
}

static Chunk *
chunk_create_from_hypercube_and_table_after_lock(const Hypertable *ht, Hypercube *cube,
												 Oid chunk_table_relid, const char *schema_name,
												 const char *table_name, const char *prefix)
{
	Oid current_chunk_schemaid = get_rel_namespace(chunk_table_relid);
	Oid new_chunk_schemaid = InvalidOid;
	Chunk *chunk;

	Assert(OidIsValid(chunk_table_relid));
	Assert(OidIsValid(current_chunk_schemaid));

	/* Insert any new dimension slices into metadata */
	ts_dimension_slice_insert_multi(cube->slices, cube->num_slices);
	chunk = chunk_create_object(ht, cube, schema_name, table_name, prefix, get_next_chunk_id());
	chunk->table_id = chunk_table_relid;
	chunk->hypertable_relid = ht->main_table_relid;
	Assert(OidIsValid(ht->main_table_relid));

	new_chunk_schemaid = get_namespace_oid(NameStr(chunk->fd.schema_name), false);

	if (current_chunk_schemaid != new_chunk_schemaid)
	{
		Relation chunk_rel = table_open(chunk_table_relid, AccessExclusiveLock);
		ObjectAddresses *objects;

		CheckSetNamespace(current_chunk_schemaid, new_chunk_schemaid);
		objects = new_object_addresses();
		AlterTableNamespaceInternal(chunk_rel, current_chunk_schemaid, new_chunk_schemaid, objects);
		free_object_addresses(objects);
		table_close(chunk_rel, NoLock);
		/* Make changes visible */
		CommandCounterIncrement();
	}

	if (namestrcmp(&chunk->fd.table_name, get_rel_name(chunk_table_relid)) != 0)
	{
		/* Renaming will acquire and keep an AccessExclusivelock on the chunk
		 * table */
		RenameRelationInternal(chunk_table_relid, NameStr(chunk->fd.table_name), true, false);
		/* Make changes visible */
		CommandCounterIncrement();
	}

	/* Note that we do not automatically add constrains and triggers to the
	 * chunk table when the chunk is created from an existing table. However,
	 * PostgreSQL currently validates that CHECK constraints exists, but no
	 * validation is done for other objects, including triggers, UNIQUE,
	 * PRIMARY KEY, and FOREIGN KEY constraints. We might want to either
	 * enforce that these constraints exist prior to creating the chunk from a
	 * table, or we ensure that they are automatically added when the chunk is
	 * created. However, for the latter case, we risk duplicating constraints
	 * and triggers if some of them already exist on the chunk table prior to
	 * creating the chunk from it. */
	chunk_add_constraints(chunk);
	chunk_insert_into_metadata_after_lock(chunk);
	chunk_add_inheritance(chunk, ht);
	chunk_create_table_constraints(chunk);

	return chunk;
}

static Chunk *
chunk_create_from_point_after_lock(const Hypertable *ht, const Point *p, const char *schema_name,
								   const char *table_name, const char *prefix)
{
	Hyperspace *hs = ht->space;
	Hypercube *cube;
	ScanTupLock tuplock = {
		.lockmode = LockTupleKeyShare,
		.waitpolicy = LockWaitBlock,
	};

	/*
	 * If the user has enabled adaptive chunking, call the function to
	 * calculate and set the new chunk time interval.
	 */
	calculate_and_set_new_chunk_interval(ht, p);

	/* Calculate the hypercube for a new chunk that covers the tuple's point.
	 *
	 * We lock the tuple in KEY SHARE mode since we are concerned with
	 * ensuring that it is not deleted (or the key value changed) while we are
	 * adding chunk constraints (in `ts_chunk_constraints_insert_metadata`
	 * called in `chunk_create_metadata_after_lock`). The range of a dimension
	 * slice does not change, but we should use the weakest lock possible to
	 * not unnecessarily block other operations. */
	cube = ts_hypercube_calculate_from_point(hs, p, &tuplock);

	/* Resolve collisions with other chunks by cutting the new hypercube */
	chunk_collision_resolve(ht, cube, p);

	return chunk_create_from_hypercube_after_lock(ht, cube, schema_name, table_name, prefix);
}

Chunk *
ts_chunk_find_or_create_without_cuts(const Hypertable *ht, Hypercube *hc, const char *schema_name,
									 const char *table_name, Oid chunk_table_relid, bool *created)
{
	ChunkStub *stub;
	Chunk *chunk = NULL;

	DEBUG_WAITPOINT("find_or_create_chunk_start");

	stub = chunk_collides(ht, hc);

	if (NULL == stub)
	{
		/* Serialize chunk creation around the root hypertable */
		LockRelationOid(ht->main_table_relid, ShareUpdateExclusiveLock);

		/* Check again after lock */
		stub = chunk_collides(ht, hc);

		if (NULL == stub)
		{
			ScanTupLock tuplock = {
				.lockmode = LockTupleKeyShare,
				.waitpolicy = LockWaitBlock,
			};

			/* Lock all slices that already exist to ensure they remain when we
			 * commit since we won't create those slices ourselves. */
			ts_hypercube_find_existing_slices(hc, &tuplock);

			if (OidIsValid(chunk_table_relid))
				chunk = chunk_create_from_hypercube_and_table_after_lock(ht,
																		 hc,
																		 chunk_table_relid,
																		 schema_name,
																		 table_name,
																		 NULL);
			else
				chunk =
					chunk_create_from_hypercube_after_lock(ht, hc, schema_name, table_name, NULL);

			if (NULL != created)
				*created = true;

			ASSERT_IS_VALID_CHUNK(chunk);

			DEBUG_WAITPOINT("find_or_create_chunk_created");

			return chunk;
		}

		/* We didn't need the lock, so release it */
		UnlockRelationOid(ht->main_table_relid, ShareUpdateExclusiveLock);
	}

	Assert(NULL != stub);

	/* We can only use an existing chunk if it has identical dimensional
	 * constraints. Otherwise, throw an error */
	if (!ts_hypercube_equal(stub->cube, hc))
		ereport(ERROR,
				(errcode(ERRCODE_TS_CHUNK_COLLISION),
				 errmsg("chunk creation failed due to collision")));

	/* chunk_collides only returned a stub, so we need to lookup the full
	 * chunk. */
	chunk = ts_chunk_get_by_id(stub->id, true);

	if (NULL != created)
		*created = false;

	DEBUG_WAITPOINT("find_or_create_chunk_found");

	ASSERT_IS_VALID_CHUNK(chunk);

	return chunk;
}

/*
 * Create a chunk through insertion of a tuple at a given point.
 */
Chunk *
ts_chunk_create_from_point(const Hypertable *ht, const Point *p, const char *schema,
						   const char *prefix)
{
	Chunk *chunk;

	/*
	 * Serialize chunk creation around a lock on the "main table" to avoid
	 * multiple processes trying to create the same chunk. We use a
	 * ShareUpdateExclusiveLock, which is the weakest lock possible that
	 * conflicts with itself. The lock needs to be held until transaction end.
	 */
	LockRelationOid(ht->main_table_relid, ShareUpdateExclusiveLock);

	/* Recheck if someone else created the chunk before we got the table
	 * lock. The returned chunk will have all slices locked so that they
	 * aren't removed. */
	chunk = chunk_find(ht, p, true, true);

	if (NULL == chunk)
	{
		if (hypertable_is_distributed_member(ht))
			ereport(ERROR,
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
					 errmsg("distributed hypertable member cannot create chunk on its own"),
					 errhint("Chunk creation should only happen through an access node.")));

		chunk = chunk_create_from_point_after_lock(ht, p, schema, NULL, prefix);
	}
	else
	{
		/* Chunk was not created, so we can release the lock early */
		UnlockRelationOid(ht->main_table_relid, ShareUpdateExclusiveLock);
	}

	ASSERT_IS_VALID_CHUNK(chunk);

	return chunk;
}

ChunkStub *
ts_chunk_stub_create(int32 id, int16 num_constraints)
{
	ChunkStub *stub;

	stub = palloc0(sizeof(*stub));
	stub->id = id;

	if (num_constraints > 0)
		stub->constraints = ts_chunk_constraints_alloc(num_constraints, CurrentMemoryContext);

	return stub;
}

Chunk *
ts_chunk_create_base(int32 id, int16 num_constraints, const char relkind)
{
	Chunk *chunk;

	chunk = palloc0(sizeof(Chunk));
	chunk->fd.id = id;
	chunk->fd.compressed_chunk_id = INVALID_CHUNK_ID;
	chunk->relkind = relkind;

	if (num_constraints > 0)
		chunk->constraints = ts_chunk_constraints_alloc(num_constraints, CurrentMemoryContext);

	return chunk;
}

/*
 * Build a chunk from a chunk tuple and a stub.
 *
 * The stub allows the chunk to be constructed more efficiently. But if the stub
 * is not "valid", dimension slices and constraints are fully
 * rescanned/recreated.
 */
Chunk *
ts_chunk_build_from_tuple_and_stub(Chunk **chunkptr, TupleInfo *ti, const ChunkStub *stub)
{
	Chunk *chunk = NULL;
	int num_constraints_hint = stub ? stub->constraints->num_constraints : 2;

	if (NULL == chunkptr)
		chunkptr = &chunk;

	if (NULL == *chunkptr)
		*chunkptr = MemoryContextAllocZero(ti->mctx, sizeof(Chunk));

	chunk = *chunkptr;
	ts_chunk_formdata_fill(&chunk->fd, ti);

	/*
	 * When searching for the chunk stub matching the dimensional point, we
	 * only scanned for dimensional constraints. We now need to rescan the
	 * constraints to also get the inherited constraints.
	 */
	chunk->constraints =
		ts_chunk_constraint_scan_by_chunk_id(chunk->fd.id, num_constraints_hint, ti->mctx);

	/* If a stub is provided then reuse its hypercube. Note that stubs that
	 * are results of a point or range scan might be incomplete (in terms of
	 * number of slices and constraints). Only a chunk stub that matches in
	 * all dimensions will have a complete hypercube. Thus, we need to check
	 * the validity of the stub before we can reuse it.
	 */
	if (chunk_stub_is_valid(stub, chunk->constraints->num_dimension_constraints))
	{
		MemoryContext oldctx = MemoryContextSwitchTo(ti->mctx);

		chunk->cube = ts_hypercube_copy(stub->cube);
		MemoryContextSwitchTo(oldctx);

		/*
		 * The hypercube slices were filled in during the scan. Now we need to
		 * sort them in dimension order.
		 */
		ts_hypercube_slice_sort(chunk->cube);
	}
	else
	{
		ScanIterator it = ts_dimension_slice_scan_iterator_create(ti->mctx);
		chunk->cube = ts_hypercube_from_constraints(chunk->constraints, &it);
		ts_scan_iterator_close(&it);
	}

	return chunk;
}

static ScanFilterResult
chunk_tuple_dropped_filter(const TupleInfo *ti, void *arg)
{
	ChunkStubScanCtx *stubctx = arg;
	bool isnull;
	Datum dropped = slot_getattr(ti->slot, Anum_chunk_dropped, &isnull);

	Assert(!isnull);
	stubctx->is_dropped = DatumGetBool(dropped);

	return stubctx->is_dropped ? SCAN_EXCLUDE : SCAN_INCLUDE;
}

/* This is a modified version of chunk_tuple_dropped_filter that does
 * not use ChunkStubScanCtx as the arg, it just ignores the passed in
 * argument.
 * We need a variant as the ScannerCtx assumes that the the filter function
 * and tuple_found function share the argument.
 */
static ScanFilterResult
chunk_check_ignorearg_dropped_filter(const TupleInfo *ti, void *arg)
{
	bool isnull;
	Datum dropped = slot_getattr(ti->slot, Anum_chunk_dropped, &isnull);

	Assert(!isnull);
	bool is_dropped = DatumGetBool(dropped);

	return is_dropped ? SCAN_EXCLUDE : SCAN_INCLUDE;
}

static ScanTupleResult
chunk_tuple_found(TupleInfo *ti, void *arg)
{
	ChunkStubScanCtx *stubctx = arg;
	Chunk *chunk;

	chunk = ts_chunk_build_from_tuple_and_stub(&stubctx->chunk, ti, stubctx->stub);
	Assert(!chunk->fd.dropped);

	/* Fill in table relids. Note that we cannot do this in
	 * ts_chunk_build_from_tuple_and_stub() since chunk_resurrect() also uses
	 * that function and, in that case, the chunk object is needed to create
	 * the data table and related objects. */
	chunk->table_id = get_relname_relid(chunk->fd.table_name.data,
										get_namespace_oid(chunk->fd.schema_name.data, true));
	chunk->hypertable_relid = ts_hypertable_id_to_relid(chunk->fd.hypertable_id);
	chunk->relkind = get_rel_relkind(chunk->table_id);

	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
		chunk->data_nodes = ts_chunk_data_node_scan_by_chunk_id(chunk->fd.id, ti->mctx);

	return SCAN_DONE;
}

/* Create a chunk by scanning on chunk ID. A stub must be provided as input. */
static Chunk *
chunk_create_from_stub(ChunkStubScanCtx *stubctx)
{
	ScanKeyData scankey[1];
	Catalog *catalog = ts_catalog_get();
	int num_found;
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CHUNK),
		.index = catalog_get_index(catalog, CHUNK, CHUNK_ID_INDEX),
		.nkeys = 1,
		.scankey = scankey,
		.data = stubctx,
		.filter = chunk_tuple_dropped_filter,
		.tuple_found = chunk_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/*
	 * Perform an index scan on chunk ID.
	 */
	ScanKeyInit(&scankey[0],
				Anum_chunk_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(stubctx->stub->id));

	num_found = ts_scanner_scan(&scanctx);

	Assert(num_found == 0 || num_found == 1);

	if (stubctx->is_dropped)
	{
		Assert(num_found == 0);
		return NULL;
	}

	if (num_found != 1)
		elog(ERROR, "no chunk found with ID %d", stubctx->stub->id);

	Assert(NULL != stubctx->chunk);

	return stubctx->chunk;
}

/*
 * Initialize a chunk scan context.
 *
 * A chunk scan context is used to join chunk-related information from metadata
 * tables during scans.
 */
static void
chunk_scan_ctx_init(ChunkScanCtx *ctx, const Hyperspace *hs, const Point *p)
{
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ChunkScanEntry),
		.hcxt = CurrentMemoryContext,
	};

	memset(ctx, 0, sizeof(*ctx));
	ctx->htab = hash_create("chunk-scan-context", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	ctx->space = hs;
	ctx->point = p;
	ctx->lockmode = NoLock;
}

/*
 * Destroy the chunk scan context.
 *
 * This will free the hash table in the context, but not the chunks within since
 * they are not allocated on the hash tables memory context.
 */
static void
chunk_scan_ctx_destroy(ChunkScanCtx *ctx)
{
	hash_destroy(ctx->htab);
}

static inline void
dimension_slice_and_chunk_constraint_join(ChunkScanCtx *scanctx, const DimensionVec *vec)
{
	int i;

	for (i = 0; i < vec->num_slices; i++)
	{
		/*
		 * For each dimension slice, find matching constraints. These will be
		 * saved in the scan context
		 */
		ts_chunk_constraint_scan_by_dimension_slice(vec->slices[i], scanctx, CurrentMemoryContext);
	}
}

/*
 * Scan for the chunk that encloses the given point.
 *
 * In each dimension there can be one or more slices that match the point's
 * coordinate in that dimension. Slices are collected in the scan context's hash
 * table according to the chunk IDs they are associated with. A slice might
 * represent the dimensional bound of multiple chunks, and thus is added to all
 * the hash table slots of those chunks. At the end of the scan there will be at
 * most one chunk that has a complete set of slices, since a point cannot belong
 * to two chunks.
 */
static void
chunk_point_scan(ChunkScanCtx *scanctx, const Point *p, bool lock_slices)
{
	int i;

	/* Scan all dimensions for slices enclosing the point */
	for (i = 0; i < scanctx->space->num_dimensions; i++)
	{
		DimensionVec *vec;
		ScanTupLock tuplock = {
			.lockmode = LockTupleKeyShare,
			.waitpolicy = LockWaitBlock,
		};

		vec = ts_dimension_slice_scan_limit(scanctx->space->dimensions[i].fd.id,
											p->coordinates[i],
											0,
											lock_slices ? &tuplock : NULL);

		dimension_slice_and_chunk_constraint_join(scanctx, vec);
	}
}

/*
 * Scan for chunks that collide with the given hypercube.
 *
 * Collisions are determined using axis-aligned bounding box collision detection
 * generalized to N dimensions. Slices are collected in the scan context's hash
 * table according to the chunk IDs they are associated with. A slice might
 * represent the dimensional bound of multiple chunks, and thus is added to all
 * the hash table slots of those chunks. At the end of the scan, those chunks
 * that have a full set of slices are the ones that actually collide with the
 * given hypercube.
 *
 * Chunks in the scan context that do not collide (do not have a full set of
 * slices), might still be important for ensuring alignment in those dimensions
 * that require alignment.
 */
static void
chunk_collision_scan(ChunkScanCtx *scanctx, const Hypercube *cube)
{
	int i;

	/* Scan all dimensions for colliding slices */
	for (i = 0; i < scanctx->space->num_dimensions; i++)
	{
		DimensionVec *vec;
		DimensionSlice *slice = cube->slices[i];

		vec = dimension_slice_collision_scan(slice->fd.dimension_id,
											 slice->fd.range_start,
											 slice->fd.range_end);

		/* Add the slices to all the chunks they are associated with */
		dimension_slice_and_chunk_constraint_join(scanctx, vec);
	}
}

/*
 * Apply a function to each stub in the scan context's hash table. If the limit
 * is greater than zero only a limited number of chunks will be processed.
 *
 * The chunk handler function (on_chunk_func) should return CHUNK_PROCESSED if
 * the chunk should be considered processed and count towards the given
 * limit. CHUNK_IGNORE can be returned to have a chunk NOT count towards the
 * limit. CHUNK_DONE counts the chunk but aborts processing irrespective of
 * whether the limit is reached or not.
 *
 * Returns the number of processed chunks.
 */
static int
chunk_scan_ctx_foreach_chunk_stub(ChunkScanCtx *ctx, on_chunk_stub_func on_chunk, uint16 limit)
{
	HASH_SEQ_STATUS status;
	ChunkScanEntry *entry;

	ctx->num_processed = 0;
	hash_seq_init(&status, ctx->htab);

	for (entry = hash_seq_search(&status); entry != NULL; entry = hash_seq_search(&status))
	{
		switch (on_chunk(ctx, entry->stub))
		{
			case CHUNK_DONE:
				ctx->num_processed++;
				hash_seq_term(&status);
				return ctx->num_processed;
			case CHUNK_PROCESSED:
				ctx->num_processed++;

				if (limit > 0 && ctx->num_processed == limit)
				{
					hash_seq_term(&status);
					return ctx->num_processed;
				}
				break;
			case CHUNK_IGNORED:
				break;
		}
	}

	return ctx->num_processed;
}

static ChunkResult
set_complete_chunk(ChunkScanCtx *scanctx, ChunkStub *stub)
{
	if (chunk_stub_is_complete(stub, scanctx->space))
	{
		scanctx->data = stub;
#ifdef USE_ASSERT_CHECKING
		return CHUNK_PROCESSED;
#else
		return CHUNK_DONE;
#endif
	}
	return CHUNK_IGNORED;
}

typedef struct ChunkScanCtxAddChunkData
{
	Chunk *chunks;
	uint64 max_chunks;
	uint64 num_chunks;
} ChunkScanCtxAddChunkData;

static ChunkResult
chunk_scan_context_add_chunk(ChunkScanCtx *scanctx, ChunkStub *stub)
{
	ChunkScanCtxAddChunkData *data = scanctx->data;
	ChunkStubScanCtx stubctx = {
		.chunk = &data->chunks[data->num_chunks],
		.stub = stub,
	};

	Assert(data->num_chunks < data->max_chunks);
	chunk_create_from_stub(&stubctx);

	if (stubctx.is_dropped)
		return CHUNK_IGNORED;

	data->num_chunks++;

	return CHUNK_PROCESSED;
}

/* Finds the first chunk that has a complete set of constraints. There should be
 * only one such chunk in the scan context when scanning for the chunk that
 * holds a particular tuple/point. */
static ChunkStub *
chunk_scan_ctx_get_chunk_stub(ChunkScanCtx *ctx)
{
	ctx->data = NULL;

#ifdef USE_ASSERT_CHECKING
	{
		int n = chunk_scan_ctx_foreach_chunk_stub(ctx, set_complete_chunk, 0);

		Assert(n == 0 || n == 1);
	}
#else
	chunk_scan_ctx_foreach_chunk_stub(ctx, set_complete_chunk, 1);
#endif

	return ctx->data;
}

/*
 * Resurrect a chunk from a tombstone.
 *
 * A chunk can be dropped while retaining its metadata as a tombstone. Such a
 * chunk is marked with dropped=true.
 *
 * This function resurrects such a dropped chunk based on the original metadata,
 * including recreating the table and related objects.
 */
static Chunk *
chunk_resurrect(const Hypertable *ht, const ChunkStub *stub)
{
	ScanIterator iterator;
	Chunk *chunk = NULL;
	int count = 0;

	Assert(NULL != stub->constraints);
	Assert(NULL != stub->cube);

	iterator = ts_scan_iterator_create(CHUNK, RowExclusiveLock, CurrentMemoryContext);
	ts_chunk_scan_iterator_set_chunk_id(&iterator, stub->id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		HeapTuple new_tuple;

		Assert(count == 0 && chunk == NULL);
		chunk = ts_chunk_build_from_tuple_and_stub(NULL, ti, stub);
		Assert(chunk->fd.dropped);

		/* Create data table and related objects */
		chunk->hypertable_relid = ht->main_table_relid;
		chunk->relkind = hypertable_chunk_relkind(ht);
		if (chunk->relkind == RELKIND_FOREIGN_TABLE)
		{
			chunk->data_nodes = ts_chunk_data_node_scan_by_chunk_id(chunk->fd.id, ti->mctx);
			/* If the Data-Node replica list information has been deleted reassign them */
			if (!chunk->data_nodes)
				chunk->data_nodes = chunk_assign_data_nodes(chunk, ht);
		}
		chunk->table_id = chunk_create_table(chunk, ht);
		chunk_create_table_constraints(chunk);

		/* Finally, update the chunk tuple to no longer be a tombstone */
		chunk->fd.dropped = false;
		new_tuple = chunk_formdata_make_tuple(&chunk->fd, ts_scan_iterator_tupledesc(&iterator));
		ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
		heap_freetuple(new_tuple);
		count++;

		/* Assume there's only one match. (We break early to avoid scanning
		 * also the updated tuple.) */
		break;
	}

	ts_scan_iterator_close(&iterator);

	Assert(count == 0 || count == 1);

	/* If count == 0 and chunk is NULL here, the tombstone (metadata) must
	 * have been removed before we had a chance to resurrect the chunk */
	return chunk;
}

/*
 * Find a chunk matching a point in a hypertable's N-dimensional hyperspace.
 *
 * This involves:
 *
 * 1) For each dimension:
 *	  - Find all dimension slices that match the dimension
 * 2) For each dimension slice:
 *	  - Find all chunk constraints matching the dimension slice
 * 3) For each matching chunk constraint
 *	  - Insert a chunk stub into a hash table and add the constraint to the chunk
 *	  - If chunk already exists in hash table, add the constraint to the chunk
 * 4) At the end of the scan, only one chunk in the hash table should have
 *	  N number of constraints. This is the matching chunk.
 *
 * NOTE: this function allocates transient data, e.g., dimension slice,
 * constraints and chunks, that in the end are not part of the returned
 * chunk. Therefore, this scan should be executed on a transient memory
 * context. The returned chunk needs to be copied into another memory context in
 * case it needs to live beyond the lifetime of the other data.
 */
static Chunk *
chunk_find(const Hypertable *ht, const Point *p, bool resurrect, bool lock_slices)
{
	ChunkStub *stub;
	Chunk *chunk = NULL;
	ChunkScanCtx ctx;

	/* The scan context will keep the state accumulated during the scan */
	chunk_scan_ctx_init(&ctx, ht->space, p);

	/* Abort the scan when the chunk is found */
	ctx.early_abort = true;

	/* Scan for the chunk matching the point */
	chunk_point_scan(&ctx, p, lock_slices);

	/* Find the stub that has N matching dimension constraints */
	stub = chunk_scan_ctx_get_chunk_stub(&ctx);

	chunk_scan_ctx_destroy(&ctx);

	if (NULL != stub)
	{
		ChunkStubScanCtx stubctx = {
			.stub = stub,
		};

		/* Fill in the rest of the chunk's data from the chunk table, unless
		 * the chunk is marked as dropped. */
		chunk = chunk_create_from_stub(&stubctx);

		/* Check if the found metadata is a tombstone (dropped=true) */
		if (stubctx.is_dropped)
		{
			Assert(chunk == NULL);

			/* Resurrect the chunk if requested by the caller */
			if (resurrect)
				chunk = chunk_resurrect(ht, stub);
		}
	}

	ASSERT_IS_NULL_OR_VALID_CHUNK(chunk);

	return chunk;
}

Chunk *
ts_chunk_find(const Hypertable *ht, const Point *p, bool lock_slices)
{
	return chunk_find(ht, p, false, lock_slices);
}

/*
 * Find all the chunks in hyperspace that include elements (dimension slices)
 * calculated by given range constraints and return the corresponding
 * ChunkScanCxt. It is the caller's responsibility to destroy this context after
 * usage.
 */
static void
chunks_find_all_in_range_limit(const Hypertable *ht, const Dimension *time_dim,
							   StrategyNumber start_strategy, int64 start_value,
							   StrategyNumber end_strategy, int64 end_value, int limit,
							   uint64 *num_found, ScanTupLock *tuplock, ChunkScanCtx *ctx)
{
	DimensionVec *slices;

	Assert(ht != NULL);

	/* must have been checked earlier that this is the case */
	Assert(time_dim != NULL);

	slices = ts_dimension_slice_scan_range_limit(time_dim->fd.id,
												 start_strategy,
												 start_value,
												 end_strategy,
												 end_value,
												 limit,
												 tuplock);

	/* The scan context will keep the state accumulated during the scan */
	chunk_scan_ctx_init(ctx, ht->space, NULL);

	/* No abort when the first chunk is found */
	ctx->early_abort = false;

	/* Scan for chunks that are in range */
	dimension_slice_and_chunk_constraint_join(ctx, slices);

	*num_found += hash_get_num_entries(ctx->htab);
}

/* show_chunks SQL function handler */
Datum
ts_chunk_show_chunks(PG_FUNCTION_ARGS)
{
	/*
	 * chunks_return_srf is called even when it is not the first call but only
	 * after doing some computation first
	 */
	if (SRF_IS_FIRSTCALL())
	{
		FuncCallContext *funcctx;
		Oid relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
		Hypertable *ht;
		const Dimension *time_dim;
		Cache *hcache;
		int64 older_than = PG_INT64_MAX;
		int64 newer_than = PG_INT64_MIN;
		Oid time_type;

		hcache = ts_hypertable_cache_pin();
		ht = find_hypertable_from_table_or_cagg(hcache, relid, true);
		Assert(ht != NULL);
		time_dim = hyperspace_get_open_dimension(ht->space, 0);

		if (time_dim)
			time_type = ts_dimension_get_partition_type(time_dim);
		else
			time_type = InvalidOid;

		if (!PG_ARGISNULL(1))
			older_than = ts_time_value_from_arg(PG_GETARG_DATUM(1),
												get_fn_expr_argtype(fcinfo->flinfo, 1),
												time_type);

		if (!PG_ARGISNULL(2))
			newer_than = ts_time_value_from_arg(PG_GETARG_DATUM(2),
												get_fn_expr_argtype(fcinfo->flinfo, 2),
												time_type);

		funcctx = SRF_FIRSTCALL_INIT();
		funcctx->user_fctx = get_chunks_in_time_range(ht,
													  older_than,
													  newer_than,
													  "show_chunks",
													  funcctx->multi_call_memory_ctx,
													  &funcctx->max_calls,
													  NULL);
		ts_cache_release(hcache);
	}

	return chunks_return_srf(fcinfo);
}

static Chunk *
get_chunks_in_time_range(Hypertable *ht, int64 older_than, int64 newer_than,
						 const char *caller_name, MemoryContext mctx, uint64 *num_chunks_returned,
						 ScanTupLock *tuplock)
{
	MemoryContext oldcontext;
	ChunkScanCtx chunk_scan_ctx;
	Chunk *chunks;
	ChunkScanCtxAddChunkData data;
	const Dimension *time_dim;
	StrategyNumber start_strategy;
	StrategyNumber end_strategy;
	uint64 num_chunks = 0;

	if (older_than <= newer_than)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid time range"),
				 errhint("The start of the time range must be before the end.")));

	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
		elog(ERROR, "invalid operation on compressed hypertable");

	start_strategy = (newer_than == PG_INT64_MIN) ? InvalidStrategy : BTGreaterEqualStrategyNumber;
	end_strategy = (older_than == PG_INT64_MAX) ? InvalidStrategy : BTLessStrategyNumber;
	time_dim = hyperspace_get_open_dimension(ht->space, 0);

	oldcontext = MemoryContextSwitchTo(mctx);
	chunks_find_all_in_range_limit(ht,
								   time_dim,
								   start_strategy,
								   newer_than,
								   end_strategy,
								   older_than,
								   -1,
								   &num_chunks,
								   tuplock,
								   &chunk_scan_ctx);
	MemoryContextSwitchTo(oldcontext);

	chunks = MemoryContextAllocZero(mctx, sizeof(Chunk) * num_chunks);
	data = (ChunkScanCtxAddChunkData){
		.chunks = chunks,
		.max_chunks = num_chunks,
		.num_chunks = 0,
	};

	/* Get all the chunks from the context */
	chunk_scan_ctx.data = &data;
	chunk_scan_ctx_foreach_chunk_stub(&chunk_scan_ctx, chunk_scan_context_add_chunk, -1);
	/*
	 * only affects ctx.htab Got all the chunk already so can now safely
	 * destroy the context
	 */
	chunk_scan_ctx_destroy(&chunk_scan_ctx);

	*num_chunks_returned = data.num_chunks;
	qsort(chunks, *num_chunks_returned, sizeof(Chunk), chunk_cmp);

#ifdef USE_ASSERT_CHECKING
	do
	{
		uint64 i = 0;
		/* Assert that we never return dropped chunks */
		for (i = 0; i < *num_chunks_returned; i++)
			ASSERT_IS_VALID_CHUNK(&chunks[i]);
	} while (false);
#endif

	return chunks;
}

List *
ts_chunk_data_nodes_copy(const Chunk *chunk)
{
	List *lcopy = NIL;
	ListCell *lc;

	foreach (lc, chunk->data_nodes)
	{
		ChunkDataNode *node = lfirst(lc);
		ChunkDataNode *copy = palloc(sizeof(ChunkDataNode));

		memcpy(copy, node, sizeof(ChunkDataNode));

		lcopy = lappend(lcopy, copy);
	}

	return lcopy;
}

Chunk *
ts_chunk_copy(const Chunk *chunk)
{
	Chunk *copy;

	ASSERT_IS_VALID_CHUNK(chunk);
	copy = palloc(sizeof(Chunk));
	memcpy(copy, chunk, sizeof(Chunk));

	if (NULL != chunk->constraints)
		copy->constraints = ts_chunk_constraints_copy(chunk->constraints);

	if (NULL != chunk->cube)
		copy->cube = ts_hypercube_copy(chunk->cube);

	copy->data_nodes = ts_chunk_data_nodes_copy(chunk);

	return copy;
}

static int
chunk_scan_internal(int indexid, ScanKeyData scankey[], int nkeys, tuple_filter_func filter,
					tuple_found_func tuple_found, void *data, int limit, ScanDirection scandir,
					LOCKMODE lockmode, MemoryContext mctx)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx ctx = {
		.table = catalog_get_table_id(catalog, CHUNK),
		.index = catalog_get_index(catalog, CHUNK, indexid),
		.nkeys = nkeys,
		.data = data,
		.scankey = scankey,
		.filter = filter,
		.tuple_found = tuple_found,
		.limit = limit,
		.lockmode = lockmode,
		.scandirection = scandir,
		.result_mctx = mctx,
	};

	return ts_scanner_scan(&ctx);
}

/*
 * Get a window of chunks that "precedes" the given dimensional point.
 *
 * For instance, if the dimension is "time", then given a point in time the
 * function returns the recent chunks that come before the chunk that includes
 * that point. The count parameter determines the number or slices the window
 * should include in the given dimension. Note, that with multi-dimensional
 * partitioning, there might be multiple chunks in each dimensional slice that
 * all precede the given point. For instance, the example below shows two
 * different situations that each go "back" two slices (count = 2) in the
 * x-dimension, but returns two vs. eight chunks due to different
 * partitioning.
 *
 * '_____________
 * '|   |   | * |
 * '|___|___|___|
 * '
 * '
 * '____ ________
 * '|   |   | * |
 * '|___|___|___|
 * '|   |   |   |
 * '|___|___|___|
 * '|   |   |   |
 * '|___|___|___|
 * '|   |   |   |
 * '|___|___|___|
 *
 * Note that the returned chunks will be allocated on the given memory
 * context, including the list itself. So, beware of not leaking the list if
 * the chunks are later cached somewhere else.
 */
List *
ts_chunk_get_window(int32 dimension_id, int64 point, int count, MemoryContext mctx)
{
	List *chunks = NIL;
	DimensionVec *dimvec;
	int i;

	/* Scan for "count" slices that precede the point in the given dimension */
	dimvec = ts_dimension_slice_scan_by_dimension_before_point(dimension_id,
															   point,
															   count,
															   BackwardScanDirection,
															   mctx);

	/*
	 * For each slice, join with any constraints that reference the slice.
	 * There might be multiple constraints for each slice in case of
	 * multi-dimensional partitioning.
	 */
	for (i = 0; i < dimvec->num_slices; i++)
	{
		DimensionSlice *slice = dimvec->slices[i];
		ChunkConstraints *ccs = ts_chunk_constraints_alloc(1, mctx);
		int j;

		ts_chunk_constraint_scan_by_dimension_slice_id(slice->fd.id, ccs, mctx);

		/* For each constraint, find the corresponding chunk */
		for (j = 0; j < ccs->num_constraints; j++)
		{
			ChunkConstraint *cc = &ccs->constraints[j];
			Chunk *chunk = ts_chunk_get_by_id(cc->fd.chunk_id, false);
			MemoryContext old;
			ScanIterator it;

			/* Dropped chunks do not contain valid data and must not be returned */
			if (!chunk)
				continue;
			chunk->constraints = ts_chunk_constraint_scan_by_chunk_id(chunk->fd.id, 1, mctx);

			it = ts_dimension_slice_scan_iterator_create(mctx);
			chunk->cube = ts_hypercube_from_constraints(chunk->constraints, &it);
			ts_scan_iterator_close(&it);

			/* Allocate the list on the same memory context as the chunks */
			old = MemoryContextSwitchTo(mctx);
			chunks = lappend(chunks, chunk);
			MemoryContextSwitchTo(old);
		}
	}

#ifdef USE_ASSERT_CHECKING
	/* Assert that we never return dropped chunks */
	do
	{
		ListCell *lc;

		foreach (lc, chunks)
		{
			Chunk *chunk = lfirst(lc);
			ASSERT_IS_VALID_CHUNK(chunk);
		}
	} while (false);
#endif

	return chunks;
}

static Chunk *
chunk_scan_find(int indexid, ScanKeyData scankey[], int nkeys, MemoryContext mctx,
				bool fail_if_not_found, const DisplayKeyData displaykey[])
{
	ChunkStubScanCtx stubctx = { 0 };
	Chunk *chunk;
	int num_found;

	num_found = chunk_scan_internal(indexid,
									scankey,
									nkeys,
									chunk_tuple_dropped_filter,
									chunk_tuple_found,
									&stubctx,
									1,
									ForwardScanDirection,
									AccessShareLock,
									mctx);
	Assert(num_found == 0 || (num_found == 1 && !stubctx.is_dropped));
	chunk = stubctx.chunk;

	switch (num_found)
	{
		case 0:
			if (fail_if_not_found)
			{
				int i = 0;
				StringInfo info = makeStringInfo();
				while (i < nkeys)
				{
					appendStringInfo(info,
									 "%s: %s",
									 displaykey[i].name,
									 displaykey[i].as_string(scankey[i].sk_argument));
					if (++i < nkeys)
						appendStringInfoString(info, ", ");
				}
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("chunk not found"),
						 errdetail("%s", info->data)));
			}
			break;
		case 1:
			ASSERT_IS_VALID_CHUNK(chunk);
			break;
		default:
			elog(ERROR, "expected a single chunk, found %d", num_found);
	}

	return chunk;
}

Chunk *
ts_chunk_get_by_name_with_memory_context(const char *schema_name, const char *table_name,
										 MemoryContext mctx, bool fail_if_not_found)
{
	NameData schema, table;
	ScanKeyData scankey[2];
	static const DisplayKeyData displaykey[2] = {
		[0] = { .name = "schema_name", .as_string = DatumGetNameString },
		[1] = { .name = "table_name", .as_string = DatumGetNameString },
	};

	/* Early check for rogue input */
	if (schema_name == NULL || table_name == NULL)
	{
		if (fail_if_not_found)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("chunk not found"),
					 errdetail("schema_name: %s, table_name: %s",
							   schema_name ? schema_name : "<null>",
							   table_name ? table_name : "<null>")));
		else
			return NULL;
	}

	namestrcpy(&schema, schema_name);
	namestrcpy(&table, table_name);

	/*
	 * Perform an index scan on chunk name.
	 */
	ScanKeyInit(&scankey[0],
				Anum_chunk_schema_name_idx_schema_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				NameGetDatum(&schema));
	ScanKeyInit(&scankey[1],
				Anum_chunk_schema_name_idx_table_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				NameGetDatum(&table));

	return chunk_scan_find(CHUNK_SCHEMA_NAME_INDEX,
						   scankey,
						   2,
						   mctx,
						   fail_if_not_found,
						   displaykey);
}

Chunk *
ts_chunk_get_by_relid(Oid relid, bool fail_if_not_found)
{
	char *schema;
	char *table;

	if (!OidIsValid(relid))
	{
		if (fail_if_not_found)
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid Oid")));
		else
			return NULL;
	}

	schema = get_namespace_name(get_rel_namespace(relid));
	table = get_rel_name(relid);
	return chunk_get_by_name(schema, table, fail_if_not_found);
}

void
ts_chunk_free(Chunk *chunk)
{
	if (chunk->cube)
	{
		ts_hypercube_free(chunk->cube);
	}

	if (chunk->constraints)
	{
		ChunkConstraints *c = chunk->constraints;
		pfree(c->constraints);
		pfree(c);
	}

	list_free(chunk->data_nodes);

	pfree(chunk);
}

static const char *
DatumGetInt32AsString(Datum datum)
{
	char *buf = (char *) palloc(12); /* sign, 10 digits, '\0' */
	pg_ltoa(DatumGetInt32(datum), buf);
	return buf;
}

Chunk *
ts_chunk_get_by_id(int32 id, bool fail_if_not_found)
{
	ScanKeyData scankey[1];
	static const DisplayKeyData displaykey[1] = {
		[0] = { .name = "id", .as_string = DatumGetInt32AsString },
	};

	/*
	 * Perform an index scan on chunk id.
	 */
	ScanKeyInit(&scankey[0], Anum_chunk_idx_id, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(id));

	return chunk_scan_find(CHUNK_ID_INDEX,
						   scankey,
						   1,
						   CurrentMemoryContext,
						   fail_if_not_found,
						   displaykey);
}

/*
 * Number of chunks created after given chunk.
 * If chunk2.id > chunk1.id then chunk2 is created after chunk1
 */
int
ts_chunk_num_of_chunks_created_after(const Chunk *chunk)
{
	ScanKeyData scankey[1];

	/*
	 * Try to find chunks with a greater Id then a given chunk
	 */
	ScanKeyInit(&scankey[0],
				Anum_chunk_idx_id,
				BTGreaterStrategyNumber,
				F_INT4GT,
				Int32GetDatum(chunk->fd.id));

	return chunk_scan_internal(CHUNK_ID_INDEX,
							   scankey,
							   1,
							   NULL,
							   NULL,
							   NULL,
							   0,
							   ForwardScanDirection,
							   AccessShareLock,
							   CurrentMemoryContext);
}

/*
 * Simple scans provide lightweight ways to access chunk information without the
 * overhead of getting a full chunk (i.e., no extra metadata, like constraints,
 * are joined in). This function forms the basis of a number of lookup functions
 * that, e.g., translates a chunk relid to a chunk_id, or vice versa.
 */
static bool
chunk_simple_scan(ScanIterator *iterator, FormData_chunk *form, bool missing_ok,
				  const DisplayKeyData displaykey[])
{
	int count = 0;

	ts_scanner_foreach(iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(iterator);
		ts_chunk_formdata_fill(form, ti);

		if (!form->dropped)
			count++;
	}

	Assert(count == 0 || count == 1);

	if (count == 0 && !missing_ok)
	{
		int i = 0;
		StringInfo info = makeStringInfo();
		while (i < iterator->ctx.nkeys)
		{
			appendStringInfo(info,
							 "%s: %s",
							 displaykey[i].name,
							 displaykey[i].as_string(iterator->ctx.scankey[i].sk_argument));
			if (++i < iterator->ctx.nkeys)
				appendStringInfoString(info, ", ");
		}
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("chunk not found")));
	}

	return count == 1;
}

static bool
chunk_simple_scan_by_name(const char *schema, const char *table, FormData_chunk *form,
						  bool missing_ok)
{
	ScanIterator iterator;
	static const DisplayKeyData displaykey[] = {
		[0] = { .name = "schema_name", .as_string = DatumGetNameString },
		[1] = { .name = "table_name", .as_string = DatumGetNameString },
	};

	if (schema == NULL || table == NULL)
		return false;

	iterator = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);
	init_scan_by_qualified_table_name(&iterator, schema, table);

	return chunk_simple_scan(&iterator, form, missing_ok, displaykey);
}

static bool
chunk_simple_scan_by_relid(Oid relid, FormData_chunk *form, bool missing_ok)
{
	bool found = false;

	if (OidIsValid(relid))
	{
		const char *table = get_rel_name(relid);

		if (table != NULL)
		{
			Oid nspid = get_rel_namespace(relid);
			const char *schema = get_namespace_name(nspid);

			found = chunk_simple_scan_by_name(schema, table, form, missing_ok);
		}
	}

	if (!found && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("chunk with relid %u not found", relid)));

	return found;
}

static bool
chunk_simple_scan_by_id(int32 chunk_id, FormData_chunk *form, bool missing_ok)
{
	ScanIterator iterator;
	static const DisplayKeyData displaykey[] = {
		[0] = { .name = "id", .as_string = DatumGetInt32AsString },
	};

	iterator = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);
	ts_chunk_scan_iterator_set_chunk_id(&iterator, chunk_id);

	return chunk_simple_scan(&iterator, form, missing_ok, displaykey);
}

/*
 * Lookup a Chunk ID from a chunk's relid.
 */
Datum
ts_chunk_id_from_relid(PG_FUNCTION_ARGS)
{
	static Oid last_relid = InvalidOid;
	static int32 last_id = 0;
	Oid relid = PG_GETARG_OID(0);
	FormData_chunk form;

	if (last_relid == relid)
		return last_id;

	chunk_simple_scan_by_relid(relid, &form, false);

	last_relid = relid;
	last_id = form.id;

	PG_RETURN_INT32(last_id);
}

bool
ts_chunk_exists_relid(Oid relid)
{
	FormData_chunk form;

	return chunk_simple_scan_by_relid(relid, &form, true);
}

/*
 * Get the relid of a chunk given its ID.
 */
Oid
ts_chunk_get_relid(int32 chunk_id, bool missing_ok)
{
	FormData_chunk form = { 0 };
	Oid relid = InvalidOid;

	if (chunk_simple_scan_by_id(chunk_id, &form, missing_ok))
	{
		Oid schemaid = get_namespace_oid(NameStr(form.schema_name), missing_ok);

		if (OidIsValid(schemaid))
			relid = get_relname_relid(NameStr(form.table_name), schemaid);
	}

	if (!OidIsValid(relid) && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("chunk with id %d not found", chunk_id)));

	return relid;
}

/*
 * Get the schema (namespace) of a chunk given its ID.
 *
 * This is a lightweight way to get the schema of a chunk without creating a
 * full Chunk object that joins in constraints, etc.
 */
Oid
ts_chunk_get_schema_id(int32 chunk_id, bool missing_ok)
{
	FormData_chunk form = { 0 };

	if (!chunk_simple_scan_by_id(chunk_id, &form, missing_ok))
		return InvalidOid;

	return get_namespace_oid(NameStr(form.schema_name), missing_ok);
}

bool
ts_chunk_get_id(const char *schema, const char *table, int32 *chunk_id, bool missing_ok)
{
	FormData_chunk form = { 0 };

	if (!chunk_simple_scan_by_name(schema, table, &form, missing_ok))
		return false;

	if (NULL != chunk_id)
		*chunk_id = form.id;

	return true;
}

/*
 * Results of deleting a chunk.
 *
 * A chunk can be deleted in two ways: (1) full delete of data and metadata,
 * (2) delete data but preserve metadata (marked with dropped=true). The
 * deletion mode (preserve or not) combined with the current state of the
 * "dropped" flag on a chunk metadata row leads to a cross-product resulting
 * in the following outcomes:
 */
typedef enum ChunkDeleteResult
{
	/* Deleted a live chunk */
	CHUNK_DELETED,
	/* Deleted a chunk previously marked "dropped" */
	CHUNK_DELETED_DROPPED,
	/* Marked a chunk as dropped instead of deleting */
	CHUNK_MARKED_DROPPED,
	/* Tried to mark a chunk as dropped when it was already marked */
	CHUNK_ALREADY_MARKED_DROPPED,
} ChunkDeleteResult;

/* Delete the chunk tuple.
 *
 * preserve_chunk_catalog_row - instead of deleting the row, mark it as dropped.
 * this is used when we need to preserve catalog information about the chunk
 * after dropping it. Currently only used when preserving continuous aggregates
 * on the chunk after the raw data was dropped. Otherwise, we'd have dangling
 * chunk ids left over in the materialization table. Preserve the space dimension
 * info about these chunks too.
 *
 * When chunk rows are preserved, the rows need to be updated to set the
 * 'dropped' flag to TRUE. But since this produces a new tuple into the
 * metadata table we will process also the new tuple in the same loop, which
 * is not only inefficent but could also lead to bugs. For now, we just ignore
 * those tuples (the CHUNK_ALREADY_MARKED_DROPPED case), but ideally we
 * shouldn't scan the updated tuples at all since it means double the number
 * of tuples to process.
 */
static ChunkDeleteResult
chunk_tuple_delete(TupleInfo *ti, DropBehavior behavior, bool preserve_chunk_catalog_row)
{
	FormData_chunk form;
	CatalogSecurityContext sec_ctx;
	ChunkConstraints *ccs = ts_chunk_constraints_alloc(2, ti->mctx);
	ChunkDeleteResult res;
	int i;

	ts_chunk_formdata_fill(&form, ti);

	if (preserve_chunk_catalog_row && form.dropped)
		return CHUNK_ALREADY_MARKED_DROPPED;

	/* if only marking as deleted, keep the constraints and dimension info */
	if (!preserve_chunk_catalog_row)
	{
		ts_chunk_constraint_delete_by_chunk_id(form.id, ccs);

		/* Check for dimension slices that are orphaned by the chunk deletion */
		for (i = 0; i < ccs->num_constraints; i++)
		{
			ChunkConstraint *cc = &ccs->constraints[i];

			/*
			 * Delete the dimension slice if there are no remaining constraints
			 * referencing it
			 */
			if (is_dimension_constraint(cc))
			{
				/*
				 * Dimension slices are shared between chunk constraints and
				 * subsequently between chunks as well. Since different chunks
				 * can reference the same dimension slice (through the chunk
				 * constraint), we must lock the dimension slice in FOR UPDATE
				 * mode *prior* to scanning the chunk constraints table. If we
				 * do not do that, we can have the following scenario:
				 *
				 * - T1: Prepares to create a chunk that uses an existing dimension slice X
				 * - T2: Deletes a chunk and dimension slice X because it is not
				 *   references by a chunk constraint.
				 * - T1: Adds a chunk constraint referencing dimension
				 *   slice X (which is about to be deleted by T2).
				 */
				ScanTupLock tuplock = {
					.lockmode = LockTupleExclusive,
					.waitpolicy = LockWaitBlock,
				};
				DimensionSlice *slice =
					ts_dimension_slice_scan_by_id_and_lock(cc->fd.dimension_slice_id,
														   &tuplock,
														   CurrentMemoryContext);
				/* If the slice is not found in the scan above, the table is
				 * broken so we do not delete the slice. We proceed
				 * anyway since users need to be able to drop broken tables or
				 * remove broken chunks. */
				if (!slice)
				{
					const Hypertable *const ht = ts_hypertable_get_by_id(form.hypertable_id);
					ereport(WARNING,
							(errmsg("unexpected state for chunk %s.%s, dropping anyway",
									quote_identifier(NameStr(form.schema_name)),
									quote_identifier(NameStr(form.table_name))),
							 errdetail("The integrity of hypertable %s.%s might be "
									   "compromised "
									   "since one of its chunks lacked a dimension slice.",
									   quote_identifier(NameStr(ht->fd.schema_name)),
									   quote_identifier(NameStr(ht->fd.table_name)))));
				}
				else if (ts_chunk_constraint_scan_by_dimension_slice_id(slice->fd.id,
																		NULL,
																		CurrentMemoryContext) == 0)
					ts_dimension_slice_delete_by_id(cc->fd.dimension_slice_id, false);
			}
		}
	}

	ts_chunk_index_delete_by_chunk_id(form.id, true);
	ts_compression_chunk_size_delete(form.id);
	ts_chunk_data_node_delete_by_chunk_id(form.id);

	/* Delete any row in bgw_policy_chunk-stats corresponding to this chunk */
	ts_bgw_policy_chunk_stats_delete_by_chunk_id(form.id);

	if (form.compressed_chunk_id != INVALID_CHUNK_ID)
	{
		Chunk *compressed_chunk = ts_chunk_get_by_id(form.compressed_chunk_id, false);

		/* The chunk may have been delete by a CASCADE */
		if (compressed_chunk != NULL)
			/* Plain drop without preserving catalog row because this is the compressed
			 * chunk */
			ts_chunk_drop(compressed_chunk, behavior, DEBUG1);
	}

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	if (!preserve_chunk_catalog_row)
	{
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));

		if (form.dropped)
			res = CHUNK_DELETED_DROPPED;
		else
			res = CHUNK_DELETED;
	}
	else
	{
		HeapTuple new_tuple;

		Assert(!form.dropped);

		form.compressed_chunk_id = INVALID_CHUNK_ID;
		form.dropped = true;
		form.status = CHUNK_STATUS_DEFAULT;
		new_tuple = chunk_formdata_make_tuple(&form, ts_scanner_get_tupledesc(ti));
		ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
		heap_freetuple(new_tuple);
		res = CHUNK_MARKED_DROPPED;
	}

	ts_catalog_restore_user(&sec_ctx);

	return res;
}

static void
init_scan_by_qualified_table_name(ScanIterator *iterator, const char *schema_name,
								  const char *table_name)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_SCHEMA_NAME_INDEX);
	ts_scan_iterator_scan_key_init(iterator,
								   Anum_chunk_schema_name_idx_schema_name,
								   BTEqualStrategyNumber,
								   F_NAMEEQ,
								   CStringGetDatum(schema_name));
	ts_scan_iterator_scan_key_init(iterator,
								   Anum_chunk_schema_name_idx_table_name,
								   BTEqualStrategyNumber,
								   F_NAMEEQ,
								   CStringGetDatum(table_name));
}

static int
chunk_delete(ScanIterator *iterator, DropBehavior behavior, bool preserve_chunk_catalog_row)
{
	int count = 0;

	ts_scanner_foreach(iterator)
	{
		ChunkDeleteResult res;

		res = chunk_tuple_delete(ts_scan_iterator_tuple_info(iterator),
								 behavior,
								 preserve_chunk_catalog_row);

		switch (res)
		{
			case CHUNK_DELETED:
			case CHUNK_MARKED_DROPPED:
				count++;
				break;
			case CHUNK_ALREADY_MARKED_DROPPED:
			case CHUNK_DELETED_DROPPED:
				break;
		}
	}

	return count;
}

static int
ts_chunk_delete_by_name_internal(const char *schema, const char *table, DropBehavior behavior,
								 bool preserve_chunk_catalog_row)
{
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, RowExclusiveLock, CurrentMemoryContext);
	int count;

	init_scan_by_qualified_table_name(&iterator, schema, table);
	count = chunk_delete(&iterator, behavior, preserve_chunk_catalog_row);

	/* (schema,table) names and (hypertable_id) are unique so should only have
	 * dropped one chunk or none (if not found) */
	Assert(count == 1 || count == 0);

	return count;
}

int
ts_chunk_delete_by_name(const char *schema, const char *table, DropBehavior behavior)
{
	return ts_chunk_delete_by_name_internal(schema, table, behavior, false);
}

static int
ts_chunk_delete_by_relid(Oid relid, DropBehavior behavior, bool preserve_chunk_catalog_row)
{
	if (!OidIsValid(relid))
		return 0;

	return ts_chunk_delete_by_name_internal(get_namespace_name(get_rel_namespace(relid)),
											get_rel_name(relid),
											behavior,
											preserve_chunk_catalog_row);
}

static void
init_scan_by_hypertable_id(ScanIterator *iterator, int32 hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_HYPERTABLE_ID_INDEX);
	ts_scan_iterator_scan_key_init(iterator,
								   Anum_chunk_hypertable_id_idx_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(hypertable_id));
}

int
ts_chunk_delete_by_hypertable_id(int32 hypertable_id)
{
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, RowExclusiveLock, CurrentMemoryContext);

	init_scan_by_hypertable_id(&iterator, hypertable_id);

	return chunk_delete(&iterator, DROP_RESTRICT, false);
}

bool
ts_chunk_exists_with_compression(int32 hypertable_id)
{
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);
	bool found = false;

	init_scan_by_hypertable_id(&iterator, hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		bool isnull_dropped;
		bool isnull_chunk_id =
			slot_attisnull(ts_scan_iterator_slot(&iterator), Anum_chunk_compressed_chunk_id);
		bool dropped = DatumGetBool(
			slot_getattr(ts_scan_iterator_slot(&iterator), Anum_chunk_dropped, &isnull_dropped));
		/* dropped is not NULLABLE */
		Assert(!isnull_dropped);

		if (!isnull_chunk_id && !dropped)
		{
			found = true;
			break;
		}
	}
	ts_scan_iterator_close(&iterator);
	return found;
}

static void
init_scan_by_compressed_chunk_id(ScanIterator *iterator, int32 compressed_chunk_id)
{
	iterator->ctx.index =
		catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_COMPRESSED_CHUNK_ID_INDEX);
	ts_scan_iterator_scan_key_init(iterator,
								   Anum_chunk_compressed_chunk_id_idx_compressed_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(compressed_chunk_id));
}

Chunk *
ts_chunk_get_compressed_chunk_parent(const Chunk *chunk)
{
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);
	Oid parent_id = InvalidOid;

	init_scan_by_compressed_chunk_id(&iterator, chunk->fd.id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		Datum datum;
		bool isnull;

		Assert(!OidIsValid(parent_id));
		datum = slot_getattr(ti->slot, Anum_chunk_id, &isnull);

		if (!isnull)
			parent_id = DatumGetObjectId(datum);
	}

	if (OidIsValid(parent_id))
		return ts_chunk_get_by_id(parent_id, true);

	return NULL;
}

bool
ts_chunk_contains_compressed_data(const Chunk *chunk)
{
	Chunk *parent_chunk = ts_chunk_get_compressed_chunk_parent(chunk);

	return parent_chunk != NULL;
}

List *
ts_chunk_get_chunk_ids_by_hypertable_id(int32 hypertable_id)
{
	List *chunkids = NIL;
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, RowExclusiveLock, CurrentMemoryContext);

	init_scan_by_hypertable_id(&iterator, hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		bool isnull;
		Datum id = slot_getattr(ts_scan_iterator_slot(&iterator), Anum_chunk_id, &isnull);
		if (!isnull)
			chunkids = lappend_int(chunkids, DatumGetInt32(id));
	}

	return chunkids;
}

static ChunkResult
chunk_recreate_constraint(ChunkScanCtx *ctx, ChunkStub *stub)
{
	ChunkConstraints *ccs = stub->constraints;
	ChunkStubScanCtx stubctx = {
		.stub = stub,
	};
	Chunk *chunk;
	int i;

	chunk = chunk_create_from_stub(&stubctx);

	if (stubctx.is_dropped)
		elog(ERROR, "should not be recreating constraints on dropped chunks");

	for (i = 0; i < ccs->num_constraints; i++)
		ts_chunk_constraint_recreate(&ccs->constraints[i], chunk->table_id);

	return CHUNK_PROCESSED;
}

void
ts_chunk_recreate_all_constraints_for_dimension(Hyperspace *hs, int32 dimension_id)
{
	DimensionVec *slices;
	ChunkScanCtx chunkctx;
	int i;

	slices = ts_dimension_slice_scan_by_dimension(dimension_id, 0);

	if (NULL == slices)
		return;

	chunk_scan_ctx_init(&chunkctx, hs, NULL);

	for (i = 0; i < slices->num_slices; i++)
		ts_chunk_constraint_scan_by_dimension_slice(slices->slices[i],
													&chunkctx,
													CurrentMemoryContext);

	chunk_scan_ctx_foreach_chunk_stub(&chunkctx, chunk_recreate_constraint, 0);
	chunk_scan_ctx_destroy(&chunkctx);
}

/*
 * Drops all FK constraints on a given chunk.
 * Currently it is used only for chunks, which have been compressed and
 * contain no data.
 */
void
ts_chunk_drop_fks(const Chunk *const chunk)
{
	Relation rel;
	List *fks;
	ListCell *lc;

	ASSERT_IS_VALID_CHUNK(chunk);

	rel = table_open(chunk->table_id, AccessShareLock);
	fks = copyObject(RelationGetFKeyList(rel));
	table_close(rel, AccessShareLock);

	foreach (lc, fks)
	{
		const ForeignKeyCacheInfo *const fk = lfirst_node(ForeignKeyCacheInfo, lc);
		ts_chunk_constraint_delete_by_constraint_name(chunk->fd.id,
													  get_constraint_name(fk->conoid),
													  true,
													  true);
	}
}

/*
 * Recreates all FK constraints on a chunk by using the constraints on the parent hypertable
 * as a template. Currently it is used only during chunk decompression, since FK constraints
 * are dropped during compression.
 */
void
ts_chunk_create_fks(const Chunk *const chunk)
{
	Relation rel;
	List *fks;
	ListCell *lc;

	ASSERT_IS_VALID_CHUNK(chunk);

	rel = table_open(chunk->hypertable_relid, AccessShareLock);
	fks = copyObject(RelationGetFKeyList(rel));
	table_close(rel, AccessShareLock);
	foreach (lc, fks)
	{
		ForeignKeyCacheInfo *fk = lfirst_node(ForeignKeyCacheInfo, lc);
		ts_chunk_constraint_create_on_chunk(chunk, fk->conoid);
	}
}

static ScanTupleResult
chunk_tuple_update_schema_and_table(TupleInfo *ti, void *data)
{
	FormData_chunk form;
	FormData_chunk *update = data;
	CatalogSecurityContext sec_ctx;
	HeapTuple new_tuple;

	ts_chunk_formdata_fill(&form, ti);

	namestrcpy(&form.schema_name, NameStr(update->schema_name));
	namestrcpy(&form.table_name, NameStr(update->table_name));

	new_tuple = chunk_formdata_make_tuple(&form, ts_scanner_get_tupledesc(ti));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	return SCAN_DONE;
}

static ScanTupleResult
chunk_tuple_update_status(TupleInfo *ti, void *data)
{
	FormData_chunk form;
	FormData_chunk *update = data;
	CatalogSecurityContext sec_ctx;
	HeapTuple new_tuple;

	ts_chunk_formdata_fill(&form, ti);
	form.status = update->status;
	new_tuple = chunk_formdata_make_tuple(&form, ts_scanner_get_tupledesc(ti));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	return SCAN_DONE;
}

static bool
chunk_update_form(FormData_chunk *form)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0], Anum_chunk_idx_id, BTEqualStrategyNumber, F_INT4EQ, form->id);

	return chunk_scan_internal(CHUNK_ID_INDEX,
							   scankey,
							   1,
							   NULL,
							   chunk_tuple_update_schema_and_table,
							   form,
							   0,
							   ForwardScanDirection,
							   RowExclusiveLock,
							   CurrentMemoryContext) > 0;
}

/* update the status flag for chunk. Should not be called directly
 * Use chunk_update_status instead
 */
static bool
chunk_update_status_internal(FormData_chunk *form)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0], Anum_chunk_idx_id, BTEqualStrategyNumber, F_INT4EQ, form->id);

	return chunk_scan_internal(CHUNK_ID_INDEX,
							   scankey,
							   1,
							   NULL,
							   chunk_tuple_update_status,
							   form,
							   0,
							   ForwardScanDirection,
							   RowExclusiveLock,
							   CurrentMemoryContext) > 0;
}

/* status update is done in 2 steps.
 * Do the equivalent of SELECT for UPDATE, followed by UPDATE
 * 1. RowShare lock to read the status.
 * 2. if status != proposed new status
 *      update status using RowExclusiveLock
 * All callers who want to update chunk status should call this function so that locks
 * are acquired correctly.
 */
static bool
chunk_update_status(FormData_chunk *form)
{
	int32 chunk_id = form->id;
	int32 new_status = form->status;
	bool success = true, dropped = false;
	/* lock the chunk tuple for update. Block till we get exclusivetuplelock */
	ScanTupLock scantuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, RowShareLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_ID_INDEX);
	iterator.ctx.tuplock = &scantuplock;

	/* see table_tuple_lock for details about flags that are set in TupleExclusive mode */
	scantuplock.lockflags = TUPLE_LOCK_FLAG_LOCK_UPDATE_IN_PROGRESS;
	if (!IsolationUsesXactSnapshot())
	{
		/* in read committed mode, we follow all updates to this tuple */
		scantuplock.lockflags |= TUPLE_LOCK_FLAG_FIND_LAST_VERSION;
	}

	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_chunk_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool dropped_isnull, status_isnull;
		int status;

		dropped = DatumGetBool(slot_getattr(ti->slot, Anum_chunk_dropped, &dropped_isnull));
		Assert(!dropped_isnull);
		status = DatumGetInt32(slot_getattr(ti->slot, Anum_chunk_status, &status_isnull));
		Assert(!status_isnull);
		if (!dropped && status != new_status)
		{
			success = chunk_update_status_internal(form); // get RowExclusiveLock and update here
		}
	}
	ts_scan_iterator_close(&iterator);
	if (dropped)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("attempt to update status(%d) on dropped chunk %d", new_status, chunk_id)));
	return success;
}

bool
ts_chunk_set_name(Chunk *chunk, const char *newname)
{
	namestrcpy(&chunk->fd.table_name, newname);

	return chunk_update_form(&chunk->fd);
}

bool
ts_chunk_set_schema(Chunk *chunk, const char *newschema)
{
	namestrcpy(&chunk->fd.schema_name, newschema);

	return chunk_update_form(&chunk->fd);
}

bool
ts_chunk_set_unordered(Chunk *chunk)
{
	return ts_chunk_add_status(chunk, CHUNK_STATUS_COMPRESSED_UNORDERED);
}

bool
ts_chunk_add_status(Chunk *chunk, int32 status)
{
	return ts_chunk_set_status(chunk, ts_set_flags_32(chunk->fd.status, status));
}

bool
ts_chunk_set_status(Chunk *chunk, int32 status)
{
	chunk->fd.status = status;

	return chunk_update_status(&chunk->fd);
}

/*
 * Setting (INVALID_CHUNK_ID, true) is valid for an Access Node. It means
 * the data nodes contain the actual compressed chunks, and the meta-chunk is
 * marked as compressed in the Access Node.
 * Setting (is_compressed => false) means that the chunk is uncompressed.
 */
static ScanTupleResult
chunk_change_compressed_status_in_tuple(TupleInfo *ti, int32 compressed_chunk_id,
										bool is_compressed)
{
	FormData_chunk form;
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;

	ts_chunk_formdata_fill(&form, ti);
	if (is_compressed)
	{
		form.compressed_chunk_id = compressed_chunk_id;
		form.status = ts_set_flags_32(form.status, CHUNK_STATUS_COMPRESSED);
	}
	else
	{
		form.compressed_chunk_id = INVALID_CHUNK_ID;
		form.status =
			ts_clear_flags_32(form.status,
							  CHUNK_STATUS_COMPRESSED | CHUNK_STATUS_COMPRESSED_UNORDERED);
	}
	new_tuple = chunk_formdata_make_tuple(&form, ts_scanner_get_tupledesc(ti));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);

	return SCAN_DONE;
}

static ScanTupleResult
chunk_clear_compressed_status_in_tuple(TupleInfo *ti, void *data)
{
	return chunk_change_compressed_status_in_tuple(ti, INVALID_CHUNK_ID, false);
}

static ScanTupleResult
chunk_set_compressed_id_in_tuple(TupleInfo *ti, void *data)
{
	int32 compressed_chunk_id = *((int32 *) data);

	return chunk_change_compressed_status_in_tuple(ti, compressed_chunk_id, true);
}

/*Assume permissions are already checked */
bool
ts_chunk_set_compressed_chunk(Chunk *chunk, int32 compressed_chunk_id)
{
	ScanKeyData scankey[1];
	ScanKeyInit(&scankey[0],
				Anum_chunk_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk->fd.id));
	return chunk_scan_internal(CHUNK_ID_INDEX,
							   scankey,
							   1,
							   chunk_check_ignorearg_dropped_filter,
							   chunk_set_compressed_id_in_tuple,
							   &compressed_chunk_id,
							   0,
							   ForwardScanDirection,
							   RowExclusiveLock,
							   CurrentMemoryContext) > 0;
}

/*Assume permissions are already checked */
bool
ts_chunk_clear_compressed_chunk(Chunk *chunk)
{
	int32 compressed_chunk_id = INVALID_CHUNK_ID;
	ScanKeyData scankey[1];
	ScanKeyInit(&scankey[0],
				Anum_chunk_idx_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk->fd.id));
	return chunk_scan_internal(CHUNK_ID_INDEX,
							   scankey,
							   1,
							   chunk_check_ignorearg_dropped_filter,
							   chunk_clear_compressed_status_in_tuple,
							   &compressed_chunk_id,
							   0,
							   ForwardScanDirection,
							   RowExclusiveLock,
							   CurrentMemoryContext) > 0;
}

/* Used as a tuple found function */
static ScanTupleResult
chunk_rename_schema_name(TupleInfo *ti, void *data)
{
	FormData_chunk form;
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;

	ts_chunk_formdata_fill(&form, ti);
	/* Rename schema name */
	namestrcpy(&form.schema_name, (char *) data);
	new_tuple = chunk_formdata_make_tuple(&form, ts_scanner_get_tupledesc(ti));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	return SCAN_CONTINUE;
}

/* Go through the internal chunk table and rename all matching schemas */
void
ts_chunks_rename_schema_name(char *old_schema, char *new_schema)
{
	NameData old_schema_name;
	ScanKeyData scankey[1];
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CHUNK),
		.index = catalog_get_index(catalog, CHUNK, CHUNK_SCHEMA_NAME_INDEX),
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = chunk_rename_schema_name,
		.data = new_schema,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};

	namestrcpy(&old_schema_name, old_schema);

	ScanKeyInit(&scankey[0],
				Anum_chunk_schema_name_idx_schema_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				NameGetDatum(&old_schema_name));

	ts_scanner_scan(&scanctx);
}

static int
chunk_cmp(const void *ch1, const void *ch2)
{
	const Chunk *v1 = ((const Chunk *) ch1);
	const Chunk *v2 = ((const Chunk *) ch2);

	if (v1->fd.hypertable_id < v2->fd.hypertable_id)
		return -1;
	if (v1->fd.hypertable_id > v2->fd.hypertable_id)
		return 1;
	if (v1->table_id < v2->table_id)
		return -1;
	if (v1->table_id > v2->table_id)
		return 1;
	return 0;
}

/*
 * This is a helper set returning function (SRF) that takes a set returning function context
 * and as argument and returns oids extracted from funcctx->user_fctx (which is Chunk*
 * array). Note that the caller needs to be registered as a set returning function for this
 * to work.
 */
static Datum
chunks_return_srf(FunctionCallInfo fcinfo)
{
	FuncCallContext *funcctx;
	uint64 call_cntr;
	TupleDesc tupdesc;
	Chunk *result_set;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		/* Build a tuple descriptor for our result type */
		/* not quite necessary */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_SCALAR)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	result_set = (Chunk *) funcctx->user_fctx;

	/* do when there is more left to send */
	if (call_cntr < funcctx->max_calls)
		SRF_RETURN_NEXT(funcctx, result_set[call_cntr].table_id);
	else /* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
}

static void
ts_chunk_drop_internal(const Chunk *chunk, DropBehavior behavior, int32 log_level,
					   bool preserve_catalog_row)
{
	ObjectAddress objaddr = {
		.classId = RelationRelationId,
		.objectId = chunk->table_id,
	};

	if (log_level >= 0)
		elog(log_level,
			 "dropping chunk %s.%s",
			 chunk->fd.schema_name.data,
			 chunk->fd.table_name.data);

	/* Remove the chunk from the chunk table */
	ts_chunk_delete_by_relid(chunk->table_id, behavior, preserve_catalog_row);

	/* Drop the table */
	performDeletion(&objaddr, behavior, 0);
}

void
ts_chunk_drop(const Chunk *chunk, DropBehavior behavior, int32 log_level)
{
	ts_chunk_drop_internal(chunk, behavior, log_level, false);
}

void
ts_chunk_drop_preserve_catalog_row(const Chunk *chunk, DropBehavior behavior, int32 log_level)
{
	ts_chunk_drop_internal(chunk, behavior, log_level, true);
}

static void
lock_referenced_tables(Oid table_relid)
{
	List *fk_relids = NIL;
	ListCell *lf;
	List *cachedfkeys = NIL;
	Relation table_rel = table_open(table_relid, AccessShareLock);

	/* this list is from the relcache and can disappear with a cache flush, so
	 * no further catalog access till we save the fk relids */
	cachedfkeys = RelationGetFKeyList(table_rel);
	foreach (lf, cachedfkeys)
	{
		ForeignKeyCacheInfo *cachedfk = lfirst_node(ForeignKeyCacheInfo, lf);

		/* conrelid should always be that of the table we're considering */
		Assert(cachedfk->conrelid == RelationGetRelid(table_rel));
		fk_relids = lappend_oid(fk_relids, cachedfk->confrelid);
	}
	table_close(table_rel, AccessShareLock);
	foreach (lf, fk_relids)
		LockRelationOid(lfirst_oid(lf), AccessExclusiveLock);
}

List *
ts_chunk_do_drop_chunks(Hypertable *ht, int64 older_than, int64 newer_than, int32 log_level,
						List **affected_data_nodes)

{
	uint64 i = 0;
	uint64 num_chunks = 0;
	Chunk *chunks;
	List *dropped_chunk_names = NIL;
	const char *schema_name, *table_name;
	const int32 hypertable_id = ht->fd.id;
	bool has_continuous_aggs;
	List *data_nodes = NIL;
	const MemoryContext oldcontext = CurrentMemoryContext;
	ScanTupLock tuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};

	ts_hypertable_permissions_check(ht->main_table_relid, GetUserId());

	/* We have a FK between hypertable H and PAR. Hypertable H has number of
	 * chunks C1, C2, etc. When we execute "drop table C", PG acquires locks
	 * on C and PAR. If we have a query as "select * from hypertable", this
	 * acquires a lock on C and PAR as well. But the order of the locks is not
	 * the same and results in deadlocks. - github issue #865 We hope to
	 * alleviate the problem by acquiring a lock on PAR before executing the
	 * drop table stmt. This is not fool-proof as we could have multiple
	 * fkrelids and the order of lock acquisition for these could differ as
	 * well. Do not unlock - let the transaction semantics take care of it. */
	lock_referenced_tables(ht->main_table_relid);

	switch (ts_continuous_agg_hypertable_status(hypertable_id))
	{
		case HypertableIsMaterialization:
			has_continuous_aggs = false;
			break;
		case HypertableIsMaterializationAndRaw:
			has_continuous_aggs = true;
			break;
		case HypertableIsRawTable:
			has_continuous_aggs = true;
			break;
		default:
			has_continuous_aggs = false;
			break;
	}

	PG_TRY();
	{
		chunks = get_chunks_in_time_range(ht,
										  older_than,
										  newer_than,
										  DROP_CHUNKS_FUNCNAME,
										  CurrentMemoryContext,
										  &num_chunks,
										  &tuplock);
	}
	PG_CATCH();
	{
		ErrorData *edata;
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		if (edata->sqlerrcode == ERRCODE_LOCK_NOT_AVAILABLE)
		{
			FlushErrorState();
			edata->detail = edata->message;
			edata->message =
				psprintf("some chunks could not be read since they are being concurrently updated");
		}
		ReThrowError(edata);
	}
	PG_END_TRY();

	DEBUG_WAITPOINT("drop_chunks_chunks_found");

	if (has_continuous_aggs)
	{
		int i;

		/* Exclusively lock all chunks, and invalidate the continuous
		 * aggregates in the regions covered by the chunks. We do this in two
		 * steps: first lock all the chunks and then invalidate the
		 * regions. Since we are going to drop the chunks, there is no point
		 * in allowing inserts into them.
		 *
		 * Locking prevents further modification of the dropped region during
		 * this transaction, which allows moving the invalidation threshold
		 * without having to worry about new invalidations while
		 * refreshing. */
		for (i = 0; i < num_chunks; i++)
		{
			LockRelationOid(chunks[i].table_id, ExclusiveLock);

			Assert(hyperspace_get_open_dimension(ht->space, 0)->fd.id ==
				   chunks[i].cube->slices[0]->fd.dimension_id);
		}

		DEBUG_WAITPOINT("drop_chunks_locked");

		/* Invalidate the dropped region to indicate that it was modified.
		 *
		 * The invalidation will allow the refresh command on a continuous
		 * aggregate to see that this region was dropped and and will
		 * therefore be able to refresh accordingly.*/
		for (i = 0; i < num_chunks; i++)
		{
			int64 start = ts_chunk_primary_dimension_start(&chunks[i]);
			int64 end = ts_chunk_primary_dimension_end(&chunks[i]);

			ts_cm_functions->continuous_agg_invalidate_raw_ht(ht, start, end);
		}
	}

	for (i = 0; i < num_chunks; i++)
	{
		char *chunk_name;
		ListCell *lc;

		ASSERT_IS_VALID_CHUNK(&chunks[i]);

		/* store chunk name for output */
		schema_name = quote_identifier(chunks[i].fd.schema_name.data);
		table_name = quote_identifier(chunks[i].fd.table_name.data);
		chunk_name = psprintf("%s.%s", schema_name, table_name);
		dropped_chunk_names = lappend(dropped_chunk_names, chunk_name);

		if (has_continuous_aggs)
			ts_chunk_drop_preserve_catalog_row(chunks + i, DROP_RESTRICT, log_level);
		else
			ts_chunk_drop(chunks + i, DROP_RESTRICT, log_level);

		/* Collect a list of affected data nodes so that we know which data
		 * nodes we need to drop chunks on */
		foreach (lc, chunks[i].data_nodes)
		{
			ChunkDataNode *cdn = lfirst(lc);
			data_nodes = list_append_unique_oid(data_nodes, cdn->foreign_server_oid);
		}
	}

	if (affected_data_nodes)
		*affected_data_nodes = data_nodes;

	DEBUG_WAITPOINT("drop_chunks_end");

	return dropped_chunk_names;
}

/*
 * This is a helper set returning function (SRF) that takes a set returning function context
 * and as argument and returns cstrings extracted from funcctx->user_fctx (which is a List).
 * Note that the caller needs to be registered as a set returning function for this to work.
 */
static Datum
list_return_srf(FunctionCallInfo fcinfo)
{
	FuncCallContext *funcctx;
	uint64 call_cntr;
	TupleDesc tupdesc;
	List *result_set;
	Datum retval;

	/* stuff done only on the first call of the function */
	if (SRF_IS_FIRSTCALL())
	{
		/* Build a tuple descriptor for our result type */
		/* not quite necessary */
		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_SCALAR)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context "
							"that cannot accept type record")));
	}

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	call_cntr = funcctx->call_cntr;
	result_set = castNode(List, funcctx->user_fctx);

	/* do when there is more left to send */
	if (call_cntr < funcctx->max_calls)
	{
		/* store return value and increment linked list */
		retval = CStringGetTextDatum(linitial(result_set));
		funcctx->user_fctx = list_delete_first(result_set);
		SRF_RETURN_NEXT(funcctx, retval);
	}
	else /* do when there is no more left */
		SRF_RETURN_DONE(funcctx);
}

/*
 * Find either the hypertable or the materialized hypertable, if the relid is
 * a continuous aggregate, for the relid.
 *
 * If allow_matht is false, relid should be a cagg or a hypertable.
 * If allow_matht is true, materialized hypertable is also permitted as relid
 */
static Hypertable *
find_hypertable_from_table_or_cagg(Cache *hcache, Oid relid, bool allow_matht)
{
	const char *rel_name;
	Hypertable *ht;

	rel_name = get_rel_name(relid);

	if (!rel_name)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("invalid hypertable or continuous aggregate")));

	ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

	if (ht)
	{
		const ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);
		switch (status)
		{
			case HypertableIsMaterialization:
			case HypertableIsMaterializationAndRaw:
				if (!allow_matht)
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("operation not supported on materialized hypertable"),
							 errhint("Try the operation on the continuous aggregate instead."),
							 errdetail("Hypertable \"%s\" is a materialized hypertable.",
									   rel_name)));
				}
				break;

			default:
				break;
		}
	}
	else
	{
		ContinuousAgg *const cagg = ts_continuous_agg_find_by_relid(relid);

		if (!cagg)
			ereport(ERROR,
					(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
					 errmsg("\"%s\" is not a hypertable or a continuous aggregate", rel_name),
					 errhint("The operation is only possible on a hypertable or continuous"
							 " aggregate.")));

		ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);

		if (!ht)
			ereport(ERROR,
					(errcode(ERRCODE_TS_INTERNAL_ERROR),
					 errmsg("no materialized table for continuous aggregate"),
					 errdetail("Continuous aggregate \"%s\" had a materialized hypertable"
							   " with id %d but it was not found in the hypertable "
							   "catalog.",
							   rel_name,
							   cagg->data.mat_hypertable_id)));
	}

	return ht;
}

Datum
ts_chunk_drop_chunks(PG_FUNCTION_ARGS)
{
	MemoryContext oldcontext;
	FuncCallContext *funcctx;
	Hypertable *ht;
	List *dc_temp = NIL;
	List *dc_names = NIL;
	Oid relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	int64 older_than = PG_INT64_MAX;
	int64 newer_than = PG_INT64_MIN;
	bool verbose;
	int elevel;
	List *data_node_oids = NIL;
	Cache *hcache;
	const Dimension *time_dim;
	Oid time_type;

	TS_PREVENT_FUNC_IF_READ_ONLY();

	/*
	 * When past the first call of the SRF, dropping has already been completed,
	 * so we just return the next chunk in the list of dropped chunks.
	 */
	if (!SRF_IS_FIRSTCALL())
		return list_return_srf(fcinfo);

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid hypertable or continuous aggregate"),
				 errhint("Specify a hypertable or continuous aggregate.")));

	if (PG_ARGISNULL(1) && PG_ARGISNULL(2))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid time range for dropping chunks"),
				 errhint("At least one of older_than and newer_than must be provided.")));

	/* Find either the hypertable or view, or error out if the relid is
	 * neither.
	 *
	 * We should improve the printout since it can either be a proper relid
	 * that does not refer to a hypertable or a continuous aggregate, or a
	 * relid that does not refer to anything at all. */
	hcache = ts_hypertable_cache_pin();
	ht = find_hypertable_from_table_or_cagg(hcache, relid, false);
	Assert(ht != NULL);
	time_dim = hyperspace_get_open_dimension(ht->space, 0);

	if (!time_dim)
		elog(ERROR, "hypertable has no open partitioning dimension");

	time_type = ts_dimension_get_partition_type(time_dim);

	if (!PG_ARGISNULL(1))
		older_than = ts_time_value_from_arg(PG_GETARG_DATUM(1),
											get_fn_expr_argtype(fcinfo->flinfo, 1),
											time_type);

	if (!PG_ARGISNULL(2))
		newer_than = ts_time_value_from_arg(PG_GETARG_DATUM(2),
											get_fn_expr_argtype(fcinfo->flinfo, 2),
											time_type);

	verbose = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
	elevel = verbose ? INFO : DEBUG2;

	/* Initial multi function call setup */
	funcctx = SRF_FIRSTCALL_INIT();

	/* Drop chunks and store their names for return */
	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	PG_TRY();
	{
		dc_temp = ts_chunk_do_drop_chunks(ht, older_than, newer_than, elevel, &data_node_oids);
	}
	PG_CATCH();
	{
		/* An error is raised if there are dependent objects, but the original
		 * message is not very helpful in suggesting that you should use
		 * CASCADE (we don't support it), so we replace the hint with a more
		 * accurate hint for our situation. */
		ErrorData *edata;

		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();

		if (edata->sqlerrcode == ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
			edata->hint = pstrdup("Use DROP ... to drop the dependent objects.");

		ts_cache_release(hcache);
		ReThrowError(edata);
	}
	PG_END_TRY();
	ts_cache_release(hcache);
	dc_names = list_concat(dc_names, dc_temp);

	MemoryContextSwitchTo(oldcontext);

	if (data_node_oids != NIL)
		ts_cm_functions->func_call_on_data_nodes(fcinfo, data_node_oids);

	/* store data for multi function call */
	funcctx->max_calls = list_length(dc_names);
	funcctx->user_fctx = dc_names;

	return list_return_srf(fcinfo);
}

/**
 * This function is used to explicitly specify chunks that are being scanned. It's being
 * processed in the planning phase and removed from the query tree. This means that the
 * actual function implementation will only be executed if something went wrong during
 * explicit chunk exclusion.
 */
Datum
ts_chunks_in(PG_FUNCTION_ARGS)
{
	const char *funcname = get_func_name(FC_FN_OID(fcinfo));

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("illegal invocation of %s function", funcname),
			 errhint("The %s function must appear in the WHERE clause and can only"
					 " be combined with AND operator.",
					 funcname)));
	pg_unreachable();
}

/* Return the compression status for the chunk
 */
ChunkCompressionStatus
ts_chunk_get_compression_status(int32 chunk_id)
{
	ChunkCompressionStatus st = CHUNK_COMPRESS_NONE;
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_ID_INDEX);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_chunk_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool dropped_isnull, status_isnull;
		Datum status;

		bool dropped = DatumGetBool(slot_getattr(ti->slot, Anum_chunk_dropped, &dropped_isnull));
		Assert(!dropped_isnull);

		status = slot_getattr(ti->slot, Anum_chunk_status, &status_isnull);
		Assert(!status_isnull);
		/* Note that dropped attribute takes precedence over everything else.
		 * We should not check status attribute for dropped chunks
		 */
		if (!dropped)
		{
			bool status_is_compressed =
				ts_flags_are_set_32(DatumGetInt32(status), CHUNK_STATUS_COMPRESSED);
			bool status_is_unordered =
				ts_flags_are_set_32(DatumGetInt32(status), CHUNK_STATUS_COMPRESSED_UNORDERED);
			if (status_is_compressed)
			{
				if (status_is_unordered)
					st = CHUNK_COMPRESS_UNORDERED;
				else
					st = CHUNK_COMPRESS_ORDERED;
			}
			else
			{
				Assert(!status_is_unordered);
				st = CHUNK_COMPRESS_NONE;
			}
		}
		else
			st = CHUNK_DROPPED;
	}
	ts_scan_iterator_close(&iterator);
	return st;
}

/*Note that only a compressed chunk can have unordered flag set */
bool
ts_chunk_is_unordered(const Chunk *chunk)
{
	return ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_COMPRESSED_UNORDERED);
}

bool
ts_chunk_is_compressed(const Chunk *chunk)
{
	return ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_COMPRESSED);
}

bool
ts_chunk_is_uncompressed_or_unordered(const Chunk *chunk)
{
	return (ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_COMPRESSED_UNORDERED) ||
			!ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_COMPRESSED));
}

Datum
ts_chunk_show(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->show_chunk(fcinfo);
}

Datum
ts_chunk_create(PG_FUNCTION_ARGS)
{
	return ts_cm_functions->create_chunk(fcinfo);
}

/**
 * Get the chunk status.
 *
 * Values returned are documented above and is a bitwise or of the values.
 *
 * @see CHUNK_STATUS_DEFAULT
 * @see CHUNK_STATUS_COMPRESSED
 * @see CHUNK_STATUS_COMPRESSED_UNORDERED
 */
Datum
ts_chunk_status(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_GETARG_OID(0);
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, /* fail_if_not_found */ true);
	PG_RETURN_INT32(chunk->fd.status);
}

/*
 * Lock the chunk if the lockmode demands it.
 *
 * Also check that the chunk relation actually exists after the lock is
 * acquired. Return true if no locking is necessary or the chunk relation
 * exists and the lock was successfully acquired. Otherwise return false.
 */
bool
ts_chunk_lock_if_exists(Oid chunk_oid, LOCKMODE chunk_lockmode)
{
	/* No lock is requested, so assume relation exists */
	if (chunk_lockmode != NoLock)
	{
		/* Get the lock to synchronize against concurrent drop */
		LockRelationOid(chunk_oid, chunk_lockmode);

		/*
		 * Now that we have the lock, double-check to see if the relation
		 * really exists or not.  If not, assume it was dropped while we
		 * waited to acquire lock, and ignore it.
		 */
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(chunk_oid)))
		{
			/* Release useless lock */
			UnlockRelationOid(chunk_oid, chunk_lockmode);
			/* And ignore this relation */
			return false;
		}
	}

	return true;
}

int
ts_chunk_oid_cmp(const void *p1, const void *p2)
{
	const Chunk *c1 = *((const Chunk **) p1);
	const Chunk *c2 = *((const Chunk **) p2);

	return oid_cmp(&c1->table_id, &c2->table_id);
}

ScanIterator
ts_chunk_scan_iterator_create(MemoryContext result_mcxt)
{
	ScanIterator it = ts_scan_iterator_create(CHUNK, AccessShareLock, result_mcxt);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;

	return it;
}

void
ts_chunk_scan_iterator_set_chunk_id(ScanIterator *it, int32 chunk_id)
{
	it->ctx.index = catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_ID_INDEX);
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(it,
								   Anum_chunk_idx_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));
}
