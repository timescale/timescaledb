/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <access/htup.h>
#include <access/htup_details.h>
#include <access/reloptions.h>
#include <access/tupdesc.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_class.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_inherits.h>
#include <catalog/pg_opfamily.h>
#include <catalog/pg_trigger.h>
#include <catalog/pg_type.h>
#include <catalog/toasting.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/trigger.h>
#include <executor/executor.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <nodes/execnodes.h>
#include <nodes/makefuncs.h>
#include <storage/lmgr.h>
#include <tcop/tcopprot.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/palloc.h>
#include <utils/syscache.h>
#include <utils/timestamp.h>

#include "chunk.h"

#include "compat/compat.h"
#include "bgw_policy/chunk_stats.h"
#include "cache.h"
#include "chunk_index.h"
#include "chunk_scan.h"
#include "cross_module_fn.h"
#include "debug_assert.h"
#include "debug_point.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "errors.h"
#include "export.h"
#include "extension.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "osm_callbacks.h"
#include "partitioning.h"
#include "process_utility.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "time_utils.h"
#include "trigger.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/chunk_column_stats.h"
#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/compression_settings.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_watermark.h"
#include "utils.h"

TS_FUNCTION_INFO_V1(ts_chunk_show_chunks);
TS_FUNCTION_INFO_V1(ts_chunk_drop_chunks);
TS_FUNCTION_INFO_V1(ts_chunk_drop_single_chunk);
TS_FUNCTION_INFO_V1(ts_chunk_attach_osm_table_chunk);
TS_FUNCTION_INFO_V1(ts_chunk_drop_osm_chunk);
TS_FUNCTION_INFO_V1(ts_chunk_id_from_relid);
TS_FUNCTION_INFO_V1(ts_chunk_show);
TS_FUNCTION_INFO_V1(ts_chunk_create);
TS_FUNCTION_INFO_V1(ts_chunk_status);

static bool ts_chunk_add_status(Chunk *chunk, int32 status);

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
chunk_stub_is_valid(const ChunkStub *stub, int16 expected_slices)
{
	return stub && stub->id > 0 && stub->constraints && expected_slices == stub->cube->num_slices &&
		   stub->cube->num_slices == stub->constraints->num_dimension_constraints;
}

typedef ChunkResult (*on_chunk_stub_func)(ChunkScanCtx *ctx, ChunkStub *stub);
static void chunk_scan_ctx_init(ChunkScanCtx *ctx, const Hypertable *ht, const Point *point);
static void chunk_scan_ctx_destroy(ChunkScanCtx *ctx);
static void chunk_collision_scan(ChunkScanCtx *scanctx, const Hypercube *cube);
static int chunk_scan_ctx_foreach_chunk_stub(ChunkScanCtx *ctx, on_chunk_stub_func on_chunk,
											 uint16 limit);
static Datum show_chunks_return_srf(FunctionCallInfo fcinfo);
static int chunk_cmp(const void *ch1, const void *ch2);
static int chunk_point_find_chunk_id(const Hypertable *ht, const Point *p);
static void init_scan_by_qualified_table_name(ScanIterator *iterator, const char *schema_name,
											  const char *table_name);
static Chunk *get_chunks_in_time_range(Hypertable *ht, int64 older_than, int64 newer_than,
									   MemoryContext mctx, uint64 *num_chunks_returned,
									   ScanTupLock *tuplock);
static Chunk *chunk_resurrect(const Hypertable *ht, int chunk_id);
static Chunk *get_chunks_in_creation_time_range(Hypertable *ht, int64 older_than, int64 newer_than,
												MemoryContext mctx, uint64 *num_chunks_returned,
												ScanTupLock *tupLock);

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
	values[AttrNumberGetAttrOffset(Anum_chunk_osm_chunk)] = BoolGetDatum(fd->osm_chunk);
	values[AttrNumberGetAttrOffset(Anum_chunk_creation_time)] = Int64GetDatum(fd->creation_time);

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
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_chunk_osm_chunk)]);
	Assert(!nulls[AttrNumberGetAttrOffset(Anum_chunk_creation_time)]);

	fd->id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_id)]);
	fd->hypertable_id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_hypertable_id)]);
	namestrcpy(&fd->schema_name,
			   DatumGetCString(values[AttrNumberGetAttrOffset(Anum_chunk_schema_name)]));
	namestrcpy(&fd->table_name,
			   DatumGetCString(values[AttrNumberGetAttrOffset(Anum_chunk_table_name)]));

	if (nulls[AttrNumberGetAttrOffset(Anum_chunk_compressed_chunk_id)])
		fd->compressed_chunk_id = INVALID_CHUNK_ID;
	else
		fd->compressed_chunk_id =
			DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_compressed_chunk_id)]);

	fd->dropped = DatumGetBool(values[AttrNumberGetAttrOffset(Anum_chunk_dropped)]);
	fd->status = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_status)]);
	fd->osm_chunk = DatumGetBool(values[AttrNumberGetAttrOffset(Anum_chunk_osm_chunk)]);
	fd->creation_time = DatumGetInt64(values[AttrNumberGetAttrOffset(Anum_chunk_creation_time)]);

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
	const Hyperspace *space = scanctx->ht->space;
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
	const Hyperspace *space = scanctx->ht->space;
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
	const Hyperspace *space = scanctx->ht->space;

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

	chunk_scan_ctx_init(&scanctx, ht, NULL);

	/* Scan for all chunks that collide with the hypercube of the new chunk */
	chunk_collision_scan(&scanctx, hc);
	scanctx.data = &info;

	/* Find chunks that collide */
	chunk_scan_ctx_foreach_chunk_stub(&scanctx, check_for_collisions, 0);

	chunk_scan_ctx_destroy(&scanctx);

	return info.colliding_chunk;
}

/*-
 * Resolve collisions and perform alignment.
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

	chunk_scan_ctx_init(&scanctx, ht, p);

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
	List *alter_cmds = NIL;

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
			alter_cmds = lappend(alter_cmds, cmd);
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
				alter_cmds = lappend(alter_cmds, cmd);
			}
		}

		ReleaseSysCache(tuple);
	}

	if (alter_cmds != NIL)
	{
		ts_alter_table_with_event_trigger(chunk_oid, NULL, alter_cmds, false);
		list_free_deep(alter_cmds);
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
copy_hypertable_acl_to_relid(const Hypertable *ht, const Oid owner_id, const Oid relid)
{
	ts_copy_relation_acl(ht->main_table_relid, relid, owner_id);
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

		/*
		 * Some options require being table owner to set for example statistics
		 * so we have to set them before restoring security context
		 */
		set_attoptions(rel, objaddr.objectId);

		if (uid != saved_uid)
			SetUserIdAndSecContext(saved_uid, sec_ctx);
	}
	else
		elog(ERROR, "invalid relkind \"%c\" when creating chunk", chunk->relkind);

	table_close(rel, AccessShareLock);

	return objaddr.objectId;
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
	const char relkind = RELKIND_RELATION;

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

		len = snprintf(NameStr(chunk->fd.table_name),
					   NAMEDATALEN,
					   "%s_%d_chunk",
					   prefix,
					   chunk->fd.id);

		if (len >= NAMEDATALEN)
			elog(ERROR, "chunk table name too long");
	}
	else
		namestrcpy(&chunk->fd.table_name, table_name);

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

/*
 * Ensure the replica identity setting of a chunk matches that of the root
 * table.
 */
static void
chunk_set_replica_identity(const Chunk *chunk)
{
	Relation ht_rel = relation_open(chunk->hypertable_relid, AccessShareLock);
	ReplicaIdentityStmt stmt = {
		.type = T_ReplicaIdentityStmt,
		.identity_type = ht_rel->rd_rel->relreplident,
	};
	AlterTableCmd cmd = {
		.type = T_AlterTableCmd,
		.def = (Node *) &stmt,
		.subtype = AT_ReplicaIdentity,
	};
	CatalogSecurityContext sec_ctx;

	if (stmt.identity_type == REPLICA_IDENTITY_INDEX)
	{
		ChunkIndexMapping idxm;

		/* Lookup the corresponding chunk index. If this index is
		 * dropped, the behavior is the same as NOTHING (as per PG
		 * documentation). */
		if (!ts_chunk_index_get_by_hypertable_indexrelid(chunk, ht_rel->rd_replidindex, &idxm))
			stmt.identity_type = REPLICA_IDENTITY_NOTHING;
		else
			stmt.name = get_rel_name(idxm.indexoid);
	}

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	AlterTableInternal(chunk->table_id, list_make1(&cmd), false);
	ts_catalog_restore_user(&sec_ctx);
	table_close(ht_rel, NoLock);
}

static void
chunk_create_table_constraints(const Hypertable *ht, const Chunk *chunk)
{
	/* Create the chunk's constraints, triggers, and indexes */
	ts_chunk_constraints_create(ht, chunk);

	if (chunk->relkind == RELKIND_RELATION && !IS_OSM_CHUNK(chunk))
	{
		ts_trigger_create_all_on_chunk(chunk);
		ts_chunk_index_create_all(chunk->fd.hypertable_id,
								  chunk->hypertable_relid,
								  chunk->fd.id,
								  chunk->table_id,
								  InvalidOid);

		chunk_set_replica_identity(chunk);
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

	ts_alter_table_with_event_trigger(chunk->table_id, NULL, list_make1(&drop_inh_cmd), false);
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
	chunk_insert_check_hook_type osm_chunk_insert_hook = ts_get_osm_chunk_insert_hook();

	if (osm_chunk_insert_hook)
	{
		/* OSM only uses first dimension. */
		Dimension *dim = &ht->space->dimensions[0];
		/* convert to PG timestamp from timescaledb internal format */
		int64 range_start =
			ts_internal_to_time_int64(cube->slices[0]->fd.range_start, dim->fd.column_type);
		int64 range_end =
			ts_internal_to_time_int64(cube->slices[0]->fd.range_end, dim->fd.column_type);

		int chunk_exists = osm_chunk_insert_hook(ht->main_table_relid, range_start, range_end);

		if (chunk_exists)
		{
			Oid outfuncid = InvalidOid;
			bool isvarlena;

			Datum start_ts =
				ts_internal_to_time_value(cube->slices[0]->fd.range_start, dim->fd.column_type);
			Datum end_ts =
				ts_internal_to_time_value(cube->slices[0]->fd.range_end, dim->fd.column_type);
			getTypeOutputInfo(dim->fd.column_type, &outfuncid, &isvarlena);
			Assert(!isvarlena);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("distributed hypertable member cannot create chunk on its own"),
					 errmsg("Cannot insert into tiered chunk range of %s.%s - attempt to create "
							"new chunk "
							"with range  [%s %s] failed",
							NameStr(ht->fd.schema_name),
							NameStr(ht->fd.table_name),
							DatumGetCString(OidFunctionCall1(outfuncid, start_ts)),
							DatumGetCString(OidFunctionCall1(outfuncid, end_ts))),
					 errhint(
						 "Hypertable has tiered data with time range that overlaps the insert")));
		}
	}
	/* Insert any new dimension slices into metadata */
	ts_dimension_slice_insert_multi(cube->slices, cube->num_slices);

	Chunk *chunk = chunk_create_only_table_after_lock(ht,
													  cube,
													  schema_name,
													  table_name,
													  prefix,
													  get_next_chunk_id());

	/* Insert any new chunk column stats entries into the catalog */
	ts_chunk_column_stats_insert(ht, chunk);

	chunk_add_constraints(chunk);
	chunk_insert_into_metadata_after_lock(chunk);
	chunk_create_table_constraints(ht, chunk);

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
		.objtype = OBJECT_TABLE,
		.relation = makeRangeVar((char *) NameStr(chunk->fd.schema_name),
								 (char *) NameStr(chunk->fd.table_name),
								 0),
	};
	LOCKMODE lockmode = AlterTableGetLockLevel(alterstmt.cmds);
	AlterTableUtilityContext atcontext = {
		.relid = AlterTableLookupRelation(&alterstmt, lockmode),
	};

	AlterTable(&alterstmt, lockmode, &atcontext);
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
	chunk_create_table_constraints(ht, chunk);

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
	if (OidIsValid(chunk_table_relid) || !ts_hypercube_equal(stub->cube, hc))
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
 * Find the chunk containing the given point, locking all its dimension slices
 * for share. NULL if not found.
 */
Chunk *
ts_chunk_find_for_point(const Hypertable *ht, const Point *p)
{
	int chunk_id = chunk_point_find_chunk_id(ht, p);
	if (chunk_id == INVALID_CHUNK_ID)
	{
		return NULL;
	}

	/* The chunk might be dropped, so we don't fail if we haven't found it. */
	return ts_chunk_get_by_id(chunk_id, /* fail_if_not_found = */ false);
}

/*
 * Create a chunk through insertion of a tuple at a given point.
 */
Chunk *
ts_chunk_create_for_point(const Hypertable *ht, const Point *p, bool *found, const char *schema,
						  const char *prefix)
{
	/*
	 * We're going to have to resurrect or create the chunk.
	 * Serialize chunk creation around a lock on the "main table" to avoid
	 * multiple processes trying to create the same chunk. We use a
	 * ShareUpdateExclusiveLock, which is the weakest lock possible that
	 * conflicts with itself. The lock needs to be held until transaction end.
	 */
	LockRelationOid(ht->main_table_relid, ShareUpdateExclusiveLock);

	DEBUG_WAITPOINT("chunk_create_for_point");

	/*
	 * Recheck if someone else created the chunk before we got the table
	 * lock. The returned chunk will have all slices locked so that they
	 * aren't removed.
	 */
	int chunk_id = chunk_point_find_chunk_id(ht, p);
	if (chunk_id != INVALID_CHUNK_ID)
	{
		/* The chunk might be dropped, so we don't fail if we haven't found it. */
		Chunk *chunk = ts_chunk_get_by_id(chunk_id, /* fail_if_not_found = */ false);
		if (chunk != NULL)
		{
			/*
			 * Chunk was not created by us but by someone else, so we can
			 * release the lock early.
			 */
			UnlockRelationOid(ht->main_table_relid, ShareUpdateExclusiveLock);
			if (found)
				*found = true;
			return chunk;
		}

		/*
		 * If we managed to find some metadata for the chunk (chunk_id != INVALID_CHUNK_ID),
		 * but it is marked as dropped, try to resurrect it.
		 * Not sure if this ever worked for distributed hypertables.
		 */
		chunk = chunk_resurrect(ht, chunk_id);
		if (chunk != NULL)
		{
			if (found)
				*found = true;
			return chunk;
		}
	}

	/* Create the chunk normally. */
	if (found)
		*found = false;

	Chunk *chunk = chunk_create_from_point_after_lock(ht, p, schema, NULL, prefix);

	ASSERT_IS_VALID_CHUNK(chunk);

	return chunk;
}

static void
scan_add_chunk_context(ChunkScanCtx *ctx, int32 chunk_id, List *dimension_vecs, List **l_chunk_ids)
{
	bool found = false;
	ChunkScanEntry *entry = hash_search(ctx->htab, &chunk_id, HASH_ENTER, &found);
	if (!found)
	{
		entry->stub = NULL;
		entry->num_dimension_constraints = 0;
	}

	entry->num_dimension_constraints++;

	/*
	 * A chunk is complete when we've found slices for all required dimensions,
	 * i.e., a complete subspace.
	 */
	if (entry->num_dimension_constraints == list_length(dimension_vecs))
	{
		*l_chunk_ids = lappend_int(*l_chunk_ids, entry->chunk_id);
	}
}

/*
 * Find the chunks that belong to the subspace identified by the given dimension
 * vectors. We might be restricting only some dimensions, so this subspace is
 * not a hypercube, but a hyperplane of some order.
 * Returns a list of matching chunk ids.
 */
List *
ts_chunk_id_find_in_subspace(Hypertable *ht, List *dimension_vecs)
{
	List *chunk_ids = NIL;

	ChunkScanCtx ctx;
	chunk_scan_ctx_init(&ctx, ht, /* point = */ NULL);

	ScanIterator iterator = ts_chunk_constraint_scan_iterator_create(CurrentMemoryContext);

	ListCell *lc;
	foreach (lc, dimension_vecs)
	{
		const DimensionVec *vec = lfirst(lc);

		/*
		 * If it's an entry of type DIMENSION_TYPE_STATS then we need to get
		 * the chunks using the _timescaledb_catalog.chunk_column_stats catalog.
		 */
		Assert(vec->dri != NULL);
		if (vec->dri->dimension->type == DIMENSION_TYPE_STATS)
		{
			ListCell *lc;
			List *range_chunk_ids;

			Assert(vec->num_slices == 0);
			range_chunk_ids = ts_chunk_column_stats_get_chunk_ids_by_scan(vec->dri);

			/* add these chunks to the context appropriately. */
			foreach (lc, range_chunk_ids)
			{
				int32 chunk_id = lfirst_int(lc);

				scan_add_chunk_context(&ctx, chunk_id, dimension_vecs, &chunk_ids);
			}
			continue;
		}

		/*
		 * We shouldn't see a dimension with zero matching dimension slices.
		 * That would mean that no chunks match at all, this should have been
		 * handled earlier by gather_restriction_dimension_vectors().
		 */
		Assert(vec->num_slices > 0);
		for (int i = 0; i < vec->num_slices; i++)
		{
			const DimensionSlice *slice = vec->slices[i];

			ts_chunk_constraint_scan_iterator_set_slice_id(&iterator, slice->fd.id);
			ts_scan_iterator_start_or_restart_scan(&iterator);

			while (ts_scan_iterator_next(&iterator) != NULL)
			{
				TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
				bool PG_USED_FOR_ASSERTS_ONLY isnull = true;
				Datum datum = slot_getattr(ti->slot, Anum_chunk_constraint_chunk_id, &isnull);
				Assert(!isnull);
				int32 current_chunk_id = DatumGetInt32(datum);
				Assert(current_chunk_id != INVALID_CHUNK_ID);

				/*
				 * We have only the dimension constraints here, because we're searching
				 * by dimension slice id.
				 */
				Assert(!slot_attisnull(ts_scan_iterator_slot(&iterator),
									   Anum_chunk_constraint_dimension_slice_id));
				scan_add_chunk_context(&ctx, current_chunk_id, dimension_vecs, &chunk_ids);
			}
		}
	}

	ts_scan_iterator_close(&iterator);

	chunk_scan_ctx_destroy(&ctx);

	return chunk_ids;
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
	chunk->fd.creation_time = GetCurrentTimestamp();

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
		ScanIterator it = ts_dimension_slice_scan_iterator_create(NULL, ti->mctx);
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
	chunk->table_id =
		ts_get_relation_relid(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), false);

	chunk->hypertable_relid = ts_hypertable_id_to_relid(chunk->fd.hypertable_id, false);

	chunk->relkind = get_rel_relkind(chunk->table_id);

	Ensure(chunk->relkind > 0,
		   "relkind for chunk \"%s\".\"%s\" is invalid",
		   NameStr(chunk->fd.schema_name),
		   NameStr(chunk->fd.table_name));

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
chunk_scan_ctx_init(ChunkScanCtx *ctx, const Hypertable *ht, const Point *point)
{
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ChunkScanEntry),
		.hcxt = CurrentMemoryContext,
	};

	memset(ctx, 0, sizeof(*ctx));
	ctx->htab = hash_create("chunk-scan-context", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	ctx->ht = ht;
	ctx->point = point;
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
	for (i = 0; i < scanctx->ht->space->num_dimensions; i++)
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
chunk_resurrect(const Hypertable *ht, int chunk_id)
{
	ScanIterator iterator;
	Chunk *chunk = NULL;
	PG_USED_FOR_ASSERTS_ONLY int count = 0;

	Assert(chunk_id != INVALID_CHUNK_ID);

	iterator = ts_scan_iterator_create(CHUNK, RowExclusiveLock, CurrentMemoryContext);
	ts_chunk_scan_iterator_set_chunk_id(&iterator, chunk_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		HeapTuple new_tuple;

		Assert(count == 0 && chunk == NULL);
		chunk = ts_chunk_build_from_tuple_and_stub(/* chunkptr = */ NULL,
												   ti,
												   /* stub = */ NULL);
		Assert(chunk->fd.dropped);

		/* Create data table and related objects */
		chunk->hypertable_relid = ht->main_table_relid;
		chunk->relkind = RELKIND_RELATION;
		chunk->table_id = chunk_create_table(chunk, ht);
		chunk_create_table_constraints(ht, chunk);

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
 * Scan for the chunk that encloses the given point.
 *
 * In each dimension there can be one or more slices that match the point's
 * coordinate in that dimension. Slices are collected in the scan context's hash
 * table according to the chunk IDs they are associated with. A slice might
 * represent the dimensional bound of multiple chunks, and thus is added to all
 * the hash table slots of those chunks. At the end of the scan there will be at
 * most one chunk that has a complete set of slices, since a point cannot belong
 * to two chunks.
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
static int
chunk_point_find_chunk_id(const Hypertable *ht, const Point *p)
{
	int matching_chunk_id = 0;

	/* The scan context will keep the state accumulated during the scan */
	ChunkScanCtx ctx;
	chunk_scan_ctx_init(&ctx, ht, p);

	/* Scan all dimensions for slices enclosing the point */
	List *all_slices = NIL;
	for (int dimension_index = 0; dimension_index < ctx.ht->space->num_dimensions;
		 dimension_index++)
	{
		ts_dimension_slice_scan_list(ctx.ht->space->dimensions[dimension_index].fd.id,
									 p->coordinates[dimension_index],
									 &all_slices);
	}

	/* Find constraints matching dimension slices. */
	ScanIterator iterator = ts_chunk_constraint_scan_iterator_create(CurrentMemoryContext);

	ListCell *lc;
	foreach (lc, all_slices)
	{
		DimensionSlice *slice = (DimensionSlice *) lfirst(lc);

		ts_chunk_constraint_scan_iterator_set_slice_id(&iterator, slice->fd.id);
		ts_scan_iterator_start_or_restart_scan(&iterator);

		while (ts_scan_iterator_next(&iterator) != NULL)
		{
			TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
			bool PG_USED_FOR_ASSERTS_ONLY isnull = true;
			Datum datum = slot_getattr(ti->slot, Anum_chunk_constraint_chunk_id, &isnull);
			Assert(!isnull);
			int32 current_chunk_id = DatumGetInt32(datum);
			Assert(current_chunk_id != INVALID_CHUNK_ID);

			bool found = false;
			ChunkScanEntry *entry = hash_search(ctx.htab, &current_chunk_id, HASH_ENTER, &found);
			if (!found)
			{
				entry->stub = NULL;
				entry->num_dimension_constraints = 0;
			}

			/*
			 * We have only the dimension constraints here, because we're searching
			 * by dimension slice id.
			 */
			Assert(!slot_attisnull(ts_scan_iterator_slot(&iterator),
								   Anum_chunk_constraint_dimension_slice_id));
			entry->num_dimension_constraints++;

			/*
			 * A chunk is complete when we've found slices for all its dimensions,
			 * i.e., a complete hypercube. Only one chunk matches a given hyperspace
			 * point, so we can stop early.
			 */
			if (entry->num_dimension_constraints == ctx.ht->space->num_dimensions)
			{
				matching_chunk_id = entry->chunk_id;
				break;
			}
		}

		if (matching_chunk_id != INVALID_CHUNK_ID)
		{
			break;
		}
	}

	ts_scan_iterator_close(&iterator);

	chunk_scan_ctx_destroy(&ctx);

	return matching_chunk_id;
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
	chunk_scan_ctx_init(ctx, ht, NULL);

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
	 * show_chunks_return_srf is called even when it is not the first call but only
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
		int64 created_before = PG_INT64_MAX;
		int64 created_after = PG_INT64_MIN;
		Oid time_type;
		Oid arg_type;
		bool older_newer = false;
		bool before_after = false;

		hcache = ts_hypertable_cache_pin();
		ht = ts_resolve_hypertable_from_table_or_cagg(hcache, relid, true);
		Assert(ht != NULL);
		time_dim = hyperspace_get_open_dimension(ht->space, 0);

		if (!time_dim)
			time_dim = hyperspace_get_closed_dimension(ht->space, 0);

		if (time_dim && IS_CLOSED_DIMENSION(time_dim) && (!PG_ARGISNULL(1) || !PG_ARGISNULL(2)))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot specify \"older_than\" or \"newer_than\" for "
							"\"closed\"-like partitioning types"),
					 errhint("Use \"created_before\" and/or \"created_after\" which rely on the "
							 "chunk creation time values.")));

		if (time_dim)
			time_type = ts_dimension_get_partition_type(time_dim);
		else
			time_type = InvalidOid;

		/* note that arg_types will be the same for all specified "ANY" elements for a given call */
		arg_type = InvalidOid;
		if (!PG_ARGISNULL(1))
		{
			arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
			older_than = ts_time_value_from_arg(PG_GETARG_DATUM(1), arg_type, time_type, true);
			older_newer = true;
		}

		if (!PG_ARGISNULL(2))
		{
			arg_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
			newer_than = ts_time_value_from_arg(PG_GETARG_DATUM(2), arg_type, time_type, true);
			older_newer = true;
		}

		/*
		 * We cannot have a mix of [older_than/newer_than] and [created_before/created_after].
		 * So, check that first. Note that created_before/created_after have a type of
		 * TIMESTAMPTZOID regardless of the partitioning type.
		 */

		if (!PG_ARGISNULL(3))
		{
			if (older_newer)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cannot specify \"older_than\" or \"newer_than\" together with "
								"\"created_before\""
								"or \"created_after\"")));

			arg_type = get_fn_expr_argtype(fcinfo->flinfo, 3);
			/* We use the existing function for various type/conversion checks */
			created_before =
				ts_time_value_from_arg(PG_GETARG_DATUM(3), arg_type, TIMESTAMPTZOID, false);
			/* convert into int64 format for comparisons */
			created_before = ts_internal_to_time_int64(created_before, TIMESTAMPTZOID);
			before_after = true;
		}

		if (!PG_ARGISNULL(4))
		{
			if (older_newer)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cannot specify \"older_than\" or \"newer_than\" together with "
								"\"created_before\""
								"or \"created_after\"")));

			arg_type = get_fn_expr_argtype(fcinfo->flinfo, 4);
			/* We use the existing function for various type/conversion checks */
			created_after =
				ts_time_value_from_arg(PG_GETARG_DATUM(4), arg_type, TIMESTAMPTZOID, false);
			/* convert into int64 format for comparisons */
			created_after = ts_internal_to_time_int64(created_after, TIMESTAMPTZOID);
			before_after = true;
		}

		/* if both have not been specified then default to older_newer */
		if (!older_newer && !before_after)
			older_newer = true;

		funcctx = SRF_FIRSTCALL_INIT();
		/*
		 * For INTEGER type dimensions, we support querying using intervals or any
		 * timestamp or date input. For such INTEGER dimensions, we get the chunks
		 * using their creation time values.
		 */
		if (IS_INTEGER_TYPE(time_type) && (arg_type == INTERVALOID || IS_TIMESTAMP_TYPE(arg_type)))
		{
			/* check that we use proper inputs for such cases */
			if (older_newer)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("cannot specify \"older_than\" and/or \"newer_than\" for "
								"\"integer\"-like partitioning types"),
						 errhint(
							 "Use \"created_before\" and/or \"created_after\" which rely on the "
							 "chunk creation time values.")));
			funcctx->user_fctx = get_chunks_in_creation_time_range(ht,
																   created_before,
																   created_after,
																   funcctx->multi_call_memory_ctx,
																   &funcctx->max_calls,
																   NULL);
		}
		else
		{
			/* check that we use proper inputs for such cases */
			if (!older_newer)
			{
				funcctx->user_fctx =
					get_chunks_in_creation_time_range(ht,
													  created_before,
													  created_after,
													  funcctx->multi_call_memory_ctx,
													  &funcctx->max_calls,
													  NULL);
			}
			else
				funcctx->user_fctx = get_chunks_in_time_range(ht,
															  older_than,
															  newer_than,
															  funcctx->multi_call_memory_ctx,
															  &funcctx->max_calls,
															  NULL);
		}
		ts_cache_release(hcache);
	}

	return show_chunks_return_srf(fcinfo);
}

static Chunk *
get_chunks_in_time_range(Hypertable *ht, int64 older_than, int64 newer_than, MemoryContext mctx,
						 uint64 *num_chunks_returned, ScanTupLock *tuplock)
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

	if (time_dim == NULL)
		time_dim = hyperspace_get_closed_dimension(ht->space, 0);

	Ensure(time_dim != NULL,
		   "partitioning dimension not found for hypertable \"%s\".\"%s\"",
		   NameStr(ht->fd.schema_name),
		   NameStr(ht->fd.table_name));

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

			it = ts_dimension_slice_scan_iterator_create(NULL, mctx);
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

bool
ts_chunk_simple_scan_by_reloid(Oid reloid, FormData_chunk *form, bool missing_ok)
{
	bool found = false;

	if (OidIsValid(reloid))
	{
		const char *table = get_rel_name(reloid);

		if (table != NULL)
		{
			Oid nspid = get_rel_namespace(reloid);
			const char *schema = get_namespace_name(nspid);

			found = chunk_simple_scan_by_name(schema, table, form, missing_ok);
		}
	}

	if (!found && !missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("chunk with reloid %u not found", reloid)));

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

	ts_chunk_simple_scan_by_reloid(relid, &form, false);

	last_relid = relid;
	last_id = form.id;

	PG_RETURN_INT32(last_id);
}

bool
ts_chunk_exists_relid(Oid relid)
{
	FormData_chunk form;

	return ts_chunk_simple_scan_by_reloid(relid, &form, true);
}

/*
 * Returns 0 if there is no chunk with such reloid.
 */
int32
ts_chunk_get_hypertable_id_by_reloid(Oid reloid)
{
	FormData_chunk form;

	if (ts_chunk_simple_scan_by_reloid(reloid, &form, /* missing_ok = */ true))
	{
		return form.hypertable_id;
	}

	return 0;
}

FormData_chunk
ts_chunk_get_formdata(int32 chunk_id)
{
	FormData_chunk fd;
	chunk_simple_scan_by_id(chunk_id, &fd, /* missing_ok = */ false);
	return fd;
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
		relid = ts_get_relation_relid(NameStr(form.schema_name), NameStr(form.table_name), true);

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
 * is not only inefficient but could also lead to bugs. For now, we just ignore
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
														   CurrentMemoryContext,
														   AccessShareLock);
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

	/* Delete any row in bgw_policy_chunk-stats corresponding to this chunk */
	ts_bgw_policy_chunk_stats_delete_by_chunk_id(form.id);

	/* Delete any rows in _timescaledb_catalog.chunk_column_stats corresponding to this chunk */
	ts_chunk_column_stats_delete_by_chunk_id(form.id);

	if (form.compressed_chunk_id != INVALID_CHUNK_ID)
	{
		Chunk *compressed_chunk = ts_chunk_get_by_id(form.compressed_chunk_id, false);

		/* The chunk may have been delete by a CASCADE */
		if (compressed_chunk != NULL)
		{
			/* Plain drop without preserving catalog row because this is the compressed
			 * chunk */
			ts_compression_settings_delete(compressed_chunk->table_id);
			ts_chunk_drop(compressed_chunk, behavior, DEBUG1);
		}
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
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);

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

/* Return list of chunks that belong to the given hypertable.
 *
 * The returned chunk objects will not have any constraints or dimension
 * information filled in.
 */
List *
ts_chunk_get_by_hypertable_id(int32 hypertable_id)
{
	List *chunks = NIL;
	Oid hypertable_relid = ts_hypertable_id_to_relid(hypertable_id, false);

	ScanIterator iterator = ts_scan_iterator_create(CHUNK, RowExclusiveLock, CurrentMemoryContext);

	init_scan_by_hypertable_id(&iterator, hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		Chunk *chunk = palloc0(sizeof(Chunk));
		ts_chunk_formdata_fill(&chunk->fd, ti);

		chunk->hypertable_relid = hypertable_relid;

		if (!chunk->fd.dropped)
		{
			chunk->table_id = ts_get_relation_relid(NameStr(chunk->fd.schema_name),
													NameStr(chunk->fd.table_name),
													false);
		}

		chunks = lappend(chunks, chunk);
	}

	return chunks;
}

static ChunkResult
chunk_recreate_constraint(ChunkScanCtx *ctx, ChunkStub *stub)
{
	ChunkStubScanCtx stubctx = {
		.stub = stub,
	};
	Chunk *chunk = chunk_create_from_stub(&stubctx);

	if (stubctx.is_dropped)
		elog(ERROR, "should not be recreating constraints on dropped chunks");

	ts_chunk_constraints_recreate(ctx->ht, chunk);

	return CHUNK_PROCESSED;
}

void
ts_chunk_recreate_all_constraints_for_dimension(Hypertable *ht, int32 dimension_id)
{
	DimensionVec *slices;
	ChunkScanCtx chunkctx;
	int i;

	slices = ts_dimension_slice_scan_by_dimension(dimension_id, 0);

	if (NULL == slices)
		return;

	chunk_scan_ctx_init(&chunkctx, ht, NULL);

	for (i = 0; i < slices->num_slices; i++)
		ts_chunk_constraint_scan_by_dimension_slice(slices->slices[i],
													&chunkctx,
													CurrentMemoryContext);

	chunk_scan_ctx_foreach_chunk_stub(&chunkctx, chunk_recreate_constraint, 0);
	chunk_scan_ctx_destroy(&chunkctx);
}

/*
 * Chunk catalog updates are done in three steps.
 * This is achieved by following this sequence:
 *   1: call lock_chunk_tuple: this finds most recent version of tuple,
 *   locks it, fills TID and data
 *   2: make changes to the data
 *   3: call chunk_update_catalog_tuple with the TID and updated data
 *
 * This is equivalent to SELECT for UPDATE, followed by UPDATE
 *
 * All callers who want to update chunk tuples should respect this so that locks
 * are acquired correctly.
 *
 */
static void
chunk_update_catalog_tuple(ItemPointer tid, FormData_chunk *update)
{
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;
	Catalog *catalog = ts_catalog_get();
	Oid table = catalog_get_table_id(catalog, CHUNK);
	Relation chunk_rel = relation_open(table, RowExclusiveLock);

	new_tuple = chunk_formdata_make_tuple(update, chunk_rel->rd_att);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(chunk_rel, tid, new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	relation_close(chunk_rel, NoLock);
}
/*
 * This function locks the  timescaledb_catalog.chunk tuple (corresponding to chunk_id ) in
 * LockTupleExclusiveMode. It blocks till the lock is acquired. The tid and data (corresponding to
 * the locked tuple) are returned via tid and form arguments. Anyone updating/deleting a chunk entry
 * from the catalog table is expected to first call this function. Refer to
 * chunk_update_catalog_tuple() for more details.
 */
static bool
lock_chunk_tuple(int32 chunk_id, ItemPointer tid, FormData_chunk *form)
{
	ScanTupLock scantuplock = {
		.waitpolicy = LockWaitBlock,
		.lockmode = LockTupleExclusive,
	};
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, RowShareLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_ID_INDEX);
	iterator.ctx.tuplock = &scantuplock;
	/* Keeping the lock since we presumably want to update the tuple */
	iterator.ctx.flags = SCANNER_F_KEEPLOCK;

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
	bool success = false;
	bool dropped, dropped_isnull;

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);

		if (ti->lockresult != TM_Ok)
		{
			if (IsolationUsesXactSnapshot())
			{
				/* For Repeatable Read and Serializable isolation level report error
				 * if we cannot lock the tuple
				 */
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));
			}
			else
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unable to lock chunk catalog tuple, lock result is %d for chunk "
								"ID (%d)",
								ti->lockresult,
								chunk_id)));
			}
		}

		dropped = DatumGetBool(slot_getattr(ti->slot, Anum_chunk_dropped, &dropped_isnull));
		Assert(!dropped_isnull);
		if (!dropped)
		{
			ts_chunk_formdata_fill(form, ti);
			ItemPointer result_tid = ts_scanner_get_tuple_tid(ti);
			tid->ip_blkid = result_tid->ip_blkid;
			tid->ip_posid = result_tid->ip_posid;
			success = true;
			break;
		}
	}
	ts_scan_iterator_close(&iterator);

	return success;
}

bool
ts_chunk_set_name(Chunk *chunk, const char *newname)
{
	FormData_chunk form;
	ItemPointerData tid;
	bool PG_USED_FOR_ASSERTS_ONLY found;

	found = lock_chunk_tuple(chunk->fd.id, &tid, &form);
	Assert(found);

	namestrcpy(&form.table_name, newname);

	chunk_update_catalog_tuple(&tid, &form);
	return true;
}

bool
ts_chunk_set_schema(Chunk *chunk, const char *newschema)
{
	FormData_chunk form;
	ItemPointerData tid;
	bool PG_USED_FOR_ASSERTS_ONLY found;

	found = lock_chunk_tuple(chunk->fd.id, &tid, &form);
	Assert(found);

	namestrcpy(&form.schema_name, newschema);

	chunk_update_catalog_tuple(&tid, &form);
	return true;
}

bool
ts_chunk_set_unordered(Chunk *chunk)
{
	Assert(ts_chunk_is_compressed(chunk));
	return ts_chunk_add_status(chunk, CHUNK_STATUS_COMPRESSED_UNORDERED);
}

bool
ts_chunk_set_partial(Chunk *chunk)
{
	bool set_status;

	Assert(ts_chunk_is_compressed(chunk));
	set_status = ts_chunk_add_status(chunk, CHUNK_STATUS_COMPRESSED_PARTIAL);

	/*
	 * If the status was set then convert the corresponding
	 * _timescaledb_catalog.chunk_column_stats entries "INVALID".
	 */
	if (set_status)
		ts_chunk_column_stats_set_invalid(chunk->fd.hypertable_id, chunk->fd.id);

	return set_status;
}

/* No inserts, updates, and deletes are permitted on a frozen chunk.
 * Compression policies etc do not run on a frozen chunk.
 * Only valid operation is dropping the chunk
 */
bool
ts_chunk_set_frozen(Chunk *chunk)
{
	return ts_chunk_add_status(chunk, CHUNK_STATUS_FROZEN);
}

bool
ts_chunk_unset_frozen(Chunk *chunk)
{
	return ts_chunk_clear_status(chunk, CHUNK_STATUS_FROZEN);
}

bool
ts_chunk_is_frozen(Chunk *chunk)
{
	return ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_FROZEN);
}

/* only caller used to be ts_chunk_unset_frozen. This code was in PG14 block as we run into
 * a "defined but unset" error in CI/CD builds for PG < 14. But now called from recompress as well
 */
bool
ts_chunk_clear_status(Chunk *chunk, int32 status)
{
	/* only frozen status can be cleared for a frozen chunk */
	if (status != CHUNK_STATUS_FROZEN && ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_FROZEN))
	{
		/* chunk in frozen state cannot be modified */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot modify frozen chunk status"),
				 errdetail("chunk id = %d attempt to clear status %d , current status %x ",
						   chunk->fd.id,
						   status,
						   chunk->fd.status)));
	}

	FormData_chunk form;
	ItemPointerData tid;
	bool PG_USED_FOR_ASSERTS_ONLY found;

	found = lock_chunk_tuple(chunk->fd.id, &tid, &form);
	Assert(found);

	/* applying the flags after locking the metadata tuple */
	int32 old_status = form.status;
	form.status = ts_clear_flags_32(form.status, status);
	chunk->fd.status = form.status;

	/* Row-level locks are released at transaction end or during savepoint rollback */
	if (old_status != form.status)
		chunk_update_catalog_tuple(&tid, &form);

	return true;
}

static bool
ts_chunk_add_status(Chunk *chunk, int32 status)
{
	bool status_set = false;
	if (ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_FROZEN))
	{
		/* chunk in frozen state cannot be modified */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot modify frozen chunk status"),
				 errdetail("chunk id = %d attempt to set status %d , current status %x ",
						   chunk->fd.id,
						   status,
						   chunk->fd.status)));
	}
	FormData_chunk form;
	ItemPointerData tid;
	bool PG_USED_FOR_ASSERTS_ONLY found;

	found = lock_chunk_tuple(chunk->fd.id, &tid, &form);
	Assert(found);

	/* Somebody could update the status before we are able to lock it so check again */
	if (ts_flags_are_set_32(form.status, CHUNK_STATUS_FROZEN))
	{
		/* chunk in frozen state cannot be modified */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot modify frozen chunk status"),
				 errdetail("chunk id = %d attempt to set status %d , current status %d ",
						   chunk->fd.id,
						   status,
						   form.status)));
	}

	/* applying the flags after locking the metadata tuple */
	int32 old_status = form.status;
	form.status = ts_set_flags_32(form.status, status);
	chunk->fd.status = form.status;

	/* Row-level locks are released at transaction end or during savepoint rollback */
	if (old_status != form.status)
	{
		chunk_update_catalog_tuple(&tid, &form);
		status_set = true;
	}

	return status_set;
}

/*Assume permissions are already checked */
bool
ts_chunk_set_compressed_chunk(Chunk *chunk, int32 compressed_chunk_id)
{
	uint32 flags = CHUNK_STATUS_COMPRESSED;
	uint32 mstatus = ts_set_flags_32(chunk->fd.status, flags);
	if (ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_FROZEN))
	{
		/* chunk in frozen state cannot be modified */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot modify frozen chunk status"),
				 errdetail("chunk id = %d attempt to set status %d , current status %d ",
						   chunk->fd.id,
						   mstatus,
						   chunk->fd.status)));
	}

	FormData_chunk form;
	ItemPointerData tid;
	bool PG_USED_FOR_ASSERTS_ONLY found;

	found = lock_chunk_tuple(chunk->fd.id, &tid, &form);
	Assert(found);

	/* Somebody could update the status before we are able to lock it so check again */
	if (ts_flags_are_set_32(form.status, CHUNK_STATUS_FROZEN))
	{
		/* chunk in frozen state cannot be modified */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot modify frozen chunk status"),
				 errdetail("chunk id = %d attempt to set status %d , current status %d ",
						   chunk->fd.id,
						   mstatus,
						   form.status)));
	}

	/* re-applying the flags after locking the metadata tuple */
	form.status = ts_set_flags_32(form.status, flags);
	form.compressed_chunk_id = compressed_chunk_id;

	chunk->fd.compressed_chunk_id = form.compressed_chunk_id;
	chunk->fd.status = form.status;

	chunk_update_catalog_tuple(&tid, &form);
	return true;
}

/*Assume permissions are already checked */
bool
ts_chunk_clear_compressed_chunk(Chunk *chunk)
{
	uint32 flags = CHUNK_STATUS_COMPRESSED | CHUNK_STATUS_COMPRESSED_UNORDERED |
				   CHUNK_STATUS_COMPRESSED_PARTIAL;
	uint32 mstatus = ts_clear_flags_32(chunk->fd.status, flags);
	if (ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_FROZEN))
	{
		/* chunk in frozen state cannot be modified */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot modify frozen chunk status"),
				 errdetail("chunk id = %d attempt to set status %d , current status %d ",
						   chunk->fd.id,
						   mstatus,
						   chunk->fd.status)));
	}

	FormData_chunk form;
	ItemPointerData tid;
	bool PG_USED_FOR_ASSERTS_ONLY found;

	found = lock_chunk_tuple(chunk->fd.id, &tid, &form);
	Assert(found);

	/* Somebody could update the status before we are able to lock it so check again */
	if (ts_flags_are_set_32(form.status, CHUNK_STATUS_FROZEN))
	{
		/* chunk in frozen state cannot be modified */
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("cannot modify frozen chunk status"),
				 errdetail("chunk id = %d attempt to set status %d , current status %d ",
						   chunk->fd.id,
						   mstatus,
						   form.status)));
	}

	/* re-applying the flags after locking the metadata tuple */
	form.status = ts_clear_flags_32(form.status, flags);
	form.compressed_chunk_id = INVALID_CHUNK_ID;

	chunk->fd.compressed_chunk_id = form.compressed_chunk_id;
	chunk->fd.status = form.status;

	chunk_update_catalog_tuple(&tid, &form);
	return true;
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
show_chunks_return_srf(FunctionCallInfo fcinfo)
{
	FuncCallContext *funcctx;
	uint64 call_cntr;
	TupleDesc tupdesc;
	Chunk *result_set;
	Chunk *curr_chunk;

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

	/*
	 * skip if it's an OSM chunk. Ideally this check could be done deep down in
	 * functions like "chunk_scan_context_add_chunk", "chunk_tuple_dropped_filter"
	 * etc. but they are used by other APIs like drop_chunks, chunk_scan_find, etc
	 * which need access to the OSM chunk. Trying to unify scan functions across
	 * all such usages seems to be too much of an overhaul as compared to this.
	 *
	 * Check the index appropriately first.
	 */
	if (call_cntr < funcctx->max_calls)
	{
		curr_chunk = &result_set[call_cntr];
		if (IS_OSM_CHUNK(curr_chunk))
		{
			call_cntr = ++funcctx->call_cntr;
		}
	}

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
			 NameStr(chunk->fd.schema_name),
			 NameStr(chunk->fd.table_name));

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
						Oid time_type, Oid arg_type, bool older_newer)

{
	uint64 num_chunks = 0;
	Chunk *chunks;
	const char *schema_name, *table_name;
	const int32 hypertable_id = ht->fd.id;
	bool has_continuous_aggs, is_materialization_hypertable;
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

	is_materialization_hypertable = false;

	switch (ts_continuous_agg_hypertable_status(hypertable_id))
	{
		case HypertableIsMaterialization:
			has_continuous_aggs = false;
			is_materialization_hypertable = true;
			break;
		case HypertableIsMaterializationAndRaw:
			has_continuous_aggs = true;
			is_materialization_hypertable = true;
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
		/*
		 * For INTEGER type dimensions, we support querying using intervals or any
		 * timestamp or date input. For such INTEGER dimensions, we get the chunks
		 * using their creation time values.
		 */
		if (IS_INTEGER_TYPE(time_type) && (arg_type == INTERVALOID || IS_TIMESTAMP_TYPE(arg_type)))
		{
			chunks = get_chunks_in_creation_time_range(ht,
													   older_than,
													   newer_than,
													   CurrentMemoryContext,
													   &num_chunks,
													   &tuplock);
		}
		else
		{
			if (!older_newer)
				chunks = get_chunks_in_creation_time_range(ht,
														   older_than,
														   newer_than,
														   CurrentMemoryContext,
														   &num_chunks,
														   &tuplock);
			else
				chunks = get_chunks_in_time_range(ht,
												  older_than,
												  newer_than,
												  CurrentMemoryContext,
												  &num_chunks,
												  &tuplock);
		}
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

	int32 osm_chunk_id = ts_chunk_get_osm_chunk_id(ht->fd.id);
	if (has_continuous_aggs)
	{
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
		for (uint64 i = 0; i < num_chunks; i++)
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
		for (uint64 i = 0; i < num_chunks; i++)
		{
			if (osm_chunk_id == chunks[i].fd.id)
			{
				// we do not rebuild continuous aggs if tiered data is dropped */
				continue;
			}
			int64 start = ts_chunk_primary_dimension_start(&chunks[i]);
			int64 end = ts_chunk_primary_dimension_end(&chunks[i]);

			ts_cm_functions->continuous_agg_invalidate_raw_ht(ht, start, end);
		}
	}

	bool all_caggs_finalized = ts_continuous_agg_hypertable_all_finalized(hypertable_id);
	List *dropped_chunk_names = NIL;
	for (uint64 i = 0; i < num_chunks; i++)
	{
		char *chunk_name;

		ASSERT_IS_VALID_CHUNK(&chunks[i]);

		/* frozen chunks are skipped. Not dropped. */
		if (!ts_chunk_validate_chunk_status_for_operation(&chunks[i],
														  CHUNK_DROP,
														  false /*throw_error */) ||
			osm_chunk_id == chunks[i].fd.id)
		{
			continue;
		}

		/* store chunk name for output */
		schema_name = quote_identifier(NameStr(chunks[i].fd.schema_name));
		table_name = quote_identifier(NameStr(chunks[i].fd.table_name));
		chunk_name = psprintf("%s.%s", schema_name, table_name);
		dropped_chunk_names = lappend(dropped_chunk_names, chunk_name);

		if (has_continuous_aggs && !all_caggs_finalized)
			ts_chunk_drop_preserve_catalog_row(chunks + i, DROP_RESTRICT, log_level);
		else
			ts_chunk_drop(chunks + i, DROP_RESTRICT, log_level);
	}
	// if we have tiered chunks cascade drop to tiering layer as well
	if (osm_chunk_id != INVALID_CHUNK_ID)
	{
		hypertable_drop_chunks_hook_type osm_drop_chunks_hook =
			ts_get_osm_hypertable_drop_chunks_hook();
		if (osm_drop_chunks_hook)
		{
			ListCell *lc;
			Dimension *dim = &ht->space->dimensions[0];
			/* convert to PG timestamp from timescaledb internal format */
			int64 range_start = ts_internal_to_time_int64(newer_than, dim->fd.column_type);
			int64 range_end = ts_internal_to_time_int64(older_than, dim->fd.column_type);
			Chunk *osm_chunk = ts_chunk_get_by_id(osm_chunk_id, true);
			List *osm_dropped_names = osm_drop_chunks_hook(osm_chunk->table_id,
														   NameStr(ht->fd.schema_name),
														   NameStr(ht->fd.table_name),
														   range_start,
														   range_end);
			foreach (lc, osm_dropped_names)
			{
				dropped_chunk_names = lappend(dropped_chunk_names, lfirst(lc));
			}
		}
	}

	/* When dropping chunks for a given CAgg then force set the watermark */
	if (is_materialization_hypertable)
	{
		bool isnull;
		int64 watermark = ts_hypertable_get_open_dim_max_value(ht, 0, &isnull);
		ts_cagg_watermark_update(ht, watermark, isnull, true);
	}

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

Datum
ts_chunk_drop_single_chunk(PG_FUNCTION_ARGS)
{
	Oid chunk_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	char *chunk_table_name = get_rel_name(chunk_relid);
	char *chunk_schema_name = get_namespace_name(get_rel_namespace(chunk_relid));

	const Chunk *ch = ts_chunk_get_by_name_with_memory_context(chunk_schema_name,
															   chunk_table_name,
															   CurrentMemoryContext,
															   true);
	Assert(ch != NULL);
	ts_chunk_validate_chunk_status_for_operation(ch, CHUNK_DROP, true /*throw_error */);
	/* do not drop any chunk dependencies */
	ts_chunk_drop(ch, DROP_RESTRICT, LOG);
	PG_RETURN_BOOL(true);
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

	/*
	 * Marked volatile to suppress the -Wclobbered warning. The warning is
	 * actually incorrect because these values are not used after longjmp.
	 */
	volatile int64 older_than = PG_INT64_MAX;
	volatile int64 newer_than = PG_INT64_MIN;
	volatile int64 created_before = PG_INT64_MAX;
	volatile int64 created_after = PG_INT64_MIN;
	volatile bool older_newer = false;
	volatile bool before_after = false;

	bool verbose;
	int elevel;
	Cache *hcache;
	const Dimension *time_dim;
	Oid time_type;
	Oid arg_type;

	TS_PREVENT_FUNC_IF_READ_ONLY();
	arg_type = InvalidOid;

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

	/* Find either the hypertable or view, or error out if the relid is
	 * neither.
	 *
	 * We should improve the printout since it can either be a proper relid
	 * that does not refer to a hypertable or a continuous aggregate, or a
	 * relid that does not refer to anything at all. */
	hcache = ts_hypertable_cache_pin();
	ht = ts_resolve_hypertable_from_table_or_cagg(hcache, relid, false);
	Assert(ht != NULL);
	time_dim = hyperspace_get_open_dimension(ht->space, 0);

	if (!time_dim)
		elog(ERROR, "hypertable has no open partitioning dimension");

	time_type = ts_dimension_get_partition_type(time_dim);

	/* note that arg_types will be the same for all specified "ANY" elements for a given call */
	if (!PG_ARGISNULL(1))
	{
		arg_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
		older_than = ts_time_value_from_arg(PG_GETARG_DATUM(1), arg_type, time_type, true);
		older_newer = true;
	}

	if (!PG_ARGISNULL(2))
	{
		arg_type = get_fn_expr_argtype(fcinfo->flinfo, 2);
		newer_than = ts_time_value_from_arg(PG_GETARG_DATUM(2), arg_type, time_type, true);
		older_newer = true;
	}

	/*
	 * We cannot have a mix of [older_than/newer_than] and [created_before/created_after].
	 * So, check that first. Note that created_before/created_after have a type of
	 * TIMESTAMPTZOID regardless of the partitioning type.
	 */

	if (!PG_ARGISNULL(4))
	{
		if (older_newer)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot specify \"older_than\" or \"newer_than\" together with "
							"\"created_before\""
							"or \"created_after\""),
					 errhint("\"older_than\" and/or \"newer_than\" is recommended with "
							 "\"time\"-like partitioning"
							 " and  \"created_before\" and/or \"created_after\" is recommended "
							 "with \"integer\"-like"
							 " partitioning.")));

		arg_type = get_fn_expr_argtype(fcinfo->flinfo, 4);
		/* We use the existing function for various type/conversion checks */
		created_before =
			ts_time_value_from_arg(PG_GETARG_DATUM(4), arg_type, TIMESTAMPTZOID, false);
		/* convert into int64 format for comparisons */
		created_before = ts_internal_to_time_int64(created_before, TIMESTAMPTZOID);
		before_after = true;
		older_than = created_before;
	}

	if (!PG_ARGISNULL(5))
	{
		if (older_newer)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot specify \"older_than\" or \"newer_than\" together with "
							"\"created_before\""
							" or \"created_after\""),
					 errhint("\"older_than\" and/or \"newer_than\" is recommended with "
							 "\"time\"-like partitioning"
							 " and  \"created_before\" and/or \"created_after\" is recommended "
							 "with \"integer\"-like"
							 " partitioning.")));
		arg_type = get_fn_expr_argtype(fcinfo->flinfo, 5);
		/* We use the existing function for various type/conversion checks */
		created_after = ts_time_value_from_arg(PG_GETARG_DATUM(5), arg_type, TIMESTAMPTZOID, false);
		/* convert into int64 format for comparisons */
		created_after = ts_internal_to_time_int64(created_after, TIMESTAMPTZOID);
		before_after = true;
		newer_than = created_after;
	}

	/* if both have not been specified then error out */
	if (!older_newer && !before_after)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid time range for dropping chunks"),
				 errhint("At least one of older_than/newer_than or created_before/created_after"
						 " must be provided.")));

	/*
	 * For INTEGER type dimensions, we support querying using intervals or any
	 * timestamp or date input. For such INTEGER dimensions, we get the chunks
	 * using their creation time values.
	 */
	if (IS_INTEGER_TYPE(time_type) && (arg_type == INTERVALOID || IS_TIMESTAMP_TYPE(arg_type)))
	{
		/* check that we use proper inputs for such cases */
		if (older_newer)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("cannot specify \"older_than\" and/or \"newer_than\" for "
							"\"integer\"-like partitioning types"),
					 errhint("Use \"created_before\" and/or \"created_after\" which rely on the "
							 "chunk creation time values.")));
	}

	verbose = PG_ARGISNULL(3) ? false : PG_GETARG_BOOL(3);
	elevel = verbose ? INFO : DEBUG2;

	/* Initial multi function call setup */
	funcctx = SRF_FIRSTCALL_INIT();

	/* Drop chunks and store their names for return */
	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	PG_TRY();
	{
		dc_temp = ts_chunk_do_drop_chunks(ht,
										  older_than,
										  newer_than,
										  elevel,
										  time_type,
										  arg_type,
										  older_newer);
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

	/* store data for multi function call */
	funcctx->max_calls = list_length(dc_names);
	funcctx->user_fctx = dc_names;

	return list_return_srf(fcinfo);
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
			bool status_is_partial =
				ts_flags_are_set_32(DatumGetInt32(status), CHUNK_STATUS_COMPRESSED_PARTIAL);
			if (status_is_compressed)
			{
				if (status_is_unordered || status_is_partial)
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

/* Note that only a compressed chunk can have unordered flag set */
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
ts_chunk_needs_recompression(const Chunk *chunk)
{
	Assert(ts_chunk_is_compressed(chunk));
	return ts_chunk_is_partial(chunk) || ts_chunk_is_unordered(chunk);
}

/* Note that only a compressed chunk can have partial flag set */
bool
ts_chunk_is_partial(const Chunk *chunk)
{
	return ts_flags_are_set_32(chunk->fd.status, CHUNK_STATUS_COMPRESSED_PARTIAL);
}

static const char *
get_chunk_operation_str(ChunkOperation cmd)
{
	switch (cmd)
	{
		case CHUNK_INSERT:
			return "Insert";
		case CHUNK_DELETE:
			return "Delete";
		case CHUNK_UPDATE:
			return "Update";
		case CHUNK_COMPRESS:
			return "compress_chunk";
		case CHUNK_DECOMPRESS:
			return "decompress_chunk";
		case CHUNK_DROP:
			return "drop_chunk";
		default:
			return "Unsupported";
	}
}

bool
ts_chunk_validate_chunk_status_for_operation(const Chunk *chunk, ChunkOperation cmd,
											 bool throw_error)
{
	Oid chunk_relid = chunk->table_id;
	int32 chunk_status = chunk->fd.status;

	/*
	 * Block everything but DELETE on OSM chunks.
	 */
	if (chunk->fd.osm_chunk)
	{
		switch (cmd)
		{
			case CHUNK_DROP:
				return true;
				break;

			default:
				if (throw_error)
					elog(ERROR,
						 "%s not permitted on tiered chunk \"%s\" ",
						 get_chunk_operation_str(cmd),
						 get_rel_name(chunk_relid));
				return false;
				break;
		}
	}

	/* Handle frozen chunks */
	if (ts_flags_are_set_32(chunk_status, CHUNK_STATUS_FROZEN))
	{
		/* Data modification is not permitted on a frozen chunk */
		switch (cmd)
		{
			case CHUNK_INSERT:
			case CHUNK_DELETE:
			case CHUNK_UPDATE:
			case CHUNK_COMPRESS:
			case CHUNK_DECOMPRESS:
			case CHUNK_DROP:
			{
				if (throw_error)
					elog(ERROR,
						 "%s not permitted on frozen chunk \"%s\" ",
						 get_chunk_operation_str(cmd),
						 get_rel_name(chunk_relid));
				return false;
				break;
			}
			default:
				break; /*supported operations */
		}
	}
	/* Handle unfrozen chunks */
	else
	{
		switch (cmd)
		{
			/* supported operations */
			case CHUNK_INSERT:
			case CHUNK_DELETE:
			case CHUNK_UPDATE:
				break;
			/* Only uncompressed chunks can be compressed */
			case CHUNK_COMPRESS:
			{
				if (ts_flags_are_set_32(chunk_status, CHUNK_STATUS_COMPRESSED))
					ereport((throw_error ? ERROR : NOTICE),
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("chunk \"%s\" is already compressed",
									get_rel_name(chunk_relid))));
				return false;
			}
			/* Only compressed chunks can be decompressed */
			case CHUNK_DECOMPRESS:
			{
				if (!ts_flags_are_set_32(chunk_status, CHUNK_STATUS_COMPRESSED))
					ereport((throw_error ? ERROR : NOTICE),
							(errcode(ERRCODE_DUPLICATE_OBJECT),
							 errmsg("chunk \"%s\" is already decompressed",
									get_rel_name(chunk_relid))));
				return false;
			}
			default:
				break;
		}
	}

	return true;
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
 * Values returned are documented above and is a bitwise or of the
 * CHUNK_STATUS_XXX values.
 *
 * @see CHUNK_STATUS_DEFAULT
 * @see CHUNK_STATUS_COMPRESSED
 * @see CHUNK_STATUS_COMPRESSED_UNORDERED
 * @see CHUNK_STATUS_FROZEN
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

/*
 * Create a hypercube for the OSM chunk
 * The initial range for the OSM chunk will be from INT64_MAX - 1 to INT64_MAX.
 * This range was chosen to minimize interference with tuple routing and
 * occupy a range outside of potential values as there must be no overlap
 * between the hypercube occupied by the osm chunk and actual chunks.
 */
static Hypercube *
fill_hypercube_for_osm_chunk(Hyperspace *hs)
{
	Hypercube *cube = ts_hypercube_alloc(hs->num_dimensions);
	Assert(hs->num_dimensions == 1); // does not work with partitioned range
	for (int i = 0; i < hs->num_dimensions; i++)
	{
		const Dimension *dim = &hs->dimensions[i];
		Assert(dim->type == DIMENSION_TYPE_OPEN);
		cube->slices[i] = ts_dimension_slice_create(dim->fd.id, PG_INT64_MAX - 1, PG_INT64_MAX);
		cube->num_slices++;
	}
	Assert(cube->num_slices == 1);
	return cube;
}

/* adds foreign table as a chunk to the hypertable.
 * creates a dummy chunk constraint for the time dimension.
 * These constraints are recorded in the chunk-dimension slice metadata.
 * They are NOT added as CHECK constraints on the foreign table.
 *
 * Does not add any inheritable constraints or indexes that are already
 * defined on the hypertable.
 *
 * This is used to add an OSM table as a chunk.
 * Set the osm_chunk flag to true.
 */
static void
add_foreign_table_as_chunk(Oid relid, Hypertable *parent_ht)
{
	Hyperspace *hs = parent_ht->space;
	Catalog *catalog = ts_catalog_get();
	CatalogSecurityContext sec_ctx;
	Chunk *chunk;
	char *relschema = get_namespace_name(get_rel_namespace(relid));
	char *relname = get_rel_name(relid);

	Oid ht_ownerid = ts_rel_get_owner(parent_ht->main_table_relid);

	if (!has_privs_of_role(GetUserId(), ht_ownerid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of hypertable \"%s\"",
						get_rel_name(parent_ht->main_table_relid))));

	Assert(get_rel_relkind(relid) == RELKIND_FOREIGN_TABLE);
	if (hs->num_dimensions > 1)
		elog(ERROR,
			 "cannot attach a  foreign table to a hypertable that has more than 1 dimension");
	/* Create a new chunk based on the hypercube */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	chunk = ts_chunk_create_base(ts_catalog_table_next_seq_id(catalog, CHUNK),
								 hs->num_dimensions,
								 RELKIND_RELATION);
	ts_catalog_restore_user(&sec_ctx);

	/* fill in the correct table_name for the chunk*/
	chunk->fd.hypertable_id = hs->hypertable_id;
	chunk->fd.osm_chunk = true; /* this is an OSM chunk */
	chunk->cube = fill_hypercube_for_osm_chunk(hs);
	chunk->hypertable_relid = parent_ht->main_table_relid;
	chunk->constraints = ts_chunk_constraints_alloc(1, CurrentMemoryContext);

	namestrcpy(&chunk->fd.schema_name, relschema);
	namestrcpy(&chunk->fd.table_name, relname);

	/* Insert chunk */
	ts_chunk_insert_lock(chunk, RowExclusiveLock);

	/* insert dimension slices if they do not exist.
	 */
	ts_dimension_slice_insert_multi(chunk->cube->slices, chunk->cube->num_slices);
	/* check constraints are not automatically created for foreign tables.
	 * See: ts_chunk_constraints_add_dimension_constraints.
	 * Collect all the check constraints from the hypertable and add them to the
	 * foreign table. Otherwise, cannot add as child of the hypertable (pg inheritance
	 * code will error. Note that the name of the check constraint on the hypertable
	 * and the foreign table chunk should match.
	 */
	ts_chunk_constraints_add_inheritable_check_constraints(chunk->constraints,
														   chunk->fd.id,
														   chunk->relkind,
														   chunk->hypertable_relid);
	chunk_create_table_constraints(parent_ht, chunk);
	/* Add dimension constraints for the chunk */
	ts_chunk_constraints_add_dimension_constraints(chunk->constraints, chunk->fd.id, chunk->cube);
	ts_chunk_constraints_insert_metadata(chunk->constraints);
	chunk_add_inheritance(chunk, parent_ht);
	/*
	 * Update hypertable entry with tiering status information.
	 * XXX: For compatibility reasons, we set the noncontiguous flag, but
	 * this should be reverted as soon as the newer version of the OSM extension
	 * is rolled out.
	 * Noncontiguous flag should not be set since the chunk should be empty upon
	 * creation, with an invalid range assigned, so ordered append should be allowed.
	 * Once the data is moved into the OSM chunk, then our catalog should be
	 * updated with proper API calls from the OSM extension.
	 */
	parent_ht->fd.status =
		ts_set_flags_32(parent_ht->fd.status,
						HYPERTABLE_STATUS_OSM | HYPERTABLE_STATUS_OSM_CHUNK_NONCONTIGUOUS);
	ts_hypertable_update_status_osm(parent_ht);
}

void
ts_chunk_merge_on_dimension(const Hypertable *ht, Chunk *chunk, const Chunk *merge_chunk,
							int32 dimension_id)
{
	const DimensionSlice *slice, *merge_slice;
	int num_ccs = 0;
	bool dimension_slice_found = false;

	if (chunk->hypertable_relid != merge_chunk->hypertable_relid)
		ereport(ERROR,
				(errmsg("cannot merge chunks from different hypertables"),
				 errhint("chunk 1: \"%s\", chunk 2: \"%s\"",
						 get_rel_name(chunk->table_id),
						 get_rel_name(merge_chunk->table_id))));

	for (int i = 0; i < chunk->cube->num_slices; i++)
	{
		if (chunk->cube->slices[i]->fd.dimension_id == dimension_id)
		{
			slice = chunk->cube->slices[i];
			merge_slice = merge_chunk->cube->slices[i];
			dimension_slice_found = true;
		}
		else if (chunk->cube->slices[i]->fd.id != merge_chunk->cube->slices[i]->fd.id)
		{
			/* If the slices do not match (except on time dimension), we cannot merge the chunks. */
			ereport(ERROR,
					(errmsg("cannot merge chunks with different partitioning schemas"),
					 errhint("chunk 1: \"%s\", chunk 2: \"%s\" have different slices on "
							 "dimension ID %d",
							 get_rel_name(chunk->table_id),
							 get_rel_name(merge_chunk->table_id),
							 chunk->cube->slices[i]->fd.dimension_id)));
		}
	}

	if (!dimension_slice_found)
		ereport(ERROR,
				(errmsg("cannot find slice for merging dimension"),
				 errhint("chunk 1: \"%s\", chunk 2: \"%s\", dimension ID %d",
						 get_rel_name(chunk->table_id),
						 get_rel_name(merge_chunk->table_id),
						 dimension_id)));

	if (slice->fd.range_end != merge_slice->fd.range_start)
		ereport(ERROR,
				(errmsg("cannot merge non-adjacent chunks over supplied dimension"),
				 errhint("chunk 1: \"%s\", chunk 2: \"%s\", dimension ID %d",
						 get_rel_name(chunk->table_id),
						 get_rel_name(merge_chunk->table_id),
						 dimension_id)));

	num_ccs =
		ts_chunk_constraint_scan_by_dimension_slice_id(slice->fd.id, NULL, CurrentMemoryContext);

	/* There should always be an associated chunk constraint to a dimension slice.
	 * This can only occur when the catalog metadata is corrupt.
	 */
	if (num_ccs <= 0)
		ereport(ERROR,
				(errmsg("missing chunk constraint for dimension slice"),
				 errhint("chunk: \"%s\", slice ID %d",
						 get_rel_name(chunk->table_id),
						 slice->fd.id)));

	DimensionSlice *new_slice =
		ts_dimension_slice_create(dimension_id, slice->fd.range_start, merge_slice->fd.range_end);

	/* Only if there is exactly one chunk constraint for the merged dimension slice
	 * we can go ahead and delete it since we are dropping the chunk.
	 */
	if (num_ccs == 1)
		ts_dimension_slice_delete_by_id(slice->fd.id, false);

	/* Check for dimension slice already exists, if not create a new one. */
	ScanTupLock tuplock = {
		.lockmode = LockTupleKeyShare,
		.waitpolicy = LockWaitBlock,
	};
	if (!ts_dimension_slice_scan_for_existing(new_slice, &tuplock))
	{
		ts_dimension_slice_insert(new_slice);
	}

	ts_chunk_constraint_update_slice_id(chunk->fd.id, slice->fd.id, new_slice->fd.id);
	ChunkConstraints *ccs = ts_chunk_constraints_alloc(1, CurrentMemoryContext);
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, AccessShareLock, CurrentMemoryContext);

	ts_chunk_constraint_scan_iterator_set_slice_id(&iterator, new_slice->fd.id);

	ts_scanner_foreach(&iterator)
	{
		bool isnull;
		Datum d;

		d = slot_getattr(ts_scan_iterator_slot(&iterator), Anum_chunk_constraint_chunk_id, &isnull);

		if (!isnull && DatumGetInt32(d) == chunk->fd.id)
		{
			num_ccs++;
			ts_chunk_constraints_add_from_tuple(ccs, ts_scan_iterator_tuple_info(&iterator));
		}
	}

	if (num_ccs <= 0)
		ereport(ERROR,
				(errmsg("missing chunk constraint for merged dimension slice"),
				 errhint("chunk: \"%s\", slice ID %d",
						 get_rel_name(chunk->table_id),
						 new_slice->fd.id)));

	/* Update the slice in the chunk's hypercube. Needed to make recreate constraints work. */
	for (int i = 0; i < chunk->cube->num_slices; i++)
	{
		if (chunk->cube->slices[i]->fd.dimension_id == dimension_id)
		{
			chunk->cube->slices[i] = new_slice;
			break;
		}
	}

	/* Delete the old constraint */
	for (int i = 0; i < chunk->constraints->num_constraints; i++)
	{
		const ChunkConstraint *cc = &chunk->constraints->constraints[i];

		if (cc->fd.dimension_slice_id == slice->fd.id)
		{
			ObjectAddress constrobj = {
				.classId = ConstraintRelationId,
				.objectId = get_relation_constraint_oid(chunk->table_id,
														NameStr(cc->fd.constraint_name),
														false),
			};

			performDeletion(&constrobj, DROP_RESTRICT, 0);
			break;
		}
	}

	/* We have to recreate the chunk constraints since we are changing
	 * table constraints when updating the slice.
	 */
	ChunkConstraints *oldccs = chunk->constraints;
	chunk->constraints = ccs;
	ts_process_utility_set_expect_chunk_modification(true);
	ts_chunk_constraints_create(ht, chunk);
	ts_process_utility_set_expect_chunk_modification(false);
	chunk->constraints = oldccs;

	ts_chunk_drop(merge_chunk, DROP_RESTRICT, 1);
}

/* Internal API used by OSM extension. OSM table is a foreign table that is
 * attached as a chunk of the hypertable. A chunk needs dimension constraints. We
 * add dummy constraints for the OSM chunk and then attach it to the hypertable.
 * OSM extension is responsible for maintaining any constraints on this table.
 */
Datum
ts_chunk_attach_osm_table_chunk(PG_FUNCTION_ARGS)
{
	Oid hypertable_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Oid ftable_relid = PG_ARGISNULL(1) ? InvalidOid : PG_GETARG_OID(1);
	bool ret = false;

	Cache *hcache;
	Hypertable *ht =
		ts_hypertable_cache_get_cache_and_entry(hypertable_relid, CACHE_FLAG_MISSING_OK, &hcache);

	if (!ht)
	{
		char *name = get_rel_name(hypertable_relid);

		if (!name)
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid Oid")));
		else
			elog(ERROR, "\"%s\" is not a hypertable", name);
	}

	if (get_rel_relkind(ftable_relid) == RELKIND_FOREIGN_TABLE)
	{
		add_foreign_table_as_chunk(ftable_relid, ht);
		ret = true;
	}
	ts_cache_release(hcache);

	PG_RETURN_BOOL(ret);
}

static ScanTupleResult
chunk_tuple_osm_chunk_found(TupleInfo *ti, void *arg)
{
	bool isnull;
	Datum osm_chunk = slot_getattr(ti->slot, Anum_chunk_osm_chunk, &isnull);

	Assert(!isnull);
	bool is_osm_chunk = DatumGetBool(osm_chunk);

	if (!is_osm_chunk)
		return SCAN_CONTINUE;

	int *chunk_id = (int *) arg;
	Datum chunk_id_datum = slot_getattr(ti->slot, Anum_chunk_id, &isnull);
	Assert(!isnull);
	*chunk_id = DatumGetInt32(chunk_id_datum);
	return SCAN_DONE;
}

/* get OSM chunk id associated with the hypertable */
int
ts_chunk_get_osm_chunk_id(int hypertable_id)
{
	int chunk_id = INVALID_CHUNK_ID;
	ScanKeyData scankey[2];
	bool is_osm_chunk = true;
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CHUNK),
		.index = catalog_get_index(catalog, CHUNK, CHUNK_OSM_CHUNK_INDEX),
		.nkeys = 2,
		.scankey = scankey,
		.data = &chunk_id,
		.tuple_found = chunk_tuple_osm_chunk_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/*
	 * Perform an index scan on hypertable ID + osm_chunk
	 */
	ScanKeyInit(&scankey[0],
				Anum_chunk_osm_chunk_idx_osm_chunk,
				BTEqualStrategyNumber,
				F_BOOLEQ,
				BoolGetDatum(is_osm_chunk));
	ScanKeyInit(&scankey[1],
				Anum_chunk_osm_chunk_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));

	int num_found = ts_scanner_scan(&scanctx);

	if (num_found > 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_TS_INTERNAL_ERROR),
				 errmsg("More than 1 OSM chunk found for hypertable (%d)", hypertable_id)));
	}

	return chunk_id;
}

/* Upon creation, OSM chunks are assigned an invalid range [INT64_MAX -1, infinity) */
bool
ts_osm_chunk_range_is_invalid(int64 range_start, int64 range_end)
{
	return ((range_end == PG_INT64_MAX) && (range_start == range_end - 1));
}

int32
ts_chunk_get_osm_slice_id(int32 chunk_id, int32 time_dim_id)
{
	Chunk *chunk = ts_chunk_get_by_id(chunk_id, true);
	const DimensionSlice *ds = ts_hypercube_get_slice_by_dimension_id(chunk->cube, time_dim_id);
	const int slice_id = ds->fd.id;
	return slice_id;
}

/*
 * Initialization and access method for ChunkVec. This needs to be extended to support additional
 * operations.
 */
static ChunkVec *
chunk_vec_expand(ChunkVec *chunks, uint32 new_capacity)
{
	if (chunks != NULL && chunks->capacity >= new_capacity)
		return chunks;

	if (chunks == NULL)
		chunks = palloc(CHUNK_VEC_SIZE(new_capacity));
	else
		chunks = repalloc(chunks, CHUNK_VEC_SIZE(new_capacity));

	chunks->capacity = new_capacity;

	return chunks;
}

ChunkVec *
ts_chunk_vec_create(int32 capacity)
{
	ChunkVec *chunks = chunk_vec_expand(NULL, capacity);

	chunks->num_chunks = 0;
	return chunks;
}

ChunkVec *
ts_chunk_vec_sort(ChunkVec **chunks)
{
	ChunkVec *vec = *chunks;

	if (vec->num_chunks > 1)
		qsort(&vec->chunks, vec->num_chunks, sizeof(Chunk), chunk_cmp);

	return vec;
}

ChunkVec *
ts_chunk_vec_add_from_tuple(ChunkVec **chunks, TupleInfo *ti)
{
	Chunk *chunkptr = NULL;
	ChunkVec *vec;
	int num_constraints_hint;
	ScanIterator it;

	vec = *chunks;
	num_constraints_hint = 2;

	if (vec->num_chunks + 1 > vec->capacity)
		*chunks = vec = chunk_vec_expand(vec, vec->capacity + DEFAULT_CHUNK_VEC_SIZE);

	chunkptr = &vec->chunks[vec->num_chunks++];
	ts_chunk_formdata_fill(&chunkptr->fd, ti);

	/*
	 * Get the chunk constraints, hypercube slices and relation details.
	 */
	chunkptr->constraints =
		ts_chunk_constraint_scan_by_chunk_id(chunkptr->fd.id, num_constraints_hint, ti->mctx);

	it = ts_dimension_slice_scan_iterator_create(NULL, ti->mctx);
	chunkptr->cube = ts_hypercube_from_constraints(chunkptr->constraints, &it);
	ts_scan_iterator_close(&it);

	chunkptr->table_id = ts_get_relation_relid(NameStr(chunkptr->fd.schema_name),
											   NameStr(chunkptr->fd.table_name),
											   true);
	chunkptr->hypertable_relid = ts_hypertable_id_to_relid(chunkptr->fd.hypertable_id, false);
	chunkptr->relkind = get_rel_relkind(chunkptr->table_id);

	return vec;
}

/*
 * Prepare for an index scan for all the chunks matching the given hypertable_id and range criteria.
 */
static int
chunk_creation_time_set_range(ScanIterator *it, Oid hypertable_id, StrategyNumber start_strategy,
							  int64 start_value, StrategyNumber end_strategy, int64 end_value)
{
	TypeCacheEntry *tce;

	it->ctx.index =
		catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_HYPERTABLE_ID_CREATION_TIME_INDEX);
	ts_scan_iterator_scan_key_reset(it);

	ts_scan_iterator_scan_key_init(it,
								   Anum_chunk_hypertable_id_creation_time_idx_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(hypertable_id));

	tce = lookup_type_cache(TIMESTAMPTZOID, TYPECACHE_BTREE_OPFAMILY);

	/* If both are valid strategies then a proper scan within these limits will be performed */
	if (start_strategy != InvalidStrategy)
	{
		Oid opno =
			get_opfamily_member(tce->btree_opf, TIMESTAMPTZOID, TIMESTAMPTZOID, start_strategy);
		Oid proc = get_opcode(opno);

		Assert(OidIsValid(proc));

		ts_scan_iterator_scan_key_init(it,
									   Anum_chunk_hypertable_id_creation_time_idx_creation_time,
									   start_strategy,
									   proc,
									   Int64GetDatum(start_value));
	}
	if (end_strategy != InvalidStrategy)
	{
		Oid opno =
			get_opfamily_member(tce->btree_opf, TIMESTAMPTZOID, TIMESTAMPTZOID, end_strategy);
		Oid proc = get_opcode(opno);

		Assert(OidIsValid(proc));

		ts_scan_iterator_scan_key_init(it,
									   Anum_chunk_hypertable_id_creation_time_idx_creation_time,
									   end_strategy,
									   proc,
									   Int64GetDatum(end_value));
	}

	return it->ctx.nkeys;
}

/*
 * Perform an index scan for given hypertable_id and chunk creation time range.
 *
 * Returns an array of chunks using ChunkVec the encloses all the chunks satisfying the range
 * criteria.
 */
static Chunk *
get_chunks_in_creation_time_range_limit(Hypertable *ht, StrategyNumber start_strategy,
										int64 start_value, StrategyNumber end_strategy,
										int64 end_value, int limit, uint64 *num_chunks,
										ScanTupLock *tupLock)
{
	ScanIterator it;
	ChunkVec *chunk_vec = NULL;

	it = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;
	it.ctx.tuplock = tupLock;

	chunk_creation_time_set_range(&it,
								  ht->fd.id,
								  start_strategy,
								  start_value,
								  end_strategy,
								  end_value);
	it.ctx.limit = limit;

#ifdef TS_DEBUG
	/* allow testing of the chunk vec expansion in debug builds */
	if (limit == -1)
		limit = 1;
#endif

	chunk_vec = ts_chunk_vec_create(limit > 0 ? limit : DEFAULT_CHUNK_VEC_SIZE);

	ts_scanner_foreach(&it)
	{
		bool dropped_isnull, dropped;

		TupleInfo *ti = ts_scan_iterator_tuple_info(&it);

		/* only add chunks that are not dropped */
		dropped = DatumGetBool(slot_getattr(ti->slot, Anum_chunk_dropped, &dropped_isnull));
		Assert(!dropped_isnull);
		if (!dropped)
			ts_chunk_vec_add_from_tuple(&chunk_vec, ti);
	}

	ts_scan_iterator_close(&it);

	ts_chunk_vec_sort(&chunk_vec);

	*num_chunks = chunk_vec->num_chunks;

	return chunk_vec->chunks;
}

Chunk *
get_chunks_in_creation_time_range(Hypertable *ht, int64 older_than, int64 newer_than,
								  MemoryContext mctx, uint64 *num_chunks_returned,
								  ScanTupLock *tupLock)
{
	MemoryContext oldcontext;
	Chunk *chunks = NULL;
	StrategyNumber start_strategy;
	StrategyNumber end_strategy;
	uint64 num_chunks = 0;

	if (older_than <= newer_than)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid chunk creation time range"),
				 errhint("The start of the time range must be before the end.")));

	start_strategy = (newer_than == PG_INT64_MIN) ? InvalidStrategy : BTGreaterEqualStrategyNumber;
	end_strategy = (older_than == PG_INT64_MAX) ? InvalidStrategy : BTLessStrategyNumber;

	oldcontext = MemoryContextSwitchTo(mctx);
	chunks = get_chunks_in_creation_time_range_limit(ht,
													 start_strategy,
													 newer_than,
													 end_strategy,
													 older_than,
													 -1,
													 &num_chunks,
													 tupLock);
	MemoryContextSwitchTo(oldcontext);
	*num_chunks_returned = num_chunks;

	return chunks;
}

Datum
ts_chunk_drop_osm_chunk(PG_FUNCTION_ARGS)
{
	Oid hypertable_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_resolve_hypertable_from_table_or_cagg(hcache, hypertable_relid, true);
	int32 osm_chunk_id = ts_chunk_get_osm_chunk_id(ht->fd.id);
	Chunk *osm_chunk = ts_chunk_get_by_id(osm_chunk_id, true);

	ts_chunk_validate_chunk_status_for_operation(osm_chunk, CHUNK_DROP, true);

	/* do not drop any chunk dependencies */
	ts_chunk_drop(osm_chunk, DROP_RESTRICT, LOG);

	/* reset hypertable OSM status */
	ht->fd.status =
		ts_clear_flags_32(ht->fd.status,
						  HYPERTABLE_STATUS_OSM | HYPERTABLE_STATUS_OSM_CHUNK_NONCONTIGUOUS);
	ts_hypertable_update_status_osm(ht);
	ts_cache_release(hcache);
	PG_RETURN_BOOL(true);
}
