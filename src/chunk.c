#include <postgres.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>
#include <catalog/indexing.h>
#include <catalog/pg_inherits.h>
#include <catalog/toasting.h>
#include <commands/trigger.h>
#include <commands/tablecmds.h>
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

#include "chunk.h"
#include "chunk_index.h"
#include "catalog.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "partitioning.h"
#include "hypertable.h"
#include "hypercube.h"
#include "scanner.h"
#include "process_utility.h"
#include "trigger.h"
#include "compat.h"
#include "utils.h"

typedef bool (*on_chunk_func) (ChunkScanCtx *ctx, Chunk *chunk);

static void chunk_scan_ctx_init(ChunkScanCtx *ctx, Hyperspace *hs, Point *p);
static void chunk_scan_ctx_destroy(ChunkScanCtx *ctx);
static void chunk_collision_scan(ChunkScanCtx *scanctx, Hypercube *cube);
static int	chunk_scan_ctx_foreach_chunk(ChunkScanCtx *ctx, on_chunk_func on_chunk, uint16 limit);


static void
chunk_insert_relation(Relation rel, Chunk *chunk)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_chunk];
	bool		nulls[Natts_chunk] = {false};
	CatalogSecurityContext sec_ctx;

	memset(values, 0, sizeof(values));
	values[Anum_chunk_id - 1] = Int32GetDatum(chunk->fd.id);
	values[Anum_chunk_hypertable_id - 1] = Int32GetDatum(chunk->fd.hypertable_id);
	values[Anum_chunk_schema_name - 1] = NameGetDatum(&chunk->fd.schema_name);
	values[Anum_chunk_table_name - 1] = NameGetDatum(&chunk->fd.table_name);

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_insert_values(rel, desc, values, nulls);
	catalog_restore_user(&sec_ctx);
}

static void
chunk_insert_lock(Chunk *chunk, LOCKMODE lock)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;

	rel = heap_open(catalog->tables[CHUNK].id, lock);
	chunk_insert_relation(rel, chunk);
	heap_close(rel, lock);
}

static void
chunk_fill(Chunk *chunk, HeapTuple tuple)
{
	memcpy(&chunk->fd, GETSTRUCT(tuple), sizeof(FormData_chunk));
	chunk->table_id = get_relname_relid(chunk->fd.table_name.data,
										get_namespace_oid(chunk->fd.schema_name.data, true));
	chunk->hypertable_relid = inheritance_parent_relid(chunk->table_id);
}

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
static bool
do_dimension_alignment(ChunkScanCtx *scanctx, Chunk *chunk)
{
	Hypercube  *cube = scanctx->data;
	Hyperspace *space = scanctx->space;
	int			i;

	for (i = 0; i < space->num_dimensions; i++)
	{
		Dimension  *dim = &space->dimensions[i];
		DimensionSlice *chunk_slice,
				   *cube_slice;
		int64		coord = scanctx->point->coordinates[i];

		if (!dim->fd.aligned)
			continue;

		/*
		 * The chunk might not have a slice for each dimension, so we cannot
		 * use array indexing. Fetch slice by dimension ID instead.
		 */
		chunk_slice = hypercube_get_slice_by_dimension_id(chunk->cube, dim->fd.id);

		if (NULL == chunk_slice)
			continue;

		cube_slice = cube->slices[i];

		/*
		 * Only cut-to-align if the slices collide and are not identical
		 * (i.e., if we are reusing an existing slice we should not cut it)
		 */
		if (!dimension_slices_equal(cube_slice, chunk_slice) &&
			dimension_slices_collide(cube_slice, chunk_slice))
			dimension_slice_cut(cube_slice, chunk_slice, coord);
	}

	return true;
}

/*
 * Calculate, and potentially set, a new chunk interval for an open dimension.
 */
static bool
calculate_and_set_new_chunk_interval(Hypertable *ht, Point *p)
{
	Hyperspace *hs = ht->space;
	Dimension  *dim = NULL;
	Datum		datum;
	int64		chunk_interval,
				coord;
	int			i;

	if (!OidIsValid(ht->chunk_sizing_func) ||
		ht->fd.chunk_target_size <= 0)
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
		elog(WARNING, "adaptive chunking enabled on hypertable \"%s\" without an open (time) dimension",
			 get_rel_name(ht->main_table_relid));

		return false;
	}

	coord = p->coordinates[i];
	datum = OidFunctionCall3(ht->chunk_sizing_func, dim->fd.id, coord, ht->fd.chunk_target_size);
	chunk_interval = DatumGetInt64(datum);

	/* Check if the function didn't set and interval or nothing changed */
	if (chunk_interval <= 0 || chunk_interval == dim->fd.interval_length)
		return false;

	/* Update the dimension */
	dimension_set_chunk_interval(dim, chunk_interval);

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
static bool
do_collision_resolution(ChunkScanCtx *scanctx, Chunk *chunk)
{
	Hypercube  *cube = scanctx->data;
	Hyperspace *space = scanctx->space;
	int			i;

	if (chunk->cube->num_slices != space->num_dimensions ||
		!hypercubes_collide(cube, chunk->cube))
		return true;

	for (i = 0; i < space->num_dimensions; i++)
	{
		DimensionSlice *cube_slice = cube->slices[i];
		DimensionSlice *chunk_slice = chunk->cube->slices[i];
		int64		coord = scanctx->point->coordinates[i];

		/*
		 * Only cut if we aren't reusing an existing slice and there is a
		 * collision
		 */
		if (!dimension_slices_equal(cube_slice, chunk_slice) &&
			dimension_slices_collide(cube_slice, chunk_slice))
		{
			dimension_slice_cut(cube_slice, chunk_slice, coord);

			/*
			 * Redo the collision check after each cut since cutting in one
			 * dimension might have resolved the collision in another
			 */
			if (!hypercubes_collide(cube, chunk->cube))
				return true;
		}
	}

	Assert(!hypercubes_collide(cube, chunk->cube));

	return true;
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
chunk_collision_resolve(Hyperspace *hs, Hypercube *cube, Point *p)
{
	ChunkScanCtx scanctx;

	chunk_scan_ctx_init(&scanctx, hs, p);

	/* Scan for all chunks that collide with the hypercube of the new chunk */
	chunk_collision_scan(&scanctx, cube);
	scanctx.data = cube;

	/* Cut the hypercube in any aligned dimensions */
	chunk_scan_ctx_foreach_chunk(&scanctx, do_dimension_alignment, 0);

	/*
	 * If there are any remaining collisions with chunks, then cut-to-fit to
	 * resolve those collisions
	 */
	chunk_scan_ctx_foreach_chunk(&scanctx, do_collision_resolution, 0);

	chunk_scan_ctx_destroy(&scanctx);
}

static int
chunk_add_constraints(Chunk *chunk)
{
	int			num_added;

	num_added = chunk_constraints_add_dimension_constraints(chunk->constraints,
															chunk->fd.id,
															chunk->cube);
	num_added += chunk_constraints_add_inheritable_constraints(chunk->constraints,
															   chunk->fd.id,
															   chunk->hypertable_relid);

	return num_added;
}

static List *
get_reloptions(Oid relid)
{
	HeapTuple	tuple;
	Datum		datum;
	bool		isnull;
	List	   *options;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions,
							&isnull);

	options = untransformRelOptions(datum);

	ReleaseSysCache(tuple);

	return options;
}

/* applies the attributes and statistics target for columns on the hypertable
   to columns on the chunk */
static void
set_attoptions(Relation ht_rel, Oid chunk_oid)
{
	TupleDesc	tupleDesc = RelationGetDescr(ht_rel);
	int			natts = tupleDesc->natts;
	int			attno;

	for (attno = 1; attno <= natts; attno++)
	{
		Form_pg_attribute attribute = tupleDesc->attrs[attno - 1];
		char	   *attributeName = NameStr(attribute->attname);
		HeapTuple	tuple;
		Datum		options;
		bool		isnull;

		/* Ignore dropped */
		if (attribute->attisdropped)
			continue;

		tuple = SearchSysCacheAttName(RelationGetRelid(ht_rel), attributeName);

		Assert(tuple != NULL);

		/*
		 * Pass down the attribute options (ALTER TABLE ALTER COLUMN SET
		 * attribute_option)
		 */
		options = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attoptions,
								  &isnull);

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
		options = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attstattarget,
								  &isnull);
		if (!isnull)
		{
			int32		target = DatumGetInt32(options);

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
	Datum		toast_options = transformRelOptions((Datum) 0,
													stmt->options,
													"toast",
													validnsps,
													true,
													false);

	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	NewRelationCreateToastTable(chunk_oid, toast_options);
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
static Oid
chunk_create_table(Chunk *chunk, Hypertable *ht)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;
	ObjectAddress objaddr;
	int			sec_ctx;
	CreateStmt	stmt = {
		.type = T_CreateStmt,
		.relation = makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), 0),
		.inhRelations = list_make1(makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), 0)),
		.tablespacename = hypertable_select_tablespace_name(ht, chunk),
		.options = get_reloptions(ht->main_table_relid),
	};
	Oid			uid,
				saved_uid;

	rel = heap_open(ht->main_table_relid, AccessShareLock);

	/*
	 * If the chunk is created in the internal schema, become the catalog
	 * owner, otherwise become the hypertable owner
	 */
	if (namestrcmp(&chunk->fd.schema_name, INTERNAL_SCHEMA_NAME) == 0)
		uid = catalog->owner_uid;
	else
		uid = rel->rd_rel->relowner;

	GetUserIdAndSecContext(&saved_uid, &sec_ctx);

	if (uid != saved_uid)
		SetUserIdAndSecContext(uid, sec_ctx | SECURITY_LOCAL_USERID_CHANGE);

	objaddr = DefineRelation(&stmt,
							 RELKIND_RELATION,
							 rel->rd_rel->relowner,
							 NULL
#if PG10
							 ,NULL
#endif
		);

	/*
	 * need to create a toast table explicitly for some of the option setting
	 * to work
	 */
	create_toast_table(&stmt, objaddr.objectId);

	if (uid != saved_uid)
		SetUserIdAndSecContext(saved_uid, sec_ctx);


	set_attoptions(rel, objaddr.objectId);

	heap_close(rel, AccessShareLock);

	return objaddr.objectId;
}

static Chunk *
chunk_create_after_lock(Hypertable *ht, Point *p, const char *schema, const char *prefix)
{
	Hyperspace *hs = ht->space;
	Catalog    *catalog = catalog_get();
	CatalogSecurityContext sec_ctx;
	Hypercube  *cube;
	Chunk	   *chunk;

	/*
	 * If the user has enabled adaptive chunking, call the function to
	 * calculate and set the new chunk time interval.
	 */
	calculate_and_set_new_chunk_interval(ht, p);

	/* Calculate the hypercube for a new chunk that covers the tuple's point */
	cube = hypercube_calculate_from_point(hs, p);

	/* Resolve collisions with other chunks by cutting the new hypercube */
	chunk_collision_resolve(hs, cube, p);

	/* Create a new chunk based on the hypercube */
	catalog_become_owner(catalog, &sec_ctx);
	chunk = chunk_create_stub(catalog_table_next_seq_id(catalog, CHUNK), hs->num_dimensions);
	catalog_restore_user(&sec_ctx);

	chunk->fd.hypertable_id = hs->hypertable_id;
	chunk->cube = cube;
	chunk->hypertable_relid = ht->main_table_relid;
	namestrcpy(&chunk->fd.schema_name, schema);
	snprintf(chunk->fd.table_name.data, NAMEDATALEN,
			 "%s_%d_chunk", prefix, chunk->fd.id);

	/* Insert chunk */
	chunk_insert_lock(chunk, RowExclusiveLock);

	/* Insert any new dimension slices */
	dimension_slice_insert_multi(cube->slices, cube->num_slices);

	/* Add metadata for dimensional and inheritable constraints */
	chunk_add_constraints(chunk);

	/* Create the actual table relation for the chunk */
	chunk->table_id = chunk_create_table(chunk, ht);

	if (!OidIsValid(chunk->table_id))
		elog(ERROR, "could not create chunk table");

	/* Create the chunk's constraints, triggers, and indexes */
	chunk_constraints_create(chunk->constraints,
							 chunk->table_id,
							 chunk->fd.id,
							 chunk->hypertable_relid,
							 chunk->fd.hypertable_id);

	trigger_create_all_on_chunk(ht, chunk);

	chunk_index_create_all(chunk->fd.hypertable_id,
						   chunk->hypertable_relid,
						   chunk->fd.id,
						   chunk->table_id);

	return chunk;
}

Chunk *
chunk_create(Hypertable *ht, Point *p, const char *schema, const char *prefix)
{
	Chunk	   *chunk;

	/*
	 * Serialize chunk creation around a lock on the "main table" to avoid
	 * multiple processes trying to create the same chunk. We use a
	 * ShareUpdateExclusiveLock, which is the weakest lock possible that
	 * conflicts with itself. The lock needs to be held until transaction end.
	 */
	LockRelationOid(ht->main_table_relid, ShareUpdateExclusiveLock);

	/* Recheck if someone else created the chunk before we got the table lock */
	chunk = chunk_find(ht->space, p);

	if (NULL == chunk)
		chunk = chunk_create_after_lock(ht, p, schema, prefix);

	Assert(chunk != NULL);

	return chunk;
}

Chunk *
chunk_create_stub(int32 id, int16 num_constraints)
{
	Chunk	   *chunk;

	chunk = palloc0(sizeof(Chunk));
	chunk->fd.id = id;

	if (num_constraints > 0)
		chunk->constraints = chunk_constraints_alloc(num_constraints, CurrentMemoryContext);

	return chunk;
}


static bool
chunk_tuple_found(TupleInfo *ti, void *arg)
{
	Chunk	   *chunk = arg;

	chunk_fill(chunk, ti->tuple);
	return false;
}

void
chunk_free(Chunk *chunk)
{
	if (NULL != chunk->cube)
		hypercube_free(chunk->cube);

	pfree(chunk);
}

/* Fill in a chunk stub. The stub data structure needs the chunk ID and constraints set.
 * The rest of the fields will be filled in from the table data. */
static Chunk *
chunk_fill_stub(Chunk *chunk_stub, bool tuplock)
{
	ScanKeyData scankey[1];
	Catalog    *catalog = catalog_get();
	int			num_found;
	ScannerCtx	ctx = {
		.table = catalog->tables[CHUNK].id,
		.index = catalog->tables[CHUNK].index_ids[CHUNK_ID_INDEX],
		.nkeys = 1,
		.scankey = scankey,
		.data = chunk_stub,
		.tuple_found = chunk_tuple_found,
		.lockmode = AccessShareLock,
		.tuplock = {
			.lockmode = LockTupleShare,
			.enabled = tuplock,
		},
		.scandirection = ForwardScanDirection,
	};

	/*
	 * Perform an index scan on chunk ID.
	 */
	ScanKeyInit(&scankey[0], Anum_chunk_id, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(chunk_stub->fd.id));

	num_found = scanner_scan(&ctx);

	if (num_found != 1)
		elog(ERROR, "no chunk found with ID %d", chunk_stub->fd.id);

	if (NULL == chunk_stub->cube)
		chunk_stub->cube = hypercube_from_constraints(chunk_stub->constraints, CurrentMemoryContext);
	else

		/*
		 * The hypercube slices were filled in during the scan. Now we need to
		 * sort them in dimension order.
		 */
		hypercube_slice_sort(chunk_stub->cube);

	return chunk_stub;
}

/*
 * Initialize a chunk scan context.
 *
 * A chunk scan context is used to join chunk-related information from metadata
 * tables during scans.
 */
static void
chunk_scan_ctx_init(ChunkScanCtx *ctx, Hyperspace *hs, Point *p)
{
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ChunkScanEntry),
		.hcxt = CurrentMemoryContext,
	};

	ctx->htab = hash_create("chunk-scan-context", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	ctx->space = hs;
	ctx->point = p;
	ctx->early_abort = false;
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
dimension_slice_and_chunk_constraint_join(ChunkScanCtx *scanctx, DimensionVec *vec)
{
	int			i;

	for (i = 0; i < vec->num_slices; i++)
	{
		/*
		 * For each dimension slice, find matching constraints. These will be
		 * saved in the scan context
		 */
		chunk_constraint_scan_by_dimension_slice(vec->slices[i], scanctx, CurrentMemoryContext);
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
chunk_point_scan(ChunkScanCtx *scanctx, Point *p)
{
	int			i;

	/* Scan all dimensions for slices enclosing the point */
	for (i = 0; i < scanctx->space->num_dimensions; i++)
	{
		DimensionVec *vec;

		vec = dimension_slice_scan(scanctx->space->dimensions[i].fd.id,
								   p->coordinates[i]);

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
chunk_collision_scan(ChunkScanCtx *scanctx, Hypercube *cube)
{
	int			i;

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
 * Apply a function to each chunk in the scan context's hash table. If the limit
 * is greater than zero only a limited number of chunks will be processed.
 *
 * The chunk handler function (on_chunk_func) should return true if the chunk
 * should be considered processed and count towards the given limit, otherwise
 * false.
 *
 * Returns the number of processed chunks.
 */
static int
chunk_scan_ctx_foreach_chunk(ChunkScanCtx *ctx,
							 on_chunk_func on_chunk,
							 uint16 limit)
{
	HASH_SEQ_STATUS status;
	ChunkScanEntry *entry;
	uint16		num_found = 0;

	hash_seq_init(&status, ctx->htab);

	for (entry = hash_seq_search(&status);
		 entry != NULL;
		 entry = hash_seq_search(&status))
	{
		if (on_chunk(ctx, entry->chunk))
		{
			num_found++;

			if (limit > 0 && num_found == limit)
			{
				hash_seq_term(&status);
				return num_found;
			}
		}
	}

	return num_found;
}

/* Returns true if the chunk has a full set of constraints, otherwise
 * false. Used to find a chunk matching a point in an N-dimensional
 * hyperspace. */
static bool
chunk_is_complete(ChunkScanCtx *scanctx, Chunk *chunk)
{
	if (scanctx->space->num_dimensions != chunk->constraints->num_dimension_constraints)
		return false;

	return true;
}

static bool
set_complete_chunk(ChunkScanCtx *scanctx, Chunk *chunk)
{
	if (chunk_is_complete(scanctx, chunk))
	{
		scanctx->data = chunk;
		return true;
	}
	return false;
}

/* Finds the first chunk that has a complete set of constraints. There should be
 * only one such chunk in the scan context when scanning for the chunk that
 * holds a particular tuple/point. */
static Chunk *
chunk_scan_ctx_get_chunk(ChunkScanCtx *ctx)
{
	ctx->data = NULL;

	chunk_scan_ctx_foreach_chunk(ctx, set_complete_chunk, 1);

	return ctx->data;
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
 *	  - Insert a (stub) chunk in a hash table and add the constraint to the chunk
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
Chunk *
chunk_find(Hyperspace *hs, Point *p)
{
	Chunk	   *chunk;
	ChunkScanCtx ctx;

	/* The scan context will keep the state accumulated during the scan */
	chunk_scan_ctx_init(&ctx, hs, p);

	/* Abort the scan when the chunk is found */
	ctx.early_abort = true;

	/* Scan for the chunk matching the point */
	chunk_point_scan(&ctx, p);

	/* Find the chunk that has N matching dimension constraints */
	chunk = chunk_scan_ctx_get_chunk(&ctx);

	chunk_scan_ctx_destroy(&ctx);

	if (NULL != chunk)
	{
		/* Fill in the rest of the chunk's data from the chunk table */
		chunk_fill_stub(chunk, false);

		/*
		 * When searching for the chunk that matches the point, we only
		 * scanned for dimensional constraints. We now need to rescan the
		 * constraints to also get the inherited constraints.
		 */
		chunk->constraints = chunk_constraint_scan_by_chunk_id(chunk->fd.id,
															   hs->num_dimensions,
															   CurrentMemoryContext);
	}

	return chunk;
}

static bool
append_chunk_oid(ChunkScanCtx *scanctx, Chunk *chunk)
{
	if (!chunk_is_complete(scanctx, chunk))
		return false;

	/* Fill in the rest of the chunk's data from the chunk table */
	chunk_fill_stub(chunk, false);

	if (scanctx->lockmode != NoLock)
		LockRelationOid(chunk->table_id, scanctx->lockmode);

	scanctx->data = lappend_oid(scanctx->data, chunk->table_id);
	return true;
}

List *
chunk_find_all_oids(Hyperspace *hs, List *dimension_vecs, LOCKMODE lockmode)
{
	List	   *oid_list = NIL;
	ChunkScanCtx ctx;
	ListCell   *lc;

	/* The scan context will keep the state accumulated during the scan */
	chunk_scan_ctx_init(&ctx, hs, NULL);

	/* Do not abort the scan when one chunk is found */
	ctx.early_abort = false;
	ctx.lockmode = lockmode;

	/* Scan all dimensions for slices enclosing the point */
	foreach(lc, dimension_vecs)
	{
		DimensionVec *vec = lfirst(lc);

		dimension_slice_and_chunk_constraint_join(&ctx, vec);
	}

	ctx.data = NIL;
	chunk_scan_ctx_foreach_chunk(&ctx, append_chunk_oid, 0);
	oid_list = ctx.data;

	chunk_scan_ctx_destroy(&ctx);

	return oid_list;
}

Chunk *
chunk_copy(Chunk *chunk)
{
	Chunk	   *copy;

	copy = palloc(sizeof(Chunk));
	memcpy(copy, chunk, sizeof(Chunk));

	if (NULL != chunk->constraints)
		copy->constraints = chunk_constraints_copy(chunk->constraints);

	if (NULL != chunk->cube)
		copy->cube = hypercube_copy(chunk->cube);

	return copy;
}

static int
chunk_scan_internal(int indexid,
					ScanKeyData scankey[],
					int nkeys,
					tuple_found_func tuple_found,
					void *data,
					int limit,
					ScanDirection scandir,
					LOCKMODE lockmode,
					MemoryContext mctx)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	ctx = {
		.table = catalog->tables[CHUNK].id,
		.index = catalog->tables[CHUNK].index_ids[indexid],
		.nkeys = nkeys,
		.data = data,
		.scankey = scankey,
		.tuple_found = tuple_found,
		.limit = limit,
		.lockmode = lockmode,
		.scandirection = scandir,
		.result_mctx = mctx,
	};

	return scanner_scan(&ctx);
}

/*
 * Get a window of chunks that "preceed" the given dimensional point.
 *
 * For instance, if the dimension is "time", then given a point in time the
 * function returns the recent chunks that come before the chunk that includes
 * that point. The count parameter determines the number or slices the window
 * should include in the given dimension. Note, that with multi-dimensional
 * partitioning, there might be multiple chunks in each dimensional slice that
 * all preceed the given point. For instance, the example below shows two
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
 * context, inlcuding the list itself. So, beware of not leaking the list if
 * the chunks are later cached somewhere else.
 */
List *
chunk_get_window(int32 dimension_id, int64 point, int count, MemoryContext mctx)
{
	List	   *chunks = NIL;
	DimensionVec *dimvec;
	int			i;

	/* Scan for "count" slices that preceeds the point in the given dimension */
	dimvec = dimension_slice_scan_by_dimension_before_point(dimension_id,
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
		ChunkConstraints *ccs = chunk_constraints_alloc(1, mctx);
		int			j;

		chunk_constraint_scan_by_dimension_slice_id(slice->fd.id, ccs, mctx);

		/* For each constraint, find the corresponding chunk */
		for (j = 0; j < ccs->num_constraints; j++)
		{
			ChunkConstraint *cc = &ccs->constraints[j];
			Chunk	   *chunk = chunk_get_by_id(cc->fd.chunk_id, 0, true);
			MemoryContext old;

			chunk->constraints = chunk_constraint_scan_by_chunk_id(chunk->fd.id, 1, mctx);
			chunk->cube = hypercube_from_constraints(chunk->constraints, mctx);

			/* Allocate the list on the same memory context as the chunks */
			old = MemoryContextSwitchTo(mctx);
			chunks = lappend(chunks, chunk);
			MemoryContextSwitchTo(old);
		}
	}

	return chunks;
}

static Chunk *
chunk_scan_find(int indexid,
				ScanKeyData scankey[],
				int nkeys,
				int16 num_constraints,
				MemoryContext mctx,
				bool fail_if_not_found)
{
	Chunk	   *chunk = MemoryContextAllocZero(mctx, sizeof(Chunk));
	int			num_found;

	num_found = chunk_scan_internal(indexid,
									scankey,
									nkeys,
									chunk_tuple_found,
									chunk,
									num_constraints,
									ForwardScanDirection,
									AccessShareLock,
									mctx);

	switch (num_found)
	{
		case 0:
			if (fail_if_not_found)
				elog(ERROR, "chunk not found");
			pfree(chunk);
			chunk = NULL;
			break;
		case 1:
			if (num_constraints > 0)
			{
				chunk->constraints = chunk_constraint_scan_by_chunk_id(chunk->fd.id, num_constraints, mctx);
				chunk->cube = hypercube_from_constraints(chunk->constraints, mctx);
			}
			break;
		default:
			elog(ERROR, "unexpected number of chunks found: %d", num_found);
	}

	return chunk;
}

Chunk *
chunk_get_by_name_with_memory_context(const char *schema_name,
									  const char *table_name,
									  int16 num_constraints,
									  MemoryContext mctx,
									  bool fail_if_not_found)
{
	NameData	schema,
				table;
	ScanKeyData scankey[2];

	namestrcpy(&schema, schema_name);
	namestrcpy(&table, table_name);

	/*
	 * Perform an index scan on chunk name.
	 */
	ScanKeyInit(&scankey[0], Anum_chunk_schema_name_idx_schema_name, BTEqualStrategyNumber,
				F_NAMEEQ, NameGetDatum(&schema));
	ScanKeyInit(&scankey[1], Anum_chunk_schema_name_idx_table_name, BTEqualStrategyNumber,
				F_NAMEEQ, NameGetDatum(&table));

	return chunk_scan_find(CHUNK_SCHEMA_NAME_INDEX, scankey, 2,
						   num_constraints, mctx, fail_if_not_found);
}

Chunk *
chunk_get_by_relid(Oid relid, int16 num_constraints, bool fail_if_not_found)
{
	char	   *schema;
	char	   *table;

	if (!OidIsValid(relid))
		return NULL;

	schema = get_namespace_name(get_rel_namespace(relid));
	table = get_rel_name(relid);
	return chunk_get_by_name(schema, table, num_constraints, fail_if_not_found);
}

Chunk *
chunk_get_by_id(int32 id, int16 num_constraints, bool fail_if_not_found)
{
	ScanKeyData scankey[1];

	/*
	 * Perform an index scan on chunk id.
	 */
	ScanKeyInit(&scankey[0], Anum_chunk_idx_id, BTEqualStrategyNumber,
				F_INT4EQ, id);

	return chunk_scan_find(CHUNK_ID_INDEX,
						   scankey,
						   1,
						   num_constraints,
						   CurrentMemoryContext,
						   fail_if_not_found);
}

bool
chunk_exists(const char *schema_name, const char *table_name)
{
	return chunk_get_by_name(schema_name, table_name, 0, false) != NULL;
}

bool
chunk_exists_relid(Oid relid)
{
	return chunk_get_by_relid(relid, 0, false) != NULL;
}

static bool
chunk_tuple_delete(TupleInfo *ti, void *data)
{
	FormData_chunk *form = (FormData_chunk *) GETSTRUCT(ti->tuple);
	CatalogSecurityContext sec_ctx;
	ChunkConstraints *ccs = chunk_constraints_alloc(2, ti->mctx);
	int			i;

	chunk_constraint_delete_by_chunk_id(form->id, ccs);
	chunk_index_delete_by_chunk_id(form->id, true);

	/* Check for dimension slices that are orphaned by the chunk deletion */
	for (i = 0; i < ccs->num_constraints; i++)
	{
		ChunkConstraint *cc = &ccs->constraints[i];

		/*
		 * Delete the dimension slice if there are no remaining constraints
		 * referencing it
		 */
		if (is_dimension_constraint(cc) &&
			chunk_constraint_scan_by_dimension_slice_id(cc->fd.dimension_slice_id, NULL, CurrentMemoryContext) == 0)
			dimension_slice_delete_by_id(cc->fd.dimension_slice_id, false);
	}

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_delete(ti->scanrel, ti->tuple);
	catalog_restore_user(&sec_ctx);

	return true;
}

int
chunk_delete_by_name(const char *schema, const char *table)
{
	ScanKeyData scankey[2];

	ScanKeyInit(&scankey[0], Anum_chunk_schema_name_idx_schema_name, BTEqualStrategyNumber,
				F_NAMEEQ, DirectFunctionCall1(namein, CStringGetDatum(schema)));
	ScanKeyInit(&scankey[1], Anum_chunk_schema_name_idx_table_name, BTEqualStrategyNumber,
				F_NAMEEQ, DirectFunctionCall1(namein, CStringGetDatum(table)));

	return chunk_scan_internal(CHUNK_SCHEMA_NAME_INDEX, scankey, 2,
							   chunk_tuple_delete, NULL, 0,
							   ForwardScanDirection,
							   RowExclusiveLock,
							   CurrentMemoryContext);
}

int
chunk_delete_by_relid(Oid relid)
{
	if (!OidIsValid(relid))
		return 0;

	return chunk_delete_by_name(get_namespace_name(get_rel_namespace(relid)), get_rel_name(relid));
}

int
chunk_delete_by_hypertable_id(int32 hypertable_id)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0], Anum_chunk_hypertable_id_idx_hypertable_id, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(hypertable_id));

	return chunk_scan_internal(CHUNK_HYPERTABLE_ID_INDEX, scankey, 1,
							   chunk_tuple_delete, NULL, 0,
							   ForwardScanDirection,
							   RowExclusiveLock,
							   CurrentMemoryContext);
}

static bool
chunk_recreate_constraint(ChunkScanCtx *ctx, Chunk *chunk)
{
	ChunkConstraints *ccs = chunk->constraints;
	int			i;

	chunk_fill_stub(chunk, false);

	for (i = 0; i < ccs->num_constraints; i++)
		chunk_constraint_recreate(&ccs->constraints[i], chunk->table_id);

	return true;
}

void
chunk_recreate_all_constraints_for_dimension(Hyperspace *hs, int32 dimension_id)
{
	DimensionVec *slices;
	ChunkScanCtx chunkctx;
	int			i;

	slices = dimension_slice_scan_by_dimension(dimension_id, 0);

	if (NULL == slices)
		return;

	chunk_scan_ctx_init(&chunkctx, hs, NULL);

	for (i = 0; i < slices->num_slices; i++)
		chunk_constraint_scan_by_dimension_slice(slices->slices[i], &chunkctx, CurrentMemoryContext);

	chunk_scan_ctx_foreach_chunk(&chunkctx, chunk_recreate_constraint, 0);
	chunk_scan_ctx_destroy(&chunkctx);
}

static bool
chunk_tuple_update(TupleInfo *ti, void *data)
{
	HeapTuple	tuple = heap_copytuple(ti->tuple);
	FormData_chunk *form = (FormData_chunk *) GETSTRUCT(tuple);
	FormData_chunk *update = data;
	CatalogSecurityContext sec_ctx;

	namecpy(&form->schema_name, &update->schema_name);
	namecpy(&form->table_name, &update->table_name);

	catalog_become_owner(catalog_get(), &sec_ctx);
	catalog_update(ti->scanrel, tuple);
	catalog_restore_user(&sec_ctx);

	heap_freetuple(tuple);

	return false;
}

static bool
chunk_update_form(FormData_chunk *form)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0], Anum_chunk_idx_id, BTEqualStrategyNumber,
				F_INT4EQ, form->id);

	return chunk_scan_internal(CHUNK_ID_INDEX,
							   scankey,
							   1,
							   chunk_tuple_update,
							   form,
							   0,
							   ForwardScanDirection,
							   AccessShareLock,
							   CurrentMemoryContext) > 0;
}

bool
chunk_set_name(Chunk *chunk, const char *newname)
{
	namestrcpy(&chunk->fd.table_name, newname);

	return chunk_update_form(&chunk->fd);
}

bool
chunk_set_schema(Chunk *chunk, const char *newschema)
{
	namestrcpy(&chunk->fd.schema_name, newschema);

	return chunk_update_form(&chunk->fd);
}
