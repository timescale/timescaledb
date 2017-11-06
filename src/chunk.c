#include <postgres.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>
#include <catalog/indexing.h>
#include <commands/trigger.h>
#include <tcop/tcopprot.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/hsearch.h>

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

typedef bool (*on_chunk_func) (ChunkScanCtx *ctx, Chunk *chunk);

static void chunk_scan_ctx_init(ChunkScanCtx *ctx, Hyperspace *hs, Point *p);
static void chunk_scan_ctx_destroy(ChunkScanCtx *ctx);
static void chunk_collision_scan(ChunkScanCtx *scanctx, Hypercube *cube);
static int	chunk_scan_ctx_foreach_chunk(ChunkScanCtx *ctx, on_chunk_func on_chunk, uint16 limit);

static void
chunk_fill(Chunk *chunk, HeapTuple tuple)
{
	memcpy(&chunk->fd, GETSTRUCT(tuple), sizeof(FormData_chunk));
	chunk->table_id = get_relname_relid(chunk->fd.table_name.data,
					   get_namespace_oid(chunk->fd.schema_name.data, false));
}

Chunk *
chunk_create_from_tuple(HeapTuple tuple, int16 num_constraints)
{
	Chunk	   *chunk;

	chunk = palloc0(CHUNK_SIZE(num_constraints));
	chunk->capacity = num_constraints;
	chunk->num_constraints = 0;

	chunk_fill(chunk, tuple);

	chunk_constraint_scan_by_chunk_id(chunk);
	chunk->cube = hypercube_from_constraints(chunk->constraints, chunk->num_constraints);

	return chunk;
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

static Chunk *
chunk_create_after_lock(Hypertable *ht, Point *p, const char *schema, const char *prefix)
{
	Oid			schema_oid = get_namespace_oid(schema, false);
	Hyperspace *hs = ht->space;
	Catalog    *catalog = catalog_get();
	CatalogSecurityContext sec_ctx;
	Hypercube  *cube;
	Chunk	   *chunk;
	int			i;

	catalog_become_owner(catalog, &sec_ctx);

	/* Calculate the hypercube for a new chunk that covers the tuple's point */
	cube = hypercube_calculate_from_point(hs, p);

	/* Resolve collisions with other chunks by cutting the new hypercube */
	chunk_collision_resolve(hs, cube, p);

	/* Create a new chunk based on the hypercube */
	chunk = chunk_create_stub(catalog_table_next_seq_id(catalog, CHUNK), hs->num_dimensions);
	chunk->fd.hypertable_id = hs->hypertable_id;
	chunk->cube = cube;
	chunk->num_constraints = chunk->capacity;
	namestrcpy(&chunk->fd.schema_name, schema);

	snprintf(chunk->fd.table_name.data, NAMEDATALEN,
			 "%s_%d_chunk", prefix, chunk->fd.id);

	/* Insert any new dimension slices */
	dimension_slice_insert_multi(cube->slices, cube->num_slices);

	/* All slices now have assigned ID's so update the chunk's constraints */
	for (i = 0; i < hs->num_dimensions; i++)
	{
		chunk->constraints[i].fd.chunk_id = chunk->fd.id;
		chunk->constraints[i].fd.dimension_slice_id = cube->slices[i]->fd.id;
	}

	/* Insert the new chunk constraints */
	chunk_constraint_insert_multi(chunk->constraints, chunk->num_constraints);
	process_utility_set_expect_chunk_modification(true);
	CatalogInternalCall4(CHUNK_CREATE, Int32GetDatum(chunk->fd.id), Int32GetDatum(hs->hypertable_id),
			   CStringGetDatum(schema), NameGetDatum(&chunk->fd.table_name));
	process_utility_set_expect_chunk_modification(false);

	chunk->table_id = get_relname_relid(NameStr(chunk->fd.table_name), schema_oid);

	trigger_create_on_all_chunks(ht, chunk);

	/* Create all indexes on the chunk */
	chunk_index_create_all(ht->fd.id, ht->main_table_relid, chunk->fd.id, chunk->table_id);

	catalog_restore_user(&sec_ctx);

	return chunk;
}

Chunk *
chunk_create(Hypertable *ht, Point *p, const char *schema, const char *prefix)
{
	Catalog    *catalog = catalog_get();
	Chunk	   *chunk;
	Relation	rel;

	rel = heap_open(catalog->tables[CHUNK].id, ExclusiveLock);

	/* Recheck if someone else created the chunk before we got the table lock */
	chunk = chunk_find(ht->space, p);

	if (NULL == chunk)
		chunk = chunk_create_after_lock(ht, p, schema, prefix);

	heap_close(rel, ExclusiveLock);

	Assert(chunk != NULL);

	return chunk;
}

Chunk *
chunk_create_stub(int32 id, int16 num_constraints)
{
	Chunk	   *chunk;

	chunk = palloc0(CHUNK_SIZE(num_constraints));
	chunk->capacity = num_constraints;
	chunk->num_constraints = 0;
	chunk->fd.id = id;

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
		.scantype = ScannerTypeIndex,
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
		elog(ERROR, "No chunk found with ID %d", chunk_stub->fd.id);

	if (NULL == chunk_stub->cube)
		chunk_stub->cube = hypercube_from_constraints(chunk_stub->constraints,
												chunk_stub->num_constraints);
	else

		/*
		 * The hypercube slices were filled in during the scan. Now we need to
		 * sort them in dimension order.
		 */
		hypercube_slice_sort(chunk_stub->cube);

	return chunk_stub;
}

bool
chunk_add_constraint(Chunk *chunk, ChunkConstraint *constraint)
{
	if (chunk->capacity == chunk->num_constraints)
		return false;

	memcpy(&chunk->constraints[chunk->num_constraints++], constraint, sizeof(ChunkConstraint));
	return true;
}

bool
chunk_add_constraint_from_tuple(Chunk *chunk, HeapTuple constraint_tuple)
{
	if (chunk->capacity == chunk->num_constraints)
		return false;

	memcpy(&chunk->constraints[chunk->num_constraints++],
		   GETSTRUCT(constraint_tuple), sizeof(FormData_chunk_constraint));
	return true;
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
		chunk_constraint_scan_by_dimension_slice_id(vec->slices[i], scanctx);
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
	if (scanctx->space->num_dimensions != chunk->num_constraints)
		return false;

	scanctx->data = chunk;
	return true;
}

/* Finds the first chunk that has a complete set of constraints. There should be
 * only one such chunk in the scan context when scanning for the chunk that
 * holds a particular tuple/point. */
static Chunk *
chunk_scan_ctx_get_chunk(ChunkScanCtx *ctx)
{
	ctx->data = NULL;

	chunk_scan_ctx_foreach_chunk(ctx, chunk_is_complete, 1);

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

	/* Find the chunk that has N matching constraints */
	chunk = chunk_scan_ctx_get_chunk(&ctx);

	chunk_scan_ctx_destroy(&ctx);

	if (NULL != chunk)
		/* Fill in the rest of the chunk's data from the chunk table */
		chunk_fill_stub(chunk, false);

	return chunk;
}

Chunk *
chunk_copy(Chunk *chunk)
{
	Chunk	   *copy;
	size_t		nbytes = CHUNK_SIZE(chunk->capacity);

	copy = palloc(nbytes);
	memcpy(copy, chunk, nbytes);

	if (NULL != chunk->cube)
		copy->cube = hypercube_copy(chunk->cube);

	return copy;
}

static Chunk *
chunk_scan_internal(int indexid,
					ScanKeyData scankey[],
					int nkeys,
					int16 num_constraints,
					bool fail_if_not_found)
{
	Catalog    *catalog = catalog_get();
	Chunk	   *chunk = chunk_create_stub(0, num_constraints);
	ScannerCtx	ctx = {
		.table = catalog->tables[CHUNK].id,
		.index = catalog->tables[CHUNK].index_ids[indexid],
		.scantype = ScannerTypeIndex,
		.nkeys = nkeys,
		.data = chunk,
		.scankey = scankey,
		.tuple_found = chunk_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};
	int			num_found = scanner_scan(&ctx);

	switch (num_found)
	{
		case 0:
			if (fail_if_not_found)
				elog(ERROR, "Chunk not found");
			break;
		case 1:
			if (num_constraints > 0)
			{
				chunk = chunk_constraint_scan_by_chunk_id(chunk);
				chunk->cube = hypercube_from_constraints(chunk->constraints, num_constraints);
			}
			return chunk;
		default:
			elog(ERROR, "Unexpected number of chunks found: %d", num_found);
	}
	return NULL;
}

Chunk *
chunk_get_by_name(const char *schema_name, const char *table_name,
				  int16 num_constraints, bool fail_if_not_found)
{
	ScanKeyData scankey[2];

	/*
	 * Perform an index scan on chunk name.
	 */
	ScanKeyInit(&scankey[0], Anum_chunk_schema_name_idx_schema_name, BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(schema_name));
	ScanKeyInit(&scankey[1], Anum_chunk_schema_name_idx_table_name, BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(table_name));

	return chunk_scan_internal(CHUNK_SCHEMA_NAME_INDEX, scankey, 2,
							   num_constraints, fail_if_not_found);
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

	return chunk_scan_internal(CHUNK_ID_INDEX, scankey, 1,
							   num_constraints, fail_if_not_found);
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
