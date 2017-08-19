#include <postgres.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits.h>
#include <catalog/indexing.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <access/htup_details.h>

#include "chunk.h"
#include "catalog.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "partitioning.h"
#include "metadata_queries.h"
#include "scanner.h"
#include "hypertable.h"
#include "hypertable_cache.h"

static Oid
chunk_relation_get_parent_oid(Relation inherits_rel, Oid child_relid)
{
	SysScanDesc scan;
	ScanKeyData key;
	HeapTuple	tuple;
	Oid			parent_relid = InvalidOid;

	ScanKeyInit(&key,
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				child_relid);

	scan = systable_beginscan(inherits_rel, InheritsRelidSeqnoIndexId,
							  true, NULL, 1, &key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_inherits inh = (Form_pg_inherits) GETSTRUCT(tuple);

		parent_relid = inh->inhparent;
	}
	systable_endscan(scan);

	return parent_relid;
}

static Oid
chunk_get_parent_oid(Oid chunk_relid)
{
	Relation	rel;
	Oid			parent_relid;

	rel = heap_open(InheritsRelationId, AccessShareLock);
	parent_relid = chunk_relation_get_parent_oid(rel, chunk_relid);
	heap_close(rel, AccessShareLock);

	return parent_relid;
}

static void
chunk_fill(Chunk *chunk, HeapTuple tuple)
{
	memcpy(&chunk->fd, GETSTRUCT(tuple), sizeof(FormData_chunk));
	chunk->table_id = get_relname_relid(chunk->fd.table_name.data,
					   get_namespace_oid(chunk->fd.schema_name.data, false));
	chunk->parent_id = chunk_get_parent_oid(chunk->table_id);
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

Chunk *
chunk_create(Hyperspace *hs, Point *p)
{
	Chunk	   *chunk;

	chunk = spi_chunk_create(hs, p);
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

	chunk_stub->cube = hypercube_from_constraints(chunk_stub->constraints, chunk_stub->num_constraints);

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

static void
chunk_scan_ctx_init(ChunkScanCtx *ctx, int16 num_dimensions)
{
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ChunkScanEntry),
		.hcxt = CurrentMemoryContext,
	};

	ctx->htab = hash_create("chunk-scan-context", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	ctx->num_dimensions = num_dimensions;
}

static void
chunk_scan_ctx_destroy(ChunkScanCtx *ctx)
{
	hash_destroy(ctx->htab);
}

static Chunk *
chunk_scan_ctx_find_chunk(ChunkScanCtx *ctx)
{
	HASH_SEQ_STATUS status;
	ChunkScanEntry *entry;

	hash_seq_init(&status, ctx->htab);

	for (entry = hash_seq_search(&status);
		 entry != NULL;
		 entry = hash_seq_search(&status))
	{
		Chunk	   *chunk = entry->chunk;

		if (chunk->num_constraints == ctx->num_dimensions)
		{
			hash_seq_term(&status);
			return chunk;
		}
	}

	return NULL;
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
	int16		num_dimensions = HYPERSPACE_NUM_DIMENSIONS(hs);
	int			i,
				j;

	/* The scan context will keep the state accumulated during the scan */
	chunk_scan_ctx_init(&ctx, num_dimensions);

	/* First, scan all dimensions for matching slices */
	for (i = 0; i < HYPERSPACE_NUM_DIMENSIONS(hs); i++)
	{
		DimensionVec *vec;

		vec = dimension_slice_scan(hs->dimensions[i].fd.id, p->coordinates[i]);

		for (j = 0; j < vec->num_slices; j++)

			/*
			 * For each dimension slice, find matching constraints. These will
			 * be saved in the scan context
			 */
			chunk_constraint_scan_by_dimension_slice_id(vec->slices[j], &ctx);
	}

	/* Find the chunk that has N matching constraints */
	chunk = chunk_scan_ctx_find_chunk(&ctx);
	chunk_scan_ctx_destroy(&ctx);

	if (NULL != chunk)
	{
		/* Fill in the rest of the chunk's data from the chunk table */
		chunk_fill_stub(chunk, false);
	}

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


Chunk *
chunk_get(int32 id, int16 num_constraints)
{
	return chunk_fill_stub(chunk_create_stub(id, num_constraints), false);
}


Chunk *
chunk_get_by_name(const char *schema_name, const char *table_name,
				  int16 num_constraints, bool fail_if_not_found)
{
	ScanKeyData scankey[2];
	Catalog    *catalog = catalog_get();
	int			num_found;
	Chunk	   *chunk = chunk_create_stub(0, num_constraints);
	ScannerCtx	ctx = {
		.table = catalog->tables[CHUNK].id,
		.index = catalog->tables[CHUNK].index_ids[CHUNK_SCHEMA_NAME_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 2,
		.data = chunk,
		.scankey = scankey,
		.tuple_found = chunk_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/*
	 * Perform an index scan on chunk name.
	 */
	ScanKeyInit(&scankey[0], Anum_chunk_schema_name_idx_schema_name, BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(schema_name));

	ScanKeyInit(&scankey[1], Anum_chunk_schema_name_idx_table_name, BTEqualStrategyNumber,
				F_NAMEEQ, CStringGetDatum(table_name));

	num_found = scanner_scan(&ctx);

	if (num_found > 1)
		elog(ERROR, "Unexpected number of chunks found");

	if (num_found == 0 && fail_if_not_found)
		elog(ERROR, "Chunk with name %s.%s not found", schema_name, table_name);

	if (num_found == 1)
	{
		chunk = chunk_constraint_scan_by_chunk_id(chunk);
		chunk->cube = hypercube_from_constraints(chunk->constraints, num_constraints);
		return chunk;
	}

	return NULL;
}

Chunk *
chunk_get_by_oid(Oid chunk_relid)
{
	Relation	rel;
	Oid			parent_relid;
	Cache	   *htcache;
	Chunk	   *chunk = NULL;

	rel = heap_open(InheritsRelationId, AccessShareLock);

	parent_relid = chunk_relation_get_parent_oid(rel, chunk_relid);

	htcache = hypertable_cache_pin();

	if (OidIsValid(parent_relid))
	{
		Hypertable *ht;

		ht = hypertable_cache_get_entry(htcache, parent_relid);

		if (NULL != ht)
		{
			const char *schema = get_namespace_name(get_rel_namespace(chunk_relid));
			const char *relname = get_rel_name(chunk_relid);

			chunk = chunk_get_by_name(schema, relname, ht->fd.num_dimensions, false);
		}
	}

	heap_close(rel, AccessShareLock);
	cache_release(htcache);

	return chunk;
}

DimensionSlice *
chunk_get_dimension_slice(Chunk *chunk, int32 dimension_id)
{
	int			i;

	for (i = 0; i < chunk->cube->num_slices; i++)
	{
		DimensionSlice *slice = chunk->cube->slices[i];

		if (slice->fd.dimension_id == dimension_id)
			return slice;
	}
	return NULL;
}

ChunkConstraint *
chunk_get_dimension_constraint_by_slice_id(Chunk *chunk, int32 slice_id)
{
	int			i;

	for (i = 0; i < chunk->num_constraints; i++)
	{
		ChunkConstraint *constraint = &chunk->constraints[i];

		if (constraint->fd.dimension_slice_id == slice_id)
			return constraint;
	}
	return NULL;
}

ChunkConstraint *
chunk_get_dimension_constraint_by_constraint_name(Chunk *chunk, const char *conname)
{
	int			i;

	for (i = 0; i < chunk->num_constraints; i++)
	{
		ChunkConstraint *constraint = &chunk->constraints[i];

		if (strncmp(NameStr(constraint->fd.constraint_name), conname, NAMEDATALEN) == 0)
			return constraint;
	}
	return NULL;
}

/*
 * Get a chunk's constraint for a given dimension.
 */
ChunkConstraint *
chunk_get_dimension_constraint(Chunk *chunk, int32 dimension_id)
{
	DimensionSlice *slice = chunk_get_dimension_slice(chunk, dimension_id);

	if (NULL == slice)
		return NULL;

	return chunk_get_dimension_constraint_by_slice_id(chunk, slice->fd.id);
}
