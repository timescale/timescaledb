#include <postgres.h>
#include <catalog/namespace.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>
#include <access/htup_details.h>

#include "chunk.h"
#include "catalog.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "partitioning.h"
#include "metadata_queries.h"
#include "scanner.h"

Chunk *
chunk_create_from_tuple(HeapTuple tuple, int16 num_constraints)
{
	Chunk	   *chunk;

	chunk = palloc0(CHUNK_SIZE(num_constraints));
	memcpy(&chunk->fd, GETSTRUCT(tuple), sizeof(FormData_chunk));
	chunk->capacity = num_constraints;
	chunk->num_constraints = 0;
	chunk->table_id = get_relname_relid(chunk->fd.table_name.data,
										get_namespace_oid(chunk->fd.schema_name.data, false));

	return chunk;
}

Chunk *
chunk_create_new(Hyperspace *hs, Point *p)
{
	Chunk *chunk;

	chunk = spi_chunk_create(hs, p);
	Assert(chunk != NULL);

	chunk_constraint_scan_by_chunk_id(chunk);
	chunk->cube = hypercube_from_constraints(chunk->constraints, chunk->num_constraints);

	return chunk;
}

Chunk *
chunk_get_or_create_new(Hyperspace *hs, Point *p)
{
	Chunk *chunk;

	chunk = spi_chunk_get_or_create(hs, p);
	Assert(chunk != NULL);

	chunk_constraint_scan_by_chunk_id(chunk);
	chunk->cube = hypercube_from_constraints(chunk->constraints, chunk->num_constraints);

	return chunk;
}

static bool
chunk_tuple_found(TupleInfo *ti, void *arg)
{
	Chunk *chunk = arg;
	memcpy(&chunk->fd, GETSTRUCT(ti->tuple), sizeof(FormData_chunk));
	chunk->table_id = get_relname_relid(chunk->fd.table_name.data,
										get_namespace_oid(chunk->fd.schema_name.data, false));
	return false;
}

/* Fill in a chunk stub. The stub data structure needs the chunk ID set. The
 * rest of the fields will be filled in from the table data. */
static Chunk *
chunk_scan(Chunk *chunk_stub, bool tuplock)
{
	ScanKeyData scankey[1];
	Catalog    *catalog = catalog_get();
	int num_found;
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
	ScanKeyInit(&scankey[0], Anum_chunk_id,	BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(chunk_stub->fd.id));

	num_found = scanner_scan(&ctx);

	if (num_found != 1)
		elog(ERROR, "No chunk found with ID %d", chunk_stub->fd.id);

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
		Chunk *chunk = entry->chunk;

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
 *    - Find all dimension slices that match the dimension
 * 2) For each dimension slice:
 *    - Find all chunk constraints matching the dimension slice
 * 3) For each matching chunk constraint
 *    - Insert a (stub) chunk in a hash table and add the constraint to the chunk
 *    - If chunk already exists in hash table, add the constraint to the chunk
 * 4) At the end of the scan, only one chunk in the hash table should have
 *    N number of constraints. This is the matching chunk.
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
	Chunk *chunk;
	ChunkScanCtx ctx;
	int16 num_dimensions = HYPERSPACE_NUM_DIMENSIONS(hs);
	int i, j;

	/* The scan context will keep the state accumulated during the scan */
	chunk_scan_ctx_init(&ctx, num_dimensions);

	/* First, scan all dimensions for matching slices */
	for (i = 0; i < HYPERSPACE_NUM_DIMENSIONS(hs); i++)
	{
		DimensionVec *vec;

		vec = dimension_slice_scan(hs->dimensions[i].fd.id, p->coordinates[i]);

		for (j = 0; j < vec->num_slices; j++)
			/* For each dimension slice, find matching constraints. These will
			 * be saved in the scan context */
			chunk_constraint_scan_by_dimension_slice_id(vec->slices[j], &ctx);
	}

	/* Find the chunk that has N matching constraints */
	chunk = chunk_scan_ctx_find_chunk(&ctx);
	chunk_scan_ctx_destroy(&ctx);

	if (NULL != chunk)
	{
		chunk->cube = hypercube_from_constraints(chunk->constraints, chunk->num_constraints);

		/* Fill in the rest of the chunk's data from the chunk table */
		chunk_scan(chunk, false);
	}

	return chunk;
}

Chunk *
chunk_copy(Chunk *chunk)
{
	Chunk *copy;
	size_t nbytes = CHUNK_SIZE(chunk->capacity);

	copy = palloc(nbytes);
	memcpy(copy, chunk, nbytes);

	if (NULL != chunk->cube)
		copy->cube = hypercube_copy(chunk->cube);

	return copy;
}
