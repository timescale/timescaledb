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
	chunk->num_constraint_slots = num_constraints;
	chunk->num_constraints = 0;
    chunk->table_id = get_relname_relid(chunk->fd.table_name.data,
										get_namespace_oid(chunk->fd.schema_name.data, false));
	
	return chunk;
}

Chunk *
chunk_create_new(Hyperspace *hs, Point *p)
{
	Chunk *chunk;
	
	/* NOTE: Currently supports only two dimensions */
	Assert(hs->num_open_dimensions == 1 && hs->num_closed_dimensions <= 1);	

	if (hs->num_closed_dimensions == 1) 
		chunk = spi_chunk_create(hs->open_dimensions[0]->fd.id,
								 p->coordinates[0],
								 hs->closed_dimensions[0]->fd.id,
								 p->coordinates[1],
								 HYPERSPACE_NUM_DIMENSIONS(hs));
	else
		chunk = spi_chunk_create(hs->open_dimensions[0]->fd.id,
								 p->coordinates[0], 0, 0,
								 HYPERSPACE_NUM_DIMENSIONS(hs));
	Assert(chunk != NULL);
	
	chunk_constraint_scan(chunk);

	chunk->cube = hypercube_from_constraints(chunk->constraints, chunk->num_constraints);
	
	return chunk;
}

Chunk *
chunk_get_or_create_new(Hyperspace *hs, Point *p)
{
	Chunk *chunk;
	
	/* NOTE: Currently supports only two dimensions */
	Assert(hs->num_open_dimensions == 1 && hs->num_closed_dimensions <= 1);	

	if (hs->num_closed_dimensions == 1) 
		chunk = spi_chunk_get_or_create(hs->open_dimensions[0]->fd.id,
										p->coordinates[0],
										hs->closed_dimensions[0]->fd.id,
										p->coordinates[1],
										HYPERSPACE_NUM_DIMENSIONS(hs));
	else
		chunk = spi_chunk_get_or_create(hs->open_dimensions[0]->fd.id,
										p->coordinates[0], 0, 0,
										HYPERSPACE_NUM_DIMENSIONS(hs));
	Assert(chunk != NULL);
	
	chunk_constraint_scan(chunk);

	return chunk;
}

static bool
chunk_tuple_found(TupleInfo *ti, void *arg)
{
	Chunk *chunk = arg;
	memcpy(&chunk->fd, GETSTRUCT(ti->tuple), sizeof(FormData_chunk));   
	return false;
}

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
	if (chunk->num_constraint_slots == chunk->num_constraints)
		return false;
	
	memcpy(&chunk->constraints[chunk->num_constraints++], constraint, sizeof(ChunkConstraint));
	return true;
}

bool
chunk_add_constraint_from_tuple(Chunk *chunk, HeapTuple constraint_tuple)
{
	if (chunk->num_constraint_slots == chunk->num_constraints)
		return false;

	memcpy(&chunk->constraints[chunk->num_constraints++],
		   GETSTRUCT(constraint_tuple), sizeof(FormData_chunk_constraint));
	return true;
}

static void
chunk_scan_ctx_init(ChunkScanState *ctx, int16 num_dimensions, MemoryContext elm_mctx)
{
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ChunkScanEntry),
		.hcxt =  AllocSetContextCreate(CurrentMemoryContext,
									   "chunk-scan",
									   ALLOCSET_DEFAULT_SIZES),
	};

	ctx->htab = hash_create("chunk-scan", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
	ctx->elm_mctx = elm_mctx;
	ctx->num_dimensions = num_dimensions;
	ctx->num_elements = 0;
}

static void
chunk_scan_ctx_destroy(ChunkScanState *cs)
{
	hash_destroy(cs->htab);
}

static Chunk *
chunk_scan_ctx_find_chunk(ChunkScanState *ctx)
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
			elog(NOTICE, "Found chunk %d", chunk->fd.id);
			hash_seq_term(&status);
			return chunk;
		}

		elog(NOTICE, "Found non-matching chunk %d with %d constraints",
			 chunk->fd.id, chunk->num_constraints);
	}

	return NULL;
}

Chunk *
chunk_find(Hyperspace *hs, Point *p)
{
	Chunk *chunk;
	ChunkScanState scanCtx;
	int16 num_dimensions = HYPERSPACE_NUM_DIMENSIONS(hs);
	int i, j;
	
	chunk_scan_ctx_init(&scanCtx, num_dimensions, CurrentMemoryContext);

	elog(NOTICE, "##### Scanning for chunk");
	
	for (i = 0; i < hs->num_open_dimensions; i++)
	{
		DimensionVec *vec;

		elog(NOTICE, "Scanning dimension %s", NameStr(hs->open_dimensions[i]->fd.column_name));

		vec = dimension_slice_scan(hs->open_dimensions[i]->fd.id, p->coordinates[i]);

		elog(NOTICE, "Found %d slices", vec->num_slices);
		
		for (j = 0; j < vec->num_slices; j++)
			chunk_constraint_scan_by_dimension_slice(vec->slices[j], &scanCtx);
	}

	for (i = 0; i < hs->num_closed_dimensions; i++)
	{
		DimensionVec *vec;

		elog(NOTICE, "Scanning dimension %s", NameStr(hs->closed_dimensions[i]->fd.column_name));

		vec = dimension_slice_scan(hs->closed_dimensions[i]->fd.id,
								   p->coordinates[hs->num_open_dimensions + i]);

		elog(NOTICE, "Found %d slices", vec->num_slices);
		
		for (j = 0; j < vec->num_slices; j++)
			chunk_constraint_scan_by_dimension_slice(vec->slices[j], &scanCtx);
	}

	chunk = chunk_scan_ctx_find_chunk(&scanCtx);

	chunk_scan_ctx_destroy(&scanCtx);

	if (NULL != chunk) {
		chunk->cube = hypercube_from_constraints(chunk->constraints, chunk->num_constraints);
		chunk_scan(chunk, false);
	}
	
	return chunk;
}
