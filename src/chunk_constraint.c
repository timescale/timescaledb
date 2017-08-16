#include <postgres.h>
#include <utils/hsearch.h>

#include "scanner.h"
#include "chunk_constraint.h"
#include "dimension_slice.h"
#include "chunk.h"

static inline ChunkConstraint *
chunk_constraint_fill(ChunkConstraint *cc, HeapTuple tuple)
{
	memcpy(&cc->fd, GETSTRUCT(tuple), sizeof(FormData_chunk_constraint));
	return cc;
}

static bool
chunk_constraint_tuple_found(TupleInfo *ti, void *data)
{
	Chunk	   *chunk = data;

	if (!heap_attisnull(ti->tuple, Anum_chunk_constraint_dimension_slice_id))
	{
		chunk_constraint_fill(&chunk->constraints[chunk->num_constraints++], ti->tuple);
	}
	if (chunk->capacity == chunk->num_constraints)
		return false;

	return true;
}

/*
 * Scan all the chunk's constraints based on the chunk ID.
 *
 * Memory for the constraints is already allocated in the chunk, so this simply
 * fills in the data in the chunk's constraints array.
 */
Chunk *
chunk_constraint_scan_by_chunk_id(Chunk *chunk)
{
	Catalog    *catalog = catalog_get();
	ScanKeyData scankey[1];
	int			num_found;
	ScannerCtx	scanCtx = {
		.table = catalog->tables[CHUNK_CONSTRAINT].id,
		.index = catalog->tables[CHUNK_CONSTRAINT].index_ids[CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = chunk,
		.tuple_found = chunk_constraint_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	chunk->num_constraints = 0;

	ScanKeyInit(&scankey[0], Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_chunk_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk->fd.id));

	num_found = scanner_scan(&scanCtx);

	if (num_found != chunk->num_constraints)
		elog(ERROR, "Unexpected number of constraints found for chunk %d", chunk->fd.id);

	return chunk;
}

static bool
chunk_constraint_dimension_id_tuple_found(TupleInfo *ti, void *data)
{
	ChunkScanCtx *ctx = data;
	ChunkConstraint constraint;
	Chunk	   *chunk;
	ChunkScanEntry *entry;
	bool		found;

	chunk_constraint_fill(&constraint, ti->tuple);

	entry = hash_search(ctx->htab, &constraint.fd.chunk_id, HASH_ENTER, &found);

	if (!found)
	{
		chunk = chunk_create_stub(constraint.fd.chunk_id, ctx->num_dimensions);
		entry->chunk = chunk;
	}
	else
	{
		chunk = entry->chunk;
	}

	chunk_add_constraint(chunk, &constraint);

	/*
	 * If the chunk has N constraints, it is the chunk we are looking for and
	 * the scan can be aborted.
	 */
	if (chunk->num_constraints == ctx->num_dimensions)
		return false;

	return true;
}

int
chunk_constraint_scan_by_dimension_slice_id(DimensionSlice *slice, ChunkScanCtx *ctx)
{
	Catalog    *catalog = catalog_get();
	ScanKeyData scankey[1];
	int			num_found;
	ScannerCtx	scanCtx = {
		.table = catalog->tables[CHUNK_CONSTRAINT].id,
		.index = catalog->tables[CHUNK_CONSTRAINT].index_ids[CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = ctx,
		.tuple_found = chunk_constraint_dimension_id_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0], Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_dimension_slice_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(slice->fd.id));

	num_found = scanner_scan(&scanCtx);

	return num_found;
}
