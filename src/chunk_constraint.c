#include <postgres.h>
#include <utils/hsearch.h>

#include "scanner.h"
#include "chunk_constraint.h"
#include "dimension_slice.h"
#include "chunk.h"

static inline ChunkConstraint *
chunk_constraint_from_form_data(Form_chunk_constraint fd)
{
	ChunkConstraint *cc;
	cc = palloc0(sizeof(ChunkConstraint));
	memcpy(&cc->fd, fd, sizeof(FormData_chunk_constraint));
	return cc;
}

static inline ChunkConstraint *
chunk_constraint_fill(ChunkConstraint *cc, HeapTuple tuple)
{
	memcpy(&cc->fd, GETSTRUCT(tuple), sizeof(FormData_chunk_constraint));
	return cc;
}

static inline ChunkConstraint *
chunk_constraint_from_tuple(HeapTuple tuple)
{
	return chunk_constraint_from_form_data((Form_chunk_constraint ) GETSTRUCT(tuple));
}

typedef struct ChunkConstraintCtx
{
	Chunk *chunk;
} ChunkConstraintCtx;

static bool
chunk_constraint_tuple_found(TupleInfo *ti, void *data)
{
	ChunkConstraintCtx *ctx = data;

	chunk_constraint_fill(&ctx->chunk->constraints[ctx->chunk->num_constraints++], ti->tuple);

	if (ctx->chunk->num_constraint_slots == ctx->chunk->num_constraints)
		return false;

	return true;
}

Chunk *
chunk_constraint_scan(Chunk *chunk)
{
	Catalog    *catalog = catalog_get();
	ScanKeyData scankey[1];
	ChunkConstraintCtx data = {
		.chunk = chunk,
	};
	int num_found;
	ScannerCtx	scanCtx = {
		.table = catalog->tables[CHUNK_CONSTRAINT].id,
		.index = catalog->tables[CHUNK_CONSTRAINT].index_ids[CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &data,
		.tuple_found = chunk_constraint_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	chunk->num_constraints = 0;
	
	ScanKeyInit(&scankey[0], Anum_chunk_constraint_chunk_id_dimension_id_idx_chunk_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk->fd.id));

	num_found = scanner_scan(&scanCtx);

	if (num_found != chunk->num_constraints)
		elog(ERROR, "Unexpected number of constraints found for chunk %d", chunk->fd.id);

	return chunk;
}

static bool
chunk_constraint_dimension_id_tuple_found(TupleInfo *ti, void *data)
{
	ChunkScanState *ctx = data;
	ChunkConstraint constraint;
	Chunk *chunk;
	ChunkScanEntry *entry;
	bool found;

	chunk_constraint_fill(&constraint, ti->tuple);

	elog(NOTICE, "Finding chunk %d", constraint.fd.chunk_id);
	
	entry = hash_search(ctx->htab, &constraint.fd.chunk_id, HASH_ENTER, &found);
	
	if (!found)
	{
		chunk = MemoryContextAlloc(ctx->elm_mctx, CHUNK_SIZE(ctx->num_dimensions));
		chunk->fd.id = constraint.fd.chunk_id;
		chunk->num_constraint_slots = ctx->num_dimensions;
		entry->chunk = chunk;
	} else {
		chunk = entry->chunk;
	}

	chunk_add_constraint(chunk, &constraint);
	elog(NOTICE, "Added constraint (%d,%d) for chunk %d num_constraints=%d",
		 constraint.fd.chunk_id, constraint.fd.dimension_slice_id, chunk->fd.id,
		 chunk->num_constraints);

	return true;
}

int
chunk_constraint_scan_by_dimension_slice(DimensionSlice *slice, ChunkScanState *ctx)
{
	Catalog    *catalog = catalog_get();
	ScanKeyData scankey[1];
	int num_found;   
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

	ctx->slice = slice;
	
	ScanKeyInit(&scankey[0], Anum_chunk_constraint_chunk_id_dimension_id_idx_dimension_slice_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(slice->fd.id));

	num_found = scanner_scan(&scanCtx);

	return num_found;
}
