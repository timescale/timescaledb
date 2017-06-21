#include <postgres.h>

#include "scanner.h"
#include "chunk_constraint.h"
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
	int16 num_constraints_found;
} ChunkConstraintCtx;

static bool
chunk_constraint_tuple_found(TupleInfo *ti, void *data)
{
	ChunkConstraintCtx *ctx = data;

	chunk_constraint_fill(&ctx->chunk->constraints[ctx->num_constraints_found++], ti->tuple);

	if (ctx->num_constraints_found == ctx->chunk->num_constraints)
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
		.num_constraints_found = 0,
	};
	int num_found;
	ScannerCtx	scanCtx = {
		.table = catalog->tables[CHUNK_CONSTRAINT].id,
		.index = catalog->tables[CHUNK_CONSTRAINT].index_ids[CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_ID_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &data,
		.tuple_found = chunk_constraint_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};
	
	/* Perform an index scan for slice matching the dimension's ID and which
	 * encloses the coordinate */
	ScanKeyInit(&scankey[0], Anum_chunk_constraint_chunk_id_dimension_id_idx_chunk_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk->fd.id));

	num_found = scanner_scan(&scanCtx);

	if (num_found != chunk->num_constraints)
		elog(ERROR, "Unexpected number of constraints found for chunk %d", chunk->fd.id);

	return chunk;
}
