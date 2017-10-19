#include <postgres.h>
#include <utils/hsearch.h>
#include <utils/rel.h>
#include <access/heapam.h>
#include <access/xact.h>
#include <catalog/indexing.h>

#include "scanner.h"
#include "chunk_constraint.h"
#include "dimension_slice.h"
#include "hypercube.h"
#include "chunk.h"

static inline ChunkConstraint *
chunk_constraint_fill(ChunkConstraint *cc, HeapTuple tuple)
{
	memcpy(&cc->fd, GETSTRUCT(tuple), sizeof(FormData_chunk_constraint));
	return cc;
}

static bool
chunk_constraint_for_dimension_slice(TupleInfo *ti, void *data)
{
	return !heap_attisnull(ti->tuple, Anum_chunk_constraint_dimension_slice_id);
}

static bool
chunk_constraint_tuple_found(TupleInfo *ti, void *data)
{
	Chunk	   *chunk = data;

	chunk_constraint_fill(&chunk->constraints[chunk->num_constraints++], ti->tuple);

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
		.limit = chunk->num_constraints,
		.data = chunk,
		.filter = chunk_constraint_for_dimension_slice,
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

typedef struct ChunkConstraintScanData
{
	ChunkScanCtx *scanctx;
	DimensionSlice *slice;
} ChunkConstraintScanData;

static bool
chunk_constraint_dimension_id_tuple_found(TupleInfo *ti, void *data)
{
	ChunkConstraintScanData *ccsd = data;
	ChunkScanCtx *scanctx = ccsd->scanctx;
	Hyperspace *hs = scanctx->space;
	ChunkConstraint constraint;
	Chunk	   *chunk;
	ChunkScanEntry *entry;
	bool		found;

	chunk_constraint_fill(&constraint, ti->tuple);

	entry = hash_search(scanctx->htab, &constraint.fd.chunk_id, HASH_ENTER, &found);

	if (!found)
	{
		chunk = chunk_create_stub(constraint.fd.chunk_id, hs->num_dimensions);
		chunk->cube = hypercube_alloc(hs->num_dimensions);
		entry->chunk = chunk;
	}
	else
		chunk = entry->chunk;

	chunk_add_constraint(chunk, &constraint);
	hypercube_add_slice(chunk->cube, ccsd->slice);

	/*
	 * If the chunk has N constraints, it is the chunk we are looking for and
	 * the scan can be aborted.
	 */
	if (scanctx->early_abort && chunk->num_constraints == hs->num_dimensions)
		return false;

	return true;
}

/*
 * Scan for all chunk constraints that match the given slice ID. The chunk
 * constraints are saved in the chunk scan context.
 */
int
chunk_constraint_scan_by_dimension_slice_id(DimensionSlice *slice, ChunkScanCtx *ctx)
{
	Catalog    *catalog = catalog_get();
	ScanKeyData scankey[1];
	int			num_found;
	ChunkConstraintScanData data = {
		.scanctx = ctx,
		.slice = slice,
	};
	ScannerCtx	scanCtx = {
		.table = catalog->tables[CHUNK_CONSTRAINT].id,
		.index = catalog->tables[CHUNK_CONSTRAINT].index_ids[CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &data,
		.filter = chunk_constraint_for_dimension_slice,
		.tuple_found = chunk_constraint_dimension_id_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0], Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_dimension_slice_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(slice->fd.id));

	num_found = scanner_scan(&scanCtx);

	return num_found;
}

static inline void
chunk_constraint_insert_relation(Relation rel, ChunkConstraint *constraint)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_chunk_constraint];
	bool		nulls[Natts_chunk_constraint] = {false};
	NameData	constraint_name;

	/* quiet valgrind */
	memset(constraint_name.data, 0, NAMEDATALEN);
	snprintf(constraint_name.data, NAMEDATALEN, "constraint_%d", constraint->fd.dimension_slice_id);

	memset(values, 0, sizeof(values));
	values[Anum_chunk_constraint_chunk_id - 1] = constraint->fd.chunk_id;
	values[Anum_chunk_constraint_dimension_slice_id - 1] = constraint->fd.dimension_slice_id;
	values[Anum_chunk_constraint_constraint_name - 1] = NameGetDatum(&constraint_name);

	nulls[Anum_chunk_constraint_hypertable_constraint_name - 1] = true;

	catalog_insert_values(rel, desc, values, nulls);
}

/*
 * Insert chunk constraints into the catalog.
 */
void
chunk_constraint_insert_multi(ChunkConstraint *constraints, Size num_constraints)
{
	Catalog    *catalog = catalog_get();
	Relation	rel;
	Size		i;

	rel = heap_open(catalog->tables[CHUNK_CONSTRAINT].id, RowExclusiveLock);

	for (i = 0; i < num_constraints; i++)
		chunk_constraint_insert_relation(rel, &constraints[i]);

	heap_close(rel, RowExclusiveLock);
}
