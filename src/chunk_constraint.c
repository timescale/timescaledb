#include <postgres.h>

#include "scanner.h"
#include "chunk_constraint.h"

static inline ChunkConstraint *
chunk_constraint_from_form_data(Form_chunk_constraint fd)
{
	ChunkConstraint *cc;
	cc = palloc0(sizeof(ChunkConstraint));
	memcpy(&cc->fd, fd, sizeof(FormData_chunk_constraint));
	return cc;
}

static inline ChunkConstraint *
chunk_constraint_from_tuple(HeapTuple tuple)
{
	return chunk_constraint_from_form_data((Form_chunk_constraint ) GETSTRUCT(tuple));
}

static bool
chunk_constraint_tuple_found(TupleInfo *ti, void *data)
{
	List **l = data;
	*l = lappend(*l, chunk_constraint_from_tuple(ti->tuple));
	return true;
}

List *
chunk_constraint_scan(int32 chunk_id)
{
	Catalog    *catalog = catalog_get();
	List * result = NULL;
	ScanKeyData scankey[1];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[CHUNK_CONSTRAINT].id,
		.index = catalog->tables[CHUNK_CONSTRAINT].index_ids[CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_ID_IDX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = &result,
		.tuple_found = chunk_constraint_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan for slice matching the dimension's ID and which
	 * encloses the coordinate */
	ScanKeyInit(&scankey[0], Anum_chunk_constraint_chunk_id_dimension_id_idx_chunk_id,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(chunk_id));

	scanner_scan(&scanCtx);

	return result;
}
