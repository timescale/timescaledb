#include <postgres.h>
#include <utils/hsearch.h>
#include <utils/rel.h>
#include <access/heapam.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/pg_constraint.h>
#include <catalog/objectaddress.h>

#include "scanner.h"
#include "chunk_constraint.h"
#include "chunk_index.h"
#include "dimension_slice.h"
#include "hypercube.h"
#include "chunk.h"
#include "hypertable.h"
#include "errors.h"
#include "process_utility.h"

#define create_chunk_constraint(chunk, constraint_oid)		\
	DatumGetObjectId(CatalogInternalCall2(DDL_CREATE_CHUNK_CONSTRAINT, \
						 Int32GetDatum(chunk->fd.id),	\
						 ObjectIdGetDatum(constraint_oid)))

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
	values[Anum_chunk_constraint_chunk_id - 1] = Int32GetDatum(constraint->fd.chunk_id);
	values[Anum_chunk_constraint_dimension_slice_id - 1] = Int32GetDatum(constraint->fd.dimension_slice_id);
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

static bool
chunk_constraint_need_on_chunk(Form_pg_constraint conform)
{
	if (conform->contype == CONSTRAINT_CHECK)
	{
		/*
		 * check and not null constraints handled by regular inheritance (from
		 * docs): All check constraints and not-null constraints on a parent
		 * table are automatically inherited by its children, unless
		 * explicitly specified otherwise with NO INHERIT clauses. Other types
		 * of constraints (unique, primary key, and foreign key constraints)
		 * are not inherited."
		 */
		if (conform->connoinherit)
		{
			ereport(ERROR,
					(errcode(ERRCODE_IO_OPERATION_NOT_SUPPORTED),
					 errmsg("NO INHERIT option not supported on hypertables: %s", conform->conname.data)
					 ));
		}
		return false;
	}
	return true;
}

static void
chunk_constraint_create_on_chunk_impl(Hypertable *ht, Chunk *chunk, HeapTuple constraint_htup)
{
	Form_pg_constraint pg_constraint = (Form_pg_constraint) GETSTRUCT(constraint_htup);

	if (chunk_constraint_need_on_chunk(pg_constraint))
	{
		Oid			hypertable_constraint_oid = HeapTupleGetOid(constraint_htup);
		Oid			chunk_constraint_oid;
		CatalogSecurityContext sec_ctx;

		catalog_become_owner(catalog_get(), &sec_ctx);
		process_utility_set_expect_chunk_modification(true);
		chunk_constraint_oid = create_chunk_constraint(chunk, hypertable_constraint_oid);
		process_utility_set_expect_chunk_modification(false);
		catalog_restore_user(&sec_ctx);

		Assert(OidIsValid(chunk_constraint_oid));

		if (OidIsValid(pg_constraint->conindid) && pg_constraint->contype != CONSTRAINT_FOREIGN)
		{
			chunk_index_create_from_constraint(ht->fd.id, hypertable_constraint_oid, chunk->fd.id, chunk_constraint_oid);
		}
	}
}

void
chunk_constraint_create_all_on_chunk(Hypertable *ht, Chunk *chunk)
{
	ScanKeyData skey;
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	htup;

	ScanKeyInit(&skey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, ht->main_table_relid);

	rel = heap_open(ConstraintRelationId, AccessShareLock);
	scan = systable_beginscan(rel, ConstraintRelidIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		chunk_constraint_create_on_chunk_impl(ht, chunk, htup);
	}
	systable_endscan(scan);
	heap_close(rel, AccessShareLock);
}

void
chunk_constraint_create_on_chunk(Hypertable *ht, Chunk *chunk, Oid constraintOid)
{
	Relation	rel;
	HeapTuple	htup;

	rel = heap_open(ConstraintRelationId, AccessShareLock);
	htup = get_catalog_object_by_oid(rel, constraintOid);
	chunk_constraint_create_on_chunk_impl(ht, chunk, htup);
	heap_close(rel, AccessShareLock);
}
