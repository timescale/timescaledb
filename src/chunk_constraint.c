#include <postgres.h>
#include <utils/hsearch.h>
#include <utils/relcache.h>
#include <utils/rel.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <access/heapam.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_constraint_fn.h>
#include <catalog/objectaddress.h>
#include <commands/tablecmds.h>
#include <catalog/dependency.h>
#include <funcapi.h>
#include <nodes/makefuncs.h>

#include "scanner.h"
#include "chunk_constraint.h"
#include "chunk_index.h"
#include "dimension_vector.h"
#include "dimension_slice.h"
#include "hypercube.h"
#include "chunk.h"
#include "hypertable.h"
#include "errors.h"
#include "process_utility.h"

#define DEFAULT_EXTRA_CONSTRAINTS_SIZE 4

#define CHUNK_CONSTRAINTS_SIZE(num_constraints)		\
	((sizeof(ChunkConstraint) * num_constraints))

ChunkConstraints *
chunk_constraints_alloc(int size_hint, MemoryContext mctx)
{
	ChunkConstraints *ccs = MemoryContextAlloc(mctx, sizeof(ChunkConstraints));

	ccs->mctx = mctx;
	ccs->capacity = size_hint + DEFAULT_EXTRA_CONSTRAINTS_SIZE;
	ccs->num_constraints = 0;
	ccs->num_dimension_constraints = 0;
	ccs->constraints = MemoryContextAllocZero(mctx, CHUNK_CONSTRAINTS_SIZE(ccs->capacity));

	return ccs;
}

ChunkConstraints *
chunk_constraints_copy(ChunkConstraints *ccs)
{
	ChunkConstraints *copy = palloc(sizeof(ChunkConstraints));

	memcpy(copy, ccs, sizeof(ChunkConstraints));
	copy->constraints = palloc0(CHUNK_CONSTRAINTS_SIZE(ccs->capacity));
	memcpy(copy->constraints, ccs->constraints, CHUNK_CONSTRAINTS_SIZE(ccs->num_constraints));

	return copy;
}

static void
chunk_constraints_expand(ChunkConstraints *ccs, int16 new_capacity)
{
	MemoryContext old;

	if (new_capacity <= ccs->capacity)
		return;

	old = MemoryContextSwitchTo(ccs->mctx);
	ccs->capacity = new_capacity;
	ccs->constraints = repalloc(ccs->constraints, CHUNK_CONSTRAINTS_SIZE(new_capacity));
	MemoryContextSwitchTo(old);
}

static
void
chunk_constraint_choose_name(Name dst,
							 bool is_dimension,
							 int32 dimension_slice_id,
							 const char *hypertable_constraint_name,
							 int32 chunk_id)
{
	if (is_dimension)
	{
		snprintf(NameStr(*dst), NAMEDATALEN, "constraint_%d",
				 dimension_slice_id);
	}
	else
	{
		char		constrname[100];
		CatalogSecurityContext sec_ctx;

		Assert(hypertable_constraint_name != NULL);

		catalog_become_owner(catalog_get(), &sec_ctx);
		snprintf(constrname,
				 100,
				 "%d_" INT64_FORMAT "_%s",
				 chunk_id,
				 catalog_table_next_seq_id(catalog_get(), CHUNK_CONSTRAINT),
				 hypertable_constraint_name);
		catalog_restore_user(&sec_ctx);

		namestrcpy(dst, constrname);
	}
}

static ChunkConstraint *
chunk_constraints_add(ChunkConstraints *ccs,
					  int32 chunk_id,
					  int32 dimension_slice_id,
					  const char *constraint_name,
					  const char *hypertable_constraint_name)
{
	ChunkConstraint *cc;

	chunk_constraints_expand(ccs, ccs->num_constraints + 1);
	cc = &ccs->constraints[ccs->num_constraints++];
	cc->fd.chunk_id = chunk_id;
	cc->fd.dimension_slice_id = dimension_slice_id;

	if (NULL == constraint_name)
	{
		chunk_constraint_choose_name(&cc->fd.constraint_name,
									 is_dimension_constraint(cc),
									 cc->fd.dimension_slice_id,
									 hypertable_constraint_name,
									 cc->fd.chunk_id);

		if (is_dimension_constraint(cc))
			namestrcpy(&cc->fd.hypertable_constraint_name, "");
	}
	else
		namestrcpy(&cc->fd.constraint_name, constraint_name);

	if (NULL != hypertable_constraint_name)
		namestrcpy(&cc->fd.hypertable_constraint_name, hypertable_constraint_name);

	if (is_dimension_constraint(cc))
		ccs->num_dimension_constraints++;

	return cc;
}

static void
chunk_constraint_fill_tuple_values(ChunkConstraint *cc,
								   Datum values[Natts_chunk_constraint],
								   bool nulls[Natts_chunk_constraint])
{
	memset(values, 0, sizeof(Datum) * Natts_chunk_constraint);
	values[Anum_chunk_constraint_chunk_id - 1] = Int32GetDatum(cc->fd.chunk_id);
	values[Anum_chunk_constraint_dimension_slice_id - 1] = Int32GetDatum(cc->fd.dimension_slice_id);
	values[Anum_chunk_constraint_constraint_name - 1] = NameGetDatum(&cc->fd.constraint_name);
	values[Anum_chunk_constraint_hypertable_constraint_name - 1] =
		NameGetDatum(&cc->fd.hypertable_constraint_name);

	if (is_dimension_constraint(cc))
		nulls[Anum_chunk_constraint_hypertable_constraint_name - 1] = true;
	else
		nulls[Anum_chunk_constraint_dimension_slice_id - 1] = true;
}

static void
chunk_constraint_insert_relation(Relation rel, ChunkConstraint *cc)
{
	TupleDesc	desc = RelationGetDescr(rel);
	Datum		values[Natts_chunk_constraint];
	bool		nulls[Natts_chunk_constraint] = {false};

	chunk_constraint_fill_tuple_values(cc, values, nulls);
	catalog_insert_values(rel, desc, values, nulls);
}

/*
 * Insert multiple chunk constraints into the metadata catalog.
 */
static void
chunk_constraints_insert(ChunkConstraints *ccs)
{
	Catalog    *catalog = catalog_get();
	CatalogSecurityContext sec_ctx;
	Relation	rel;
	int			i;

	rel = heap_open(catalog->tables[CHUNK_CONSTRAINT].id, RowExclusiveLock);

	catalog_become_owner(catalog_get(), &sec_ctx);

	for (i = 0; i < ccs->num_constraints; i++)
		chunk_constraint_insert_relation(rel, &ccs->constraints[i]);

	catalog_restore_user(&sec_ctx);
	heap_close(rel, RowExclusiveLock);
}

/*
 * Insert a single chunk constraints into the metadata catalog.
 */
static void
chunk_constraint_insert(ChunkConstraint *constraint)
{
	Catalog    *catalog = catalog_get();
	CatalogSecurityContext sec_ctx;
	Relation	rel;

	rel = heap_open(catalog->tables[CHUNK_CONSTRAINT].id, RowExclusiveLock);

	catalog_become_owner(catalog_get(), &sec_ctx);
	chunk_constraint_insert_relation(rel, constraint);
	catalog_restore_user(&sec_ctx);
	heap_close(rel, RowExclusiveLock);
}


static ChunkConstraint *
chunk_constraints_add_from_tuple(ChunkConstraints *ccs, TupleInfo *ti)
{
	bool		nulls[Natts_chunk_constraint];
	Datum		values[Natts_chunk_constraint];
	int32		dimension_slice_id;
	Name		constraint_name;
	Name		hypertable_constraint_name;

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	constraint_name = DatumGetName(values[Anum_chunk_constraint_constraint_name - 1]);

	if (nulls[Anum_chunk_constraint_dimension_slice_id - 1])
	{
		dimension_slice_id = 0;
		hypertable_constraint_name = DatumGetName(values[Anum_chunk_constraint_hypertable_constraint_name - 1]);
	}
	else
	{
		dimension_slice_id = DatumGetInt32(values[Anum_chunk_constraint_dimension_slice_id - 1]);
		hypertable_constraint_name = DatumGetName(DirectFunctionCall1(namein, CStringGetDatum("")));
	}

	return chunk_constraints_add(ccs,
								 DatumGetInt32(values[Anum_chunk_constraint_chunk_id - 1]),
								 dimension_slice_id,
								 NameStr(*constraint_name),
								 NameStr(*hypertable_constraint_name));
}

/*
 * Add a constraint to a chunk table.
 */
static Oid
chunk_constraint_create_on_table(ChunkConstraint *cc, Oid chunk_oid)
{
	HeapTuple	tuple;
	Datum		values[Natts_chunk_constraint];
	bool		nulls[Natts_chunk_constraint] = {false};
	CatalogSecurityContext sec_ctx;
	Relation	rel;

	chunk_constraint_fill_tuple_values(cc, values, nulls);

	rel = RelationIdGetRelation(catalog_table_get_id(catalog_get(), CHUNK_CONSTRAINT));
	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	RelationClose(rel);

	catalog_become_owner(catalog_get(), &sec_ctx);
	CatalogInternalCall1(DDL_ADD_CHUNK_CONSTRAINT, HeapTupleGetDatum(tuple));
	catalog_restore_user(&sec_ctx);

	return get_relation_constraint_oid(chunk_oid, NameStr(cc->fd.constraint_name), true);
}

/*
 * Create a constraint on a chunk table, including adding relevant metadata to
 * the catalog.
 */
static Oid
chunk_constraint_create(ChunkConstraint *cc,
						Oid chunk_oid,
						int32 chunk_id,
						Oid hypertable_oid,
						int32 hypertable_id)
{
	Oid			chunk_constraint_oid;

	process_utility_set_expect_chunk_modification(true);
	chunk_constraint_oid = chunk_constraint_create_on_table(cc, chunk_oid);
	process_utility_set_expect_chunk_modification(false);

	/*
	 * The table constraint might not have been created if this constraint
	 * corresponds to a dimension slice that covers the entire range of values
	 * in the particular dimension. In that case, there is no need to add a
	 * table constraint.
	 */
	if (!OidIsValid(chunk_constraint_oid))
		return InvalidOid;

	if (!is_dimension_constraint(cc))
	{
		Oid			hypertable_constraint_oid = get_relation_constraint_oid(hypertable_oid,
																			NameStr(cc->fd.hypertable_constraint_name),
																			false);
		HeapTuple	tuple = SearchSysCache1(CONSTROID, hypertable_constraint_oid);

		if (HeapTupleIsValid(tuple))
		{
			FormData_pg_constraint *constr = (FormData_pg_constraint *) GETSTRUCT(tuple);

			if (OidIsValid(constr->conindid) && constr->contype != CONSTRAINT_FOREIGN)
				chunk_index_create_from_constraint(hypertable_id,
												   hypertable_constraint_oid,
												   chunk_id,
												   chunk_constraint_oid);

			ReleaseSysCache(tuple);
		}

	}

	return chunk_constraint_oid;
}

/*
 * Create a set of constraints on a chunk table.
 */
void
chunk_constraints_create(ChunkConstraints *ccs,
						 Oid chunk_oid,
						 int32 chunk_id,
						 Oid hypertable_oid,
						 int32 hypertable_id)
{
	int			i;

	chunk_constraints_insert(ccs);

	for (i = 0; i < ccs->num_constraints; i++)
		chunk_constraint_create(&ccs->constraints[i],
								chunk_oid,
								chunk_id,
								hypertable_oid,
								hypertable_id);
}

/*
 * Scan filter function for only getting dimension constraints.
 */
static bool
chunk_constraint_for_dimension_slice(TupleInfo *ti, void *data)
{
	return !heap_attisnull(ti->tuple, Anum_chunk_constraint_dimension_slice_id);
}

static bool
chunk_constraint_tuple_found(TupleInfo *ti, void *data)
{
	ChunkConstraints *ccs = data;

	if (NULL != ccs)
		chunk_constraints_add_from_tuple(ccs, ti);

	return true;
}

static int
chunk_constraint_scan_internal(int indexid,
							   ScanKeyData *scankey,
							   int nkeys,
							   tuple_found_func tuple_found,
							   tuple_found_func tuple_filter,
							   void *data,
							   LOCKMODE lockmode,
							   MemoryContext mctx)
{
	Catalog    *catalog = catalog_get();
	ScannerCtx	scanctx = {
		.table = catalog->tables[CHUNK_CONSTRAINT].id,
		.index = CATALOG_INDEX(catalog, CHUNK_CONSTRAINT, indexid),
		.nkeys = nkeys,
		.scankey = scankey,
		.data = data,
		.tuple_found = tuple_found,
		.filter = tuple_filter,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
		.result_mctx = mctx,
	};

	return scanner_scan(&scanctx);
}

/*
 * Scan for chunk constraints given a chunk ID.
 */
static int
chunk_constraint_scan_by_chunk_id_internal(int32 chunk_id,
										   tuple_found_func tuple_found,
										   tuple_found_func tuple_filter,
										   void *data,
										   LOCKMODE lockmode,
										   MemoryContext mctx)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_id));

	return chunk_constraint_scan_internal(CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX,
										  scankey,
										  1,
										  tuple_found,
										  tuple_filter,
										  data,
										  lockmode,
										  mctx);
}

static int
chunk_constraint_scan_by_chunk_id_constraint_name_internal(int32 chunk_id,
														   const char *constraint_name,
														   tuple_found_func tuple_found,
														   tuple_found_func tuple_filter,
														   void *data,
														   LOCKMODE lockmode)
{
	ScanKeyData scankey[2];


	ScanKeyInit(&scankey[0],
				Anum_chunk_constraint_chunk_id_constraint_name_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_id));

	ScanKeyInit(&scankey[1],
				Anum_chunk_constraint_chunk_id_constraint_name_idx_constraint_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(constraint_name)));

	return chunk_constraint_scan_internal(CHUNK_CONSTRAINT_CHUNK_ID_CONSTRAINT_NAME_IDX,
										  scankey,
										  2,
										  tuple_found,
										  tuple_filter,
										  data,
										  lockmode,
										  CurrentMemoryContext);
}

/*
 * Scan all the chunk's constraints given its chunk ID.
 *
 * Returns a set of chunk constraints.
 */
ChunkConstraints *
chunk_constraint_scan_by_chunk_id(int32 chunk_id, Size num_constraints_hint, MemoryContext mctx)
{
	ChunkConstraints *constraints = chunk_constraints_alloc(num_constraints_hint, mctx);
	int			num_found;

	num_found = chunk_constraint_scan_by_chunk_id_internal(chunk_id,
														   chunk_constraint_tuple_found,
														   NULL,
														   constraints,
														   AccessShareLock,
														   mctx);

	if (num_found != constraints->num_constraints)
		elog(ERROR, "unexpected number of constraints found for chunk ID %d", chunk_id);

	return constraints;
}

typedef struct ChunkConstraintScanData
{
	ChunkScanCtx *scanctx;
	DimensionSlice *slice;
} ChunkConstraintScanData;

static bool
chunk_constraint_dimension_slice_id_tuple_found(TupleInfo *ti, void *data)
{
	ChunkConstraintScanData *ccsd = data;
	ChunkScanCtx *scanctx = ccsd->scanctx;
	Hyperspace *hs = scanctx->space;
	Chunk	   *chunk;
	ChunkScanEntry *entry;
	bool		found;
	int32		chunk_id = heap_getattr(ti->tuple, Anum_chunk_constraint_chunk_id, ti->desc, &found);

	Assert(!heap_attisnull(ti->tuple, Anum_chunk_constraint_dimension_slice_id));

	entry = hash_search(scanctx->htab, &chunk_id, HASH_ENTER, &found);

	if (!found)
	{
		chunk = chunk_create_stub(chunk_id, hs->num_dimensions);
		chunk->cube = hypercube_alloc(hs->num_dimensions);
		entry->chunk = chunk;
	}
	else
		chunk = entry->chunk;

	chunk_constraints_add_from_tuple(chunk->constraints, ti);

	hypercube_add_slice(chunk->cube, ccsd->slice);

	if (scanctx->early_abort &&
		chunk->constraints->num_dimension_constraints == hs->num_dimensions)
		return false;

	return true;
}

static int
scan_by_dimension_slice_id(int32 dimension_slice_id,
						   tuple_found_func tuple_found,
						   void *data,
						   MemoryContext mctx)
{
	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_dimension_slice_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_slice_id));

	return chunk_constraint_scan_internal(CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX,
										  scankey,
										  1,
										  tuple_found,
										  chunk_constraint_for_dimension_slice,
										  data,
										  AccessShareLock,
										  mctx);
}

/*
 * Scan for all chunk constraints that match the given slice ID. The chunk
 * constraints are saved in the chunk scan context.
 */
int
chunk_constraint_scan_by_dimension_slice(DimensionSlice *slice, ChunkScanCtx *ctx, MemoryContext mctx)
{
	ChunkConstraintScanData data = {
		.scanctx = ctx,
		.slice = slice,
	};

	return scan_by_dimension_slice_id(slice->fd.id,
									  chunk_constraint_dimension_slice_id_tuple_found,
									  &data,
									  mctx);
}

/*
 * Scan for chunk constraints given a dimension slice ID.
 *
 * Optionally, collect all chunk constraints if ChunkConstraints is non-NULL.
 */
int
chunk_constraint_scan_by_dimension_slice_id(int32 dimension_slice_id, ChunkConstraints *ccs, MemoryContext mctx)
{
	return scan_by_dimension_slice_id(dimension_slice_id,
									  chunk_constraint_tuple_found,
									  ccs,
									  mctx);
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
		return false;
	}
	return true;
}

int
chunk_constraints_add_dimension_constraints(ChunkConstraints *ccs,
											int32 chunk_id,
											Hypercube *cube)
{
	int			i;

	for (i = 0; i < cube->num_slices; i++)
		chunk_constraints_add(ccs, chunk_id, cube->slices[i]->fd.id, NULL, NULL);

	return cube->num_slices;
}

int
chunk_constraints_add_inheritable_constraints(ChunkConstraints *ccs,
											  int32 chunk_id,
											  Oid hypertable_oid)
{
	ScanKeyData skey;
	Relation	rel;
	SysScanDesc scan;
	HeapTuple	htup;
	int			num_added = 0;

	ScanKeyInit(&skey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, hypertable_oid);

	rel = heap_open(ConstraintRelationId, AccessShareLock);
	scan = systable_beginscan(rel, ConstraintRelidIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		Form_pg_constraint pg_constraint = (Form_pg_constraint) GETSTRUCT(htup);

		if (chunk_constraint_need_on_chunk(pg_constraint))
		{
			chunk_constraints_add(ccs, chunk_id, 0, NULL, NameStr(pg_constraint->conname));
			num_added++;
		}
	}

	systable_endscan(scan);
	heap_close(rel, AccessShareLock);

	return num_added;
}

void
chunk_constraint_create_on_chunk(Chunk *chunk, Oid constraint_oid)
{
	const char *constrname;
	ChunkConstraint *cc;

	constrname = get_constraint_name(constraint_oid);
	cc = chunk_constraints_add(chunk->constraints, chunk->fd.id, 0, NULL, constrname);

	chunk_constraint_insert(cc);

	chunk_constraint_create(cc,
							chunk->table_id,
							chunk->fd.id,
							chunk->hypertable_relid,
							chunk->fd.hypertable_id);
}

typedef struct ConstraintInfo
{
	const char *hypertable_constraint_name;
	ChunkConstraints *ccs;
	bool		delete_metadata;
	bool		drop_constraint;
} ConstraintInfo;

typedef struct RenameHypertableConstraintInfo
{
	ConstraintInfo base;
	const char *newname;
} RenameHypertableConstraintInfo;

typedef struct GetNameFromHypertableConstraintInfo
{
	ConstraintInfo base;

	Name		chunk_constraint_name;
} GetNameFromHypertableConstraintInfo;



/*
 * Delete a chunk constraint tuple.
 *
 * Optionally, the data argument is a ConstraintInfo.
 */
static bool
chunk_constraint_delete_tuple(TupleInfo *ti, void *data)
{
	ConstraintInfo *info = data;
	bool		isnull;
	Datum		constrname = heap_getattr(ti->tuple, Anum_chunk_constraint_constraint_name,
										  ti->desc, &isnull);
	int32		chunk_id = DatumGetInt32(heap_getattr(ti->tuple, Anum_chunk_constraint_chunk_id,
													  ti->desc, &isnull));
	Chunk	   *chunk = chunk_get_by_id(chunk_id, 0, true);
	ObjectAddress constrobj = {
		.classId = ConstraintRelationId,
		.objectId = get_relation_constraint_oid(chunk->table_id,
												NameStr(*DatumGetName(constrname)), true),
	};
	Oid			index_relid = get_constraint_index(constrobj.objectId);

	/* Collect the deleted constraints */
	if (NULL != info->ccs)
		chunk_constraint_tuple_found(ti, info->ccs);

	if (info->delete_metadata)
	{
		/*
		 * If this is an index constraint, we need to cleanup the index
		 * metadata. Don't drop the index though, since that will happend when
		 * the constraint is dropped.
		 */
		if (OidIsValid(index_relid))
			chunk_index_delete(chunk, index_relid, false);

		catalog_delete(ti->scanrel, ti->tuple);
	}

	if (info->drop_constraint && OidIsValid(constrobj.objectId))
		performDeletion(&constrobj, DROP_RESTRICT, 0);
	return true;
}

static bool
hypertable_constraint_tuple_filter(TupleInfo *ti, void *data)
{
	ConstraintInfo *info = data;
	bool		nulls[Natts_chunk_constraint];
	Datum		values[Natts_chunk_constraint];
	const char *constrname;

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	if (nulls[Anum_chunk_constraint_hypertable_constraint_name - 1])
		return false;

	constrname = NameStr(*DatumGetName(values[Anum_chunk_constraint_hypertable_constraint_name - 1]));

	return NULL != info->hypertable_constraint_name &&
		strcmp(info->hypertable_constraint_name, constrname) == 0;
}

int
chunk_constraint_delete_by_hypertable_constraint_name(int32 chunk_id,
													  char *hypertable_constraint_name,
													  bool delete_metadata, bool drop_constraint)
{
	ConstraintInfo info = {
		.hypertable_constraint_name = hypertable_constraint_name,
		.delete_metadata = delete_metadata,
		.drop_constraint = drop_constraint
	};

	return chunk_constraint_scan_by_chunk_id_internal(chunk_id,
													  chunk_constraint_delete_tuple,
													  hypertable_constraint_tuple_filter,
													  &info,
													  RowExclusiveLock,
													  CurrentMemoryContext);
}

int
chunk_constraint_delete_by_constraint_name(int32 chunk_id, const char *constraint_name,
										   bool delete_metadata, bool drop_constraint)
{
	ConstraintInfo info = {
		.delete_metadata = delete_metadata,
		.drop_constraint = drop_constraint
	};

	return chunk_constraint_scan_by_chunk_id_constraint_name_internal(chunk_id,
																	  constraint_name,
																	  chunk_constraint_delete_tuple,
																	  NULL,
																	  &info,
																	  RowExclusiveLock);
}

/*
 * Delete all constraints for a chunk. Optionally, collect the deleted constraints.
 */
int
chunk_constraint_delete_by_chunk_id(int32 chunk_id, ChunkConstraints *ccs)
{
	ConstraintInfo info = {
		.ccs = ccs,
		.delete_metadata = true,
		.drop_constraint = true,
	};

	return chunk_constraint_scan_by_chunk_id_internal(chunk_id,
													  chunk_constraint_delete_tuple,
													  NULL,
													  &info,
													  RowExclusiveLock,
													  CurrentMemoryContext);
}

int
chunk_constraint_delete_by_dimension_slice_id(int32 dimension_slice_id)
{
	ConstraintInfo info = {
		.delete_metadata = true,
		.drop_constraint = true,
	};

	ScanKeyData scankey[1];

	ScanKeyInit(&scankey[0],
				Anum_chunk_constraint_dimension_slice_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(dimension_slice_id));

	return chunk_constraint_scan_internal(INVALID_INDEXID,
										  scankey,
										  1,
										  chunk_constraint_delete_tuple,
										  NULL,
										  &info,
										  RowExclusiveLock,
										  CurrentMemoryContext);
}

void
chunk_constraint_recreate(ChunkConstraint *cc, Oid chunk_oid)
{
	ObjectAddress constrobj = {
		.classId = ConstraintRelationId,
		.objectId = get_relation_constraint_oid(chunk_oid,
												NameStr(cc->fd.constraint_name), false),
	};

	performDeletion(&constrobj, DROP_RESTRICT, 0);
	chunk_constraint_create_on_table(cc, chunk_oid);
}

static void
chunk_constraint_rename_on_chunk_table(int32 chunk_id, char *old_name, char *new_name)
{
	Chunk	   *chunk = chunk_get_by_id(chunk_id, 0, true);
	RenameStmt	rename = {
		.renameType = OBJECT_TABCONSTRAINT,
		.relation = makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), 0),
		.subname = old_name,
		.newname = new_name,
	};

	RenameConstraint(&rename);
}

static bool
chunk_constraint_rename_hypertable_tuple(TupleInfo *ti, void *data)
{
	RenameHypertableConstraintInfo *info = data;

	bool		nulls[Natts_chunk_constraint];
	Datum		values[Natts_chunk_constraint];
	bool		repl[Natts_chunk_constraint] = {false};

	HeapTuple	tuple;
	NameData	new_hypertable_constraint_name;
	NameData	new_chunk_constraint_name;
	Name		old_chunk_constraint_name;
	int32		chunk_id;

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	chunk_id = DatumGetInt32(values[Anum_chunk_constraint_chunk_id - 1]);
	namestrcpy(&new_hypertable_constraint_name, info->newname);
	chunk_constraint_choose_name(&new_chunk_constraint_name,
								 false,
								 0,
								 info->newname,
								 chunk_id);

	values[Anum_chunk_constraint_hypertable_constraint_name - 1] = NameGetDatum(&new_hypertable_constraint_name);
	repl[Anum_chunk_constraint_hypertable_constraint_name - 1] = true;
	old_chunk_constraint_name = DatumGetName(values[Anum_chunk_constraint_constraint_name - 1]);
	values[Anum_chunk_constraint_constraint_name - 1] = NameGetDatum(&new_chunk_constraint_name);
	repl[Anum_chunk_constraint_constraint_name - 1] = true;

	chunk_constraint_rename_on_chunk_table(chunk_id,
										   NameStr(*old_chunk_constraint_name),
										   NameStr(new_chunk_constraint_name));

	tuple = heap_modify_tuple(ti->tuple, ti->desc, values, nulls, repl);
	catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);

	return true;
}

int
chunk_constraint_rename_hypertable_constraint(int32 chunk_id, const char *oldname, const char *newname)
{
	RenameHypertableConstraintInfo info = {
		.base = {
			.hypertable_constraint_name = oldname,
		},
		.newname = newname,
	};

	return chunk_constraint_scan_by_chunk_id_internal(chunk_id,
													  chunk_constraint_rename_hypertable_tuple,
													  hypertable_constraint_tuple_filter,
													  &info,
													  RowExclusiveLock,
													  CurrentMemoryContext);
}

static bool
chunk_constraint_get_name_from_hypertable_tuple(TupleInfo *ti, void *data)
{
	GetNameFromHypertableConstraintInfo *info = data;

	bool		nulls[Natts_chunk_constraint];
	Datum		values[Natts_chunk_constraint];


	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);
	info->chunk_constraint_name = DatumGetName(values[Anum_chunk_constraint_constraint_name - 1]);

	return false;
}

char *
chunk_constraint_get_name_from_hypertable_constraint(Oid chunk_relid, const char *hypertable_constraint_name)
{
	Chunk	   *chunk = chunk_get_by_relid(chunk_relid, 0, true);
	GetNameFromHypertableConstraintInfo info = {
		.base = {
			.hypertable_constraint_name = hypertable_constraint_name,
		},
		.chunk_constraint_name = NULL,
	};

	chunk_constraint_scan_by_chunk_id_internal(chunk->fd.id,
											   chunk_constraint_get_name_from_hypertable_tuple,
											   hypertable_constraint_tuple_filter,
											   &info,
											   RowExclusiveLock,
											   CurrentMemoryContext);
	return NameStr(*info.chunk_constraint_name);
}
