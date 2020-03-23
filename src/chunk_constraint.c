/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
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
#include <catalog/objectaddress.h>
#include <commands/tablecmds.h>
#include <catalog/dependency.h>
#include <funcapi.h>
#include <nodes/makefuncs.h>

#include <catalog/pg_constraint.h>
#include "compat.h"
#if PG11_LT /* PG11 consolidates pg_foo_fn.h -> pg_foo.h */
#include <catalog/pg_constraint_fn.h>
#endif

#include "export.h"
#include "scan_iterator.h"
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

#define CHUNK_CONSTRAINTS_SIZE(num_constraints) (sizeof(ChunkConstraint) * (num_constraints))

ChunkConstraints *
ts_chunk_constraints_alloc(int size_hint, MemoryContext mctx)
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
ts_chunk_constraints_copy(ChunkConstraints *ccs)
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

static void
chunk_constraint_choose_name(Name dst, bool is_dimension, int32 dimension_slice_id,
							 const char *hypertable_constraint_name, int32 chunk_id)
{
	if (is_dimension)
	{
		snprintf(NameStr(*dst), NAMEDATALEN, "constraint_%d", dimension_slice_id);
	}
	else
	{
		char constrname[100];
		CatalogSecurityContext sec_ctx;

		Assert(hypertable_constraint_name != NULL);

		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		snprintf(constrname,
				 100,
				 "%d_" INT64_FORMAT "_%s",
				 chunk_id,
				 ts_catalog_table_next_seq_id(ts_catalog_get(), CHUNK_CONSTRAINT),
				 hypertable_constraint_name);
		ts_catalog_restore_user(&sec_ctx);

		namestrcpy(dst, constrname);
	}
}

static ChunkConstraint *
chunk_constraints_add(ChunkConstraints *ccs, int32 chunk_id, int32 dimension_slice_id,
					  const char *constraint_name, const char *hypertable_constraint_name)
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
chunk_constraint_fill_tuple_values(ChunkConstraint *cc, Datum values[Natts_chunk_constraint],
								   bool nulls[Natts_chunk_constraint])
{
	memset(values, 0, sizeof(Datum) * Natts_chunk_constraint);
	values[AttrNumberGetAttrOffset(Anum_chunk_constraint_chunk_id)] =
		Int32GetDatum(cc->fd.chunk_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_constraint_dimension_slice_id)] =
		Int32GetDatum(cc->fd.dimension_slice_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)] =
		NameGetDatum(&cc->fd.constraint_name);
	values[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)] =
		NameGetDatum(&cc->fd.hypertable_constraint_name);

	if (is_dimension_constraint(cc))
		nulls[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)] = true;
	else
		nulls[AttrNumberGetAttrOffset(Anum_chunk_constraint_dimension_slice_id)] = true;
}

static void
chunk_constraint_insert_relation(Relation rel, ChunkConstraint *cc)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_chunk_constraint];
	bool nulls[Natts_chunk_constraint] = { false };

	chunk_constraint_fill_tuple_values(cc, values, nulls);
	ts_catalog_insert_values(rel, desc, values, nulls);
}

/*
 * Insert multiple chunk constraints into the metadata catalog.
 */
void
ts_chunk_constraints_insert_metadata(ChunkConstraints *ccs)
{
	Catalog *catalog = ts_catalog_get();
	CatalogSecurityContext sec_ctx;
	Relation rel;
	int i;

	rel = table_open(catalog_get_table_id(catalog, CHUNK_CONSTRAINT), RowExclusiveLock);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	for (i = 0; i < ccs->num_constraints; i++)
		chunk_constraint_insert_relation(rel, &ccs->constraints[i]);

	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

/*
 * Insert a single chunk constraints into the metadata catalog.
 */
static void
chunk_constraint_insert(ChunkConstraint *constraint)
{
	Catalog *catalog = ts_catalog_get();
	CatalogSecurityContext sec_ctx;
	Relation rel;

	rel = table_open(catalog_get_table_id(catalog, CHUNK_CONSTRAINT), RowExclusiveLock);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	chunk_constraint_insert_relation(rel, constraint);
	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, RowExclusiveLock);
}

static ChunkConstraint *
chunk_constraints_add_from_tuple(ChunkConstraints *ccs, TupleInfo *ti)
{
	bool nulls[Natts_chunk_constraint];
	Datum values[Natts_chunk_constraint];
	int32 dimension_slice_id;
	Name constraint_name;
	Name hypertable_constraint_name;

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	constraint_name =
		DatumGetName(values[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_chunk_constraint_dimension_slice_id)])
	{
		dimension_slice_id = 0;
		hypertable_constraint_name = DatumGetName(
			values[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)]);
	}
	else
	{
		dimension_slice_id = DatumGetInt32(
			values[AttrNumberGetAttrOffset(Anum_chunk_constraint_dimension_slice_id)]);
		hypertable_constraint_name = DatumGetName(DirectFunctionCall1(namein, CStringGetDatum("")));
	}

	return chunk_constraints_add(ccs,
								 DatumGetInt32(values[AttrNumberGetAttrOffset(
									 Anum_chunk_constraint_chunk_id)]),
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
	HeapTuple tuple;
	Datum values[Natts_chunk_constraint];
	bool nulls[Natts_chunk_constraint] = { false };
	CatalogSecurityContext sec_ctx;
	Relation rel;

	chunk_constraint_fill_tuple_values(cc, values, nulls);

	rel = RelationIdGetRelation(catalog_get_table_id(ts_catalog_get(), CHUNK_CONSTRAINT));
	tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
	RelationClose(rel);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	CatalogInternalCall1(DDL_ADD_CHUNK_CONSTRAINT, HeapTupleGetDatum(tuple));
	ts_catalog_restore_user(&sec_ctx);

	return get_relation_constraint_oid(chunk_oid, NameStr(cc->fd.constraint_name), true);
}

/*
 * Create a constraint on a chunk table, including adding relevant metadata to
 * the catalog.
 */
static Oid
chunk_constraint_create(ChunkConstraint *cc, Oid chunk_oid, int32 chunk_id, Oid hypertable_oid,
						int32 hypertable_id)
{
	Oid chunk_constraint_oid;

	ts_process_utility_set_expect_chunk_modification(true);
	chunk_constraint_oid = chunk_constraint_create_on_table(cc, chunk_oid);
	ts_process_utility_set_expect_chunk_modification(false);

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
		Oid hypertable_constraint_oid =
			get_relation_constraint_oid(hypertable_oid,
										NameStr(cc->fd.hypertable_constraint_name),
										false);
		HeapTuple tuple = SearchSysCache1(CONSTROID, hypertable_constraint_oid);

		if (HeapTupleIsValid(tuple))
		{
			FormData_pg_constraint *constr = (FormData_pg_constraint *) GETSTRUCT(tuple);

			if (OidIsValid(constr->conindid) && constr->contype != CONSTRAINT_FOREIGN)
				ts_chunk_index_create_from_constraint(hypertable_id,
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
ts_chunk_constraints_create(ChunkConstraints *ccs, Oid chunk_oid, int32 chunk_id,
							Oid hypertable_oid, int32 hypertable_id)
{
	int i;

	for (i = 0; i < ccs->num_constraints; i++)
		chunk_constraint_create(&ccs->constraints[i],
								chunk_oid,
								chunk_id,
								hypertable_oid,
								hypertable_id);
}

static void
init_scan_by_chunk_id(ScanIterator *iterator, int32 chunk_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CHUNK_CONSTRAINT,
											CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX);
	ts_scan_iterator_scan_key_init(iterator,
								   Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));
}

static void
init_scan_by_dimension_slice_id(ScanIterator *iterator, int32 dimension_slice_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CHUNK_CONSTRAINT,
											CHUNK_CONSTRAINT_CHUNK_ID_DIMENSION_SLICE_ID_IDX);

	ts_scan_iterator_scan_key_init(
		iterator,
		Anum_chunk_constraint_chunk_id_dimension_slice_id_idx_dimension_slice_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(dimension_slice_id));
}

static void
init_scan_by_chunk_id_constraint_name(ScanIterator *iterator, int32 chunk_id,
									  const char *constraint_name)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CHUNK_CONSTRAINT,
											CHUNK_CONSTRAINT_CHUNK_ID_CONSTRAINT_NAME_IDX);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_chunk_constraint_chunk_id_constraint_name_idx_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));

	ts_scan_iterator_scan_key_init(
		iterator,
		Anum_chunk_constraint_chunk_id_constraint_name_idx_constraint_name,
		BTEqualStrategyNumber,
		F_NAMEEQ,
		DirectFunctionCall1(namein, CStringGetDatum(constraint_name)));
}

/*
 * Scan all the chunk's constraints given its chunk ID.
 *
 * Returns a set of chunk constraints.
 */
ChunkConstraints *
ts_chunk_constraint_scan_by_chunk_id(int32 chunk_id, Size num_constraints_hint, MemoryContext mctx)
{
	ChunkConstraints *constraints = ts_chunk_constraints_alloc(num_constraints_hint, mctx);
	ScanIterator iterator = ts_scan_iterator_create(CHUNK_CONSTRAINT, AccessShareLock, mctx);
	int num_found = 0;

	init_scan_by_chunk_id(&iterator, chunk_id);
	ts_scanner_foreach(&iterator)
	{
		num_found++;
		chunk_constraints_add_from_tuple(constraints, ts_scan_iterator_tuple_info(&iterator));
	}

	if (num_found != constraints->num_constraints)
		elog(ERROR, "unexpected number of constraints found for chunk ID %d", chunk_id);

	return constraints;
}

typedef struct ChunkConstraintScanData
{
	ChunkScanCtx *scanctx;
	DimensionSlice *slice;
} ChunkConstraintScanData;

/*
 * Scan for all chunk constraints that match the given slice ID. The chunk
 * constraints are saved in the chunk scan context.
 */
int
ts_chunk_constraint_scan_by_dimension_slice(DimensionSlice *slice, ChunkScanCtx *ctx,
											MemoryContext mctx)
{
	ScanIterator iterator = ts_scan_iterator_create(CHUNK_CONSTRAINT, AccessShareLock, mctx);
	int count = 0;

	init_scan_by_dimension_slice_id(&iterator, slice->fd.id);
	ts_scanner_foreach(&iterator)
	{
		Hyperspace *hs = ctx->space;
		ChunkStub *stub;
		ChunkScanEntry *entry;
		bool found;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		int32 chunk_id = heap_getattr(ti->tuple, Anum_chunk_constraint_chunk_id, ti->desc, &found);

		if (heap_attisnull_compat(ts_scan_iterator_tuple(&iterator),
								  Anum_chunk_constraint_dimension_slice_id,
								  ts_scan_iterator_tupledesc(&iterator)))
			continue;

		count++;

		Assert(
			!heap_attisnull_compat(ti->tuple, Anum_chunk_constraint_dimension_slice_id, ti->desc));

		entry = hash_search(ctx->htab, &chunk_id, HASH_ENTER, &found);

		if (!found)
		{
			stub = ts_chunk_stub_create(chunk_id, hs->num_dimensions);
			stub->cube = ts_hypercube_alloc(hs->num_dimensions);
			entry->stub = stub;
		}
		else
			stub = entry->stub;

		chunk_constraints_add_from_tuple(stub->constraints, ti);

		ts_hypercube_add_slice(stub->cube, slice);

		/* A stub is complete when we've added slices for all its dimensions,
		 * i.e., a complete hypercube */
		if (chunk_stub_is_complete(stub, ctx->space))
		{
			ctx->num_complete_chunks++;

			if (ctx->early_abort)
			{
				ts_scan_iterator_close(&iterator);
				break;
			}
		}
	}
	return count;
}

/*
 * Similar to chunk_constraint_scan_by_dimension_slice, but stores only chunk_ids
 * in a list, which is easier to traverse and provides deterministic chunk selection.
 */
int
ts_chunk_constraint_scan_by_dimension_slice_to_list(DimensionSlice *slice, List **list,
													MemoryContext mctx)
{
	ScanIterator iterator = ts_scan_iterator_create(CHUNK_CONSTRAINT, AccessShareLock, mctx);
	int count = 0;

	init_scan_by_dimension_slice_id(&iterator, slice->fd.id);
	ts_scanner_foreach(&iterator)
	{
		bool is_null;
		int32 chunk_id = heap_getattr(ts_scan_iterator_tuple(&iterator),
									  Anum_chunk_constraint_chunk_id,
									  ts_scan_iterator_tupledesc(&iterator),
									  &is_null);

		if (heap_attisnull_compat(ts_scan_iterator_tuple(&iterator),
								  Anum_chunk_constraint_dimension_slice_id,
								  ts_scan_iterator_tupledesc(&iterator)))
			continue;

		count++;

		Assert(!is_null);
		*list = lappend_int(*list, chunk_id);
	}
	return count;
}

/*
 * Scan for chunk constraints given a dimension slice ID.
 *
 * Optionally, collect all chunk constraints if ChunkConstraints is non-NULL.
 */
int
ts_chunk_constraint_scan_by_dimension_slice_id(int32 dimension_slice_id, ChunkConstraints *ccs,
											   MemoryContext mctx)
{
	ScanIterator iterator = ts_scan_iterator_create(CHUNK_CONSTRAINT, AccessShareLock, mctx);
	int count = 0;

	init_scan_by_dimension_slice_id(&iterator, dimension_slice_id);
	ts_scanner_foreach(&iterator)
	{
		if (heap_attisnull_compat(ts_scan_iterator_tuple(&iterator),
								  Anum_chunk_constraint_dimension_slice_id,
								  ts_scan_iterator_tupledesc(&iterator)))
			continue;

		count++;
		if (ccs != NULL)
			chunk_constraints_add_from_tuple(ccs, ts_scan_iterator_tuple_info(&iterator));
	}
	return count;
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
ts_chunk_constraints_add_dimension_constraints(ChunkConstraints *ccs, int32 chunk_id,
											   Hypercube *cube)
{
	int i;

	for (i = 0; i < cube->num_slices; i++)
		chunk_constraints_add(ccs, chunk_id, cube->slices[i]->fd.id, NULL, NULL);

	return cube->num_slices;
}

int
ts_chunk_constraints_add_inheritable_constraints(ChunkConstraints *ccs, int32 chunk_id,
												 Oid hypertable_oid)
{
	ScanKeyData skey;
	Relation rel;
	SysScanDesc scan;
	HeapTuple htup;
	int num_added = 0;

	ScanKeyInit(&skey, Anum_pg_constraint_conrelid, BTEqualStrategyNumber, F_OIDEQ, hypertable_oid);

	rel = table_open(ConstraintRelationId, AccessShareLock);
	scan = systable_beginscan(rel, ConstraintRelidTypidNameIndexId, true, NULL, 1, &skey);

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
	table_close(rel, AccessShareLock);

	return num_added;
}

void
ts_chunk_constraint_create_on_chunk(Chunk *chunk, Oid constraint_oid)
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

static bool
hypertable_constraint_matches_tuple(TupleInfo *ti, const char *hypertable_constraint_name)
{
	bool nulls[Natts_chunk_constraint];
	Datum values[Natts_chunk_constraint];
	const char *constrname;

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	if (nulls[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)])
		return false;

	constrname = NameStr(*DatumGetName(
		values[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)]));

	if (strcmp(hypertable_constraint_name, constrname) == 0)
		return true;

	return false;
}

static void
chunk_constraint_delete_metadata(TupleInfo *ti)
{
	bool isnull;
	Datum constrname =
		heap_getattr(ti->tuple, Anum_chunk_constraint_constraint_name, ti->desc, &isnull);
	int32 chunk_id =
		DatumGetInt32(heap_getattr(ti->tuple, Anum_chunk_constraint_chunk_id, ti->desc, &isnull));
	Chunk *chunk = ts_chunk_get_by_id(chunk_id, 0, true);
	Oid index_relid = get_constraint_index(
		get_relation_constraint_oid(chunk->table_id, NameStr(*DatumGetName(constrname)), true));

	/*
	 * If this is an index constraint, we need to cleanup the index
	 * metadata. Don't drop the index though, since that will happend when
	 * the constraint is dropped.
	 */
	if (OidIsValid(index_relid))
		ts_chunk_index_delete(chunk, index_relid, false);

	ts_catalog_delete(ti->scanrel, ti->tuple);
}

static void
chunk_constraint_drop_constraint(TupleInfo *ti)
{
	bool isnull;
	Datum constrname =
		heap_getattr(ti->tuple, Anum_chunk_constraint_constraint_name, ti->desc, &isnull);
	int32 chunk_id =
		DatumGetInt32(heap_getattr(ti->tuple, Anum_chunk_constraint_chunk_id, ti->desc, &isnull));
	Chunk *chunk = ts_chunk_get_by_id(chunk_id, 0, true);
	ObjectAddress constrobj = {
		.classId = ConstraintRelationId,
		.objectId =
			get_relation_constraint_oid(chunk->table_id, NameStr(*DatumGetName(constrname)), true),
	};

	if (OidIsValid(constrobj.objectId))
		performDeletion(&constrobj, DROP_RESTRICT, 0);
}

int
ts_chunk_constraint_delete_by_hypertable_constraint_name(int32 chunk_id,
														 char *hypertable_constraint_name,
														 bool delete_metadata, bool drop_constraint)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	int count = 0;

	init_scan_by_chunk_id(&iterator, chunk_id);
	ts_scanner_foreach(&iterator)
	{
		if (!hypertable_constraint_matches_tuple(ts_scan_iterator_tuple_info(&iterator),
												 hypertable_constraint_name))
			continue;

		count++;

		if (delete_metadata)
			chunk_constraint_delete_metadata(ts_scan_iterator_tuple_info(&iterator));

		if (drop_constraint)
			chunk_constraint_drop_constraint(ts_scan_iterator_tuple_info(&iterator));
	}
	return count;
}

int
ts_chunk_constraint_delete_by_constraint_name(int32 chunk_id, const char *constraint_name,
											  bool delete_metadata, bool drop_constraint)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	int count = 0;

	init_scan_by_chunk_id_constraint_name(&iterator, chunk_id, constraint_name);
	ts_scanner_foreach(&iterator)
	{
		count++;
		if (delete_metadata)
			chunk_constraint_delete_metadata(ts_scan_iterator_tuple_info(&iterator));

		if (drop_constraint)
			chunk_constraint_drop_constraint(ts_scan_iterator_tuple_info(&iterator));
	}
	return count;
}

/*
 * Delete all constraints for a chunk. Optionally, collect the deleted constraints.
 */
int
ts_chunk_constraint_delete_by_chunk_id(int32 chunk_id, ChunkConstraints *ccs)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	int count = 0;

	init_scan_by_chunk_id(&iterator, chunk_id);
	ts_scanner_foreach(&iterator)
	{
		count++;

		chunk_constraints_add_from_tuple(ccs, ts_scan_iterator_tuple_info(&iterator));
		chunk_constraint_delete_metadata(ts_scan_iterator_tuple_info(&iterator));
		chunk_constraint_drop_constraint(ts_scan_iterator_tuple_info(&iterator));
	}
	return count;
}

int
ts_chunk_constraint_delete_by_dimension_slice_id(int32 dimension_slice_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	int count = 0;

	init_scan_by_dimension_slice_id(&iterator, dimension_slice_id);
	ts_scanner_foreach(&iterator)
	{
		count++;

		chunk_constraint_delete_metadata(ts_scan_iterator_tuple_info(&iterator));
		chunk_constraint_drop_constraint(ts_scan_iterator_tuple_info(&iterator));
	}
	return count;
}

void
ts_chunk_constraint_recreate(ChunkConstraint *cc, Oid chunk_oid)
{
	ObjectAddress constrobj = {
		.classId = ConstraintRelationId,
		.objectId = get_relation_constraint_oid(chunk_oid, NameStr(cc->fd.constraint_name), false),
	};

	performDeletion(&constrobj, DROP_RESTRICT, 0);
	chunk_constraint_create_on_table(cc, chunk_oid);
}

static void
chunk_constraint_rename_on_chunk_table(int32 chunk_id, char *old_name, char *new_name)
{
	Chunk *chunk = ts_chunk_get_by_id(chunk_id, 0, true);
	RenameStmt rename = {
		.renameType = OBJECT_TABCONSTRAINT,
		.relation = makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), 0),
		.subname = old_name,
		.newname = new_name,
	};

	RenameConstraint(&rename);
}

static void
chunk_constraint_rename_hypertable_from_tuple(TupleInfo *ti, const char *newname)
{
	bool nulls[Natts_chunk_constraint];
	Datum values[Natts_chunk_constraint];
	bool repl[Natts_chunk_constraint] = { false };

	HeapTuple tuple;
	NameData new_hypertable_constraint_name;
	NameData new_chunk_constraint_name;
	Name old_chunk_constraint_name;
	int32 chunk_id;

	heap_deform_tuple(ti->tuple, ti->desc, values, nulls);

	chunk_id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_constraint_chunk_id)]);
	namestrcpy(&new_hypertable_constraint_name, newname);
	chunk_constraint_choose_name(&new_chunk_constraint_name, false, 0, newname, chunk_id);

	values[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)] =
		NameGetDatum(&new_hypertable_constraint_name);
	repl[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)] = true;
	old_chunk_constraint_name =
		DatumGetName(values[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)]);
	values[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)] =
		NameGetDatum(&new_chunk_constraint_name);
	repl[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)] = true;

	chunk_constraint_rename_on_chunk_table(chunk_id,
										   NameStr(*old_chunk_constraint_name),
										   NameStr(new_chunk_constraint_name));

	tuple = heap_modify_tuple(ti->tuple, ti->desc, values, nulls, repl);
	ts_catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);
}

int
ts_chunk_constraint_rename_hypertable_constraint(int32 chunk_id, const char *oldname,
												 const char *newname)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	int count = 0;

	init_scan_by_chunk_id(&iterator, chunk_id);
	ts_scanner_foreach(&iterator)
	{
		if (!hypertable_constraint_matches_tuple(ts_scan_iterator_tuple_info(&iterator), oldname))
			continue;

		count++;
		chunk_constraint_rename_hypertable_from_tuple(ts_scan_iterator_tuple_info(&iterator),
													  newname);
	}
	return count;
}

char *
ts_chunk_constraint_get_name_from_hypertable_constraint(Oid chunk_relid,
														const char *hypertable_constraint_name)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, 0, true);

	init_scan_by_chunk_id(&iterator, chunk->fd.id);
	ts_scanner_foreach(&iterator)
	{
		bool nulls[Natts_chunk_constraint];
		Datum values[Natts_chunk_constraint];

		if (!hypertable_constraint_matches_tuple(ts_scan_iterator_tuple_info(&iterator),
												 hypertable_constraint_name))
			continue;

		heap_deform_tuple(ts_scan_iterator_tuple(&iterator),
						  ts_scan_iterator_tupledesc(&iterator),
						  values,
						  nulls);
		ts_scan_iterator_close(&iterator);
		return NameStr(
			*DatumGetName(values[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)]));
	}
	return NULL;
}
