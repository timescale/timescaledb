/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/heapam.h>
#include <access/xact.h>
#include <catalog/dependency.h>
#include <catalog/indexing.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_constraint.h>
#include <catalog/heap.h>
#include <commands/tablecmds.h>
#include <funcapi.h>
#include <nodes/makefuncs.h>
#include <storage/lockdefs.h>
#include <utils/builtins.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/palloc.h>
#include <utils/relcache.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "export.h"
#include "scanner.h"
#include "scan_iterator.h"
#include "chunk_constraint.h"
#include "chunk_index.h"
#include "constraint.h"
#include "dimension_vector.h"
#include "dimension_slice.h"
#include "hypercube.h"
#include "chunk.h"
#include "hypertable.h"
#include "errors.h"
#include "process_utility.h"
#include "partitioning.h"

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
ts_chunk_constraints_copy(ChunkConstraints *chunk_constraints)
{
	ChunkConstraints *copy = palloc(sizeof(ChunkConstraints));

	memcpy(copy, chunk_constraints, sizeof(ChunkConstraints));
	copy->constraints = palloc0(CHUNK_CONSTRAINTS_SIZE(chunk_constraints->capacity));
	memcpy(copy->constraints,
		   chunk_constraints->constraints,
		   CHUNK_CONSTRAINTS_SIZE(chunk_constraints->num_constraints));

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
	Assert(ccs->constraints); /* repalloc() does not work with NULL argument */
	ccs->constraints = repalloc(ccs->constraints, CHUNK_CONSTRAINTS_SIZE(new_capacity));
	MemoryContextSwitchTo(old);
}

static void
chunk_constraint_dimension_choose_name(Name dst, int32 dimension_slice_id)
{
	snprintf(NameStr(*dst), NAMEDATALEN, "constraint_%d", dimension_slice_id);
}

static void
chunk_constraint_choose_name(Name dst, const char *hypertable_constraint_name, int32 chunk_id)
{
	char constrname[NAMEDATALEN];
	CatalogSecurityContext sec_ctx;

	Assert(hypertable_constraint_name != NULL);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	snprintf(constrname,
			 NAMEDATALEN,
			 "%d_" INT64_FORMAT "_%s",
			 chunk_id,
			 ts_catalog_table_next_seq_id(ts_catalog_get(), CHUNK_CONSTRAINT),
			 hypertable_constraint_name);
	ts_catalog_restore_user(&sec_ctx);

	namestrcpy(dst, constrname);
}

ChunkConstraint *
ts_chunk_constraints_add(ChunkConstraints *ccs, int32 chunk_id, int32 dimension_slice_id,
						 const char *constraint_name, const char *hypertable_constraint_name)
{
	ChunkConstraint *cc;

	chunk_constraints_expand(ccs, ccs->num_constraints + 1);
	cc = &ccs->constraints[ccs->num_constraints++];
	cc->fd.chunk_id = chunk_id;
	cc->fd.dimension_slice_id = dimension_slice_id;

	if (NULL == constraint_name)
	{
		if (is_dimension_constraint(cc))
		{
			chunk_constraint_dimension_choose_name(&cc->fd.constraint_name,
												   cc->fd.dimension_slice_id);
			namestrcpy(&cc->fd.hypertable_constraint_name, "");
		}
		else
			chunk_constraint_choose_name(&cc->fd.constraint_name,
										 hypertable_constraint_name,
										 cc->fd.chunk_id);
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
chunk_constraint_fill_tuple_values(const ChunkConstraint *cc, Datum values[Natts_chunk_constraint],
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
chunk_constraint_insert_relation(const Relation rel, const ChunkConstraint *cc)
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
ts_chunk_constraints_insert_metadata(const ChunkConstraints *ccs)
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
void
ts_chunk_constraint_insert(ChunkConstraint *constraint)
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

ChunkConstraint *
ts_chunk_constraints_add_from_tuple(ChunkConstraints *ccs, const TupleInfo *ti)
{
	bool nulls[Natts_chunk_constraint];
	Datum values[Natts_chunk_constraint];
	ChunkConstraint *constraints;
	int32 dimension_slice_id;
	Name constraint_name;
	Name hypertable_constraint_name;
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	MemoryContext oldcxt;

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	oldcxt = MemoryContextSwitchTo(ccs->mctx);

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

	constraints = ts_chunk_constraints_add(ccs,
										   DatumGetInt32(values[AttrNumberGetAttrOffset(
											   Anum_chunk_constraint_chunk_id)]),
										   dimension_slice_id,
										   NameStr(*constraint_name),
										   NameStr(*hypertable_constraint_name));

	MemoryContextSwitchTo(oldcxt);

	if (should_free)
		heap_freetuple(tuple);

	return constraints;
}

/*
 * Create a dimensional CHECK constraint for a partitioning dimension.
 */
static Constraint *
create_dimension_check_constraint(const Dimension *dim, const DimensionSlice *slice,
								  const char *name)
{
	Constraint *constr = NULL;
	bool isvarlena;
	Node *dimdef;
	ColumnRef *colref;
	Datum startdat, enddat;
	List *compexprs = NIL;
	Oid outfuncid;

	if (slice->fd.range_start == PG_INT64_MIN && slice->fd.range_end == PG_INT64_MAX)
		return NULL;

	colref = makeNode(ColumnRef);
	colref->fields = list_make1(makeString(pstrdup(NameStr(dim->fd.column_name))));
	colref->location = -1;

	/* Convert the dimensional ranges to the appropriate text/string
	 * representation for the time type. For dimensions with a
	 * partitioning/time function, use the function's output type. */
	if (dim->partitioning != NULL)
	{
		/* Both open and closed dimensions can have a partitioning function */
		PartitioningInfo *partinfo = dim->partitioning;
		List *funcname = list_make2(makeString(NameStr(partinfo->partfunc.schema)),
									makeString(NameStr(partinfo->partfunc.name)));
		dimdef = (Node *) makeFuncCall(funcname,
									   list_make1(colref),
#if PG14_GE
									   COERCE_EXPLICIT_CALL,
#endif
									   -1);

		if (IS_OPEN_DIMENSION(dim))
		{
			/* The dimension has a time function to compute the time value so
			 * need to convert the range values to the time type returned by
			 * the partitioning function. */
			getTypeOutputInfo(partinfo->partfunc.rettype, &outfuncid, &isvarlena);
			startdat = ts_internal_to_time_value(slice->fd.range_start, partinfo->partfunc.rettype);
			enddat = ts_internal_to_time_value(slice->fd.range_end, partinfo->partfunc.rettype);
		}
		else
		{
			/* Closed dimension, just use the integer output function */
			getTypeOutputInfo(INT8OID, &outfuncid, &isvarlena);
			startdat = Int64GetDatum(slice->fd.range_start);
			enddat = Int64GetDatum(slice->fd.range_end);
		}
	}
	else
	{
		/* Must be open dimension, since no partitioning function */
		Assert(IS_OPEN_DIMENSION(dim));

		dimdef = (Node *) colref;
		getTypeOutputInfo(dim->fd.column_type, &outfuncid, &isvarlena);
		startdat = ts_internal_to_time_value(slice->fd.range_start, dim->fd.column_type);
		enddat = ts_internal_to_time_value(slice->fd.range_end, dim->fd.column_type);
	}

	/* Convert internal format datums to string (output) datums */
	startdat = OidFunctionCall1(outfuncid, startdat);
	enddat = OidFunctionCall1(outfuncid, enddat);

	/* Elide range constraint for +INF or -INF */
	if (slice->fd.range_start != PG_INT64_MIN)
	{
		A_Const *start_const = makeNode(A_Const);
		memcpy(&start_const->val, makeString(DatumGetCString(startdat)), sizeof(start_const->val));
		start_const->location = -1;
		A_Expr *ge_expr = makeSimpleA_Expr(AEXPR_OP, ">=", dimdef, (Node *) start_const, -1);
		compexprs = lappend(compexprs, ge_expr);
	}

	if (slice->fd.range_end != PG_INT64_MAX)
	{
		A_Const *end_const = makeNode(A_Const);
		memcpy(&end_const->val, makeString(DatumGetCString(enddat)), sizeof(end_const->val));
		end_const->location = -1;
		A_Expr *lt_expr = makeSimpleA_Expr(AEXPR_OP, "<", dimdef, (Node *) end_const, -1);
		compexprs = lappend(compexprs, lt_expr);
	}

	constr = makeNode(Constraint);
	constr->contype = CONSTR_CHECK;
	constr->conname = name ? pstrdup(name) : NULL;
	constr->deferrable = false;
	constr->skip_validation = true;
	constr->initially_valid = true;

	Assert(list_length(compexprs) >= 1);

	if (list_length(compexprs) == 2)
		constr->raw_expr = (Node *) makeBoolExpr(AND_EXPR, compexprs, -1);
	else if (list_length(compexprs) == 1)
		constr->raw_expr = linitial(compexprs);

	return constr;
}

/*
 * Add a constraint to a chunk table.
 */
static Oid
chunk_constraint_create_on_table(const ChunkConstraint *cc, Oid chunk_oid)
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
	heap_freetuple(tuple);

	return get_relation_constraint_oid(chunk_oid, NameStr(cc->fd.constraint_name), true);
}

/*
 * Create a non-dimensional constraint on a chunk table (foreign key, trigger
 * constraint, etc.), including adding relevant metadata to the catalog.
 */
static Oid
create_non_dimensional_constraint(const ChunkConstraint *cc, Oid chunk_oid, int32 chunk_id,
								  Oid hypertable_oid, int32 hypertable_id)
{
	Oid chunk_constraint_oid;

	Assert(!is_dimension_constraint(cc));

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

	return chunk_constraint_oid;
}

static const DimensionSlice *
get_slice_with_id(const Hypercube *cube, int32 id)
{
	int i;

	for (i = 0; i < cube->num_slices; i++)
	{
		const DimensionSlice *slice = cube->slices[i];

		if (slice->fd.id == id)
			return slice;
	}

	return NULL;
}

/*
 * Create a set of constraints on a chunk table.
 */
void
ts_chunk_constraints_create(const Hypertable *ht, const Chunk *chunk)
{
	const ChunkConstraints *ccs = chunk->constraints;
	List *newconstrs = NIL;
	int i;

	for (i = 0; i < ccs->num_constraints; i++)
	{
		const ChunkConstraint *cc = &ccs->constraints[i];

		if (is_dimension_constraint(cc))
		{
			const DimensionSlice *slice = get_slice_with_id(chunk->cube, cc->fd.dimension_slice_id);
			const Dimension *dim;
			Constraint *constr;

			dim = ts_hyperspace_get_dimension_by_id(ht->space, slice->fd.dimension_id);
			Assert(dim);
			constr = create_dimension_check_constraint(dim, slice, NameStr(cc->fd.constraint_name));

			/* In some cases, a CHECK constraint is not needed. For instance,
			 * if the range is -INF to +INF. */
			if (constr != NULL)
				newconstrs = lappend(newconstrs, constr);
		}
		else
		{
			create_non_dimensional_constraint(cc,
											  chunk->table_id,
											  chunk->fd.id,
											  ht->main_table_relid,
											  ht->fd.id);
		}
	}

	if (newconstrs != NIL)
	{
		List PG_USED_FOR_ASSERTS_ONLY *cookedconstrs = NIL;
		Relation rel = table_open(chunk->table_id, AccessExclusiveLock);
		cookedconstrs = AddRelationNewConstraints(rel,
												  NIL /* List *newColDefaults */,
												  newconstrs,
												  false /* allow_merge */,
												  true /* is_local */,
												  false /* is_internal */,
												  NULL /* query string */);
		table_close(rel, NoLock);
		Assert(list_length(cookedconstrs) == list_length(newconstrs));
		CommandCounterIncrement();
	}
}

ScanIterator
ts_chunk_constraint_scan_iterator_create(MemoryContext result_mcxt)
{
	ScanIterator it = ts_scan_iterator_create(CHUNK_CONSTRAINT, AccessShareLock, result_mcxt);
	it.ctx.flags |= SCANNER_F_NOEND_AND_NOCLOSE;

	return it;
}

void
ts_chunk_constraint_scan_iterator_set_slice_id(ScanIterator *it, int32 slice_id)
{
	it->ctx.index = catalog_get_index(ts_catalog_get(),
									  CHUNK_CONSTRAINT,
									  CHUNK_CONSTRAINT_DIMENSION_SLICE_ID_IDX);
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(it,
								   Anum_chunk_constraint_dimension_slice_id_idx_dimension_slice_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(slice_id));
}

void
ts_chunk_constraint_scan_iterator_set_chunk_id(ScanIterator *it, int32 chunk_id)
{
	it->ctx.index = catalog_get_index(ts_catalog_get(),
									  CHUNK_CONSTRAINT,
									  CHUNK_CONSTRAINT_CHUNK_ID_CONSTRAINT_NAME_IDX);
	ts_scan_iterator_scan_key_reset(it);
	ts_scan_iterator_scan_key_init(it,
								   Anum_chunk_constraint_chunk_id_constraint_name_idx_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(chunk_id));
}

static void
init_scan_by_chunk_id_constraint_name(ScanIterator *iterator, int32 chunk_id,
									  const char *constraint_name)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CHUNK_CONSTRAINT,
											CHUNK_CONSTRAINT_CHUNK_ID_CONSTRAINT_NAME_IDX);

	ts_scan_iterator_scan_key_reset(iterator);
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
		CStringGetDatum(constraint_name));
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

	ts_chunk_constraint_scan_iterator_set_chunk_id(&iterator, chunk_id);
	ts_scanner_foreach(&iterator)
	{
		num_found++;
		ts_chunk_constraints_add_from_tuple(constraints, ts_scan_iterator_tuple_info(&iterator));
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
ts_chunk_constraint_scan_by_dimension_slice(const DimensionSlice *slice, ChunkScanCtx *ctx,
											MemoryContext mctx)
{
	ScanIterator iterator = ts_scan_iterator_create(CHUNK_CONSTRAINT, AccessShareLock, mctx);
	int count = 0;

	ts_chunk_constraint_scan_iterator_set_slice_id(&iterator, slice->fd.id);

	ts_scanner_foreach(&iterator)
	{
		const Hyperspace *hs = ctx->ht->space;
		ChunkStub *stub;
		ChunkScanEntry *entry;
		bool found;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		Datum datum = slot_getattr(ti->slot, Anum_chunk_constraint_chunk_id, &found);
		int32 chunk_id = DatumGetInt32(datum);

		if (slot_attisnull(ts_scan_iterator_slot(&iterator),
						   Anum_chunk_constraint_dimension_slice_id))
			continue;

		count++;

		Assert(!slot_attisnull(ti->slot, Anum_chunk_constraint_dimension_slice_id));

		entry = hash_search(ctx->htab, &chunk_id, HASH_ENTER, &found);

		if (!found)
		{
			stub = ts_chunk_stub_create(chunk_id, hs->num_dimensions);
			stub->cube = ts_hypercube_alloc(hs->num_dimensions);
			entry->stub = stub;
		}
		else
			stub = entry->stub;

		ts_chunk_constraints_add_from_tuple(stub->constraints, ti);

		ts_hypercube_add_slice(stub->cube, slice);

		/* A stub is complete when we've added slices for all its dimensions,
		 * i.e., a complete hypercube */
		if (chunk_stub_is_complete(stub, ctx->ht->space))
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
ts_chunk_constraint_scan_by_dimension_slice_to_list(const DimensionSlice *slice, List **list,
													MemoryContext mctx)
{
	ScanIterator iterator = ts_scan_iterator_create(CHUNK_CONSTRAINT, AccessShareLock, mctx);
	int count = 0;

	ts_chunk_constraint_scan_iterator_set_slice_id(&iterator, slice->fd.id);

	ts_scanner_foreach(&iterator)
	{
		bool is_null;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		Datum chunk_id;

		if (slot_attisnull(ti->slot, Anum_chunk_constraint_dimension_slice_id))
			continue;

		count++;
		chunk_id = slot_getattr(ti->slot, Anum_chunk_constraint_chunk_id, &is_null);
		Assert(!is_null);
		*list = lappend_int(*list, DatumGetInt32(chunk_id));
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

	ts_chunk_constraint_scan_iterator_set_slice_id(&iterator, dimension_slice_id);

	ts_scanner_foreach(&iterator)
	{
		if (slot_attisnull(ts_scan_iterator_slot(&iterator),
						   Anum_chunk_constraint_dimension_slice_id))
			continue;

		count++;
		if (ccs != NULL)
			ts_chunk_constraints_add_from_tuple(ccs, ts_scan_iterator_tuple_info(&iterator));
	}
	return count;
}

static bool
chunk_constraint_need_on_chunk(const char chunk_relkind, Form_pg_constraint conform)
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
	/*
	   Check if the foreign key constraint references a partition in a partitioned
	   table. In that case, we shouldn't include this constraint as we will end up
	   checking the foreign key constraint once for every partition, which obviously
	   leads to foreign key constraint violation. Instead, we only include constraints
	   referencing the parent table of the partitioned table.
	*/
	if (conform->contype == CONSTRAINT_FOREIGN && OidIsValid(conform->conparentid))
		return false;

	/* Foreign tables do not support non-check constraints, so skip them */
	if (chunk_relkind == RELKIND_FOREIGN_TABLE)
		return false;

	return true;
}

static bool
chunk_constraint_is_check(const char chunk_relkind, Form_pg_constraint conform)
{
	if (conform->contype == CONSTRAINT_CHECK)
	{
		/*
		 * check constraints supported on foreign tables (like OSM chunks)
		 */
		return true;
	}
	return false;
}

int
ts_chunk_constraints_add_dimension_constraints(ChunkConstraints *ccs, int32 chunk_id,
											   const Hypercube *cube)
{
	int i;

	for (i = 0; i < cube->num_slices; i++)
		ts_chunk_constraints_add(ccs, chunk_id, cube->slices[i]->fd.id, NULL, NULL);

	return cube->num_slices;
}

typedef struct ConstraintContext
{
	int num_added;
	char chunk_relkind;
	ChunkConstraints *ccs;
	int32 chunk_id;
} ConstraintContext;

static ConstraintProcessStatus
chunk_constraint_add(HeapTuple constraint_tuple, void *arg)
{
	ConstraintContext *cc = arg;
	Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(constraint_tuple);

	if (chunk_constraint_need_on_chunk(cc->chunk_relkind, constraint))
	{
		ts_chunk_constraints_add(cc->ccs, cc->chunk_id, 0, NULL, NameStr(constraint->conname));
		return CONSTR_PROCESSED;
	}

	return CONSTR_IGNORED;
}

int
ts_chunk_constraints_add_inheritable_constraints(ChunkConstraints *ccs, int32 chunk_id,
												 const char chunk_relkind, Oid hypertable_oid)
{
	ConstraintContext cc = {
		.chunk_relkind = chunk_relkind,
		.ccs = ccs,
		.chunk_id = chunk_id,
	};

	return ts_constraint_process(hypertable_oid, chunk_constraint_add, &cc);
}

/* check constraints have the same name as the one on the hypertable */
static ConstraintProcessStatus
chunk_constraint_add_check(HeapTuple constraint_tuple, void *arg)
{
	ConstraintContext *cc = arg;
	Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(constraint_tuple);

	if (chunk_constraint_is_check(cc->chunk_relkind, constraint))
	{
		ts_chunk_constraints_add(cc->ccs,
								 cc->chunk_id,
								 0,
								 NameStr(constraint->conname),
								 NameStr(constraint->conname));
		return CONSTR_PROCESSED;
	}

	return CONSTR_IGNORED;
}

/* Adds only inheritable check constraints */
int
ts_chunk_constraints_add_inheritable_check_constraints(ChunkConstraints *ccs, int32 chunk_id,
													   const char chunk_relkind, Oid hypertable_oid)
{
	ConstraintContext cc = {
		.chunk_relkind = chunk_relkind,
		.ccs = ccs,
		.chunk_id = chunk_id,
	};
	return ts_constraint_process(hypertable_oid, chunk_constraint_add_check, &cc);
}

void
ts_chunk_constraint_create_on_chunk(const Hypertable *ht, const Chunk *chunk, Oid constraint_oid)
{
	HeapTuple tuple;
	Form_pg_constraint con;

	tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraint_oid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for constraint %u", constraint_oid);

	con = (Form_pg_constraint) GETSTRUCT(tuple);

	if (chunk_constraint_need_on_chunk(chunk->relkind, con))
	{
		ChunkConstraint *cc = ts_chunk_constraints_add(chunk->constraints,
													   chunk->fd.id,
													   0,
													   NULL,
													   NameStr(con->conname));

		ts_chunk_constraint_insert(cc);
		create_non_dimensional_constraint(cc,
										  chunk->table_id,
										  chunk->fd.id,
										  ht->main_table_relid,
										  ht->fd.id);
	}

	ReleaseSysCache(tuple);
}

static bool
hypertable_constraint_matches_tuple(TupleInfo *ti, const char *hypertable_constraint_name)
{
	bool isnull;
	Datum name = slot_getattr(ti->slot, Anum_chunk_constraint_hypertable_constraint_name, &isnull);

	return !isnull && namestrcmp(DatumGetName(name), hypertable_constraint_name) == 0;
}

static void
chunk_constraint_delete_metadata(TupleInfo *ti)
{
	bool isnull;
	Datum constrname = slot_getattr(ti->slot, Anum_chunk_constraint_constraint_name, &isnull);
	int32 chunk_id = DatumGetInt32(slot_getattr(ti->slot, Anum_chunk_constraint_chunk_id, &isnull));
	/* Get the chunk relid. Note that, at this point, the chunk table can be
	 * deleted already */
	Oid chunk_relid = ts_chunk_get_relid(chunk_id, true);

	if (OidIsValid(chunk_relid))
	{
		Oid index_relid = get_constraint_index(
			get_relation_constraint_oid(chunk_relid, NameStr(*DatumGetName(constrname)), true));

		/*
		 * If this is an index constraint, we need to cleanup the index
		 * metadata. Don't drop the index though, since that will happend when
		 * the constraint is dropped.
		 */
		if (OidIsValid(index_relid))
			ts_chunk_index_delete(chunk_id, get_rel_name(index_relid), false);
	}

	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
}

static void
chunk_constraint_drop_constraint(TupleInfo *ti)
{
	bool isnull;
	Datum constrname = slot_getattr(ti->slot, Anum_chunk_constraint_constraint_name, &isnull);
	int32 chunk_id = DatumGetInt32(slot_getattr(ti->slot, Anum_chunk_constraint_chunk_id, &isnull));
	/* Get the chunk relid. Note that, at this point, the chunk table can be
	 * deleted already. */
	Oid chunk_relid = ts_chunk_get_relid(chunk_id, true);

	if (OidIsValid(chunk_relid))
	{
		ObjectAddress constrobj = {
			.classId = ConstraintRelationId,
			.objectId =
				get_relation_constraint_oid(chunk_relid, NameStr(*DatumGetName(constrname)), true),
		};

		if (OidIsValid(constrobj.objectId))
			performDeletion(&constrobj, DROP_RESTRICT, 0);
	}
}

int
ts_chunk_constraint_delete_by_hypertable_constraint_name(int32 chunk_id,
														 const char *hypertable_constraint_name,
														 bool delete_metadata, bool drop_constraint)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	int count = 0;

	ts_chunk_constraint_scan_iterator_set_chunk_id(&iterator, chunk_id);

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

	ts_chunk_constraint_scan_iterator_set_chunk_id(&iterator, chunk_id);

	ts_scanner_foreach(&iterator)
	{
		count++;

		ts_chunk_constraints_add_from_tuple(ccs, ts_scan_iterator_tuple_info(&iterator));
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

	ts_chunk_constraint_scan_iterator_set_slice_id(&iterator, dimension_slice_id);

	ts_scanner_foreach(&iterator)
	{
		count++;

		chunk_constraint_delete_metadata(ts_scan_iterator_tuple_info(&iterator));
		chunk_constraint_drop_constraint(ts_scan_iterator_tuple_info(&iterator));
	}
	return count;
}

void
ts_chunk_constraints_recreate(const Hypertable *ht, const Chunk *chunk)
{
	const ChunkConstraints *ccs = chunk->constraints;
	int i;

	for (i = 0; i < ccs->num_constraints; i++)
	{
		const ChunkConstraint *cc = &ccs->constraints[i];
		ObjectAddress constrobj = {
			.classId = ConstraintRelationId,
			.objectId = get_relation_constraint_oid(chunk->table_id,
													NameStr(cc->fd.constraint_name),
													false),
		};

		performDeletion(&constrobj, DROP_RESTRICT, 0);
	}

	ts_chunk_constraints_create(ht, chunk);
}

static void
chunk_constraint_rename_on_chunk_table(int32 chunk_id, const char *old_name, const char *new_name)
{
	Oid chunk_relid = ts_chunk_get_relid(chunk_id, false);
	Oid nspid = get_rel_namespace(chunk_relid);
	RenameStmt rename = {
		.renameType = OBJECT_TABCONSTRAINT,
		.relation = makeRangeVar(get_namespace_name(nspid), get_rel_name(chunk_relid), 0),
		.subname = pstrdup(old_name),
		.newname = pstrdup(new_name),
	};

	RenameConstraint(&rename);
}

static void
chunk_constraint_rename_hypertable_from_tuple(TupleInfo *ti, const char *new_name)
{
	bool nulls[Natts_chunk_constraint];
	Datum values[Natts_chunk_constraint];
	bool doReplace[Natts_chunk_constraint] = { false };
	HeapTuple tuple, new_tuple;
	TupleDesc tupdesc = ts_scanner_get_tupledesc(ti);
	NameData new_hypertable_constraint_name;
	NameData new_chunk_constraint_name;
	Name old_chunk_constraint_name;
	int32 chunk_id;
	bool should_free;

	tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	heap_deform_tuple(tuple, tupdesc, values, nulls);

	chunk_id = DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_chunk_constraint_chunk_id)]);
	namestrcpy(&new_hypertable_constraint_name, new_name);
	chunk_constraint_choose_name(&new_chunk_constraint_name, new_name, chunk_id);

	values[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)] =
		NameGetDatum(&new_hypertable_constraint_name);
	doReplace[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)] = true;
	old_chunk_constraint_name =
		DatumGetName(values[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)]);
	values[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)] =
		NameGetDatum(&new_chunk_constraint_name);
	doReplace[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)] = true;

	chunk_constraint_rename_on_chunk_table(chunk_id,
										   NameStr(*old_chunk_constraint_name),
										   NameStr(new_chunk_constraint_name));

	new_tuple = heap_modify_tuple(tuple, tupdesc, values, nulls, doReplace);

	ts_chunk_index_adjust_meta(chunk_id,
							   new_name,
							   NameStr(*old_chunk_constraint_name),
							   NameStr(new_chunk_constraint_name));

	ts_catalog_update(ti->scanrel, new_tuple);
	heap_freetuple(new_tuple);

	if (should_free)
		heap_freetuple(tuple);
}

/*
 * Adjust internal metadata after index/constraint rename
 */
int
ts_chunk_constraint_adjust_meta(int32 chunk_id, const char *ht_constraint_name,
								const char *old_name, const char *new_name)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	int count = 0;

	init_scan_by_chunk_id_constraint_name(&iterator, chunk_id, old_name);

	ts_scanner_foreach(&iterator)
	{
		bool nulls[Natts_chunk_constraint];
		bool doReplace[Natts_chunk_constraint] = { false };
		Datum values[Natts_chunk_constraint];
		bool should_free;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		HeapTuple new_tuple;

		heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

		values[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)] =
			CStringGetDatum(ht_constraint_name);
		doReplace[AttrNumberGetAttrOffset(Anum_chunk_constraint_hypertable_constraint_name)] = true;
		values[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)] =
			CStringGetDatum(new_name);
		doReplace[AttrNumberGetAttrOffset(Anum_chunk_constraint_constraint_name)] = true;

		new_tuple =
			heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls, doReplace);

		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		if (should_free)
			heap_freetuple(tuple);

		count++;
	}

	return count;
}

bool
ts_chunk_constraint_update_slice_id(int32 chunk_id, int32 old_slice_id, int32 new_slice_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);

	ts_chunk_constraint_scan_iterator_set_slice_id(&iterator, old_slice_id);

	ts_scanner_foreach(&iterator)
	{
		bool replIsnull[Natts_chunk_constraint];
		bool repl[Natts_chunk_constraint] = { false };
		Datum values[Natts_chunk_constraint];
		bool should_free, isnull;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		int32 current_chunk_id =
			DatumGetInt32(slot_getattr(ti->slot, Anum_chunk_constraint_chunk_id, &isnull));

		if (isnull || current_chunk_id != chunk_id)
			continue;

		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		HeapTuple new_tuple;

		heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull);

		values[AttrNumberGetAttrOffset(Anum_chunk_constraint_dimension_slice_id)] =
			Int32GetDatum(new_slice_id);
		repl[AttrNumberGetAttrOffset(Anum_chunk_constraint_dimension_slice_id)] = true;

		new_tuple =
			heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull, repl);

		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		if (should_free)
			heap_freetuple(tuple);

		ts_scan_iterator_close(&iterator);
		return true;
	}

	return false;
}

int
ts_chunk_constraint_rename_hypertable_constraint(int32 chunk_id, const char *old_name,
												 const char *new_name)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	int count = 0;

	ts_chunk_constraint_scan_iterator_set_chunk_id(&iterator, chunk_id);

	ts_scanner_foreach(&iterator)
	{
		if (!hypertable_constraint_matches_tuple(ts_scan_iterator_tuple_info(&iterator), old_name))
			continue;

		count++;
		chunk_constraint_rename_hypertable_from_tuple(ts_scan_iterator_tuple_info(&iterator),
													  new_name);
	}
	return count;
}

char *
ts_chunk_constraint_get_name_from_hypertable_constraint(Oid chunk_relid,
														const char *hypertable_constraint_name)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CHUNK_CONSTRAINT, RowExclusiveLock, CurrentMemoryContext);
	Datum chunk_id = DirectFunctionCall1(ts_chunk_id_from_relid, ObjectIdGetDatum(chunk_relid));

	ts_chunk_constraint_scan_iterator_set_chunk_id(&iterator, DatumGetInt32(chunk_id));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		MemoryContext oldmctx;
		bool isnull;
		Datum datum;
		char *name;

		if (!hypertable_constraint_matches_tuple(ti, hypertable_constraint_name))
			continue;

		datum = slot_getattr(ti->slot, Anum_chunk_constraint_constraint_name, &isnull);
		Assert(!isnull);

		oldmctx = MemoryContextSwitchTo(ti->mctx);
		name = pstrdup(NameStr(*DatumGetName(datum)));
		MemoryContextSwitchTo(oldmctx);
		ts_scan_iterator_close(&iterator);

		return name;
	}
	return NULL;
}
