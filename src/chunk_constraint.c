/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/heapam.h>
#include <access/xact.h>
#include <catalog/dependency.h>
#include <catalog/heap.h>
#include <catalog/indexing.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_constraint.h>
#include <commands/tablecmds.h>
#include <nodes/makefuncs.h>
#include <storage/lockdefs.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "chunk.h"
#include "chunk_constraint.h"
#include "constraint.h"
#include "debug_assert.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "errors.h"
#include "export.h"
#include "foreign_key.h"
#include "hypercube.h"
#include "hypertable.h"
#include "partitioning.h"
#include "process_utility.h"
#include "ts_catalog/catalog.h"

/*
 * Build the deterministic "<chunk_id>_<parent>" chunk-side name so DROP/RENAME
 * paths can recompute it without consulting any metadata table.
 */
void
ts_chunk_constraint_choose_name(char *dst, int32 chunk_id, const char *hypertable_constraint_name)
{
	Assert(hypertable_constraint_name != NULL);

	snprintf(dst, NAMEDATALEN, "%d_%s", chunk_id, hypertable_constraint_name);
}

/*
 * Build the CHECK constraint name a dim CHECK uses on the chunk table.
 */
static inline void
dimension_constraint_choose_name(char *dst, int32 dimension_slice_id)
{
	snprintf(dst, NAMEDATALEN, "constraint_%d", dimension_slice_id);
}

/*
 * Create a dimensional CHECK constraint node for a partitioning dimension.
 */
Constraint *
ts_chunk_constraint_dimensional_create(const Dimension *dim, const DimensionSlice *slice,
									   const char *name)
{
	Constraint *constr = NULL;
	Node *dimdef;
	ColumnRef *colref;
	List *compexprs = NIL;
	Oid type;

	if (slice->fd.range_start == PG_INT64_MIN && slice->fd.range_end == PG_INT64_MAX)
	{
		return NULL;
	}

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
		dimdef = (Node *) makeFuncCall(funcname, list_make1(colref), COERCE_EXPLICIT_CALL, -1);

		if (IS_OPEN_DIMENSION(dim))
		{
			/* The dimension has a time function to compute the time value so
			 * need to convert the range values to the time type returned by
			 * the partitioning function. */
			type = partinfo->partfunc.rettype;
		}
		else
		{
			/* Closed dimension, just use the INT8 type */
			type = INT8OID;
		}
	}
	else
	{
		/* Must be open dimension, since no partitioning function */
		Assert(IS_OPEN_DIMENSION(dim));

		dimdef = (Node *) colref;
		type = dim->fd.column_type;
	}

	/*
	 * We are forcing ISO datestyle here to prevent parsing errors with
	 * certain timezone/datestyle combinations.
	 */
	int current_datestyle = DateStyle;
	DateStyle = USE_ISO_DATES;
	char *start_str = ts_internal_to_time_string(slice->fd.range_start, type);
	char *end_str = ts_internal_to_time_string(slice->fd.range_end, type);
	DateStyle = current_datestyle;

	/* Elide range constraint for +INF or -INF */
	if (slice->fd.range_start != PG_INT64_MIN)
	{
		A_Const *start_const = makeNode(A_Const);
		memcpy(&start_const->val, makeString(start_str), sizeof(start_const->val));
		start_const->location = -1;
		A_Expr *ge_expr = makeSimpleA_Expr(AEXPR_OP, ">=", dimdef, (Node *) start_const, -1);
		compexprs = lappend(compexprs, ge_expr);
	}

	if (slice->fd.range_end != PG_INT64_MAX)
	{
		A_Const *end_const = makeNode(A_Const);
		memcpy(&end_const->val, makeString(end_str), sizeof(end_const->val));
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
#if PG18_GE
	constr->is_enforced = true;
#endif

	Assert(list_length(compexprs) >= 1);

	if (list_length(compexprs) == 2)
	{
		constr->raw_expr = (Node *) makeBoolExpr(AND_EXPR, compexprs, -1);
	}
	else if (list_length(compexprs) == 1)
	{
		constr->raw_expr = linitial(compexprs);
	}

	return constr;
}

/*
 * Decide whether a hypertable-side constraint needs to be copied to a chunk.
 *
 * CHECK and NOT NULL propagate via PG inheritance, and FKs are handled
 * separately by the chunk inheritance code. Everything else (PK, UNIQUE,
 * EXCLUSION, TRIGGER) we have to copy ourselves.
 */
static bool
chunk_constraint_need_on_chunk(Form_pg_constraint conform)
{
	if (conform->contype == CONSTRAINT_CHECK ||
		conform->contype == CONSTRAINT_FOREIGN
#if PG18_GE
		/* Avoid NOT NULL constraints
		 * https://github.com/postgres/postgres/commit/b0e96f31
		 */
		|| conform->contype == CONSTRAINT_NOTNULL
#endif
	)
	{
		return false;
	}

	return true;
}

/*
 * Add a single non-dimensional chunk-side constraint by replicating the
 * hypertable's definition. Delegates to a PL/pgSQL helper that pulls the
 * constraint definition with pg_get_constraintdef and runs ALTER TABLE.
 */
static Oid
create_non_dimensional_constraint(int32 chunk_id, const char *chunk_constraint_name,
								  const char *hypertable_constraint_name, Oid chunk_oid)
{
	NameData chunk_name_data;
	NameData hypertable_name_data;
	CatalogSecurityContext sec_ctx;

	if (ConstraintNameIsUsed(CONSTRAINT_RELATION, chunk_oid, chunk_constraint_name))
	{
		return get_relation_constraint_oid(chunk_oid, chunk_constraint_name, true);
	}

	namestrcpy(&chunk_name_data, chunk_constraint_name);
	namestrcpy(&hypertable_name_data, hypertable_constraint_name);

	ts_process_utility_set_expect_chunk_modification(true);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	CatalogInternalCall3(DDL_ADD_CHUNK_CONSTRAINT,
						 Int32GetDatum(chunk_id),
						 NameGetDatum(&chunk_name_data),
						 NameGetDatum(&hypertable_name_data));
	ts_catalog_restore_user(&sec_ctx);
	ts_process_utility_set_expect_chunk_modification(false);

	return get_relation_constraint_oid(chunk_oid, chunk_constraint_name, true);
}

typedef struct InheritableContext
{
	const Hypertable *ht;
	const Chunk *chunk;
} InheritableContext;

/*
 * Callback invoked for each hypertable constraint. For non-dim constraints
 * that need replication on the chunk side, create them via the PL/pgSQL
 * helper.
 */
static ConstraintProcessStatus
inheritable_constraint_create(HeapTuple constraint_tuple, void *arg)
{
	InheritableContext *ctx = arg;
	Form_pg_constraint constraint = (Form_pg_constraint) GETSTRUCT(constraint_tuple);
	const char *hypertable_constraint_name;
	char chunk_constraint_name[NAMEDATALEN];
	Form_pg_constraint matching_const;
	Relation chunk_rel;

	if (ctx->chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
		return CONSTR_IGNORED;
	}

	if (!chunk_constraint_need_on_chunk(constraint))
	{
		return CONSTR_IGNORED;
	}

	hypertable_constraint_name = NameStr(constraint->conname);

	/* If the chunk already has an equivalent constraint, reuse it under its
	 * existing name. */
	chunk_rel = table_open(ctx->chunk->table_id, AccessShareLock);
	matching_const = ts_constraint_find_matching(constraint_tuple, chunk_rel);
	table_close(chunk_rel, NoLock);

	if (matching_const != NULL)
	{
		return CONSTR_PROCESSED;
	}

	ts_chunk_constraint_choose_name(chunk_constraint_name,
									ctx->chunk->fd.id,
									hypertable_constraint_name);
	create_non_dimensional_constraint(ctx->chunk->fd.id,
									  chunk_constraint_name,
									  hypertable_constraint_name,
									  ctx->chunk->table_id);
	return CONSTR_PROCESSED;
}

/*
 * Create all of a chunk's constraints (dim CHECKs + inheritable non-dim).
 */
void
ts_chunk_constraints_create(const Hypertable *ht, const Chunk *chunk)
{
	List *newconstrs = NIL;

	/* Dimensional CHECKs, one per cube slice. Foreign-table chunks (OSM
	 * chunks) get their CHECKs cloned separately via
	 * ts_chunk_clone_check_constraints, so we skip dim CHECKs here for
	 * those. Skip ones that already exist on the chunk table so the
	 * merge/recreate paths can call us without tripping a
	 * "constraint already exists" error. */
	bool skip_dim_checks = (chunk->relkind == RELKIND_FOREIGN_TABLE || IS_OSM_CHUNK(chunk));
	for (int i = 0; !skip_dim_checks && i < chunk->cube->num_slices; i++)
	{
		const DimensionSlice *slice = chunk->cube->slices[i];
		const Dimension *dim = ts_hyperspace_get_dimension_by_id(ht->space, slice->fd.dimension_id);
		char name[NAMEDATALEN];
		Constraint *constr;

		Assert(dim != NULL);
		dimension_constraint_choose_name(name, slice->fd.id);

		if (OidIsValid(get_relation_constraint_oid(chunk->table_id, name, true)))
		{
			continue;
		}

		constr = ts_chunk_constraint_dimensional_create(dim, slice, name);
		if (constr != NULL)
		{
			newconstrs = lappend(newconstrs, constr);
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

	/* Non-dim inheritable constraints (PK, UNIQUE, EXCLUSION, TRIGGER). */
	if (chunk->relkind != RELKIND_FOREIGN_TABLE)
	{
		InheritableContext ctx = {
			.ht = ht,
			.chunk = chunk,
		};
		ts_constraint_process(ht->main_table_relid, inheritable_constraint_create, &ctx);
	}
}

static ConstraintProcessStatus
clone_check_constraint(HeapTuple tuple, void *arg)
{
	Form_pg_constraint con = (Form_pg_constraint) GETSTRUCT(tuple);
	Oid chunk_relid = *(Oid *) arg;

	if (con->contype != CONSTRAINT_CHECK)
	{
		return CONSTR_IGNORED;
	}

	CatalogInternalCall2(DDL_CONSTRAINT_CLONE,
						 ObjectIdGetDatum(con->oid),
						 ObjectIdGetDatum(chunk_relid));
	return CONSTR_PROCESSED;
}

/*
 * Clone CHECK constraints from a hypertable onto a foreign-table chunk.
 *
 * Foreign tables do not inherit CHECK constraints automatically, so we
 * have to recreate the hypertable's CHECKs on the foreign chunk under
 * the same name before running ALTER TABLE ... INHERIT. PostgreSQL will
 * then merge the foreign chunk's CHECKs with the parent's and propagate
 * subsequent renames or drops via normal inheritance.
 */
void
ts_chunk_clone_check_constraints(Oid chunk_relid, Oid hypertable_oid)
{
	ts_process_utility_set_expect_chunk_modification(true);
	ts_constraint_process(hypertable_oid, clone_check_constraint, &chunk_relid);
	ts_process_utility_set_expect_chunk_modification(false);
}

/*
 * Propagate a freshly-added hypertable constraint to a single chunk.
 */
void
ts_chunk_constraint_create_on_chunk(const Hypertable *ht, const Chunk *chunk, Oid constraint_oid)
{
	HeapTuple tuple;
	Form_pg_constraint con;

	tuple = SearchSysCache1(CONSTROID, ObjectIdGetDatum(constraint_oid));

	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for constraint %u", constraint_oid);
	}

	con = (Form_pg_constraint) GETSTRUCT(tuple);

	if (chunk->relkind != RELKIND_FOREIGN_TABLE)
	{
		if (con->contype == CONSTRAINT_FOREIGN && !OidIsValid(con->conparentid))
		{
			ts_chunk_inherit_outbound_fk_by_oid(chunk, constraint_oid);
		}
		else if (chunk_constraint_need_on_chunk(con))
		{
			char chunk_constraint_name[NAMEDATALEN];

			ts_chunk_constraint_choose_name(chunk_constraint_name,
											chunk->fd.id,
											NameStr(con->conname));
			create_non_dimensional_constraint(chunk->fd.id,
											  chunk_constraint_name,
											  NameStr(con->conname),
											  chunk->table_id);
		}
	}

	ReleaseSysCache(tuple);
}

/*
 * Drop the chunk's dimensional CHECK constraints and recreate them. Used
 * after a slice's range has been adjusted (for instance during chunk merge).
 */
void
ts_chunk_constraints_recreate(const Hypertable *ht, const Chunk *chunk)
{
	for (int i = 0; i < chunk->cube->num_slices; i++)
	{
		const DimensionSlice *slice = chunk->cube->slices[i];
		char name[NAMEDATALEN];
		Oid constraint_oid;

		dimension_constraint_choose_name(name, slice->fd.id);
		constraint_oid = get_relation_constraint_oid(chunk->table_id, name, true);
		if (OidIsValid(constraint_oid))
		{
			ObjectAddress addr = {
				.classId = ConstraintRelationId,
				.objectId = constraint_oid,
			};
			performDeletion(&addr, DROP_RESTRICT, 0);
		}
	}

	ts_chunk_constraints_create(ht, chunk);
	ts_chunk_copy_referencing_fk(ht, chunk);
}

char *
ts_chunk_constraint_get_name_from_hypertable_constraint(Oid chunk_relid,
														const char *hypertable_constraint_name)
{
	/* CHECK and FK chunk-side constraints keep the parent's name. */
	if (OidIsValid(get_relation_constraint_oid(chunk_relid, hypertable_constraint_name, true)))
	{
		return pstrdup(hypertable_constraint_name);
	}

	/* Unique/PK/exclusion/trigger use the deterministic "<chunk_id>_<parent>"
	 * form. */
	int32 chunk_id =
		DatumGetInt32(DirectFunctionCall1(ts_chunk_id_from_relid, ObjectIdGetDatum(chunk_relid)));

	char chunk_con_name[NAMEDATALEN];
	ts_chunk_constraint_choose_name(chunk_con_name, chunk_id, hypertable_constraint_name);
	Oid chunk_con_oid = get_relation_constraint_oid(chunk_relid, chunk_con_name, true);
	if (OidIsValid(chunk_con_oid))
	{
		return pstrdup(chunk_con_name);
	}

	return NULL;
}

static void
check_chunk_constraint_violated(Oid chunk_relid, const Dimension *dim, const DimensionSlice *slice)
{
	Relation rel;
	TupleTableSlot *slot;
	TableScanDesc scandesc;
	bool isnull;
	int attno = get_attnum(chunk_relid, NameStr(dim->fd.column_name));
	Ensure(attno != InvalidAttrNumber, "invalid attribute number");

	PushActiveSnapshot(GetLatestSnapshot());
	rel = table_open(chunk_relid, AccessShareLock);
	scandesc = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	slot = table_slot_create(rel, NULL);

	while (table_scan_getnextslot(scandesc, ForwardScanDirection, slot))
	{
		Datum datum;
		int64 value;

		datum = slot_getattr(slot, attno, &isnull);
		Assert(!isnull);

		if (NULL != dim->partitioning)
		{
			Oid collation = TupleDescAttr(slot->tts_tupleDescriptor, AttrNumberGetAttrOffset(attno))
								->attcollation;
			datum = ts_partitioning_func_apply(dim->partitioning, collation, datum);
		}

		if (dim->type == DIMENSION_TYPE_OPEN)
		{
			value = ts_time_value_to_internal(datum, ts_dimension_get_partition_type(dim));
		}
		else if (dim->type == DIMENSION_TYPE_CLOSED)
		{
			value = (int64) DatumGetInt32(datum);
		}
		else
		{
			elog(ERROR, "invalid dimension type when checking constraint");
		}

		if (value < slice->fd.range_start || value >= slice->fd.range_end)
		{
			ereport(ERROR,
					(errcode(ERRCODE_CHECK_VIOLATION),
					 errmsg("dimension constraint for column \"%s\" violated by some row",
							NameStr(dim->fd.column_name))));
		}
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scandesc);
	table_close(rel, NoLock);
	PopActiveSnapshot();
}

/*
 * Check whether the chunk has any row that violates any of its dimensional constraints
 */
void
ts_chunk_constraint_check_violated(const Chunk *chunk, const Hyperspace *hs)
{
	for (int i = 0; i < chunk->cube->num_slices; i++)
	{
		const DimensionSlice *slice = chunk->cube->slices[i];
		const Dimension *dim = ts_hyperspace_get_dimension_by_id(hs, slice->fd.dimension_id);

		Assert(dim);
		check_chunk_constraint_violated(chunk->table_id, dim, slice);
	}
}
