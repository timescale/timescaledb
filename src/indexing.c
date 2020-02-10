/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <nodes/parsenodes.h>
#include <nodes/value.h>
#include <nodes/makefuncs.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <commands/event_trigger.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <parser/parse_utilcmd.h>
#include <fmgr.h>

#include "indexing.h"
#include "compat.h"
#include "dimension.h"
#include "errors.h"
#include "hypertable_cache.h"
#include "partitioning.h"

#if PG96 || PG10 /* PG11 consolidates pg_foo_fn.h -> pg_foo.h */
#include <catalog/pg_inherits_fn.h>
#else
#include <catalog/pg_inherits.h>
#endif
static bool
index_has_attribute(List *indexelems, const char *attrname)
{
	ListCell *lc;

	foreach (lc, indexelems)
	{
		Node *node = lfirst(lc);
		const char *colname = NULL;

		/*
		 * The type of the element varies depending on whether the list is
		 * from an index or a constraint
		 */
		switch (nodeTag(node))
		{
			case T_IndexElem:
				colname = ((IndexElem *) node)->name;
				break;
			case T_String:
				colname = strVal(node);
				break;
			case T_List:
			{
				List *pair = (List *) lfirst(lc);

				if (list_length(pair) == 2 && IsA(linitial(pair), IndexElem) &&
					IsA(lsecond(pair), List))
				{
					colname = ((IndexElem *) linitial(pair))->name;
					break;
				}
			}
			/* FALLTHROUGH */
			default:
				elog(ERROR, "unsupported index list element");
		}

		if (colname != NULL && strncmp(colname, attrname, NAMEDATALEN) == 0)
			return true;
	}

	return false;
}

/*
 * Verify that index columns cover all partitioning dimensions.
 *
 * A UNIQUE, PRIMARY KEY or EXCLUSION index on a chunk must cover all
 * partitioning dimensions to guarantee uniqueness (or exclusion) across the
 * entire hypertable. Therefore we check that all dimensions are present among
 * the index columns.
 */
void
ts_indexing_verify_columns(Hyperspace *hs, List *indexelems)
{
	int i;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		Dimension *dim = &hs->dimensions[i];

		if (!index_has_attribute(indexelems, NameStr(dim->fd.column_name)))
			ereport(ERROR,
					(errcode(ERRCODE_TS_BAD_HYPERTABLE_INDEX_DEFINITION),
					 errmsg("cannot create a unique index without the column \"%s\" (used in "
							"partitioning)",
							NameStr(dim->fd.column_name))));
	}
}

/*
 * Verify index columns.
 *
 * We only care about UNIQUE, PRIMARY KEY or EXCLUSION indexes.
 */
void
ts_indexing_verify_index(Hyperspace *hs, IndexStmt *stmt)
{
	if (stmt->unique || stmt->excludeOpNames != NULL)
		ts_indexing_verify_columns(hs, stmt->indexParams);
}

/*
 * Build a list of string Values representing column names that an index covers.
 */
static List *
build_indexcolumn_list(Relation idxrel)
{
	List *columns = NIL;
	int i;

	for (i = 0; i < idxrel->rd_att->natts; i++)
	{
		Form_pg_attribute idxattr = TupleDescAttr(idxrel->rd_att, i);

		columns = lappend(columns, makeString(NameStr(idxattr->attname)));
	}

	return columns;
}

static void
create_default_index(Hypertable *ht, List *indexelems)
{
	IndexStmt stmt = {
		.type = T_IndexStmt,
		.accessMethod = DEFAULT_INDEX_TYPE,
		.idxname = NULL,
		.relation = makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), 0),
		.tableSpace = get_tablespace_name(get_rel_tablespace(ht->main_table_relid)),
		.indexParams = indexelems,
	};

	DefineIndexCompat(ht->main_table_relid,
					  &stmt,
					  InvalidOid,
					  false, /* is alter table */
					  false, /* check rights */
					  false, /* skip_build */
					  true); /* quiet */
}

static Node *
get_open_dim_expr(Dimension *dim)
{
	if (dim == NULL || dim->partitioning == NULL)
		return NULL;

	return dim->partitioning->partfunc.func_fmgr.fn_expr;
}

static char *
get_open_dim_name(Dimension *dim)
{
	if (dim == NULL || dim->partitioning != NULL)
		return NULL;

	return NameStr(dim->fd.column_name);
}

static void
create_default_indexes(Hypertable *ht, Dimension *time_dim, Dimension *space_dim, bool has_time_idx,
					   bool has_time_space_idx)
{
	IndexElem telem = {
		.type = T_IndexElem,
		.name = get_open_dim_name(time_dim),
		.ordering = SORTBY_DESC,
		.expr = get_open_dim_expr(time_dim),
	};

	/* In case we'd allow tables that are only space partitioned */
	if (NULL == time_dim)
		return;

	/* Create ("time") index */
	if (!has_time_idx)
		create_default_index(ht, list_make1(&telem));

	/* Create ("space", "time") index */
	if (space_dim != NULL && !has_time_space_idx)
	{
		IndexElem selem = {
			.type = T_IndexElem,
			.name = NameStr(space_dim->fd.column_name),
			.ordering = SORTBY_ASC,
		};

		create_default_index(ht, list_make2(&selem, &telem));
	}
}

/*
 * Verify that unique, primary and exclusion indexes on a hypertable cover all
 * partitioning columns and create any default indexes.
 *
 * Default indexes are assumed to cover the first open ("time") dimension, and,
 * optionally, the first closed ("space") dimension.
 */
static void
indexing_create_and_verify_hypertable_indexes(Hypertable *ht, bool create_default, bool verify)
{
	Relation tblrel = table_open(ht->main_table_relid, AccessShareLock);
	Dimension *time_dim = ts_hyperspace_get_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);
	Dimension *space_dim = ts_hyperspace_get_dimension(ht->space, DIMENSION_TYPE_CLOSED, 0);
	List *indexlist = RelationGetIndexList(tblrel);
	bool has_time_idx = false;
	bool has_time_space_idx = false;
	ListCell *lc;

	foreach (lc, indexlist)
	{
		Relation idxrel = index_open(lfirst_oid(lc), AccessShareLock);

		if (verify && (idxrel->rd_index->indisunique || idxrel->rd_index->indisexclusion))
			ts_indexing_verify_columns(ht->space, build_indexcolumn_list(idxrel));

		/* Check for existence of "default" indexes */
		if (create_default && NULL != time_dim)
		{
			Form_pg_attribute idxattr_time, idxattr_space;

			switch (idxrel->rd_att->natts)
			{
				case 1:
					/* ("time") index */
					idxattr_time = TupleDescAttr(idxrel->rd_att, 0);
					if (namestrcmp(&idxattr_time->attname, NameStr(time_dim->fd.column_name)) == 0)
						has_time_idx = true;
					break;
				case 2:
					/* ("space", "time") index */
					idxattr_space = TupleDescAttr(idxrel->rd_att, 0);
					idxattr_time = TupleDescAttr(idxrel->rd_att, 1);
					if (space_dim != NULL &&
						namestrcmp(&idxattr_space->attname, NameStr(space_dim->fd.column_name)) ==
							0 &&
						namestrcmp(&idxattr_time->attname, NameStr(time_dim->fd.column_name)) == 0)
						has_time_space_idx = true;
					break;
				default:
					break;
			}
		}
		index_close(idxrel, AccessShareLock);
	}

	if (create_default)
		create_default_indexes(ht, time_dim, space_dim, has_time_idx, has_time_space_idx);

	table_close(tblrel, AccessShareLock);
}

/* create the index on the root table of a hypertable.
 * based on postgres CREATE INDEX
 * https://github.com/postgres/postgres/blob/ebfe20dc706bd3238a9bdf3b44cd8f82337e86a8/src/backend/tcop/utility.c#L1291-L1374
 * despite not allowing CONCURRENT index creation now, we expect to do so soon, so this code
 * retains those code paths
 */
extern ObjectAddress
ts_indexing_root_table_create_index(IndexStmt *stmt, const char *queryString,
									bool is_multitransaction)
{
	Oid relid;
	LOCKMODE lockmode;
	ObjectAddress root_table_address;

	if (stmt->concurrent)
		PreventInTransactionBlock(true, "CREATE INDEX CONCURRENTLY");

	/*
	 * Look up the relation OID just once, right here at the
	 * beginning, so that we don't end up repeating the name
	 * lookup later and latching onto a different relation
	 * partway through.  To avoid lock upgrade hazards, it's
	 * important that we take the strongest lock that will
	 * eventually be needed here, so the lockmode calculation
	 * needs to match what DefineIndex() does.
	 */
	lockmode = stmt->concurrent ? ShareUpdateExclusiveLock : ShareLock;
	relid = RangeVarGetRelidExtendedCompat(stmt->relation,
										   lockmode,
										   0,
										   RangeVarCallbackOwnsRelation,
										   NULL);

	/*
	 * single-transaction CREATE INDEX on a hypertable tables recurses to
	 * chunks, so we must acquire locks early to avoid deadlocks.
	 *
	 * We also take the opportunity to verify that all
	 * partitions are something we can put an index on, to
	 * avoid building some indexes only to fail later.
	 */
	if (!is_multitransaction)
	{
		ListCell *lc;
		List *inheritors = NIL;

		inheritors = find_all_inheritors(relid, lockmode, NULL);
		foreach (lc, inheritors)
		{
			char relkind = get_rel_relkind(lfirst_oid(lc));

			if (relkind != RELKIND_RELATION && relkind != RELKIND_MATVIEW)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_OBJECT_DEFINITION),
						 errmsg("cannot create index on partitioned table \"%s\"",
								stmt->relation->relname),
						 errdetail("Table \"%s\" contains partitions that are foreign tables.",
								   stmt->relation->relname)));
		}
		list_free(inheritors);
	}

	/* Run parse analysis ... */
	stmt = transformIndexStmt(relid, stmt, queryString);

	/* ... and do it */
	EventTriggerAlterTableStart((Node *) stmt);

	root_table_address = DefineIndexCompat(relid, /* OID of heap relation */
										   stmt,
										   InvalidOid, /* no predefined OID */
										   false,	  /* is_alter_table */
										   true,	   /* check_rights */
										   false,	  /* skip_build */
										   false);	 /* quiet */

	return root_table_address;
}

void
ts_indexing_verify_indexes(Hypertable *ht)
{
	indexing_create_and_verify_hypertable_indexes(ht, false, true);
}

void
ts_indexing_create_default_indexes(Hypertable *ht)
{
	indexing_create_and_verify_hypertable_indexes(ht, true, false);
}

TSDLLEXPORT Oid
ts_indexing_find_clustered_index(Oid table_relid)
{
	Relation rel;
	ListCell *index;
	Oid index_relid = InvalidOid;

	rel = table_open(table_relid, AccessShareLock);

	/* We need to find the index that has indisclustered set. */
	foreach (index, RelationGetIndexList(rel))
	{
		HeapTuple idxtuple;
		Form_pg_index indexForm;

		index_relid = lfirst_oid(index);
		idxtuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_relid));
		if (!HeapTupleIsValid(idxtuple))
			elog(ERROR,
				 "cache lookup failed for index %u when looking for a clustered index",
				 index_relid);
		indexForm = (Form_pg_index) GETSTRUCT(idxtuple);

		if (indexForm->indisclustered)
		{
			ReleaseSysCache(idxtuple);
			break;
		}
		ReleaseSysCache(idxtuple);
		index_relid = InvalidOid;
	}

	table_close(rel, AccessShareLock);

	return index_relid;
}

typedef enum IndexValidity
{
	IndexInvalid = 0,
	IndexValid,
} IndexValidity;

static bool
ts_indexing_mark_as(Oid index_id, IndexValidity validity)
{
	Relation pg_index;
	HeapTuple indexTuple;
	HeapTuple new_tuple;
	Form_pg_index indexForm;
	bool was_valid;

	/* Open pg_index and fetch a writable copy of the index's tuple */
	pg_index = table_open(IndexRelationId, RowExclusiveLock);

	indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(index_id));

	if (!HeapTupleIsValid(indexTuple))
		elog(ERROR, "cache lookup failed when marking index %u", index_id);

	new_tuple = heap_copytuple(indexTuple);
	indexForm = (Form_pg_index) GETSTRUCT(new_tuple);

	was_valid = indexForm->indisvalid;

	/* Perform the requested state change on the copy */
	switch (validity)
	{
		case IndexValid:
			Assert(indexForm->indislive);
			Assert(indexForm->indisready);
			indexForm->indisvalid = true;
			break;
		case IndexInvalid:
			indexForm->indisvalid = false;
			indexForm->indisclustered = false;
			break;
	}

	/* ... and write it back */
	CatalogTupleUpdate(pg_index, &indexTuple->t_self, new_tuple);

	table_close(pg_index, RowExclusiveLock);
	return was_valid;
}

void
ts_indexing_mark_as_valid(Oid index_id)
{
	ts_indexing_mark_as(index_id, IndexValid);
}

/* returns if the index was valid */
bool
ts_indexing_mark_as_invalid(Oid index_id)
{
	return ts_indexing_mark_as(index_id, IndexInvalid);
}
