/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/value.h>
#include <nodes/makefuncs.h>
#include <catalog/index.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <commands/defrem.h>
#include <commands/tablespace.h>
#include <fmgr.h>

#include "indexing.h"
#include "compat.h"
#include "dimension.h"
#include "errors.h"
#include "hypertable_cache.h"
#include "partitioning.h"

static bool
index_has_attribute(List *indexelems, const char *attrname)
{
	ListCell   *lc;

	foreach(lc, indexelems)
	{
		Node	   *node = lfirst(lc);
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
					List	   *pair = (List *) lfirst(lc);

					if (list_length(pair) == 2 &&
						IsA(linitial(pair), IndexElem) &&
						IsA(lsecond(pair), List))
					{
						colname = ((IndexElem *) linitial(pair))->name;
						break;
					}
				}
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
	int			i;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		Dimension  *dim = &hs->dimensions[i];

		if (!index_has_attribute(indexelems, NameStr(dim->fd.column_name)))
			ereport(ERROR,
					(errcode(ERRCODE_TS_BAD_HYPERTABLE_INDEX_DEFINITION),
					 errmsg("cannot create a unique index without the column \"%s\" (used in partitioning)",
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
	List	   *columns = NIL;
	int			i;

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
	IndexStmt	stmt = {
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
					  false,	/* is alter table */
					  false,	/* check rights */
					  false,	/* skip_build */
					  true);	/* quiet */
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
create_default_indexes(Hypertable *ht,
					   Dimension *time_dim,
					   Dimension *space_dim,
					   bool has_time_idx,
					   bool has_time_space_idx)
{
	IndexElem	telem = {
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
		IndexElem	selem = {
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
	Relation	tblrel = relation_open(ht->main_table_relid, AccessShareLock);
	Dimension  *time_dim = ts_hyperspace_get_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);
	Dimension  *space_dim = ts_hyperspace_get_dimension(ht->space, DIMENSION_TYPE_CLOSED, 0);
	List	   *indexlist = RelationGetIndexList(tblrel);
	bool		has_time_idx = false;
	bool		has_time_space_idx = false;
	ListCell   *lc;

	foreach(lc, indexlist)
	{
		Relation	idxrel = relation_open(lfirst_oid(lc), AccessShareLock);

		if (verify && (idxrel->rd_index->indisunique || idxrel->rd_index->indisexclusion))
			ts_indexing_verify_columns(ht->space, build_indexcolumn_list(idxrel));

		/* Check for existence of "default" indexes */
		if (create_default && NULL != time_dim)
		{
			Form_pg_attribute idxattr_time,
						idxattr_space;

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
						namestrcmp(&idxattr_space->attname, NameStr(space_dim->fd.column_name)) == 0 &&
						namestrcmp(&idxattr_time->attname, NameStr(time_dim->fd.column_name)) == 0)
						has_time_space_idx = true;
					break;
				default:
					break;
			}
		}
		relation_close(idxrel, AccessShareLock);
	}

	if (create_default)
		create_default_indexes(ht, time_dim, space_dim, has_time_idx, has_time_space_idx);

	relation_close(tblrel, AccessShareLock);
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
