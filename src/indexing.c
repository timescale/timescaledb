#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/value.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <fmgr.h>

#include "indexing.h"
#include "compat.h"
#include "dimension.h"
#include "errors.h"
#include "hypertable_cache.h"

/*
 * Verify that index columns cover all partitioning dimensions.
 *
 * A UNIQUE, PRIMARY KEY or EXCLUSION index on a chunk must cover all
 * partitioning dimensions to guarantee uniqueness (or exclusion) across the
 * entire hypertable. Therefore we check that all dimensions are present among
 * the index columns.
 */
void
indexing_verify_columns(Hyperspace *hs, List *indexelems)
{
	int			i;

	for (i = 0; i < hs->num_dimensions; i++)
	{
		Dimension  *dim = &hs->dimensions[i];
		bool		found = false;
		ListCell   *lc;

		foreach(lc, indexelems)
		{
			Node	   *node = lfirst(lc);
			const char *colname;

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
					elog(ERROR, "Unsupported index list element");
			}

			if (namestrcmp(&dim->fd.column_name, colname) == 0)
			{
				found = true;
				break;
			}
		}

		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_IO_BAD_HYPERTABLE_INDEX_DEFINITION),
					 errmsg("Cannot create a unique index without the column: %s (used in partitioning)",
							NameStr(dim->fd.column_name))));
	}
}

/*
 * Verify index columns.
 *
 * We only care about UNIQUE, PRIMARY KEY or EXCLUSION indexes.
 */
void
indexing_verify_index(Hyperspace *hs, IndexStmt *stmt)
{
	if (stmt->unique || stmt->excludeOpNames != NULL)
		indexing_verify_columns(hs, stmt->indexParams);
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
		columns = lappend(columns, makeString(NameStr(idxrel->rd_att->attrs[i]->attname)));

	return columns;
}

TS_FUNCTION_INFO_V1(indexing_verify_hypertable_indexes);

/*
 * Verify that unique, primary and exclusion indexes on a hypertable cover all
 * partitioning columns.
 */
Datum
indexing_verify_hypertable_indexes(PG_FUNCTION_ARGS)
{
	Oid			hypertable_relid = PG_GETARG_OID(0);
	Cache	   *hcache = hypertable_cache_pin();
	Hypertable *ht = hypertable_cache_get_entry(hcache, hypertable_relid);

	if (NULL != ht)
	{
		Relation	tblrel = relation_open(hypertable_relid, AccessShareLock);
		List	   *indexlist = RelationGetIndexList(tblrel);
		ListCell   *lc;

		foreach(lc, indexlist)
		{
			Relation	idxrel = relation_open(lfirst_oid(lc), AccessShareLock);

			if (idxrel->rd_index->indisunique || idxrel->rd_index->indisexclusion)
				indexing_verify_columns(ht->space, build_indexcolumn_list(idxrel));
			relation_close(idxrel, AccessShareLock);
		}
		relation_close(tblrel, AccessShareLock);
	}

	cache_release(hcache);

	PG_RETURN_NULL();
}
