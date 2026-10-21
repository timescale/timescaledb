/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Rewrite the "last point" query
 *
 *   SELECT DISTINCT ON (k) ... FROM ht WHERE quals ORDER BY k, time DESC, ...
 *
 * into
 *
 *   SELECT l.* FROM (SELECT DISTINCT k FROM ht WHERE quals ORDER BY k) keys
 *   CROSS JOIN LATERAL (SELECT ... FROM ht WHERE quals AND ht.k = keys.k
 *                       ORDER BY time DESC, ... LIMIT 1) l
 *   ORDER BY keys.k
 *
 * The distinct keys can be produced by SkipScan, and the lateral subquery by an
 * ordered ChunkAppend that stops after the first chunk that has a matching
 * row, so the older chunks are not read at all.
 */
#include <postgres.h>

#include <access/htup_details.h>
#include <catalog/pg_attribute.h>
#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <parser/parsetree.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "dimension.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "lastpoint.h"

static bool
column_is_not_null(Oid relid, AttrNumber attno)
{
	HeapTuple tuple = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attno));
	if (!HeapTupleIsValid(tuple))
	{
		return false;
	}

	const bool notnull = ((Form_pg_attribute) GETSTRUCT(tuple))->attnotnull;
	ReleaseSysCache(tuple);
	return notnull;
}

static bool
is_simple_var(Node *node, Index varno)
{
	if (!IsA(node, Var))
	{
		return false;
	}

	Var *var = castNode(Var, node);
	return var->varno == (int) varno && var->varlevelsup == 0 && var->varattno > 0;
}

/*
 * Check the shape of the query and return the number of DISTINCT ON keys, or
 * zero if the rewrite does not apply.
 */
static int
lastpoint_num_keys(Query *parse)
{
	if (parse->commandType != CMD_SELECT || !parse->hasDistinctOn || parse->hasAggs ||
		parse->hasWindowFuncs || parse->hasTargetSRFs || parse->hasSubLinks ||
		parse->hasModifyingCTE || parse->hasForUpdate || parse->groupClause != NIL ||
		parse->groupingSets != NIL || parse->havingQual != NULL || parse->setOperations != NULL ||
		parse->cteList != NIL || parse->rowMarks != NIL || parse->limitOption != LIMIT_OPTION_COUNT)
	{
		return 0;
	}

	if (list_length(parse->rtable) != 1 || list_length(parse->jointree->fromlist) != 1 ||
		!IsA(linitial(parse->jointree->fromlist), RangeTblRef))
	{
		return 0;
	}

	RangeTblEntry *rte = linitial_node(RangeTblEntry, parse->rtable);
	if (rte->rtekind != RTE_RELATION || !rte->inh || rte->tablesample != NULL)
	{
		return 0;
	}

	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, rte->relid, CACHE_FLAG_MISSING_OK);
	AttrNumber time_attno = InvalidAttrNumber;
	if (ht != NULL)
	{
		const Dimension *dim = hyperspace_get_open_dimension(ht->space, 0);
		if (dim != NULL)
		{
			time_attno = dim->column_attno;
		}
	}
	ts_cache_release(&hcache);

	if (time_attno == InvalidAttrNumber)
	{
		return 0;
	}

	/*
	 * Postgres requires the DISTINCT ON expressions to match the leading ORDER
	 * BY expressions, and reorders the distinct clause accordingly. We also
	 * need the next ORDER BY expression to be the time dimension, so that the
	 * per-key subquery can use an ordered append.
	 */
	const int num_keys = list_length(parse->distinctClause);
	if (num_keys == 0 || list_length(parse->sortClause) <= num_keys)
	{
		return 0;
	}

	for (int i = 0; i < num_keys; i++)
	{
		SortGroupClause *distinct = list_nth_node(SortGroupClause, parse->distinctClause, i);
		SortGroupClause *sort = list_nth_node(SortGroupClause, parse->sortClause, i);
		if (distinct->tleSortGroupRef != sort->tleSortGroupRef || !OidIsValid(sort->eqop) ||
			!OidIsValid(sort->sortop))
		{
			return 0;
		}

		TargetEntry *tle = get_sortgroupclause_tle(sort, parse->targetList);
		if (!is_simple_var((Node *) tle->expr, 1))
		{
			return 0;
		}

		/*
		 * The lateral subquery matches the keys by equality, so it would not
		 * find the rows with NULL keys.
		 */
		Var *var = castNode(Var, tle->expr);
		if (!column_is_not_null(rte->relid, var->varattno))
		{
			return 0;
		}
	}

	SortGroupClause *time_sort = list_nth_node(SortGroupClause, parse->sortClause, num_keys);
	TargetEntry *time_tle = get_sortgroupclause_tle(time_sort, parse->targetList);
	if (!is_simple_var((Node *) time_tle->expr, 1) ||
		castNode(Var, time_tle->expr)->varattno != time_attno)
	{
		return 0;
	}

	if (contain_volatile_functions((Node *) parse->targetList) ||
		contain_volatile_functions(parse->jointree->quals) ||
		contain_volatile_functions(parse->limitCount) ||
		contain_volatile_functions(parse->limitOffset))
	{
		return 0;
	}

	return num_keys;
}

static Alias *
make_subquery_alias(const char *name, List *tlist)
{
	List *colnames = NIL;
	ListCell *lc;
	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (!tle->resjunk)
		{
			colnames =
				lappend(colnames, makeString(pstrdup(tle->resname ? tle->resname : "?column?")));
		}
	}
	return makeAlias(name, colnames);
}

static RangeTblEntry *
make_subquery_rte(Query *subquery, const char *name, bool lateral)
{
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->alias = make_subquery_alias(name, subquery->targetList);
	rte->eref = copyObject(rte->alias);
	rte->lateral = lateral;
	rte->inFromCl = true;
	return rte;
}

void
lastpoint_rewrite(Query *parse)
{
	const int num_keys = lastpoint_num_keys(parse);
	if (num_keys == 0)
	{
		return;
	}

	List *key_sort = list_copy_head(parse->sortClause, num_keys);

	/*
	 * The subquery producing the distinct keys.
	 */
	Query *keys = copyObject(parse);
	keys->targetList = NIL;
	keys->distinctClause = copyObject(parse->distinctClause);
	keys->sortClause = copyObject(key_sort);
	keys->hasDistinctOn = false;
	keys->limitCount = NULL;
	keys->limitOffset = NULL;
	for (int i = 0; i < num_keys; i++)
	{
		SortGroupClause *sort = list_nth_node(SortGroupClause, key_sort, i);
		TargetEntry *tle = get_sortgroupclause_tle(sort, parse->targetList);
		TargetEntry *key_tle =
			makeTargetEntry(copyObject(tle->expr),
							i + 1,
							pstrdup(
								get_rte_attribute_name(linitial_node(RangeTblEntry, parse->rtable),
													   castNode(Var, tle->expr)->varattno)),
							false);
		key_tle->ressortgroupref = sort->tleSortGroupRef;
		keys->targetList = lappend(keys->targetList, key_tle);
	}

	/*
	 * The lateral subquery producing the first row for the given keys.
	 */
	Query *row = copyObject(parse);
	row->distinctClause = NIL;
	row->hasDistinctOn = false;
	row->sortClause = list_copy_tail(parse->sortClause, num_keys);
	row->limitCount = (Node *)
		makeConst(INT8OID, -1, InvalidOid, sizeof(int64), Int64GetDatum(1), false, FLOAT8PASSBYVAL);
	row->limitOffset = NULL;
	for (int i = 0; i < num_keys; i++)
	{
		SortGroupClause *sort = list_nth_node(SortGroupClause, key_sort, i);
		TargetEntry *tle = get_sortgroupclause_tle(sort, parse->targetList);
		Var *inner = copyObject(castNode(Var, tle->expr));
		Var *outer = makeVar(1, i + 1, inner->vartype, inner->vartypmod, inner->varcollid, 1);
		Expr *eq = make_opclause(sort->eqop,
								 BOOLOID,
								 false,
								 (Expr *) inner,
								 (Expr *) outer,
								 InvalidOid,
								 inner->varcollid);
		set_opfuncid(castNode(OpExpr, eq));
		row->jointree->quals = make_and_qual(row->jointree->quals, (Node *) eq);
	}

	/*
	 * The outer query that joins them.
	 */
	List *tlist = NIL;
	ListCell *lc;
	foreach (lc, parse->targetList)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (tle->resjunk)
		{
			continue;
		}

		Var *var = makeVar(2,
						   tle->resno,
						   exprType((Node *) tle->expr),
						   exprTypmod((Node *) tle->expr),
						   exprCollation((Node *) tle->expr),
						   0);
		tlist = lappend(tlist,
						makeTargetEntry((Expr *) var, list_length(tlist) + 1, tle->resname, false));
	}

	List *sort_clause = NIL;
	Index max_ref = 0;
	foreach (lc, parse->targetList)
	{
		max_ref = Max(max_ref, lfirst_node(TargetEntry, lc)->ressortgroupref);
	}

	for (int i = 0; i < num_keys; i++)
	{
		TargetEntry *key_tle = list_nth_node(TargetEntry, keys->targetList, i);
		Var *var = makeVar(1,
						   i + 1,
						   exprType((Node *) key_tle->expr),
						   exprTypmod((Node *) key_tle->expr),
						   exprCollation((Node *) key_tle->expr),
						   0);
		TargetEntry *tle = makeTargetEntry((Expr *) var, list_length(tlist) + 1, NULL, true);
		tle->ressortgroupref = ++max_ref;
		tlist = lappend(tlist, tle);

		SortGroupClause *sort = copyObject(list_nth_node(SortGroupClause, key_sort, i));
		sort->tleSortGroupRef = tle->ressortgroupref;
		sort_clause = lappend(sort_clause, sort);
	}

	RangeTblRef *keys_ref = makeNode(RangeTblRef);
	keys_ref->rtindex = 1;
	RangeTblRef *row_ref = makeNode(RangeTblRef);
	row_ref->rtindex = 2;

	parse->rtable = list_make2(make_subquery_rte(keys, "lastpoint_keys", false),
							   make_subquery_rte(row, "lastpoint_row", true));
	parse->rteperminfos = NIL;
	parse->jointree = makeFromExpr(list_make2(keys_ref, row_ref), NULL);
	parse->targetList = tlist;
	parse->distinctClause = NIL;
	parse->hasDistinctOn = false;
	parse->sortClause = sort_clause;
}
