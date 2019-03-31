/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/relation.h>
#include <nodes/value.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>
#include <optimizer/tlist.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compat.h"
#include "guc.h"
#include "hypertable.h"
#include "hypertable_restrict_info.h"
#include "constraint_aware_append.h"
#include "planner.h"
#include "plan_ordered_append.h"

/*
 * Check if conditions for doing ordered append optimization are fulfilled
 */
bool
ts_ordered_append_should_optimize(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht, bool *reverse)
{
	SortGroupClause *sort = linitial(root->parse->sortClause);
	TargetEntry *tle = get_sortgroupref_tle(sort->tleSortGroupRef, root->parse->targetList);
	RangeTblEntry *rte = root->simple_rte_array[rel->relid];
	TypeCacheEntry *tce;
	char *column;

	/* these are checked in caller so we only Assert */
	Assert(!ts_guc_disable_optimizations && ts_guc_enable_ordered_append);

	/*
	 * only do this optimization for hypertables with 1 dimension and queries
	 * with an ORDER BY and LIMIT clause, caller checked this, so only
	 * asserting
	 */
	Assert(ht->space->num_dimensions == 1 || root->parse->sortClause != NIL ||
		   root->limit_tuples != -1.0);

	/*
	 * check that the first element of the ORDER BY clause actually matches
	 * the first dimension of the hypertable
	 */

	/* doublecheck rel actually refers to our hypertable */
	Assert(ht->space->main_table_relid == rte->relid);

	/*
	 * we only support direct column references for now
	 */
	if (!IsA(tle->expr, Var))
		return false;

	/*
	 * check Var points to a rel
	 */
	if (castNode(Var, tle->expr)->varno >= root->simple_rel_array_size)
		return false;

	/*
	 * check the ORDER BY column actually belongs to our hypertable
	 */
	if (root->simple_rte_array[castNode(Var, tle->expr)->varno] != rte)
		return false;

	/* check dimension column is our ORDER BY column */
	column = strVal(
		list_nth(rte->eref->colnames, AttrNumberGetAttrOffset(castNode(Var, tle->expr)->varattno)));
	if (namestrcmp(&ht->space->dimensions[0].fd.column_name, column) != 0)
		return false;

	/*
	 * check sort operation is either less than or greater than
	 */
	tce = lookup_type_cache(castNode(Var, tle->expr)->vartype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
	if (sort->sortop != tce->lt_opr && sort->sortop != tce->gt_opr)
		return false;

	if (reverse != NULL)
		*reverse = sort->sortop == tce->lt_opr ? false : true;

	return true;
}

/*
 * we use an existing MergeAppendPath here as starting point for creating
 * our ordered AppendPath because it has all the required information we
 * need to create our path. If pathkeys does not match the ORDER BY then
 * we return the original MergeAppendPath
 */
Path *
ts_ordered_append_path_create(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
							  MergeAppendPath *merge)
{
	ListCell *lc;
	List *sorted = NIL;
	AppendPath *append;
	double rows = 0.0;
	Cost total_cost = 0.0;

	if (list_length(merge->subpaths) == 0)
		return (Path *) merge;

	/*
	 * double check pathkeys of the MergeAppendPath actually is compatible
	 * with the order supplied in the query since that is what our children
	 * will be ordered by.
	 */
	if (!pathkeys_contained_in(root->sort_pathkeys, merge->path.pathkeys))
		return (Path *) merge;

	/* create subpaths for our append node */
	foreach (lc, merge->subpaths)
	{
		Path *child = lfirst(lc);

		/*
		 * we only include cost of children until the limit is satisfied.
		 * Cost of children past the limit will not be added to the cost
		 */
		if (rows < root->limit_tuples)
		{
			total_cost += child->total_cost;
			rows += child->rows;
		}

		/*
		 * When an index is not available on all chunks pathkeys of the child
		 * might not match pathkeys of the MergeAppendPath PostgreSQL fixes
		 * this when creating the merge append plan by inserting a sort node
		 * for the child. Unfortunately this is too late for us so we don't do
		 * this optimization for those cases for now.
		 */
		if (!pathkeys_contained_in(merge->path.pathkeys, child->pathkeys))
			return (Path *) merge;

		sorted = lappend(sorted, child);
	}

	/*
	 * we use normal postgresql append cost calculation, which means
	 * the cost estimate is rather pessimistic
	 */
#if PG96
	append = create_append_path(rel, sorted, PATH_REQ_OUTER(&merge->path), 0);
#elif PG10
	append =
		create_append_path(rel, sorted, PATH_REQ_OUTER(&merge->path), 0, merge->partitioned_rels);
#else
	append = create_append_path(root,
								rel,
								sorted,
								NULL,
								PATH_REQ_OUTER(&merge->path),
								0,
								false,
								merge->partitioned_rels,
								root->limit_tuples);
#endif

	append->path.pathkeys = merge->path.pathkeys;
	append->path.parallel_aware = false;
	append->path.parallel_safe = false;
	append->path.startup_cost = ((Path *) linitial(merge->subpaths))->startup_cost;
	append->path.total_cost = total_cost;

	return (Path *) append;
}
