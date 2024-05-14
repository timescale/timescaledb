/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/tlist.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "planner/planner.h"
#include "nodes/chunk_append/chunk_append.h"
#include "func_cache.h"
#include "guc.h"

static Var *find_equality_join_var(Var *sort_var, Index ht_relid, Oid eq_opr,
								   List *join_conditions);

static CustomPathMethods chunk_append_path_methods = {
	.CustomName = "ChunkAppend",
	.PlanCustomPath = ts_chunk_append_plan_create,
};

bool
ts_is_chunk_append_path(Path *path)
{
	return IsA(path, CustomPath) &&
		   castNode(CustomPath, path)->methods == &chunk_append_path_methods;
}

static bool
has_joins(FromExpr *jointree)
{
	return list_length(jointree->fromlist) != 1 || !IsA(linitial(jointree->fromlist), RangeTblRef);
}

/*
 * Create the appropriate subpath for the outer MergeAppend
 * node depending on the number of paths in the current group:
 * Combine two or more group members into a mergeAppend node
 * or append a single member as is.
 * Members of the same group contain data of the same chunk,
 * so they are combined into a MergeAppend node corresponding
 * to that single chunk.
 */
static void
create_group_subpath(PlannerInfo *root, RelOptInfo *rel, List *group, List *pathkeys,
					 Relids required_outer, List *partitioned_rels, List **nested_children)
{
	if (list_length(group) > 1)
	{
		MergeAppendPath *append =
			create_merge_append_path(root, rel, group, pathkeys, required_outer);
		*nested_children = lappend(*nested_children, append);
	}
	else
	{
		/* If group only has 1 member we can add it directly */
		*nested_children = lappend(*nested_children, linitial(group));
	}
}

ChunkAppendPath *
ts_chunk_append_path_copy(ChunkAppendPath *ca, List *subpaths, PathTarget *pathtarget)
{
	ListCell *lc;
	double total_cost = 0, rows = 0;
	ChunkAppendPath *new = palloc(sizeof(ChunkAppendPath));
	memcpy(new, ca, sizeof(ChunkAppendPath));
	new->cpath.custom_paths = subpaths;

	foreach (lc, subpaths)
	{
		Path *child = lfirst(lc);
		total_cost += child->total_cost;
		rows += child->rows;
	}
	new->cpath.path.total_cost = total_cost;
	new->cpath.path.rows = rows;
	new->cpath.path.pathtarget = copy_pathtarget(pathtarget);

	return new;
}

Path *
ts_chunk_append_path_create(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht, Path *subpath,
							bool parallel_aware, bool ordered, List *nested_oids)
{
	ChunkAppendPath *path;
	ListCell *lc;
	double rows = 0.0;
	Cost total_cost = 0.0;
	List *children = NIL;

	path = (ChunkAppendPath *) newNode(sizeof(ChunkAppendPath), T_CustomPath);

	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.path.parent = rel;
	path->cpath.path.pathtarget = rel->reltarget;
	path->cpath.path.param_info = subpath->param_info;

	/*
	 * We keep the pathkeys from the original path here because
	 * the original path was either a MergeAppendPath and this
	 * will become an ordered append or the original path is an
	 * AppendPath and since we do not reorder children the order
	 * will be kept intact. For the AppendPath case with pathkeys
	 * it was most likely an Append with only a single child.
	 * We could skip the ChunkAppend path creation if there is
	 * only a single child but we decided earlier that ChunkAppend
	 * would be beneficial for this query so we treat it the same
	 * as if it had multiple children.
	 */
	Assert(IsA(subpath, AppendPath) || IsA(subpath, MergeAppendPath));
	path->cpath.path.pathkeys = subpath->pathkeys;

	path->cpath.path.parallel_aware = ts_guc_enable_parallel_chunk_append ? parallel_aware : false;
	path->cpath.path.parallel_safe = subpath->parallel_safe;
	path->cpath.path.parallel_workers = subpath->parallel_workers;

	/*
	 * Set flags. We can set CUSTOMPATH_SUPPORT_BACKWARD_SCAN and
	 * CUSTOMPATH_SUPPORT_MARK_RESTORE. The only interesting flag is the first
	 * one (backward scan), but since we are not scanning a real relation we
	 * need not indicate that we support backward scans. Lower-level index
	 * scanning nodes will scan backward if necessary, so once tuples get to
	 * this node they will be in a given order already.
	 */
	path->cpath.flags = 0;
	path->cpath.methods = &chunk_append_path_methods;

	/*
	 * Figure out whether there's a hard limit on the number of rows that
	 * query_planner's result subplan needs to return.  Even if we know a
	 * hard limit overall, it doesn't apply if the query has any
	 * grouping/aggregation operations, or SRFs in the tlist.
	 */
	if (root->parse->groupClause || root->parse->groupingSets || root->parse->distinctClause ||
		root->parse->hasAggs || root->parse->hasWindowFuncs || root->hasHavingQual ||
		has_joins(root->parse->jointree) || root->limit_tuples > PG_INT32_MAX ||
		root->parse->hasTargetSRFs ||
		!pathkeys_contained_in(root->sort_pathkeys, subpath->pathkeys))
		path->limit_tuples = -1;
	else
		path->limit_tuples = (int) root->limit_tuples;

	/*
	 * check if we should do startup and runtime exclusion
	 */
	foreach (lc, rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

		/*
		 * The external parameters (e.g. from parameterized prepared statements)
		 * are constant during query run time, so we can use them for startup
		 * exclusion.
		 * The join parameters have multiple values, so they are only used for
		 * runtime exclusion.
		 */
		if (contain_mutable_functions((Node *) rinfo->clause) ||
			ts_contains_external_param((Node *) rinfo->clause))
		{
			path->startup_exclusion = true;
		}

		if (ts_guc_enable_runtime_exclusion && ts_contains_join_param((Node *) rinfo->clause))
		{
			ListCell *lc_var;

			/* We have two types of exclusion:
			 *
			 * Parent exclusion fires if the entire hypertable can be excluded.
			 * This happens if doing things like joining against a parameter
			 * value that is an empty array or NULL. It doesn't happen often,
			 * but when it does, it speeds up the query immensely. It's also cheap
			 * to check for this condition as you check this once per hypertable
			 * at runtime.
			 *
			 * Child exclusion works by seeing if there is a contradiction between
			 * the chunks constraints and the expression on parameter values. For example,
			 * it can evaluate whether a time parameter from a subquery falls outside
			 * the range of the chunk. It is more widely applicable than the parent
			 * exclusion but is also more expensive to evaluate since you have to perform
			 * the check on every chunk. Child exclusion can only apply if one of the quals
			 * involves a partitioning column.
			 *
			 */
			path->runtime_exclusion_parent = true;
			foreach (lc_var, pull_var_clause((Node *) rinfo->clause, 0))
			{
				Var *var = lfirst(lc_var);
				/*
				 * varattno 0 is whole row and varattno less than zero are
				 * system columns so we skip those even though
				 * ts_is_partitioning_column would return the correct
				 * answer for those as well
				 */
				if ((Index) var->varno == rel->relid && var->varattno > 0 &&
					ts_is_partitioning_column(ht, var->varattno))
				{
					path->runtime_exclusion_children = true;
					break;
				}
			}
		}
	}
	/*
	 * Our strategy is to use child exclusion if possible (if a partitioning
	 * column is used) and fall back to parent exclusion if we can't use child
	 * exclusion. Please note: there is no point to using both child and parent
	 * exclusion at the same time since child exclusion would always exclude
	 * the same chunks that parent exclusion would.
	 */

	if (path->runtime_exclusion_parent && path->runtime_exclusion_children)
		path->runtime_exclusion_parent = false;

	/*
	 * Make sure our subpath is either an Append or MergeAppend node
	 */
	switch (nodeTag(subpath))
	{
		case T_AppendPath:
		{
			AppendPath *append = castNode(AppendPath, subpath);

			if (append->path.parallel_aware && append->first_partial_path > 0)
				path->first_partial_path = append->first_partial_path;
			children = append->subpaths;
			break;
		}
		case T_MergeAppendPath:
			/*
			 * check if ordered append is applicable, only assert ordered here
			 * checked properly in ts_ordered_append_should_optimize
			 */
			Assert(ordered);

			/*
			 * we only push down LIMIT for ordered append
			 */
			path->pushdown_limit = true;

			children = castNode(MergeAppendPath, subpath)->subpaths;
			break;
		default:
			elog(ERROR, "invalid child of chunk append: %s", ts_get_node_name((Node *) subpath));
			break;
	}

	if (!ordered)
	{
		path->cpath.custom_paths = children;
	}
	else if (ht->space->num_dimensions == 1)
	{
		List *nested_children = NIL;
		/*
		 * Convert the sort nodes that refer to the same chunk into a single
		 * mergeAppend node to combine compressed and uncompressed chunk output.
		 *
		 * NB: We assume that the sort nodes referring the same chunk appear
		 * one after the other and so we iterate through the children examining
		 * consecutive pairs. Is it possible that this assumption is wrong?
		 */
		List *group = NIL;
		Oid relid = InvalidOid;

		foreach (lc, children)
		{
			Path *child = (Path *) lfirst(lc);
			/* Check if this is in new group */
			if (child->parent->relid != relid)
			{
				/* if previous group had members, process them */
				if (group)
				{
					create_group_subpath(root,
										 rel,
										 group,
										 path->cpath.path.pathkeys,
										 PATH_REQ_OUTER(subpath),
										 NIL,
										 &nested_children);
					group = NIL;
				}
				relid = child->parent->relid;
			}

			/* Form the new group */
			group = lappend(group, child);
		}

		if (group)
		{
			create_group_subpath(root,
								 rel,
								 group,
								 path->cpath.path.pathkeys,
								 PATH_REQ_OUTER(subpath),
								 NIL,
								 &nested_children);
		}

		path->cpath.custom_paths = nested_children;
		children = nested_children;
	}
	else
	{
		/*
		 * For space partitioning we need to change the shape of the plan
		 * into a MergeAppend for each time slice with all space partitions below
		 * The final plan for space partitioning will look like this:
		 *
		 * Custom Scan (ChunkAppend)
		 *   Hypertable: space
		 *   ->  Merge Append
		 *         Sort Key: _hyper_9_56_chunk."time"
		 *         ->  Index Scan
		 *         ->  Index Scan
		 *         ->  Index Scan
		 *   ->  Merge Append
		 *         Sort Key: _hyper_9_55_chunk."time"
		 *         ->  Index Scan
		 *         ->  Index Scan
		 *         ->  Index Scan
		 *
		 * We do not check sort order at this stage but injecting of Sort
		 * nodes happens when the plan is created instead.
		 */
		ListCell *flat = list_head(children);
		List *nested_children = NIL;
		bool has_scan_childs = false;

		foreach (lc, nested_oids)
		{
			ListCell *lc_oid;
			List *current_oids = lfirst(lc);
			List *merge_childs = NIL;
			MergeAppendPath *append;

			/*
			 * For each lc_oid, there will be 0, 1, or 2 matches in flat_list: 0 matches
			 * if child was pruned, 1 match if the chunk is uncompressed or fully compressed,
			 * 2 matches if the chunk is partially compressed.
			 * If there are 2 matches they will also be consecutive (see assumption above)
			 */
			foreach (lc_oid, current_oids)
			{
				bool is_not_pruned = true;
#ifdef USE_ASSERT_CHECKING
				int nmatches = 0;
#endif
				/* Before entering the "DO" loop, check for a valid path entry */
				if (flat == NULL)
					break;

				do
				{
					Path *child = (Path *) lfirst(flat);
					Oid parent_relid = child->parent->relid;
					is_not_pruned =
						lfirst_oid(lc_oid) == root->simple_rte_array[parent_relid]->relid;
					/* postgres may have pruned away some children already */
					if (is_not_pruned)
					{
#ifdef USE_ASSERT_CHECKING
						nmatches++;
#endif
						merge_childs = lappend(merge_childs, child);
						flat = lnext(children, flat);
						if (flat == NULL)
							break;
					}
					/* if current one matched then need to check next one for match */
				} while (is_not_pruned);
#ifdef USE_ASSERT_CHECKING
				Assert(nmatches <= 2);
#endif
			}

			if (list_length(merge_childs) > 1)
			{
				append = create_merge_append_path(root,
												  rel,
												  merge_childs,
												  path->cpath.path.pathkeys,
												  PATH_REQ_OUTER(subpath));
				nested_children = lappend(nested_children, append);
			}
			else if (list_length(merge_childs) == 1)
			{
				has_scan_childs = true;
				nested_children = lappend(nested_children, linitial(merge_childs));
			}
		}

		Assert(flat == NULL);

		/*
		 * if we do not have scans as direct children of this
		 * node we disable startup and runtime exclusion
		 * in this node
		 */
		if (!has_scan_childs)
		{
			path->startup_exclusion = false;
			path->runtime_exclusion_parent = false;
			path->runtime_exclusion_children = false;
		}

		path->cpath.custom_paths = nested_children;
	}

	foreach (lc, path->cpath.custom_paths)
	{
		Path *child = lfirst(lc);

		/*
		 * If there is a LIMIT clause we only include as many chunks as
		 * planner thinks are needed to satisfy LIMIT clause.
		 * We do this to prevent planner choosing parallel plan which might
		 * otherwise look preferable cost wise.
		 */
		if (!path->pushdown_limit || path->limit_tuples == -1 || rows < path->limit_tuples)
		{
			total_cost += child->total_cost;
			rows += child->rows;
		}
	}

	path->cpath.path.rows = rows;
	path->cpath.path.total_cost = total_cost;

	if (path->cpath.custom_paths != NIL)
		path->cpath.path.startup_cost = ((Path *) linitial(path->cpath.custom_paths))->startup_cost;

	return &path->cpath.path;
}

/*
 * Check if conditions for doing ordered append optimization are fulfilled
 */
bool
ts_ordered_append_should_optimize(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
								  List *join_conditions, int *order_attno, bool *reverse)
{
	SortGroupClause *sort = linitial(root->parse->sortClause);
	TargetEntry *tle = get_sortgroupref_tle(sort->tleSortGroupRef, root->parse->targetList);
	RangeTblEntry *rte = root->simple_rte_array[rel->relid];
	TypeCacheEntry *tce;
	char *column;
	Index ht_relid = rel->relid;
	Index sort_relid;
	Var *ht_var;
	Var *sort_var;

	/* these are checked in caller so we only Assert */
	Assert(ts_guc_enable_optimizations && ts_guc_enable_ordered_append &&
		   ts_guc_enable_chunk_append);

	/*
	 * only do this optimization for queries with an ORDER BY clause,
	 * caller checked this, so only asserting
	 */
	Assert(root->parse->sortClause != NIL);

	if (IsA(tle->expr, Var))
	{
		/* direct column reference */
		sort_var = castNode(Var, tle->expr);
	}
	else if (IsA(tle->expr, FuncExpr) && list_length(root->parse->sortClause) == 1)
	{
		/*
		 * check for bucketing functions
		 *
		 * If ORDER BY clause only has 1 expression and the expression is a
		 * bucketing function we can still do Ordered Append, the 1 expression
		 * limit could only be safely removed if we ensure chunk boundaries
		 * are not crossed.
		 *
		 * The following example demonstrates this requirement:
		 *
		 * Chunk 1 has (time, device_id)
		 * 0 1
		 * 0 2
		 *
		 * Chunk 2 has (time, device_id)
		 * 10 1
		 * 10 2
		 *
		 * The ORDER BY clause is time_bucket(100,time), device_id
		 * The result when transforming to an ordered append would be the following:
		 * (time_bucket(100, time), device_id)
		 * 0 1
		 * 0 2
		 * 0 1
		 * 0 2
		 *
		 * The order of the device_ids is wrong so we cannot safely remove the MergeAppend
		 * unless we eliminate the possibility that a bucket spans multiple chunks.
		 */
		FuncInfo *info = ts_func_cache_get_bucketing_func(castNode(FuncExpr, tle->expr)->funcid);
		Expr *transformed;

		if (info == NULL)
			return false;

		transformed = info->sort_transform(castNode(FuncExpr, tle->expr));

		if (!IsA(transformed, Var))
			return false;

		sort_var = castNode(Var, transformed);
	}
	else
		return false;

	/* ordered append won't work for system columns / whole row orderings */
	if (sort_var->varattno <= 0)
		return false;

	sort_relid = sort_var->varno;
	tce = lookup_type_cache(sort_var->vartype,
							TYPECACHE_EQ_OPR | TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/* check sort operation is either less than or greater than */
	if (sort->sortop != tce->lt_opr && sort->sortop != tce->gt_opr)
		return false;

	/*
	 * check the ORDER BY column actually belongs to our hypertable
	 */
	if (sort_relid == ht_relid)
	{
		/* ORDER BY column belongs to our hypertable */
		ht_var = sort_var;
	}
	else
	{
		/*
		 * If the ORDER BY does not match our hypertable but we are joining
		 * against another hypertable on the time column doing an ordered
		 * append here is still beneficial because we can skip the sort
		 * step for the MergeJoin
		 */
		if (join_conditions == NIL)
			return false;

		ht_var = find_equality_join_var(sort_var, ht_relid, tce->eq_opr, join_conditions);

		if (ht_var == NULL)
			return false;
	}

	/* Check hypertable column is the first dimension of the hypertable */
	column = strVal(list_nth(rte->eref->colnames, AttrNumberGetAttrOffset(ht_var->varattno)));
	if (namestrcmp(&ht->space->dimensions[0].fd.column_name, column) != 0)
		return false;

	Assert(order_attno != NULL && reverse != NULL);
	*order_attno = ht_var->varattno;
	*reverse = sort->sortop == tce->lt_opr ? false : true;

	return true;
}

/*
 * Find equality join between column referenced by sort_var and Relation
 * with relid ht_relid
 */
static Var *
find_equality_join_var(Var *sort_var, Index ht_relid, Oid eq_opr, List *join_conditions)
{
	ListCell *lc;
	Index sort_relid = sort_var->varno;

	foreach (lc, join_conditions)
	{
		OpExpr *op = lfirst(lc);

		if (op->opno == eq_opr)
		{
			Var *left = linitial(op->args);
			Var *right = lsecond(op->args);

			Assert(IsA(left, Var) && IsA(right, Var));

			/* Is this a join condition referencing our hypertable */
			if (((Index) left->varno == sort_relid && (Index) right->varno == ht_relid &&
				 left->varattno == sort_var->varattno))
			{
				return right;
			}

			if (((Index) left->varno == ht_relid && (Index) right->varno == sort_relid &&
				 right->varattno == sort_var->varattno))
			{
				return left;
			}
		}
	}

	return NULL;
}
