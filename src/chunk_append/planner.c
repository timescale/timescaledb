/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_namespace.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/placeholder.h>
#include <optimizer/planmain.h>
#include <optimizer/prep.h>
#include <optimizer/subselect.h>
#include <optimizer/tlist.h>
#include <optimizer/var.h>
#include <parser/parsetree.h>

#include "chunk_append/chunk_append.h"
#include "chunk_append/planner.h"
#include "chunk_append/exec.h"
#include "chunk_append/transform.h"
#include "planner_import.h"
#include "compat.h"
#include "guc.h"

static Sort *make_sort(Plan *lefttree, int numCols, AttrNumber *sortColIdx, Oid *sortOperators,
					   Oid *collations, bool *nullsFirst);
static AppendRelInfo *get_appendrelinfo(PlannerInfo *root, Index rti);
static Plan *adjust_childscan(PlannerInfo *root, Plan *plan, Path *path, List *pathkeys,
							  List *tlist, AttrNumber *sortColIdx);

static CustomScanMethods chunk_append_plan_methods = {
	.CustomName = "ChunkAppend",
	.CreateCustomScanState = chunk_append_state_create,
};

void
_chunk_append_init(void)
{
	RegisterCustomScanMethods(&chunk_append_plan_methods);
}

static Plan *
adjust_childscan(PlannerInfo *root, Plan *plan, Path *path, List *pathkeys, List *tlist,
				 AttrNumber *sortColIdx)
{
	AppendRelInfo *appinfo = get_appendrelinfo(root, path->parent->relid);
	int childSortCols;
	Oid *sortOperators;
	Oid *collations;
	bool *nullsFirst;

	/* push down targetlist to children */
	plan->targetlist = (List *) adjust_appendrel_attrs_compat(root, (Node *) tlist, appinfo);

	/* Compute sort column info, and adjust subplan's tlist as needed */
	plan = ts_prepare_sort_from_pathkeys(plan,
										 pathkeys,
										 path->parent->relids,
										 sortColIdx,
										 true,
										 &childSortCols,
										 &sortColIdx,
										 &sortOperators,
										 &collations,
										 &nullsFirst);

	/* inject sort node if child sort order does not match desired order */
	if (!pathkeys_contained_in(pathkeys, path->pathkeys))
	{
		plan = (Plan *)
			make_sort(plan, childSortCols, sortColIdx, sortOperators, collations, nullsFirst);
	}
	return plan;
}

Plan *
chunk_append_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path, List *tlist,
						 List *clauses, List *custom_plans)
{
	ListCell *lc_child;
	List *chunk_ri_clauses = NIL;
	List *chunk_rt_indexes = NIL;
	List *sort_options = NIL;
	List *custom_private = NIL;
	uint32 limit = 0;

	ChunkAppendPath *capath = (ChunkAppendPath *) path;
	CustomScan *cscan = makeNode(CustomScan);

	cscan->flags = path->flags;
	cscan->methods = &chunk_append_plan_methods;
	cscan->scan.scanrelid = rel->relid;

	tlist = ts_build_path_tlist(root, (Path *) path);
	cscan->custom_scan_tlist = tlist;
	cscan->scan.plan.targetlist = tlist;

	if (path->path.pathkeys == NIL)
	{
		ListCell *lc_plan, *lc_path;
		forboth (lc_path, path->custom_paths, lc_plan, custom_plans)
		{
			Plan *child_plan = lfirst(lc_plan);
			Path *child_path = lfirst(lc_path);
			AppendRelInfo *appinfo = get_appendrelinfo(root, child_path->parent->relid);

			/* push down targetlist to children */
			child_plan->targetlist =
				(List *) adjust_appendrel_attrs_compat(root, (Node *) tlist, appinfo);
		}
	}
	else
	{
		/*
		 * If this is an ordered append node we need to ensure the columns
		 * required for sorting are present in the targetlist and all children
		 * return sorted output. Children not returning sorted output will be
		 * wrapped in a sort node.
		 */
		ListCell *lc_plan, *lc_path;
		int numCols;
		AttrNumber *sortColIdx;
		Oid *sortOperators;
		Oid *collations;
		bool *nullsFirst;
		List *pathkeys = path->path.pathkeys;
		List *sort_indexes = NIL;
		List *sort_ops = NIL;
		List *sort_collations = NIL;
		List *sort_nulls = NIL;
		int i;

		/* Compute sort column info, and adjust MergeAppend's tlist as needed */
		ts_prepare_sort_from_pathkeys(&cscan->scan.plan,
									  pathkeys,
									  path->path.parent->relids,
									  NULL,
									  true,
									  &numCols,
									  &sortColIdx,
									  &sortOperators,
									  &collations,
									  &nullsFirst);

		/*
		 * collect sort information to make available to explain
		 */
		for (i = 0; i < numCols; i++)
		{
			sort_indexes = lappend_oid(sort_indexes, sortColIdx[i]);
			sort_ops = lappend_oid(sort_ops, sortOperators[i]);
			sort_collations = lappend_oid(sort_collations, collations[i]);
			sort_nulls = lappend_oid(sort_nulls, nullsFirst[i]);
		}

		sort_options = list_make4(sort_indexes, sort_ops, sort_collations, sort_nulls);

		forboth (lc_path, path->custom_paths, lc_plan, custom_plans)
		{
			if (IsA(lfirst(lc_plan), MergeAppend))
			{
				ListCell *lc_childpath, *lc_childplan;
				MergeAppend *merge_plan = castNode(MergeAppend, lfirst(lc_plan));
				MergeAppendPath *merge_path = castNode(MergeAppendPath, lfirst(lc_path));

				/* Compute sort column info, and adjust MergeAppend's tlist as needed */
				ts_prepare_sort_from_pathkeys((Plan *) merge_plan,
											  pathkeys,
											  merge_path->path.parent->relids,
											  NULL,
											  true,
											  &numCols,
											  &sortColIdx,
											  &sortOperators,
											  &collations,
											  &nullsFirst);
				forboth (lc_childpath, merge_path->subpaths, lc_childplan, merge_plan->mergeplans)
				{
					lfirst(lc_childplan) = adjust_childscan(root,
															lfirst(lc_childplan),
															lfirst(lc_childpath),
															pathkeys,
															tlist,
															sortColIdx);
				}
			}
			else
			{
				lfirst(lc_plan) = adjust_childscan(root,
												   lfirst(lc_plan),
												   lfirst(lc_path),
												   path->path.pathkeys,
												   tlist,
												   sortColIdx);
			}
		}
	}

	cscan->custom_plans = custom_plans;

	/*
	 * If we do either startup or runtime exclusion, we need to pass restrictinfo
	 * clauses into executor.
	 */
	if (capath->startup_exclusion || capath->runtime_exclusion)
	{
		foreach (lc_child, cscan->custom_plans)
		{
			Scan *scan = chunk_append_get_scan_plan(lfirst(lc_child));

			if (scan == NULL || scan->scanrelid == 0)
			{
				chunk_ri_clauses = lappend(chunk_ri_clauses, NIL);
				chunk_rt_indexes = lappend_oid(chunk_rt_indexes, 0);
			}
			else
			{
				List *chunk_clauses = NIL;
				ListCell *lc;
				AppendRelInfo *appinfo = get_appendrelinfo(root, scan->scanrelid);

				foreach (lc, clauses)
				{
					Node *clause = (Node *) ts_transform_cross_datatype_comparison(
						castNode(RestrictInfo, lfirst(lc))->clause);
					clause = adjust_appendrel_attrs_compat(root, clause, appinfo);
					chunk_clauses = lappend(chunk_clauses, clause);
				}
				chunk_ri_clauses = lappend(chunk_ri_clauses, chunk_clauses);
				chunk_rt_indexes = lappend_oid(chunk_rt_indexes, scan->scanrelid);
			}
		}
		Assert(list_length(cscan->custom_plans) == list_length(chunk_ri_clauses));
		Assert(list_length(chunk_ri_clauses) == list_length(chunk_rt_indexes));
	}

	if (capath->pushdown_limit && root->limit_tuples > 0 && root->limit_tuples <= PG_UINT32_MAX)
		limit = root->limit_tuples;

	custom_private = list_make1(
		list_make3_oid((Oid) capath->startup_exclusion, (Oid) capath->runtime_exclusion, limit));
	custom_private = lappend(custom_private, chunk_ri_clauses);
	custom_private = lappend(custom_private, chunk_rt_indexes);
	custom_private = lappend(custom_private, sort_options);

	cscan->custom_private = custom_private;

	return &cscan->scan.plan;
}

/*
 * make_sort --- basic routine to build a Sort plan node
 *
 * Caller must have built the sortColIdx, sortOperators, collations, and
 * nullsFirst arrays already.
 */
static Sort *
make_sort(Plan *lefttree, int numCols, AttrNumber *sortColIdx, Oid *sortOperators, Oid *collations,
		  bool *nullsFirst)
{
	Sort *node = makeNode(Sort);
	Plan *plan = &node->plan;

	plan->targetlist = lefttree->targetlist;
	plan->qual = NIL;
	plan->lefttree = lefttree;
	plan->righttree = NULL;
	node->numCols = numCols;
	node->sortColIdx = sortColIdx;
	node->sortOperators = sortOperators;
	node->collations = collations;
	node->nullsFirst = nullsFirst;

	return node;
}

static AppendRelInfo *
get_appendrelinfo(PlannerInfo *root, Index rti)
{
#if PG96 || PG10
	ListCell *lc;
	foreach (lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst(lc);
		if (appinfo->child_relid == rti)
			return appinfo;
	}
#else
	if (root->append_rel_array[rti])
		return root->append_rel_array[rti];
#endif
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR), errmsg("no appendrelinfo found for index %d", rti)));
	pg_unreachable();
}

Scan *
chunk_append_get_scan_plan(Plan *plan)
{
	if (plan != NULL && (IsA(plan, Sort) || IsA(plan, Result)))
		plan = plan->lefttree;

	if (plan == NULL)
		return NULL;

	switch (nodeTag(plan))
	{
		case T_BitmapHeapScan:
		case T_BitmapIndexScan:
		case T_CteScan:
		case T_ForeignScan:
		case T_FunctionScan:
		case T_IndexOnlyScan:
		case T_IndexScan:
		case T_SampleScan:
		case T_SeqScan:
		case T_SubqueryScan:
		case T_TidScan:
		case T_ValuesScan:
		case T_WorkTableScan:
			return (Scan *) plan;
			break;
		case T_CustomScan:
			if (castNode(CustomScan, plan)->scan.scanrelid > 0)
				return (Scan *) plan;
			else
				return NULL;
			break;
		case T_MergeAppend:
			return NULL;
			break;
		default:
			elog(ERROR, "invalid child of chunk append: %u", nodeTag(plan));
			return NULL;
			break;
	}
}
