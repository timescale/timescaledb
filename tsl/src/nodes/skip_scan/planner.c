/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/sysattr.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pathnodes.h>
#include <optimizer/clauses.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planmain.h>
#include <optimizer/prep.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parse_coerce.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "guc.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/constraint_aware_append/constraint_aware_append.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/skip_scan/skip_scan.h"
#include <import/planner.h>

#include <math.h>

typedef struct SkipKeyInfo
{
	/* Index clause which we'll use to skip past elements we've already seen */
	RestrictInfo *skip_clause;

	/* Is this key guaranteed to be not null? */
	bool notnull;

	/* attribute number of the distinct column on the table/chunk which provides comparison value
	 * for Skip qual */
	AttrNumber distinct_attno;

	/* attribute number of the Skip qual comparison column on the indexed table/chunk
	 * "indexed_column_attno = distinct_attno" for (SkipScan <- Index Scan) scenario,
	 * it can be different for (SkipScan <- DecompressChunk <- compressed Index Scan) scenario,
	 * in that case "indexed_column_attno" is the attribute number of the compressed chunk column
	 * corresponding to the distinct column "distinct_attno" on the decompressed chunk consumed by
	 * SkipScan
	 */
	AttrNumber indexed_column_attno;

	/* The column offset on the index we are calling DISTINCT on */
	AttrNumber scankey_attno;
	int distinct_typ_len;
	bool distinct_by_val;

	/* InvalidOid for the last skip key, always invalid for one-key SkipScan
	 * For N-key SkipScan default quals are (sk1 = p1), (sk2 = p2), .. (sk_n > p_n),
	 * we'll switch to (sk_i > p_i) when no more values for (sk_i+1 > p_i+1),
	 * so we will store "=" along with ">" comparator for keys 1..N-1.
	 */
	Oid eqcomp;
} SkipKeyInfo;

typedef struct SkipScanPath
{
	CustomPath cpath;
	IndexPath *index_path;

	/* List of skip column attributes for each skip key */
	List *skipkeyinfo;

	/* Vars referencing the distinct columns on the relation */
	List *dvars;
} SkipScanPath;

typedef struct DistinctPathInfo
{
	UpperRelationKind stage; /* What kind of Upper distinct path we are dealing with */
	Path *unique_path;		 /* If not NULL, valid Upper distinct path */
	List *
		distinct_expr; /* If not NULL, list of valid distinct expressions for Upper distinct path */
} DistinctPathInfo;

static int get_idx_key(IndexOptInfo *idxinfo, AttrNumber attno);
static List *sort_indexquals(IndexOptInfo *indexinfo, List *quals);
static OpExpr *fix_indexqual(IndexOptInfo *index, RestrictInfo *rinfo, AttrNumber scankey_attno);
static bool build_skip_qual(PlannerInfo *root, SkipKeyInfo *skinfo, IndexPath *index_path, Var *var,
							bool build_eqop);
static List *build_subpath(PlannerInfo *root, List *subpaths, DistinctPathInfo *dpinfo,
						   List *top_pathkeys);
static Var *get_distinct_var(PlannerInfo *root, Expr *tlexpr, IndexPath *index_path,
							 Path *child_path, SkipKeyInfo *skinfo);
static TargetEntry *tlist_member_match_var(Var *var, List *targetlist);

/**************************
 * SkipScan Plan Creation *
 **************************/

static CustomScanMethods skip_scan_plan_methods = {
	.CustomName = "SkipScan",
	.CreateCustomScanState = tsl_skip_scan_state_create,
};

void
_skip_scan_init(void)
{
	TryRegisterCustomScanMethods(&skip_scan_plan_methods);
}

static Plan *
setup_index_plan(CustomScan *skip_plan, Plan *child_plan)
{
	Plan *plan = child_plan;
	if (IsA(child_plan, IndexScan))
	{
		skip_plan->scan = castNode(IndexScan, child_plan)->scan;
	}
	else if (IsA(child_plan, IndexOnlyScan))
	{
		skip_plan->scan = castNode(IndexOnlyScan, child_plan)->scan;
	}
	else if (ts_is_decompress_chunk_plan(child_plan))
	{
		CustomScan *csplan = castNode(CustomScan, plan);
		skip_plan->scan = csplan->scan;
		plan = linitial(csplan->custom_plans);
	}
	else
		elog(ERROR,
			 "unsupported subplan type for SkipScan: %s",
			 ts_get_node_name((Node *) child_plan));

	return plan;
}

static Plan *
skip_scan_plan_create(PlannerInfo *root, RelOptInfo *relopt, CustomPath *best_path, List *tlist,
					  List *clauses, List *custom_plans)
{
	SkipScanPath *path = (SkipScanPath *) best_path;
	CustomScan *skip_plan = makeNode(CustomScan);
	IndexPath *index_path = path->index_path;

	Plan *child_plan = linitial(custom_plans);
	Plan *plan = setup_index_plan(skip_plan, child_plan);

	skip_plan->scan.plan.targetlist = tlist;
	skip_plan->custom_scan_tlist = list_copy(tlist);
	skip_plan->scan.plan.qual = NIL;
	skip_plan->scan.plan.type = T_CustomScan;
	skip_plan->methods = &skip_scan_plan_methods;
	skip_plan->custom_plans = custom_plans;

	/* Setup for SkipScan debug info */
	StringInfoData debuginfo;
	RangeTblEntry *indexed_rte = NULL;
	char *sep = "";
	if (ts_guc_debug_skip_scan_info)
	{
		initStringInfo(&debuginfo);
		RelOptInfo *indexed_rel = index_path->path.parent;
		indexed_rte = planner_rt_fetch(indexed_rel->relid, root);
		Oid indrelid = InvalidOid;
		if (IsA(plan, IndexScan))
		{
			IndexScan *idx_plan = castNode(IndexScan, plan);
			indrelid = idx_plan->indexid;
		}
		else if (IsA(plan, IndexOnlyScan))
		{
			IndexOnlyScan *idx_plan = castNode(IndexOnlyScan, plan);
			indrelid = idx_plan->indexid;
		}
		appendStringInfo(&debuginfo, "SkipScan used on %s(", get_rel_name(indrelid));
	}

	ListCell *lc, *lv;
	/* List of N-1 equality op Oids for N-key skipscan, stays NIL for one-key skipscan */
	List *eqcomps = NIL;
	/* List of N skipkeyinfo Int lists for N-key skipscan */
	List *skinfos = NIL;
	forboth (lc, path->skipkeyinfo, lv, path->dvars)
	{
		SkipKeyInfo *skinfo = (SkipKeyInfo *) lfirst(lc);
		Var *dvar = castNode(Var, lfirst(lv));
		OpExpr *op =
			fix_indexqual(index_path->indexinfo, skinfo->skip_clause, skinfo->scankey_attno);
		if (OidIsValid(skinfo->eqcomp))
			eqcomps = lappend_oid(eqcomps, skinfo->eqcomp);

		if (IsA(plan, IndexScan))
		{
			IndexScan *idx_plan = castNode(IndexScan, plan);
			/* we prepend skip qual here so sort_indexquals will put it as first qual for that
			 * column */
			idx_plan->indexqual =
				sort_indexquals(index_path->indexinfo, lcons(op, idx_plan->indexqual));
		}
		else if (IsA(plan, IndexOnlyScan))
		{
			IndexOnlyScan *idx_plan = castNode(IndexOnlyScan, plan);
			/* we prepend skip qual here so sort_indexquals will put it as first qual for that
			 * column */
			idx_plan->indexqual =
				sort_indexquals(index_path->indexinfo, lcons(op, idx_plan->indexqual));
		}
		else
			elog(ERROR,
				 "unsupported subplan type for SkipScan: %s",
				 ts_get_node_name((Node *) plan));

		/* get position of distinct column in tuples produced by child scan */
		TargetEntry *tle = tlist_member_match_var(dvar, child_plan->targetlist);

		SkipKeyNullStatus sknulls;
		if (skinfo->notnull)
			sknulls = SK_NOT_NULL;
		else
		{
			bool nulls_first = index_path->indexinfo->nulls_first[skinfo->scankey_attno - 1];
			if (index_path->indexscandir == BackwardScanDirection)
				nulls_first = !nulls_first;
			sknulls = (nulls_first ? SK_NULLS_FIRST : SK_NULLS_LAST);
		}
		skinfos = lappend(skinfos,
						  list_make5_int(tle->resno,
										 skinfo->distinct_by_val,
										 skinfo->distinct_typ_len,
										 sknulls,
										 skinfo->scankey_attno));
		/* Debug info about skip key */
		if (ts_guc_debug_skip_scan_info)
		{
			char *attname = get_attname(indexed_rte->relid, skinfo->indexed_column_attno, false);
			appendStringInfo(&debuginfo,
							 "%s%s %s",
							 sep,
							 attname,
							 (sknulls == SK_NOT_NULL ?
								  "NOT NULL" :
								  (sknulls == SK_NULLS_FIRST ? "NULLS FIRST" : "NULLS LAST")));
			sep = ", ";
		}
	}

	if (ts_guc_debug_skip_scan_info)
	{
		appendStringInfoString(&debuginfo, ")");
		elog(INFO, "%s", debuginfo.data);
	}

	skip_plan->custom_private = lappend(skip_plan->custom_private, skinfos);
	/* Don't need equality ops for one-key skipscan */
	if (eqcomps != NIL)
	{
		Assert(list_length(skinfos) > 1);
		skip_plan->custom_private = lappend(skip_plan->custom_private, eqcomps);
	}

	return &skip_plan->scan.plan;
}

/*************************
 * SkipScanPath Creation *
 *************************/
static CustomPathMethods skip_scan_path_methods = {
	.CustomName = "SkipScanPath",
	.PlanCustomPath = skip_scan_plan_create,
};

#if PG16_GE
typedef struct FindAggrefsContext
{
	List *aggrefs; /* all non-nested Aggrefs found in a node */
} FindAggrefsContext;

static bool
find_aggrefs_walker(Node *node, FindAggrefsContext *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Aggref))
	{
		context->aggrefs = lappend(context->aggrefs, node);
		/* don't recurse inside Aggrefs */
		return false;
	}

	return expression_tree_walker(node, find_aggrefs_walker, context);
}
#endif

static Expr *
get_distint_clause_expr(PlannerInfo *root, SortGroupClause *distinct_clause)
{
	Node *expr = get_sortgroupclause_expr(distinct_clause, root->parse->targetList);

	/* we ignore any columns that can be constified to allow for cases like DISTINCT 'abc',
	 * column */
	if (IsA(estimate_expression_value(root, expr), Const))
		return NULL;

	/* We ignore binary-compatible relabeling */
	Expr *tlexpr = (Expr *) expr;
	while (tlexpr && IsA(tlexpr, RelabelType))
		tlexpr = ((RelabelType *) tlexpr)->arg;

	if (!IsA(tlexpr, Var))
		return NULL;

	return tlexpr;
}

/* We can get upper path Distinct expression once for upper path,
 * rather than repeat this check for each child path of an upper path input
 */
static List *
get_upper_distinct_expr(PlannerInfo *root, UpperRelationKind stage)
{
	ListCell *lc;
	Expr *tlexpr = NULL;
	List *result = NULL;

	if (stage == UPPERREL_DISTINCT && root->parse->distinctClause)
	{
		/* Obtain Distinct key from the target list, we ruled out numkeys > 1 cases before.
		 * Examples of queries with 1 Distinct key but multiple target entries:
		 * SELECT dev, dev FROM t; SELECT 1, dev FROM t; SELECT dev, time FROM t WHERE time = 100;
		 */
		SortGroupClause *distinct_clause = NULL;
#if PG16_GE
		foreach (lc, root->processed_distinctClause)
		{
			distinct_clause = (SortGroupClause *) lfirst(lc);
			tlexpr = get_distint_clause_expr(root, distinct_clause);
			if (tlexpr)
				result = lappend(result, tlexpr);
			else
				return NULL;
		}
#else
		if (root->distinct_pathkeys)
		{
			foreach (lc, root->distinct_pathkeys)
			{
				PathKey *pathkey = (PathKey *) lfirst(lc);
				if (pathkey->pk_eclass->ec_sortref)
				{
					foreach (lc, root->parse->distinctClause)
					{
						SortGroupClause *clause = lfirst_node(SortGroupClause, lc);
						if (clause->tleSortGroupRef == pathkey->pk_eclass->ec_sortref)
						{
							distinct_clause = clause;
							break;
						}
					}
					if (!distinct_clause)
						return NULL;
				}
				/* We can get PathKey with ec_sortref = 0 in PG15
				 * when False filter is not pushed into a relation with distinct column (i.e. it's
				 * on top of a join), so need to support this case in PG15
				 */
				else
					return NULL;

				tlexpr = get_distint_clause_expr(root, distinct_clause);
				if (tlexpr)
					result = lappend(result, tlexpr);
				else
					return NULL;
			}
		}
		/* In PG16+ we use LIMIT instead of UpperUniquePath for (numkeys = 0),
		 * but in PG15- we would still create UpperUniquePath for (numkeys = 0), so handle this case
		 * here
		 */
		else
		{
			foreach (lc, root->parse->distinctClause)
			{
				distinct_clause = lfirst_node(SortGroupClause, lc);
				tlexpr = get_distint_clause_expr(root, distinct_clause);
				if (tlexpr)
					result = lappend(result, tlexpr);
				else
					return NULL;
			}
		}
#endif
	}
#if PG16_GE
	else if (stage == UPPERREL_GROUP_AGG)
	{
		/* Find all non-nested Aggrefs in the query target list */
		FindAggrefsContext agg_ctx = { .aggrefs = NULL };
		find_aggrefs_walker((Node *) root->parse->targetList, &agg_ctx);

		foreach (lc, agg_ctx.aggrefs)
		{
			Aggref *agg = lfirst_node(Aggref, lc);
			/* Only distinct aggs with 1 sorted argument are eligible*/
			if (agg->aggdistinct && agg->aggpresorted && list_length(agg->args) == 1)
			{
				TargetEntry *tle = (TargetEntry *) linitial(agg->args);

				Expr *expr = tle->expr;
				/* We ignore binary-compatible relabeling */
				while (expr && IsA(expr, RelabelType))
					expr = ((RelabelType *) expr)->arg;

				/* Distinct agg over a Const is OK */
				if (IsA(estimate_expression_value(root, (Node *) expr), Const))
					continue;

				/* Don't support no-var arguments */
				if (!IsA(expr, Var))
					return NULL;

				/* Don't support multiple distinct aggs over different columns */
				if (tlexpr && !tlist_member_match_var((Var *) tlexpr, agg->args))
					return NULL;

				/* If Distinct agg path has a groupby column, it needs to match Distinct agg column
				 */
				if (root->processed_groupClause)
				{
					/* Should have bailed out on gby exprs > 1 earlier
					 * Only 1-key SkipScan is supported for distinct aggregates
					 */
					Assert(list_length(root->processed_groupClause) == 1);
					SortGroupClause *sortcl =
						(SortGroupClause *) linitial(root->processed_groupClause);
					Expr *gbykey = (Expr *) get_sortgroupclause_expr(sortcl, root->processed_tlist);
					if (!equal(gbykey, expr))
						return NULL;
				}

				/* Found a valid distinct agg over a valid Var */
				if (!tlexpr)
				{
					tlexpr = expr;
					result = lappend(result, tlexpr);
				}
			}
			else
			{
				return NULL;
			}
		}
	}
#endif
	return result;
}

static void
obtain_upper_distinct_path(PlannerInfo *root, RelOptInfo *output_rel, DistinctPathInfo *dpinfo)
{
	ListCell *lc;

	/*
	 * look for Unique Path so we dont have to repeat some of
	 * the calculations done by postgres and can also assume
	 * that the DISTINCT clause is eligible for sort based
	 * DISTINCT
	 */
	if (dpinfo->stage == UPPERREL_DISTINCT)
	{
		if (!ts_guc_enable_skip_scan)
			return;

		foreach (lc, output_rel->pathlist)
		{
			if (IsA(lfirst(lc), UpperUniquePath))
			{
				UpperUniquePath *unique = (UpperUniquePath *) lfirst_node(UpperUniquePath, lc);

				/* We can handle DISTINCT on more than one key if all keys are guaranteed not-nulls.
				 * To do so, we break down the SkipScan into subproblems: first
				 * find the minimal tuple then for each prefix find all unique suffix
				 * tuples. For instance, if we are searching over (int, int), we would
				 * first find (0, 0) then find (0, N) for all N in the domain, then
				 * find (1, N), then (2, N), etc
				 */
				if (!ts_guc_enable_multikey_skip_scan && unique->numkeys > 1)
					return;

#if PG16_GE
				/* since PG16+ we no longer create UpperUniquePath with 0 numkeys,
				 * we create LIMIT path instead, so shouldn't be here with 0 numkeys
				 */
				Assert(unique->numkeys >= 1);
#endif
				dpinfo->unique_path = (Path *) unique;
				break;
			}
		}
	}
	/* Sorted inputs for Distinct aggs weren't supported until PG16 */
#if PG16_GE
	/* Look for Aggpath with eligible Distinct aggregates */
	else if (dpinfo->stage == UPPERREL_GROUP_AGG)
	{
		if (!ts_guc_enable_skip_scan_for_distinct_aggregates)
			return;

		/* Cannot apply SkipScan to distinct aggregates with more than one key */
		if (list_length(root->group_pathkeys) > 1)
			return;

		foreach (lc, output_rel->pathlist)
		{
			if (IsA(lfirst(lc), AggPath))
			{
				AggPath *unique = (AggPath *) lfirst_node(AggPath, lc);

				/* If Distinct agg path has a group key, it must match Distinct aggregate input sort
				 * key, otherwise cannot apply SkipScan
				 */
				if (unique->path.pathkeys &&
					!pathkeys_contained_in(unique->path.pathkeys, unique->subpath->pathkeys))
				{
					return;
				}

				dpinfo->unique_path = (Path *) lfirst_node(AggPath, lc);
				break;
			}
		}
	}
#endif
	else
		return;

	if (!dpinfo->unique_path)
		return;

	/* Check if we have valid distinct expression to source from the underlying index */
	dpinfo->distinct_expr = get_upper_distinct_expr(root, dpinfo->stage);
	if (!dpinfo->distinct_expr)
	{
		dpinfo->unique_path = NULL;
		return;
	}

	/* Need to make a copy of the unique path here because add_path() in the
	 * pathlist loop below might prune it if the new unique path
	 * (SkipScanPath) dominates the old one. When the unique path is pruned,
	 * the pointer will no longer be valid in the next iteration of the
	 * pathlist loop. Fortunately, the Path object is not deeply freed, so a
	 * shallow copy is enough. */
	if (dpinfo->stage == UPPERREL_DISTINCT)
	{
		UpperUniquePath *unique = makeNode(UpperUniquePath);
		memcpy(unique, lfirst_node(UpperUniquePath, lc), sizeof(UpperUniquePath));
		dpinfo->unique_path = (Path *) unique;
	}
	else if (dpinfo->stage == UPPERREL_GROUP_AGG)
	{
		AggPath *dist_agg_path = makeNode(AggPath);
		memcpy(dist_agg_path, lfirst_node(AggPath, lc), sizeof(AggPath));
		dpinfo->unique_path = (Path *) dist_agg_path;
	}
}

static SkipScanPath *skip_scan_path_create(PlannerInfo *root, Path *child_path,
										   DistinctPathInfo *dpinfo);

/*
 * Create SkipScan paths based on existing Unique paths.
 * For a Unique path on a simple relation like the following
 *
 *  Unique
 *    ->  Index Scan using skip_scan_dev_name_idx on skip_scan
 *
 * a SkipScan path like this will be created:
 *
 *  Unique
 *    ->  Custom Scan (SkipScan) on skip_scan
 *          ->  Index Scan using skip_scan_dev_name_idx on skip_scan
 *
 * For a Unique path on a hypertable with multiple chunks like the following
 *
 *  Unique
 *    ->  Merge Append
 *          Sort Key: _hyper_2_1_chunk.dev_name
 *          ->  Index Scan using _hyper_2_1_chunk_idx on _hyper_2_1_chunk
 *          ->  Index Scan using _hyper_2_2_chunk_idx on _hyper_2_2_chunk
 *
 * a SkipScan path like this will be created:
 *
 *  Unique
 *    ->  Merge Append
 *          Sort Key: _hyper_2_1_chunk.dev_name
 *          ->  Custom Scan (SkipScan) on _hyper_2_1_chunk
 *                ->  Index Scan using _hyper_2_1_chunk_idx on _hyper_2_1_chunk
 *          ->  Custom Scan (SkipScan) on _hyper_2_2_chunk
 *                ->  Index Scan using _hyper_2_2_chunk_idx on _hyper_2_2_chunk
 */
void
tsl_skip_scan_paths_add(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel,
						UpperRelationKind stage)
{
	DistinctPathInfo dpinfo = {
		.stage = stage,
		.unique_path = NULL,
		.distinct_expr = NULL,
	};

	obtain_upper_distinct_path(root, output_rel, &dpinfo);
	if (!dpinfo.unique_path)
		return;

	Assert(IsA(dpinfo.unique_path, UpperUniquePath) || IsA(dpinfo.unique_path, AggPath));
	ListCell *lc;
	foreach (lc, input_rel->pathlist)
	{
		bool has_caa = false;

		Path *subpath = lfirst(lc);

		List *top_pathkeys = NULL;

		/* Unique path has to be sorted on at least DISTINCT ON key */
		if (IsA(dpinfo.unique_path, UpperUniquePath))
		{
			if (!pathkeys_contained_in(dpinfo.unique_path->pathkeys, subpath->pathkeys))
				continue;
		}
		/* AggPath with distinct aggs may not be sorted, but the input into distinct aggs needs to
		 * be sorted */
		else if (IsA(dpinfo.unique_path, AggPath))
		{
			if (!subpath->pathkeys ||
				!pathkeys_contained_in(dpinfo.unique_path->pathkeys, subpath->pathkeys))
				continue;
			/* Need to check sortedness for inputs of Distinct aggs, so we'll keep track of the
			 * input pathkeys  */
			top_pathkeys = subpath->pathkeys;
		}

		/* If path is a ProjectionPath we strip it off for processing
		 * but also add a ProjectionPath on top of the SKipScanPaths
		 * later.
		 */
		ProjectionPath *proj = NULL;
		if (IsA(subpath, ProjectionPath))
		{
			proj = castNode(ProjectionPath, subpath);
			subpath = proj->subpath;
		}

		/* Path might be wrapped in a ConstraintAwareAppendPath if this
		 * is a MergeAppend that could benefit from runtime exclusion.
		 * We treat this similar to ProjectionPath and add it back
		 * later
		 */
		if (ts_is_constraint_aware_append_path(subpath))
		{
			subpath = linitial(castNode(CustomPath, subpath)->custom_paths);
			Assert(IsA(subpath, MergeAppendPath));
			has_caa = true;
		}

		if (IsA(subpath, IndexPath) || ts_is_decompress_chunk_path(subpath))
		{
			subpath = (Path *) skip_scan_path_create(root, subpath, &dpinfo);
			if (!subpath)
				continue;
		}
		else if (IsA(subpath, MergeAppendPath))
		{
			MergeAppendPath *merge_path = castNode(MergeAppendPath, subpath);

			List *new_paths = build_subpath(root, merge_path->subpaths, &dpinfo, top_pathkeys);

			/* build_subpath returns NULL when no SkipScanPath was created */
			if (!new_paths)
				continue;

			subpath = (Path *) create_merge_append_path(root,
														merge_path->path.parent,
														new_paths,
														merge_path->path.pathkeys,
														NULL);
			subpath->pathtarget = copy_pathtarget(merge_path->path.pathtarget);
		}
		/* We may have Append over one input which will be removed from the plan later.
		 * Consider it when it is sorted correctly. #7778
		 */
		else if (IsA(subpath, AppendPath))
		{
			AppendPath *append_path = castNode(AppendPath, subpath);

			if (list_length(append_path->subpaths) > 1)
				continue;

			List *new_paths = build_subpath(root, append_path->subpaths, &dpinfo, top_pathkeys);

			/* build_subpath returns NULL when no SkipScanPath was created */
			if (!new_paths)
				continue;

			subpath = (Path *) create_append_path(root,
												  append_path->path.parent,
												  new_paths,
												  NULL,
												  append_path->path.pathkeys,
												  NULL,
												  append_path->path.parallel_workers,
												  append_path->path.parallel_aware,
												  -1);
			subpath->pathtarget = copy_pathtarget(append_path->path.pathtarget);
		}
		else if (ts_is_chunk_append_path(subpath))
		{
			ChunkAppendPath *ca = (ChunkAppendPath *) subpath;
			List *new_paths = build_subpath(root, ca->cpath.custom_paths, &dpinfo, top_pathkeys);
			/* ChunkAppend should never be wrapped in ConstraintAwareAppendPath */
			Assert(!has_caa);

			/* build_subpath returns NULL when no SkipScanPath was created */
			if (!new_paths)
				continue;

			/* We copy the existing ChunkAppendPath here because we don't have all the
			 * information used for creating the original one and we don't want to
			 * duplicate all the checks done when creating the original one.
			 */
			subpath = (Path *) ts_chunk_append_path_copy(ca, new_paths, ca->cpath.path.pathtarget);
		}
		else
		{
			continue;
		}

		/* add ConstraintAwareAppendPath if the original path had one */
		if (has_caa)
			subpath = ts_constraint_aware_append_path_create(root, subpath);

		Path *new_unique = NULL;

		if (IsA(dpinfo.unique_path, UpperUniquePath))
		{
			UpperUniquePath *unique = (UpperUniquePath *) dpinfo.unique_path;
			new_unique = (Path *) create_upper_unique_path(root,
														   output_rel,
														   subpath,
														   unique->numkeys,
														   unique->path.rows);
			new_unique->pathtarget = unique->path.pathtarget;

			if (proj)
				new_unique =
					(Path *) create_projection_path(root,
													output_rel,
													new_unique,
													copy_pathtarget(new_unique->pathtarget));
		}
		else if (IsA(dpinfo.unique_path, AggPath))
		{
			AggPath *dist_agg_path = (AggPath *) dpinfo.unique_path;
			if (proj)
			{
				proj->subpath = subpath;
				subpath = (Path *) proj;
			}
			AggClauseCosts agg_costs;
			MemSet(&agg_costs, 0, sizeof(AggClauseCosts));
			get_agg_clause_costs(root, dist_agg_path->aggsplit, &agg_costs);
			new_unique = (Path *) create_agg_path(root,
												  output_rel,
												  subpath,
												  dist_agg_path->path.pathtarget,
												  dist_agg_path->aggstrategy,
												  dist_agg_path->aggsplit,
												  dist_agg_path->groupClause,
												  dist_agg_path->qual,
												  (const AggClauseCosts *) &agg_costs,
												  dist_agg_path->numGroups);
		}

		add_path(output_rel, new_unique);
	}
}

#if PG17_LT
static bool
attr_is_notnull(Oid relid, AttrNumber attno)
{
	HeapTuple tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attno));
	if (!HeapTupleIsValid(tp))
		return false;
	Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
	bool result = att_tup->attnotnull;
	ReleaseSysCache(tp);
	return result;
}
#endif

/* Check if skip key is guaranteed not-null */
static void
check_notnull_skipkey(SkipKeyInfo *skinfo, Path *child_path, IndexPath *index_path)
{
	ListCell *l;
	/* Quickly look through index clauses on this skip key */
	foreach (l, index_path->indexclauses)
	{
		IndexClause *ic = (IndexClause *) lfirst(l);
		/* index quals are ordered by indexcol, nothing to see if we've passed our indexcol */
		if (ic->indexcol > skinfo->scankey_attno - 1)
			break;

		/* Lossy index quals may not cover NULL values but BTree quals are never lossy  */
		Assert(!ic->lossy);

		/* We may have row comparison with skip key not being a leading col,
		 * like (col, skipcol) > (3, 5), but it can allow NULL skipcols to pass if (col>3) is true,
		 * so for row comparisons we will only look at leading "indexcol" and not at "indexcols".
		 */
		if (ic->indexcol == skinfo->scankey_attno - 1)
		{
			/* Any simple index clause but "isNull" filters out nulls */
			ListCell *lc;
			foreach (lc, ic->indexquals)
			{
				RestrictInfo *iqual = (RestrictInfo *) lfirst(lc);
				if (!(IsA(iqual->clause, NullTest) &&
					  ((NullTest *) iqual->clause)->nulltesttype == IS_NULL))
				{
					skinfo->notnull = true;
					return;
				}
			}
		}
	}

	/* Otherwise look at all non-indexqual index filters on the key (like (key+1)>5) to see if they
	 * filter out NULLs */
	RelOptInfo *indexed_rel = index_path->path.parent;
	foreach (l, index_path->indexinfo->indrestrictinfo)
	{
		RestrictInfo *ri = castNode(RestrictInfo, lfirst(l));
		Bitmapset *clause_attnos = NULL;
		pull_varattnos((Node *) ri->clause, indexed_rel->relid, &clause_attnos);
		if (bms_is_member(skinfo->indexed_column_attno - FirstLowInvalidHeapAttributeNumber,
						  clause_attnos))
		{
			if (!contain_nonstrict_functions((Node *) ri->clause))
			{
				skinfo->notnull = true;
				return;
			}
		}
	}

	/* Failing that, look at filters not pushed down into index (like col1+col2>1) to see if they
	 * filter out NULLs */
	RelOptInfo *child_rel = child_path->parent;
	foreach (l, child_rel->baserestrictinfo)
	{
		RestrictInfo *ri = castNode(RestrictInfo, lfirst(l));
		Bitmapset *clause_attnos = NULL;
		pull_varattnos((Node *) ri->clause, child_rel->relid, &clause_attnos);
		if (bms_is_member(skinfo->distinct_attno - FirstLowInvalidHeapAttributeNumber,
						  clause_attnos))
		{
			if (!contain_nonstrict_functions((Node *) ri->clause))
			{
				skinfo->notnull = true;
				return;
			}
		}
	}
}

static IndexPath *
get_compressed_index_path(ColumnarScanPath *dcpath)
{
	Path *compressed_path = linitial(dcpath->custom_path.custom_paths);
	if (IsA(compressed_path, IndexPath))
	{
		IndexPath *index_path = castNode(IndexPath, compressed_path);
		if (!pathkeys_contained_in(dcpath->required_compressed_pathkeys, compressed_path->pathkeys))
			return NULL;

		return index_path;
	}
	return NULL;
}

static SkipScanPath *
skip_scan_path_create(PlannerInfo *root, Path *child_path, DistinctPathInfo *dpinfo)
{
	IndexPath *index_path = NULL;

	if (IsA(child_path, IndexPath))
	{
		index_path = castNode(IndexPath, child_path);
	}
	else if (ts_is_decompress_chunk_path(child_path))
	{
		if (!ts_guc_enable_compressed_skip_scan)
			return NULL;

		ColumnarScanPath *dcpath = (ColumnarScanPath *) child_path;
		index_path = get_compressed_index_path(dcpath);
	}
	if (!index_path)
		return NULL;

	/* cannot use SkipScan with non-orderable index or IndexPath without pathkeys */
	if (!index_path->path.pathkeys || !index_path->indexinfo->sortopfamily)
		return NULL;

	/* orderbyops are not compatible with skipscan */
	if (index_path->indexorderbys != NIL)
		return NULL;

	SkipScanPath *skip_scan_path = (SkipScanPath *) newNode(sizeof(SkipScanPath), T_CustomPath);
	skip_scan_path->cpath.path.pathtype = T_CustomScan;
	skip_scan_path->cpath.path.pathkeys = child_path->pathkeys;
	skip_scan_path->cpath.path.pathtarget = child_path->pathtarget;
	skip_scan_path->cpath.path.param_info = child_path->param_info;
	skip_scan_path->cpath.path.parent = child_path->parent;
	skip_scan_path->cpath.custom_paths = list_make1(child_path);
	skip_scan_path->cpath.methods = &skip_scan_path_methods;

	/* While add_path may pfree paths with higher costs
	 * it will never free IndexPaths and only ever do a shallow
	 * free so reusing the IndexPath here is safe. */
	skip_scan_path->index_path = index_path;

	ListCell *lc;
	int sk_no = 0;
	int num_skipkeys = list_length(dpinfo->distinct_expr);
	foreach (lc, dpinfo->distinct_expr)
	{
		Expr *dexpr = (Expr *) lfirst(lc);

		/* Placeholder for skip key attributes */
		SkipKeyInfo *skinfo = palloc(sizeof(SkipKeyInfo));
		Var *dvar = get_distinct_var(root, dexpr, index_path, child_path, skinfo);
		if (!dvar)
		{
			pfree(skinfo);
			return NULL;
		}

		/* build skip qual this may fail if we cannot look up the operator */
		if (!build_skip_qual(root, skinfo, index_path, dvar, (++sk_no) < num_skipkeys))
		{
			pfree(skinfo);
			return NULL;
		}
		if (!skinfo->notnull)
			check_notnull_skipkey(skinfo, child_path, index_path);

		/* Multikey SkipScan is only supported in not-null mode */
		if (!skinfo->notnull && num_skipkeys > 1)
			return NULL;

		skip_scan_path->dvars = lappend(skip_scan_path->dvars, dvar);
		skip_scan_path->skipkeyinfo = lappend(skip_scan_path->skipkeyinfo, skinfo);
	}

	/* We have valid SkipScanPath: now we can cost it */
	double startup = child_path->startup_cost;
	double total = child_path->total_cost;
	double rows = child_path->rows;
	double indexscan_rows = index_path->path.rows;

	/* Also true for SkipScan over compressed chunks as can't have more distinct segmentby values
	 * than number of batches */
	int ndistinct = indexscan_rows;
	/* For SELECT DISTINCT path, #rows can cap "ndistinct",
	 * but for Distinct aggregates #rows = 1 usually, i.e. we can't cap "ndistinct" in this case.
	 */
	if (dpinfo->stage == UPPERREL_DISTINCT)
		ndistinct = Min(ndistinct, dpinfo->unique_path->rows);

	/* If we are on a chunk rather than on a PG table, we want to get "ndistinct" for this chunk,
	 * as Unique path rows may combine rows from each chunk and may not represent a true
	 * "ndistinct". Consider a hypertable with 1000 chunks, each chunk has the same 1 distinct
	 * value, Unique path will add them up and we will get "ndistinct" = 1000 instead of 1. If
	 * Unique path has "ndistinct=1" we can't go any smaller so will just accept this number.
	 */
	if (ndistinct > 1)
	{
		ndistinct =
			Max(1, floor(estimate_num_groups(root, skip_scan_path->dvars, ndistinct, NULL, NULL)));
	}
	skip_scan_path->cpath.path.rows = ndistinct;

	/* Addressing #8107: filters on the indexed data which are not index quals
	 * will require sequential scanning of indexed tuples until finding a tuple passing the filter.
	 * For some highly selective filters it may mean scanning a lot of tuples, sometimes the entire
	 * input. SeqScan may perform better than IndexScan for such filters. We need to account for
	 * such filters in the cost model i.e. cost the number of tuples to scan before passing the
	 * filter.
	 */
	List *clauses_needing_scan = NULL;
	/* If a filter is not pushed down into compessed indexed data, it's a filter for which we will
	 * need to scan and decompress until filter is passed */
	if ((Path *) index_path != child_path)
		clauses_needing_scan = child_path->parent->baserestrictinfo;
	else
	{
		/* For uncompressed index data we need a finer check for which filters on the indexed data
		 * are index quals or not */
		ListCell *lc;
		foreach (lc, index_path->indexinfo->indrestrictinfo)
		{
			RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
			bool match_found = false;
			ListCell *l;
			foreach (l, index_path->indexclauses)
			{
				IndexClause *ic = (IndexClause *) lfirst(l);
				if (ri == ic->rinfo)
				{
					match_found = true;
					break;
				}
			}
			/* This is a filter which is not an index qual: will have to scan indexed data until
			 * this filter is passed */
			if (!match_found)
				clauses_needing_scan = lappend(clauses_needing_scan, ri);
		}
	}

	/* Heuristic for accounting for previous key shifts in multikey SkipScan
	 * In general, we will have (k_1*k_2*..*k_N + k_1*..*k_N-1 + ... + k_1) shifts
	 * where numdistinct = k_1*k_2*..*k_N and for each key "k_i" we have "k_i+1" distinct values.
	 *
	 * Worst case scenario is when k_1 = numdistinct and the rest =1, then we'll have (numdistinct *
	 * N) shifts. If it's uniform for each key i.e. numdistinct = k^N we'll have less than
	 * (numdistinct * 2) shifts. We pick something in the middle to avoid evaluating k_i for each
	 * key.
	 */
	int numkeys_multiplier = 1.0 + (num_skipkeys - 1) * 0.5;

	/* We calculate SkipScan cost as ndistinct * startup_cost + (ndistinct/rows) * total_cost
	 * ndistinct * startup_cost is to account for the rescans we have to do and since startup
	 * cost for indexes does not include page access cost we add a fraction of the total cost
	 * accounting for the number of rows we expect to fetch.
	 * If the row estimate for the scan is 1 we assume that the estimate got clamped to 1
	 * and no rows would be returned by this scan and this chunk will most likely be excluded
	 * by runtime exclusion. Otherwise the cost for this path would be highly inflated due
	 * to (ndistinct / rows) * total leading to SkipScan not being chosen for queries on
	 * hypertables with a lot of excluded chunks.
	 *
	 * This is the cost of (SkipScan <- IndexScan) scenario
	 */
	if (clauses_needing_scan == NULL && (Path *) index_path == child_path)
	{
		skip_scan_path->cpath.path.startup_cost = startup;
		if (indexscan_rows > 1)
			skip_scan_path->cpath.path.total_cost =
				ndistinct * startup * numkeys_multiplier + (ndistinct / rows) * total;
		else
			skip_scan_path->cpath.path.total_cost = startup;
	}
	/* For (SkipScan <- DecompressChunks <- compressed IndexScan) scenario
	 * we will estimate cost as (ndistinct * costs( child_path LIMIT 1 OFFSET x))
	 * i.e. as if we computed "ndistinct" LIMIT 1 queries on the "child_path" after initial setup.
	 * If there is no qual above IndexScan, then OFFSET=0 (we don't need to scan tuples to pass qual
	 * before returning the 1st tuple), otherwise OFFSET = (1 / qual_selectivity - 1), i.e. we have
	 * to skip OFFSET tuples until we get the one which passes the qual.
	 */
	else
	{
		int64 offset_until_qual_pass = 0;
		if (clauses_needing_scan != NULL)
		{
			/* Avoid division by qual_selectivity = 0.0 */
			Selectivity qual_selectivity =
				Max(1.0 / (rows + 1),
					clauselist_selectivity(root, clauses_needing_scan, 0, JOIN_INNER, NULL));
			offset_until_qual_pass = Max(0, floor(1 / qual_selectivity));
		}
		adjust_limit_rows_costs(&rows, &startup, &total, offset_until_qual_pass, 1);

		skip_scan_path->cpath.path.startup_cost = startup;
		if (indexscan_rows > 1)
			skip_scan_path->cpath.path.total_cost =
				startup + (total - startup) * ndistinct * numkeys_multiplier;
		else
			skip_scan_path->cpath.path.total_cost = startup;
	}

	/* Finally, adjust SkipScan run costs with GUC multiplier (1.0 by default), to give users more
	 * control over choosing SkipScan */
	skip_scan_path->cpath.path.total_cost =
		startup +
		(skip_scan_path->cpath.path.total_cost - startup) * ts_guc_skip_scan_run_cost_multiplier;

	return skip_scan_path;
}

/* Extract the Var to use for the SkipScan and do attno mapping if required. */
static Var *
get_distinct_var(PlannerInfo *root, Expr *tlexpr, IndexPath *index_path, Path *child_path,
				 SkipKeyInfo *skinfo)
{
	RelOptInfo *rel = child_path->parent;
	RelOptInfo *indexed_rel = index_path->path.parent;

	Assert(tlexpr && IsA(tlexpr, Var));
	Var *var = castNode(Var, tlexpr);

	RangeTblEntry *ht_rte = planner_rt_fetch(var->varno, root);

	/* check whether a skip var is declared NOT NULL
	 *  it's enough to check hypertable for NOT NULL
	 *  as NOT NULL constraint will be propagated to and checked on all chunks
	 */
#if PG17_LT
	skinfo->notnull = attr_is_notnull(ht_rte->relid, var->varattno);
#else
	RelOptInfo *baserel = ((Index) var->varno == rel->relid ? rel : rel->parent);
	skinfo->notnull = bms_is_member(var->varattno, baserel->notnullattnums);
#endif

	/* If we are dealing with a hypertable Var extracted from distinctClause will point to
	 * the parent hypertable while the IndexPath will be on a Chunk.
	 * For a normal PG table they point to the same relation and we are done here. */
	if ((Index) var->varno == rel->relid)
	{
		/* Get attribute number for distinct column on a normal PG table */
		skinfo->indexed_column_attno = var->varattno;
		return var;
	}

	RangeTblEntry *chunk_rte = planner_rt_fetch(rel->relid, root);
	RangeTblEntry *indexed_rte =
		(indexed_rel == rel ? chunk_rte : planner_rt_fetch(indexed_rel->relid, root));

	/* Check for hypertable */
	if (!ts_is_hypertable(ht_rte->relid) || !bms_is_member(var->varno, rel->top_parent_relids))
		return NULL;

	char *attname = get_attname(ht_rte->relid, var->varattno, false);
	var = copyObject(var);
	var->varattno = get_attnum(chunk_rte->relid, attname);

	/* Get attribute number for distinct column on a compressed chunk */
	if (ts_is_decompress_chunk_path(child_path))
	{
		/* distinct column has to be a segmentby column */
		ColumnarScanPath *dcpath = (ColumnarScanPath *) child_path;
		if (!bms_is_member(var->varattno, dcpath->info->chunk_segmentby_attnos))
		{
			return NULL;
		}
		skinfo->indexed_column_attno = get_attnum(indexed_rte->relid, attname);
	}
	/* Get attribute number for distinct column on an uncompressed chunk */
	else
	{
		skinfo->indexed_column_attno = var->varattno;
	}

	var->varno = rel->relid;

	return var;
}

/*
 * Creates SkipScanPath for each path of subpaths that is an IndexPath
 * If no subpath can be changed to SkipScanPath returns NULL
 * otherwise returns list of new paths
 */
static List *
build_subpath(PlannerInfo *root, List *subpaths, DistinctPathInfo *dpinfo, List *top_pathkeys)
{
	bool has_skip_path = false;
	List *new_paths = NIL;
	ListCell *lc;

	foreach (lc, subpaths)
	{
		Path *child = lfirst(lc);
		if (IsA(child, IndexPath) || ts_is_decompress_chunk_path(child))
		{
			if (top_pathkeys && !pathkeys_contained_in(top_pathkeys, child->pathkeys))
				continue;

			SkipScanPath *skip_path = skip_scan_path_create(root, child, dpinfo);

			if (skip_path)
			{
				child = (Path *) skip_path;
				has_skip_path = true;
			}
		}

		new_paths = lappend(new_paths, child);
	}

	if (!has_skip_path && new_paths)
	{
		pfree(new_paths);
		return NIL;
	}

	return new_paths;
}

static bool
build_skip_qual(PlannerInfo *root, SkipKeyInfo *skinfo, IndexPath *index_path, Var *var,
				bool build_eqop)
{
	IndexOptInfo *info = index_path->indexinfo;
	Oid column_type = exprType((Node *) var);
	Oid column_collation = get_typcollation(column_type);
	TypeCacheEntry *tce = lookup_type_cache(column_type, 0);
	bool need_coerce = false;

	/*
	 * Skipscan is not applicable for the following case:
	 * We might have a path with an index that produces the correct pathkeys for the target ordering
	 * without actually including all the columns of the ORDER BY. If the path uses an index that
	 * does not include the distinct column, we cannot use it for skipscan and have to discard this
	 * path from skipscan generation. This happens, for instance, when we have an order by clause
	 * (like ORDER BY a, b) with constraints in the WHERE clause (like WHERE a = <constant>) . "a"
	 * can now be removed from the Pathkeys (since it is a constant) and the query can be satisfied
	 * by using an index on just column "b".
	 *
	 * Example query:
	 * SELECT DISTINCT ON (a) * FROM test WHERE a in (2) ORDER BY a ASC, time DESC;
	 * Since a is always 2 due to the WHERE clause we can create the correct ordering for the
	 * ORDER BY with an index that does not include the a column and only includes the time column.
	 */
	int idx_key = get_idx_key(index_path->indexinfo, skinfo->indexed_column_attno);
	if (idx_key < 0)
		return false;

	/* sk_attno of the skip qual */
	skinfo->scankey_attno = idx_key + 1;

	skinfo->distinct_attno = var->varattno;
	skinfo->distinct_by_val = tce->typbyval;
	skinfo->distinct_typ_len = tce->typlen;

	int16 strategy = info->reverse_sort[idx_key] ? BTLessStrategyNumber : BTGreaterStrategyNumber;
	if (index_path->indexscandir == BackwardScanDirection)
	{
		strategy =
			(strategy == BTLessStrategyNumber) ? BTGreaterStrategyNumber : BTLessStrategyNumber;
	}
	Oid opcintype = info->opcintype[idx_key];

	Oid comparator =
		get_opfamily_member(info->sortopfamily[idx_key], column_type, column_type, strategy);

	Oid eqop = InvalidOid;
	if (build_eqop)
		eqop = get_opfamily_member(info->sortopfamily[idx_key],
								   column_type,
								   column_type,
								   BTEqualStrategyNumber);

	/* If there is no exact operator match for the column type we have here check
	 * if we can coerce to the type of the operator class. */
	if (!OidIsValid(comparator))
	{
		if (IsBinaryCoercible(column_type, opcintype))
		{
			comparator =
				get_opfamily_member(info->sortopfamily[idx_key], opcintype, opcintype, strategy);
			if (!OidIsValid(comparator))
				return false;

			if (build_eqop)
				eqop = get_opfamily_member(info->sortopfamily[idx_key],
										   opcintype,
										   opcintype,
										   BTEqualStrategyNumber);
			need_coerce = true;
		}
		else
			return false; /* cannot use this index */
	}

	Const *prev_val = makeNullConst(need_coerce ? opcintype : column_type, -1, column_collation);
	Expr *current_val = (Expr *) makeVar(info->rel->relid /*varno*/,
										 skinfo->indexed_column_attno /*varattno*/,
										 column_type /*vartype*/,
										 -1 /*vartypmod*/,
										 column_collation /*varcollid*/,
										 0 /*varlevelsup*/);

	if (need_coerce)
	{
		CoerceViaIO *coerce = makeNode(CoerceViaIO);
		coerce->arg = current_val;
		coerce->resulttype = opcintype;
		coerce->resultcollid = column_collation;
		coerce->coerceformat = COERCE_IMPLICIT_CAST;
		coerce->location = -1;

		current_val = &coerce->xpr;
	}

	Expr *comparison_expr = make_opclause(comparator,
										  BOOLOID /*opresulttype*/,
										  false /*opretset*/,
										  current_val /*leftop*/,
										  &prev_val->xpr /*rightop*/,
										  InvalidOid /*opcollid*/,
										  info->indexcollations[idx_key] /*inputcollid*/);
	set_opfuncid(castNode(OpExpr, comparison_expr));
	skinfo->eqcomp = (build_eqop ? get_opcode(eqop) : InvalidOid);

	skinfo->skip_clause = make_simple_restrictinfo(root, comparison_expr);

	return true;
}

static int
get_idx_key(IndexOptInfo *idxinfo, AttrNumber attno)
{
	for (int i = 0; i < idxinfo->nkeycolumns; i++)
	{
		if (attno == idxinfo->indexkeys[i])
			return i;
	}
	return -1;
}

/* Sort quals according to index column order.
 * ScanKeys need to be sorted by the position of the index column
 * they are referencing but since we don't want to adjust actual
 * ScanKey array we presort qual list when creating plan.
 */
static List *
sort_indexquals(IndexOptInfo *indexinfo, List *quals)
{
	List *indexclauses[INDEX_MAX_KEYS] = { 0 };
	List *ordered_list = NIL;
	ListCell *lc;
	int i;

	foreach (lc, quals)
	{
		Bitmapset *bms = NULL;
		pull_varattnos(lfirst(lc), INDEX_VAR, &bms);
		Assert(bms_num_members(bms) >= 1);

		i = bms_next_member(bms, -1) + FirstLowInvalidHeapAttributeNumber - 1;
		indexclauses[i] = lappend(indexclauses[i], lfirst(lc));
	}

	for (i = 0; i < indexinfo->nkeycolumns; i++)
	{
		if (indexclauses[i] != NIL)
			ordered_list = list_concat(ordered_list, indexclauses[i]);
	}

	return ordered_list;
}

static OpExpr *
fix_indexqual(IndexOptInfo *index, RestrictInfo *rinfo, AttrNumber scankey_attno)
{
	/* technically our placeholder col > NULL is unsatisfiable, and in some instances
	 * the planner will realize this and use is as an excuse to remove other quals.
	 * in order to prevent this, we prepare this qual ourselves.
	 */

	/* fix_indexqual_references */
	OpExpr *op = copyObject(castNode(OpExpr, rinfo->clause));
	Assert(list_length(op->args) == 2);
	Assert(bms_equal(rinfo->left_relids, index->rel->relids));

	/* fix_indexqual_operand */
	Assert(index->indexkeys[scankey_attno - 1] != 0);
	Var *node = linitial_node(Var, pull_var_clause(linitial(op->args), 0));

	Assert((Index) ((Var *) node)->varno == index->rel->relid &&
		   ((Var *) node)->varattno == index->indexkeys[scankey_attno - 1]);

	Var *result = (Var *) copyObject(node);
	result->varno = INDEX_VAR;
	result->varattno = scankey_attno;

	linitial(op->args) = result;

	return op;
}

/*
 * tlist_member_match_var
 *    Same as tlist_member, except that we match the provided Var on the basis
 *    of varno/varattno/varlevelsup/vartype only, rather than full equal().
 *
 * This is needed in some cases where we can't be sure of an exact typmod
 * match.  For safety, though, we insist on vartype match.
 *
 * static function copied from src/backend/optimizer/util/tlist.c
 */
static TargetEntry *
tlist_member_match_var(Var *var, List *targetlist)
{
	ListCell *temp;

	foreach (temp, targetlist)
	{
		TargetEntry *tlentry = (TargetEntry *) lfirst(temp);
		Var *tlvar = (Var *) tlentry->expr;

		if (!tlvar || !IsA(tlvar, Var))
			continue;
		if (var->varno == tlvar->varno && var->varattno == tlvar->varattno &&
			var->varlevelsup == tlvar->varlevelsup && var->vartype == tlvar->vartype)
			return tlentry;
	}
	return NULL;
}
