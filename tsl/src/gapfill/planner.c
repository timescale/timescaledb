/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planner.h>
#include <utils/lsyscache.h>
#include <parser/parse_func.h>

#include "license.h"
#include "gapfill/gapfill.h"
#include "gapfill/planner.h"
#include "gapfill/exec.h"

static CustomScanMethods gapfill_plan_methods = {
	.CustomName = "GapFill",
	.CreateCustomScanState = gapfill_state_create,
};

/*
 * Remove locf and interpolate toplevel function calls
 * and function calls inside window functions
 */
static inline Expr *
gapfill_clean_expr(Expr *expr)
{
	if (IsA(expr, FuncExpr))
	{
		if (strncmp(get_func_name(castNode(FuncExpr, expr)->funcid),
					GAPFILL_LOCF_FUNCTION,
					NAMEDATALEN) == 0)
			return linitial(castNode(FuncExpr, expr)->args);
		else if (strncmp(get_func_name(castNode(FuncExpr, expr)->funcid),
						 GAPFILL_INTERPOLATE_FUNCTION,
						 NAMEDATALEN) == 0)
			return linitial(castNode(FuncExpr, expr)->args);
	}

	if (IsA(expr, WindowFunc))
	{
		if (list_length(castNode(WindowFunc, expr)->args) == 1)
			linitial(castNode(WindowFunc, expr)->args) =
				gapfill_clean_expr(linitial(castNode(WindowFunc, expr)->args));
	}

	return expr;
}

typedef struct gapfill_walker_context
{
	FuncExpr *call;
	int num_calls;
} gapfill_walker_context;

/*
 * Find time_bucket_gapfill function call
 */
static bool
gapfill_function_call_walker(FuncExpr *node, gapfill_walker_context *context)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr) &&
		strncmp(get_func_name(node->funcid), GAPFILL_FUNCTION, NAMEDATALEN) == 0)
	{
		context->call = (FuncExpr *) node;
		context->num_calls++;
	}

	return expression_tree_walker((Node *) node, gapfill_function_call_walker, context);
}

/*
 * check if ordering matches the order we need:
 * all groups need to be part of order
 * pathkeys must consist of group elements only
 * last element of pathkeys needs to be time_bucket_gapfill ASC
 */
static bool
gapfill_correct_order(PlannerInfo *root, Path *subpath, FuncExpr *func)
{
	if (list_length(subpath->pathkeys) != list_length(root->group_pathkeys))
		return false;

	if (list_length(subpath->pathkeys) > 0)
	{
		PathKey *pk = llast(subpath->pathkeys);
		EquivalenceMember *em = linitial(pk->pk_eclass->ec_members);

		/* time_bucket_gapfill is last element */
		if (BTLessStrategyNumber == pk->pk_strategy && IsA(em->em_expr, FuncExpr) &&
			((FuncExpr *) em->em_expr)->funcid == func->funcid)
		{
			ListCell *lc;

			/* check all groups are part of subpath pathkeys */
			foreach (lc, root->group_pathkeys)
			{
				if (!list_member(subpath->pathkeys, lfirst(lc)))
					return false;
			}
			return true;
		}
	}

	return false;
}

/* Create a gapfill plan node in the form of a CustomScan node. The
 * purpose of this plan node is to insert tuples for missing groups.
 *
 * Note that CustomScan nodes cannot be extended (by struct embedding) because
 * they might be copied, therefore we pass any extra info in the custom_private
 * field.
 *
 * The gapfill plan takes the original Agg node and imposes itself on top of the
 * Agg node. During execution, the gapfill node will produce the new tuples.
 */
static Plan *
gapfill_plan_create(PlannerInfo *root, RelOptInfo *rel, struct CustomPath *path, List *tlist,
					List *clauses, List *custom_plans)
{
	GapFillPath *gfpath = (GapFillPath *) path;
	CustomScan *cscan = makeNode(CustomScan);
	ListCell *lc;
	TargetEntry *tle;
	Expr *expr;
	List *tl_exprs = NIL;
	UpperRelationKind stage;
	int i;

	cscan->scan.scanrelid = 0;
	cscan->scan.plan.targetlist = tlist;
	cscan->custom_plans = custom_plans;
	cscan->custom_scan_tlist = tlist;
	cscan->flags = path->flags;
	cscan->methods = &gapfill_plan_methods;

	/*
	 * if window functions are involved the gapfill marker functions might not
	 * actually be in the targetlist at the aggregation stage
	 */

	/*
	 * we currently do not support window functions where the number of
	 * entries in the target list at window stage is different from the
	 * number of entries in the target list at aggregation stage. This can
	 * happen if a subexpression of a column expression is identical for
	 * multiple columns and postgres planner decides to split up those columns
	 * above aggregation node.
	 */
	if (root->upper_targets[UPPERREL_WINDOW] &&
		list_length(root->upper_targets[UPPERREL_WINDOW]->exprs) !=
			list_length(root->upper_targets[UPPERREL_GROUP_AGG]->exprs))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("gapfill functionality with window functions not supported")));

	for (i = 0; i < list_length(tlist); i++)
	{
		tle = list_nth(tlist, i);
		if (root->upper_targets[UPPERREL_WINDOW])
		{
			expr = list_nth(root->upper_targets[UPPERREL_WINDOW]->exprs, i);
			if (IsA(expr, WindowFunc))
				expr = linitial(castNode(WindowFunc, expr)->args);
			tl_exprs = lappend(tl_exprs, expr);
		}
		else
			tl_exprs = lappend(tl_exprs, tle->expr);
	}
	cscan->custom_private = list_make3(gfpath->func, root->parse->groupClause, tl_exprs);

	/* remove locf and interpolate function calls from targetlists */
	foreach (lc, ((Plan *) linitial(custom_plans))->targetlist)
	{
		castNode(TargetEntry, lfirst(lc))->expr =
			gapfill_clean_expr(castNode(TargetEntry, lfirst(lc))->expr);
	}
	for (stage = UPPERREL_SETOP; stage <= UPPERREL_FINAL; stage++)
	{
		if (root->upper_targets[stage])
		{
			foreach (lc, root->upper_targets[stage]->exprs)
			{
				lfirst(lc) = gapfill_clean_expr(lfirst(lc));
			}
		}
	}

	return &cscan->scan.plan;
}

static CustomPathMethods gapfill_path_methods = {
	.CustomName = "GapFill",
	.PlanCustomPath = gapfill_plan_create,
};

/*
 * Create a Gapfill Path node.
 *
 * The gap fill node needs rows to be sorted by time ASC
 * so we insert sort pathes if the query order does not match
 * that
 */
static Path *
gapfill_path_create(PlannerInfo *root, Path *subpath, FuncExpr *func)
{
	GapFillPath *path;

	path = (GapFillPath *) newNode(sizeof(GapFillPath), T_CustomPath);
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.methods = &gapfill_path_methods;

	/*
	 * parallel_safe must be false because it is not safe to execute this node
	 * in parallel, but it is safe for child nodes to be parallel
	 */
	Assert(!path->cpath.path.parallel_safe);
	path->cpath.path.rows = subpath->rows;
	path->cpath.path.parent = subpath->parent;
	path->cpath.path.param_info = subpath->param_info;
	path->cpath.path.pathtarget = subpath->pathtarget;
	path->cpath.flags = 0;
	path->cpath.path.pathkeys = subpath->pathkeys;

	if (!gapfill_correct_order(root, subpath, func))
	{
		List *new_order = NIL;
		ListCell *lc;
		PathKey *pk_func = NULL;

		/* subpath does not have correct order */
		foreach (lc, root->group_pathkeys)
		{
			PathKey *pk = lfirst(lc);
			EquivalenceMember *em = linitial(pk->pk_eclass->ec_members);

			if (!pk_func && IsA(em->em_expr, FuncExpr) &&
				((FuncExpr *) em->em_expr)->funcid == func->funcid)
			{
				if (BTLessStrategyNumber == pk->pk_strategy)
					pk_func = pk;
				else
					pk_func = make_canonical_pathkey(root,
													 pk->pk_eclass,
													 pk->pk_opfamily,
													 BTLessStrategyNumber,
													 pk->pk_nulls_first);
			}
			else
				new_order = lappend(new_order, pk);
		}
		if (!pk_func)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("no top level time_bucket_gapfill in group by clause")));

		new_order = lappend(new_order, pk_func);
		subpath = (Path *)
			create_sort_path(root, subpath->parent, subpath, new_order, root->limit_tuples);
	}

	path->cpath.path.startup_cost = subpath->startup_cost;
	path->cpath.path.total_cost = subpath->total_cost;
	path->cpath.path.pathkeys = subpath->pathkeys;
	path->cpath.custom_paths = list_make1(subpath);
	path->func = func;

	return &path->cpath.path;
}

/*
 * Prepend GapFill node to every group_rel path
 */
void
plan_add_gapfill(PlannerInfo *root, RelOptInfo *group_rel)
{
	ListCell *lc;
	Query *parse = root->parse;
	gapfill_walker_context context = { .call = NULL, .num_calls = 0 };

	if (CMD_SELECT != parse->commandType || parse->groupClause == NIL)
		return;

	/*
	 * look for time_bucket_gapfill function call
	 */
	expression_tree_walker((Node *) parse->targetList, gapfill_function_call_walker, &context);

	if (context.num_calls == 0)
		return;

	license_print_expiration_warning_if_needed();

#ifndef HAVE_INT64_TIMESTAMP

	/*
	 * starting with postgres 10 timestamps are always integer but in 9.6 this
	 * was a compile time option
	 */
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("gapfill functionality cannot be used on postgres compiled with non integer "
					"timestamps")));
#endif

	if (context.num_calls > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("multiple time_bucket_gapfill calls not allowed")));

	if (context.num_calls == 1)
	{
		List *copy = group_rel->pathlist;
		group_rel->pathlist = NIL;
		group_rel->cheapest_total_path = NULL;
		group_rel->cheapest_startup_path = NULL;
		group_rel->cheapest_unique_path = NULL;

		/* TODO parameterized paths */
		list_free(group_rel->ppilist);
		group_rel->ppilist = NULL;

		list_free(group_rel->cheapest_parameterized_paths);
		group_rel->cheapest_parameterized_paths = NULL;

		foreach (lc, copy)
		{
			add_path(group_rel, gapfill_path_create(root, lfirst(lc), context.call));
		}
		list_free(copy);
	}
}
