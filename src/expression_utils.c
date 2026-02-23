/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <nodes/primnodes.h>
#include <utils/lsyscache.h>

#include "debug_assert.h"
#include "export.h"
#include "expression_utils.h"

/*
 * This function is meant to extract the expression components to be used in a ScanKey.
 *
 * It will work on the following expression types:
 * - Var OP Expr
 *
 * Var OP Var is not supported as that will not work with scankeys.
 *
 */
bool TSDLLEXPORT
ts_extract_expr_args(Expr *expr, Var **var, Expr **arg_value, Oid *opno, Oid *opcode)
{
	List *args;
	Oid expr_opno, expr_opcode;

	switch (nodeTag(expr))
	{
		case T_OpExpr:
		{
			OpExpr *opexpr = castNode(OpExpr, expr);
			args = opexpr->args;
			expr_opno = opexpr->opno;
			expr_opcode = opexpr->opfuncid;

			if (opexpr->opresulttype != BOOLOID)
				return false;

			break;
		}
		case T_ScalarArrayOpExpr:
		{
			ScalarArrayOpExpr *sa_opexpr = castNode(ScalarArrayOpExpr, expr);
			args = sa_opexpr->args;
			expr_opno = sa_opexpr->opno;
			expr_opcode = sa_opexpr->opfuncid;
			break;
		}
		default:
			return false;
	}

	if (list_length(args) != 2)
		return false;

	Expr *leftop = linitial(args);
	Expr *rightop = lsecond(args);

	if (IsA(leftop, RelabelType))
		leftop = castNode(RelabelType, leftop)->arg;
	if (IsA(rightop, RelabelType))
		rightop = castNode(RelabelType, rightop)->arg;

	if (IsA(leftop, Var) && !IsA(rightop, Var))
	{
		/* ignore system columns */
		if (castNode(Var, leftop)->varattno <= 0)
			return false;

		*var = castNode(Var, leftop);

		*arg_value = rightop;
		*opno = expr_opno;
		if (opcode)
			*opcode = expr_opcode;
		return true;
	}
	else if (IsA(rightop, Var) && !IsA(leftop, Var))
	{
		/* ignore system columns */
		if (castNode(Var, rightop)->varattno <= 0)
			return false;

		*var = castNode(Var, rightop);
		*arg_value = leftop;
		expr_opno = get_commutator(expr_opno);
		if (!OidIsValid(expr_opno))
			return false;

		if (opcode)
		{
			expr_opcode = get_opcode(expr_opno);
			if (!OidIsValid(expr_opcode))
				return false;
			*opcode = expr_opcode;
		}

		*opno = expr_opno;

		return true;
	}

	return false;
}

/*
 * Build an output targetlist for a custom node that just references all the
 * custom scan targetlist entries.
 */
List *
ts_build_trivial_custom_output_targetlist(List *scan_targetlist)
{
	List *result = NIL;

	ListCell *lc;
	foreach (lc, scan_targetlist)
	{
		TargetEntry *scan_entry = (TargetEntry *) lfirst(lc);

		Var *var = makeVar(INDEX_VAR,
						   scan_entry->resno,
						   exprType((Node *) scan_entry->expr),
						   exprTypmod((Node *) scan_entry->expr),
						   exprCollation((Node *) scan_entry->expr),
						   /* varlevelsup = */ 0);

		TargetEntry *output_entry = makeTargetEntry((Expr *) var,
													scan_entry->resno,
													scan_entry->resname,
													scan_entry->resjunk);

		result = lappend(result, output_entry);
	}

	return result;
}

static Node *
resolve_outer_special_vars_mutator(Node *node, void *context)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (!IsA(node, Var))
	{
		return expression_tree_mutator(node, resolve_outer_special_vars_mutator, context);
	}

	Var *var = castNode(Var, node);
	CustomScan *custom = castNode(CustomScan, context);
	if ((Index) var->varno == (Index) custom->scan.scanrelid)
	{
		/*
		 * This is already the uncompressed chunk var. We can see it referenced
		 * by expressions in the output targetlist of the child scan node.
		 */
		return (Node *) copyObject(var);
	}

	if (var->varno == OUTER_VAR)
	{
		/*
		 * Reference into the output targetlist of the child scan node.
		 */
		TargetEntry *columnar_scan_tentry =
			castNode(TargetEntry, list_nth(custom->scan.plan.targetlist, var->varattno - 1));

		return resolve_outer_special_vars_mutator((Node *) columnar_scan_tentry->expr, context);
	}

	if (var->varno == INDEX_VAR)
	{
		/*
		 * This is a reference into the custom scan targetlist, we have to resolve
		 * it as well.
		 */
		var = castNode(Var,
					   castNode(TargetEntry, list_nth(custom->custom_scan_tlist, var->varattno - 1))
						   ->expr);
		Assert(var->varno > 0);

		return (Node *) copyObject(var);
	}

	Ensure(false, "encountered unexpected varno %d as an aggregate argument", var->varno);
	return node;
}

/*
 * Walk a plan tree recursively descending into child plans.
 * At each leaf, call the user-supplied callback which may replace the node.
 */
Plan *
ts_plan_tree_walker(Plan *plan, ts_plan_tree_walkerfunc func, void *context)
{
	if (!plan)
		return NULL;

	if (IsA(plan, List))
	{
		ListCell *lc;
		foreach (lc, castNode(List, plan))
		{
			lfirst(lc) = ts_plan_tree_walker(lfirst(lc), func, context);
		}
		return plan;
	}

	if (plan->lefttree)
		plan->lefttree = ts_plan_tree_walker(plan->lefttree, func, context);
	if (plan->righttree)
		plan->righttree = ts_plan_tree_walker(plan->righttree, func, context);

	if (IsA(plan, Append))
	{
		Append *append = castNode(Append, plan);
		append->appendplans =
			(List *) ts_plan_tree_walker((Plan *) append->appendplans, func, context);
	}
	else if (IsA(plan, MergeAppend))
	{
		MergeAppend *append = castNode(MergeAppend, plan);
		append->mergeplans =
			(List *) ts_plan_tree_walker((Plan *) append->mergeplans, func, context);
	}
	else if (IsA(plan, CustomScan))
	{
		CustomScan *custom = castNode(CustomScan, plan);
		custom->custom_plans =
			(List *) ts_plan_tree_walker((Plan *) custom->custom_plans, func, context);
	}
	if (IsA(plan, SubqueryScan))
	{
		SubqueryScan *subquery = castNode(SubqueryScan, plan);
		subquery->subplan = ts_plan_tree_walker(subquery->subplan, func, context);
		return plan;
	}

	return func(plan, context);
}

/*
 * Resolve the OUTER_VAR special variables, that are used in the output
 * targetlists of aggregation nodes, replacing them with the uncompressed chunk
 * variables.
 */
List *
ts_resolve_outer_special_vars(List *agg_tlist, Plan *childplan)
{
	return castNode(List, resolve_outer_special_vars_mutator((Node *) agg_tlist, childplan));
}
