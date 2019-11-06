/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parse_oper.h>
#include <catalog/pg_type.h>
#include <optimizer/cost.h>
#include <optimizer/clauses.h>
#include <optimizer/tlist.h>
#include <utils/selfuncs.h>

#include "func_cache.h"
#include "estimate.h"
#include "planner_import.h"
#include "utils.h"

#include "compat.h"
#if PG12
#include <optimizer/optimizer.h>
#endif
/*
 * This module contains functions for estimating, e.g., the number of groups
 * formed in various grouping expressions that involve time bucketing.
 */

static double estimate_max_spread_expr(PlannerInfo *root, Expr *expr);
static double group_estimate_opexpr(PlannerInfo *root, OpExpr *opexpr, double path_rows);

/* Estimate the max spread on a time var in terms of the internal time representation.
 * Note that this will happen on the hypertable var in most cases. Therefore this is
 * a huge overestimate in many cases where there is a WHERE clause on time.
 */
static double
estimate_max_spread_var(PlannerInfo *root, Var *var)
{
	VariableStatData vardata;
	Oid ltop;
	Datum max_datum, min_datum;
	volatile int64 max, min;
	volatile bool valid;

	examine_variable(root, (Node *) var, 0, &vardata);
	get_sort_group_operators(var->vartype, true, false, false, &ltop, NULL, NULL, NULL);
	valid = ts_get_variable_range(root, &vardata, ltop, &min_datum, &max_datum);
	ReleaseVariableStats(vardata);

	if (!valid)
		return INVALID_ESTIMATE;

	PG_TRY();
	{
		max = ts_time_value_to_internal(max_datum, var->vartype);
		min = ts_time_value_to_internal(min_datum, var->vartype);
	}
	PG_CATCH();
	{
		valid = false;
		FlushErrorState();
	}
	PG_END_TRY();

	if (!valid)
		return INVALID_ESTIMATE;

	return (double) (max - min);
}

static double
estimate_max_spread_opexpr(PlannerInfo *root, OpExpr *opexpr)
{
	char *function_name = get_opname(opexpr->opno);
	Expr *left;
	Expr *right;
	Expr *nonconst;

	if (list_length(opexpr->args) != 2 || strlen(function_name) != 1)
		return INVALID_ESTIMATE;

	left = linitial(opexpr->args);
	right = lsecond(opexpr->args);

	if (IsA(left, Const))
		nonconst = right;
	else if (IsA(right, Const))
		nonconst = left;
	else
		return INVALID_ESTIMATE;

	/* adding or subtracting a constant doesn't affect the range */
	if (function_name[0] == '-' || function_name[0] == '+')
		return estimate_max_spread_expr(root, nonconst);

	return INVALID_ESTIMATE;
}

/* estimate the max spread (max(value)-min(value)) of the expr */
static double
estimate_max_spread_expr(PlannerInfo *root, Expr *expr)
{
	switch (nodeTag(expr))
	{
		case T_Var:
			return estimate_max_spread_var(root, (Var *) expr);
		case T_OpExpr:
			return estimate_max_spread_opexpr(root, (OpExpr *) expr);
		default:
			return INVALID_ESTIMATE;
	}
}

/*
 * Return an estimate for the number of groups formed when expr is divided
 * into intervals of size interval_period.
 */
double
ts_estimate_group_expr_interval(PlannerInfo *root, Expr *expr, double interval_period)
{
	double max_period;

	if (interval_period <= 0)
		return INVALID_ESTIMATE;

	max_period = estimate_max_spread_expr(root, expr);
	if (!IS_VALID_ESTIMATE(max_period))
		return INVALID_ESTIMATE;

	return clamp_row_est(max_period / interval_period);
}

/* if performing integer division number of groups is less than the spread divided by the divisor.
 * Note that this is an overestimate. */
static double
group_estimate_integer_division(PlannerInfo *root, Oid opno, Node *left, Node *right)
{
	char *function_name = get_opname(opno);

	/* only handle division */
	if (function_name[0] == '/' && function_name[1] == '\0' && IsA(right, Const))
	{
		Const *c = (Const *) right;

		if (c->consttype != INT2OID && c->consttype != INT4OID && c->consttype != INT8OID)
			return INVALID_ESTIMATE;

		return ts_estimate_group_expr_interval(root, (Expr *) left, (double) c->constvalue);
	}
	return INVALID_ESTIMATE;
}

static double
group_estimate_funcexpr(PlannerInfo *root, FuncExpr *group_estimate_func, double path_rows)
{
	FuncInfo *func_est = ts_func_cache_get_bucketing_func(group_estimate_func->funcid);

	if (NULL != func_est)
		return func_est->group_estimate(root, group_estimate_func, path_rows);
	return INVALID_ESTIMATE;
}

/* Get a custom estimate for the number of groups of an expression. Return INVALID_ESTIMATE if we
 * don't have any extra knowledge and should just use the default estimate */
static double
group_estimate_expr(PlannerInfo *root, Node *expr, double path_rows)
{
	switch (nodeTag(expr))
	{
		case T_FuncExpr:
			return group_estimate_funcexpr(root, (FuncExpr *) expr, path_rows);
		case T_OpExpr:
			return group_estimate_opexpr(root, (OpExpr *) expr, path_rows);
		default:
			return INVALID_ESTIMATE;
	}
}

static double
group_estimate_opexpr(PlannerInfo *root, OpExpr *opexpr, double path_rows)
{
	Node *first;
	Node *second;
	double estimate;

	if (list_length(opexpr->args) != 2)
		return INVALID_ESTIMATE;

	first = eval_const_expressions(root, linitial(opexpr->args));
	second = eval_const_expressions(root, lsecond(opexpr->args));

	estimate = group_estimate_integer_division(root, opexpr->opno, first, second);
	if (IS_VALID_ESTIMATE(estimate))
		return estimate;

	if (IsA(first, Const))
		return group_estimate_expr(root, second, path_rows);
	if (IsA(second, Const))
		return group_estimate_expr(root, first, path_rows);
	return INVALID_ESTIMATE;
}

/*
 * Get a custom estimate for the number of groups in a query. Return
 * INVALID_ESTIMATE if we don't have any extra knowledge and should just use
 * the default estimate. This works by getting a custom estimate for any
 * groups where a custom estimate exists and multiplying that by the standard
 * estimate of the groups for which custom estimates don't exist.
 */
double
ts_estimate_group(PlannerInfo *root, double path_rows)
{
	Query *parse = root->parse;
	double d_num_groups = 1;
	List *group_exprs;
	ListCell *lc;
	bool found = false;
	List *new_group_expr = NIL;

	Assert(parse->groupClause && !parse->groupingSets);

	group_exprs = get_sortgrouplist_exprs(parse->groupClause, parse->targetList);

	foreach (lc, group_exprs)
	{
		Node *item = lfirst(lc);
		double estimate = group_estimate_expr(root, item, path_rows);

		if (IS_VALID_ESTIMATE(estimate))
		{
			found = true;
			d_num_groups *= estimate;
		}
		else
			new_group_expr = lappend(new_group_expr, item);
	}

	/* nothing custom */
	if (!found)
		return INVALID_ESTIMATE;

	/* multiply by default estimates */
	if (new_group_expr != NIL)
		d_num_groups *= estimate_num_groups(root, new_group_expr, path_rows, NULL);

	if (d_num_groups > path_rows)
		return INVALID_ESTIMATE;

	return clamp_row_est(d_num_groups);
}
