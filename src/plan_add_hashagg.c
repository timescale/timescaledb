#include <postgres.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <parser/parse_oper.h>
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/tlist.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <utils/guc.h>
#include <miscadmin.h>
#include <utils/timestamp.h>
#include <utils/lsyscache.h>
#include <utils/selfuncs.h>

#include "compat-msvc-enter.h"
#include <optimizer/cost.h>
#include "compat-msvc-exit.h"

#include "planner_import.h"
#include "plan_add_hashagg.h"
#include "extension.h"
#include "utils.h"
#include "guc.h"
#include "dimension.h"
#include "chunk_dispatch_plan.h"
#include "planner_utils.h"
#include "hypertable_insert.h"
#include "constraint_aware_append.h"
#include "compat.h"
#include "extension.h"

#define INVALID_ESTIMATE -1
#define IS_VALID_ESTIMATE(est) (est >= 0)
#define MAX_FUNCTION_ARGS 10

static double custom_group_estimate_time_bucket(PlannerInfo *root, FuncExpr *expr, double path_rows);
static double custom_group_estimate_date_trunc(PlannerInfo *root, FuncExpr *expr, double path_rows);

static double custom_group_estimate_expr(PlannerInfo *root, Node *expr, double path_rows);

typedef struct
{
	Oid			function_oid;

	bool		extension_function;
	char	   *function_name;
	int			nargs;
	double		(*custom_group_estimate_func) (PlannerInfo *root, FuncExpr *expr, double path_rows);
	Oid			arg_types[MAX_FUNCTION_ARGS];
} CustomEstimateForFunctionInfo;

typedef struct
{
	Oid			function_oid;
	CustomEstimateForFunctionInfo *entry;
} CustomEstimateForFunctionInfoHashEntry;

/* Definitions of functions with a custom group estimate.
   These will also be entered into the custom_estimate_func_hash hash table. */
static CustomEstimateForFunctionInfo custom_estimate_func_info[] =
{
	{
		.extension_function = true,
		.function_name = "time_bucket",
		.nargs = 2,
		.arg_types = {INTERVALOID, TIMESTAMPOID},
		.custom_group_estimate_func = custom_group_estimate_time_bucket
	},
	{
		.extension_function = true,
		.function_name = "time_bucket",
		.nargs = 2,
		.arg_types = {INTERVALOID, TIMESTAMPTZOID},
		.custom_group_estimate_func = custom_group_estimate_time_bucket,
	},
	{
		.extension_function = true,
		.function_name = "time_bucket",
		.nargs = 2,
		.arg_types = {INTERVALOID, DATEOID},
		.custom_group_estimate_func = custom_group_estimate_time_bucket,
	},
	{
		.function_name = "date_trunc",
		.nargs = 2,
		.arg_types = {TEXTOID, TIMESTAMPOID},
		.custom_group_estimate_func = custom_group_estimate_date_trunc,
	},
	{
		.nargs = 2,
		.custom_group_estimate_func = custom_group_estimate_date_trunc,
		.arg_types = {TEXTOID, TIMESTAMPTZOID},
		.function_name = "date_trunc",
	},
};
#define _MAX_HASHAGG_FUNCTIONS (sizeof(custom_estimate_func_info)/sizeof(custom_estimate_func_info[0]))

HTAB	   *custom_estimate_func_hash = NULL;

static bool
function_types_equal(Oid left[], Oid right[], int nargs)
{
	int			arg_index;

	for (arg_index = 0; arg_index < nargs; arg_index++)
	{
		if (left[arg_index] != right[arg_index])
			return false;
	}
	return true;
}

static void
initialize_custom_estimate_func_info()
{
	int			i = 0;
	Oid			extension_schema = extension_schema_oid();
	char	   *extension_schema_name = get_namespace_name(extension_schema);


	HASHCTL		hashctl = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(CustomEstimateForFunctionInfoHashEntry)
	};

	custom_estimate_func_hash = hash_create("custom_estimate_func_hash", _MAX_HASHAGG_FUNCTIONS, &hashctl, HASH_ELEM | HASH_BLOBS);

	for (i = 0; i < _MAX_HASHAGG_FUNCTIONS; i++)
	{
		CustomEstimateForFunctionInfo def = custom_estimate_func_info[i];
		CustomEstimateForFunctionInfoHashEntry *hash_entry;
		bool		function_found = false;
		bool		hash_found;
		FuncCandidateList funclist;

		if (def.extension_function)
			funclist = FuncnameGetCandidates(list_make2(makeString(extension_schema_name), makeString(def.function_name)),
											 def.nargs, NIL, false, false, false);
		else
			funclist = FuncnameGetCandidates(list_make1(makeString(def.function_name)),
											 def.nargs, NIL, false, false, false);

		/* check types */
		while (!function_found && funclist != NULL)
		{
			if (funclist->nargs != def.nargs || !function_types_equal(funclist->args, def.arg_types, def.nargs))
				funclist = funclist->next;
			else
				function_found = true;
		}

		if (!function_found)
			elog(ERROR, "cache lookup failed for function \"%s\" with %d args",
				 def.function_name, def.nargs);

		custom_estimate_func_info[i].function_oid = funclist->oid;

		hash_entry = hash_search(custom_estimate_func_hash, &(custom_estimate_func_info[i].function_oid), HASH_ENTER, &hash_found);
		Assert(!hash_found);
		hash_entry->entry = &custom_estimate_func_info[i];
	}
}

static CustomEstimateForFunctionInfo *
get_custom_estimate_func_info(Oid function_oid)
{
	CustomEstimateForFunctionInfoHashEntry *hash_entry;

	if (NULL == custom_estimate_func_hash)
		initialize_custom_estimate_func_info();

	hash_entry = hash_search(custom_estimate_func_hash, &function_oid, HASH_FIND, NULL);
	return (hash_entry != NULL ? hash_entry->entry : NULL);
}


static double estimate_max_spread_expr(PlannerInfo *root, Expr *expr);

/* Estimate the max spread on a time var in terms of the internal time representation.
 * Note that this will happen on the hypertable var in most cases. Therefore this is
 * a huge overestimate in many cases where there is a WHERE clause on time.
 */
static double
estimate_max_spread_var(PlannerInfo *root, Var *var)
{
	VariableStatData vardata;
	Oid			ltop;
	Datum		max_datum,
				min_datum;
	int64		max,
				min;
	bool		valid;

	examine_variable(root, (Node *) var, 0, &vardata);
	get_sort_group_operators(var->vartype, true, false, false, &ltop, NULL, NULL, NULL);
	valid = get_variable_range(root, &vardata, ltop, &min_datum, &max_datum);
	ReleaseVariableStats(vardata);

	if (!valid)
		return INVALID_ESTIMATE;

	max = time_value_to_internal(max_datum, var->vartype, true);
	min = time_value_to_internal(min_datum, var->vartype, true);

	if (max < 0 || min < 0)
		return INVALID_ESTIMATE;

	return (double) (max - min);
}

static double
estimate_max_spread_opexpr(PlannerInfo *root, OpExpr *opexpr)
{
	char	   *function_name = get_opname(opexpr->opno);
	Expr	   *left;
	Expr	   *right;
	Expr	   *nonconst;

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

/* Return an estimate for the number of groups formed when expr is divided
   into intervals of size interval_priod. */
static double
custom_group_estimate_expr_interval(PlannerInfo *root, Expr *expr, double interval_period)
{
	double		max_period;

	if (interval_period <= 0)
		return INVALID_ESTIMATE;

	max_period = estimate_max_spread_expr(root, expr);
	if (!IS_VALID_ESTIMATE(max_period))
		return INVALID_ESTIMATE;

	return clamp_row_est(max_period / interval_period);
}

/* For time_bucket this estimate currently works by seeing how many possible
 * buckets there will be if the data spans the entire hypertable. Note that
 * this is an overestimate.
 * */
static double
custom_group_estimate_time_bucket(PlannerInfo *root, FuncExpr *expr, double path_rows)
{
	Node	   *first_arg = eval_const_expressions(root, linitial(expr->args));
	Expr	   *second_arg = lsecond(expr->args);
	Const	   *c;
	Interval   *interval;

	if (!IsA(first_arg, Const))
		return INVALID_ESTIMATE;

	c = (Const *) first_arg;
	interval = DatumGetIntervalP(c->constvalue);
	return custom_group_estimate_expr_interval(root, second_arg, (double) get_interval_period_approx(interval));
}

/* For date_trunc this estimate currently works by seeing how many possible
 * buckets there will be if the data spans the entire hypertable. Note that
 * this is an overestimate.
 * */
static double
custom_group_estimate_date_trunc(PlannerInfo *root, FuncExpr *expr, double path_rows)
{
	Node	   *first_arg = eval_const_expressions(root, linitial(expr->args));
	Expr	   *second_arg = lsecond(expr->args);
	Const	   *c;
	text	   *interval;

	if (!IsA(first_arg, Const))
		return INVALID_ESTIMATE;

	c = (Const *) first_arg;
	interval = DatumGetTextPP(c->constvalue);
	return custom_group_estimate_expr_interval(root, second_arg, (double) date_trunc_interval_period_approx(interval));
}

/* if performing integer division number of groups is less than the spread divided by the divisor.
 * Note that this is an overestimate. */
static double
custom_group_estimate_integer_division(PlannerInfo *root, Oid opno, Node *left, Node *right)
{
	char	   *function_name = get_opname(opno);

	/* only handle division */
	if (function_name[0] == '/' && function_name[1] == '\0' && IsA(right, Const))
	{
		Const	   *c = (Const *) right;

		if (c->consttype != INT2OID && c->consttype != INT4OID && c->consttype != INT8OID)
			return INVALID_ESTIMATE;

		return custom_group_estimate_expr_interval(root, (Expr *) left, (double) c->constvalue);
	}
	return INVALID_ESTIMATE;
}

static double
custom_group_estimate_funcexpr(PlannerInfo *root, FuncExpr *custom_group_estimate_func, double path_rows)
{
	CustomEstimateForFunctionInfo *func_est = get_custom_estimate_func_info(custom_group_estimate_func->funcid);

	if (NULL != func_est)
		return func_est->custom_group_estimate_func(root, custom_group_estimate_func, path_rows);
	return INVALID_ESTIMATE;
}

static double
custom_group_estimate_opexpr(PlannerInfo *root, OpExpr *opexpr, double path_rows)
{
	Node	   *first;
	Node	   *second;
	double		estimate;

	if (list_length(opexpr->args) != 2)
		return INVALID_ESTIMATE;

	first = eval_const_expressions(root, linitial(opexpr->args));
	second = eval_const_expressions(root, lsecond(opexpr->args));

	estimate = custom_group_estimate_integer_division(root, opexpr->opno, first, second);
	if (IS_VALID_ESTIMATE(estimate))
		return estimate;

	if (IsA(first, Const))
		return custom_group_estimate_expr(root, second, path_rows);
	if (IsA(second, Const))
		return custom_group_estimate_expr(root, first, path_rows);
	return INVALID_ESTIMATE;
}



/* Get a custom estimate for the number of groups of an expression. Return INVALID_ESTIMATE if we don't have
 * any extra knowledge and should just use the default estimate */
static double
custom_group_estimate_expr(PlannerInfo *root, Node *expr, double path_rows)
{
	switch (nodeTag(expr))
	{
		case T_FuncExpr:
			return custom_group_estimate_funcexpr(root, (FuncExpr *) expr, path_rows);
		case T_OpExpr:
			return custom_group_estimate_opexpr(root, (OpExpr *) expr, path_rows);
		default:
			return INVALID_ESTIMATE;
	}
}

/* Get a custom estimate for the number of groups in a query. Return INVALID_ESTIMATE if we don't have
 * any extra knowledge and should just use the default estimate. This works by getting
 * a custom estimate for any groups where a custom estimate exists and multiplying that
 * by the standard estimate of the groups for which custom estimates don't exist */
static double
custom_group_estimate(PlannerInfo *root,
					  double path_rows)
{
	Query	   *parse = root->parse;
	double		d_num_groups = 1;
	List	   *group_exprs;
	ListCell   *lc;
	bool		found = false;
	List	   *new_group_expr = NIL;

	Assert(parse->groupClause && !parse->groupingSets);

	group_exprs = get_sortgrouplist_exprs(parse->groupClause,
										  parse->targetList);

	foreach(lc, group_exprs)
	{
		Node	   *item = lfirst(lc);
		double		estimate =
		custom_group_estimate_expr(root, item, path_rows);

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

/* Add a parallel HashAggregate plan.
 * This code is similar to parts of create_grouping_paths */
static void
plan_add_parallel_hashagg(PlannerInfo *root,
						  RelOptInfo *input_rel,
						  RelOptInfo *output_rel, double d_num_groups)
{
	Query	   *parse = root->parse;
	Path	   *cheapest_partial_path = linitial(input_rel->partial_pathlist);
	PathTarget *target = root->upper_targets[UPPERREL_GROUP_AGG];
	PathTarget *partial_grouping_target = make_partial_grouping_target(root, target);
	AggClauseCosts agg_partial_costs;
	AggClauseCosts agg_final_costs;
	Size		hashagg_table_size;
	double		total_groups;
	Path	   *partial_path;

	double		d_num_partial_groups = custom_group_estimate(root,
															 cheapest_partial_path->rows);

	/* don't have any special estimate */
	if (!IS_VALID_ESTIMATE(d_num_partial_groups))
		return;

	MemSet(&agg_partial_costs, 0, sizeof(AggClauseCosts));
	MemSet(&agg_final_costs, 0, sizeof(AggClauseCosts));

	if (parse->hasAggs)
	{
		/* partial phase */
		get_agg_clause_costs(root, (Node *) partial_grouping_target->exprs,
							 AGGSPLIT_INITIAL_SERIAL,
							 &agg_partial_costs);

		/* final phase */
		get_agg_clause_costs(root, (Node *) target->exprs,
							 AGGSPLIT_FINAL_DESERIAL,
							 &agg_final_costs);
		get_agg_clause_costs(root, parse->havingQual,
							 AGGSPLIT_FINAL_DESERIAL,
							 &agg_final_costs);
	}

	hashagg_table_size =
		estimate_hashagg_tablesize(cheapest_partial_path,
								   &agg_partial_costs,
								   d_num_partial_groups);

	/*
	 * Tentatively produce a partial HashAgg Path, depending on if it looks as
	 * if the hash table will fit in work_mem.
	 */
	if (hashagg_table_size >= work_mem * 1024L)
		return;

	add_partial_path(output_rel, (Path *)
					 create_agg_path(root,
									 output_rel,
									 cheapest_partial_path,
									 partial_grouping_target,
									 AGG_HASHED,
									 AGGSPLIT_INITIAL_SERIAL,
									 parse->groupClause,
									 NIL,
									 &agg_partial_costs,
									 d_num_partial_groups));

	if (!output_rel->partial_pathlist)
		return;

	partial_path = (Path *) linitial(output_rel->partial_pathlist);

	total_groups = partial_path->rows * partial_path->parallel_workers;

	partial_path = (Path *) create_gather_path(root,
											   output_rel,
											   partial_path,
											   partial_grouping_target,
											   NULL,
											   &total_groups);
	add_path(output_rel, (Path *)
			 create_agg_path(root,
							 output_rel,
							 partial_path,
							 target,
							 AGG_HASHED,
							 AGGSPLIT_FINAL_DESERIAL,
							 parse->groupClause,
							 (List *) parse->havingQual,
							 &agg_final_costs,
							 d_num_groups));
}


/* This function add a HashAggregate path, if appropriate
 * it looks like a highly modified create_grouping_paths function
 * in the postgres planner. */
void
plan_add_hashagg(PlannerInfo *root,
				 RelOptInfo *input_rel,
				 RelOptInfo *output_rel)
{
	Query	   *parse = root->parse;
	Path	   *cheapest_path = input_rel->cheapest_total_path;
	AggClauseCosts agg_costs;
	bool		can_hash;
	double		d_num_groups;
	Size		hashaggtablesize;
	PathTarget *target = root->upper_targets[UPPERREL_GROUP_AGG];
	bool		try_parallel_aggregation;


	if (parse->groupingSets || !parse->hasAggs || parse->groupClause == NIL)
		return;

	MemSet(&agg_costs, 0, sizeof(AggClauseCosts));
	get_agg_clause_costs(root, (Node *) root->processed_tlist, AGGSPLIT_SIMPLE,
						 &agg_costs);
	get_agg_clause_costs(root, parse->havingQual, AGGSPLIT_SIMPLE,
						 &agg_costs);

	can_hash = (parse->groupClause != NIL &&
				agg_costs.numOrderedAggs == 0 &&
				grouping_is_hashable(parse->groupClause));

	if (!can_hash)
		return;

	d_num_groups = custom_group_estimate(root,
										 cheapest_path->rows);

	/* don't have any special estimate */
	if (!IS_VALID_ESTIMATE(d_num_groups))
		return;

	hashaggtablesize = estimate_hashagg_tablesize(cheapest_path,
												  &agg_costs,
												  d_num_groups);

	if (hashaggtablesize >= work_mem * 1024L)
		return;

	if (!output_rel->consider_parallel)
	{
		/* Not even parallel-safe. */
		try_parallel_aggregation = false;
	}
	else if (output_rel->partial_pathlist == NIL)
	{
		/* Nothing to use as input for partial aggregate. */
		try_parallel_aggregation = false;
	}
	else if (agg_costs.hasNonPartial || agg_costs.hasNonSerial)
	{
		/* Insufficient support for partial mode. */
		try_parallel_aggregation = false;
	}
	else
	{
		/* Everything looks good. */
		try_parallel_aggregation = true;
	}

	if (try_parallel_aggregation)
		plan_add_parallel_hashagg(root, input_rel, output_rel, d_num_groups);

	/*
	 * We just need an Agg over the cheapest-total input path, since input
	 * order won't matter.
	 */
	add_path(output_rel, (Path *)
			 create_agg_path(root, output_rel,
							 cheapest_path,
							 target,
							 AGG_HASHED,
							 AGGSPLIT_SIMPLE,
							 parse->groupClause,
							 (List *) parse->havingQual,
							 &agg_costs,
							 d_num_groups));

}
