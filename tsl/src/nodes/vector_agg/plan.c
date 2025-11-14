/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <commands/explain.h>
#include <executor/executor.h>
#include <funcapi.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <utils/fmgroids.h>

#include "plan.h"

#include "exec.h"
#include "import/list.h"
#include "nodes/columnar_scan/vector_quals.h"
#include "nodes/vector_agg.h"
#include "utils.h"

static struct CustomScanMethods scan_methods = { .CustomName = VECTOR_AGG_NODE_NAME,
												 .CreateCustomScanState = vector_agg_state_create };

void
_vector_agg_init(void)
{
	TryRegisterCustomScanMethods(&scan_methods);
}

/*
 * Build an output targetlist for a custom node that just references all the
 * custom scan targetlist entries.
 */
static inline List *
build_trivial_custom_output_targetlist(List *scan_targetlist)
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
 * Resolve the OUTER_VAR special variables, that are used in the output
 * targetlists of aggregation nodes, replacing them with the uncompressed chunk
 * variables.
 */
static List *
resolve_outer_special_vars(List *agg_tlist, Plan *childplan)
{
	return castNode(List, resolve_outer_special_vars_mutator((Node *) agg_tlist, childplan));
}

/*
 * Create a vectorized aggregation node to replace the given partial aggregation
 * node.
 */
static Plan *
vector_agg_plan_create(Plan *childplan, Agg *agg, List *resolved_targetlist,
					   VectorAggGroupingType grouping_type)
{
	CustomScan *vector_agg = (CustomScan *) makeNode(CustomScan);
	vector_agg->custom_plans = list_make1(childplan);
	vector_agg->methods = &scan_methods;

	vector_agg->custom_scan_tlist = resolved_targetlist;

	/*
	 * Note that this is being called from the post-planning hook, and therefore
	 * after set_plan_refs(). The meaning of output targetlists is different from
	 * the previous planning stages, and they contain special varnos referencing
	 * the scan targetlists.
	 */
	vector_agg->scan.plan.targetlist =
		build_trivial_custom_output_targetlist(vector_agg->custom_scan_tlist);

	/*
	 * Copy the costs from the normal aggregation node, so that they show up in
	 * the EXPLAIN output. They are not used for any other purposes, because
	 * this hook is called after the planning is finished.
	 */
	vector_agg->scan.plan.plan_rows = agg->plan.plan_rows;
	vector_agg->scan.plan.plan_width = agg->plan.plan_width;
	vector_agg->scan.plan.startup_cost = agg->plan.startup_cost;
	vector_agg->scan.plan.total_cost = agg->plan.total_cost;

	vector_agg->scan.plan.parallel_aware = false;
	vector_agg->scan.plan.parallel_safe = childplan->parallel_safe;
	vector_agg->scan.plan.async_capable = false;

	vector_agg->scan.plan.plan_node_id = agg->plan.plan_node_id;

	Assert(agg->plan.qual == NIL);

	vector_agg->scan.plan.initPlan = agg->plan.initPlan;

	vector_agg->scan.plan.extParam = bms_copy(agg->plan.extParam);
	vector_agg->scan.plan.allParam = bms_copy(agg->plan.allParam);

	vector_agg->custom_private = ts_new_list(T_List, VASI_Count);
	lfirst(list_nth_cell(vector_agg->custom_private, VASI_GroupingType)) =
		makeInteger(grouping_type);

	return (Plan *) vector_agg;
}

static bool
is_vector_type(Oid typeoid)
{
	switch (typeoid)
	{
		case BOOLOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case TEXTOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case DATEOID:
		case UUIDOID:
		case INTERVALOID:
			return true;
		default:
			return false;
	}
}

static bool is_vector_var(const VectorQualInfo *vqinfo, Expr *expr);

static bool
is_vector_function(const VectorQualInfo *vqinfo, List *args, Oid funcoid, Oid resulttype,
				   Oid inputcollid)
{
	if (list_length(args) > 5)
	{
		return false;
	}

	if (!is_vector_type(resulttype))
	{
		return false;
	}

	ListCell *lc;
	foreach (lc, args)
	{
		if (!is_vector_var(vqinfo, (Expr *) lfirst(lc)))
		{
			return false;
		}
	}

	if (!func_strict(funcoid))
	{
		return false;
	}

	if (func_volatile(funcoid) == PROVOLATILE_VOLATILE)
	{
		return false;
	}

	return true;
}

/*
 * Whether the expression can be used for vectorized processing: must be a Var
 * that refers to either a bulk-decompressed or a segmentby column.
 */
static bool
is_vector_var(const VectorQualInfo *vqinfo, Expr *expr)
{
	switch (((Node *) expr)->type)
	{
		case T_Const:
		{
			Const *c = (Const *) expr;
			return is_vector_type(c->consttype);
		}

		case T_FuncExpr:
		{
			/* Can vectorize some functions! */
			FuncExpr *f = castNode(FuncExpr, expr);
			return is_vector_function(vqinfo,
									  f->args,
									  f->funcid,
									  f->funcresulttype,
									  f->inputcollid);
		}

		case T_OpExpr:
		{
			OpExpr *o = castNode(OpExpr, expr);
			return is_vector_function(vqinfo,
									  o->args,
									  o->opfuncid,
									  o->opresulttype,
									  o->inputcollid);
		}

		case T_Var:
		{
			Var *var = castNode(Var, expr);

			if (var->varattno <= 0)
			{
				/* Can't work with special attributes like tableoid. */
				return false;
			}

			Assert(var->varattno <= vqinfo->maxattno);

			const bool is_vector = vqinfo->vector_attrs && vqinfo->vector_attrs[var->varattno];

			if (is_vector)
			{
				Assert(is_vector_type(var->vartype));
			}

			return is_vector;
		}
		default:
			return false;
	}
}

/*
 * Whether we can vectorize this particular aggregate.
 */
static bool
can_vectorize_aggref(const VectorQualInfo *vqi, Aggref *aggref)
{
	if (aggref->aggdirectargs != NIL)
	{
		/* Can't process ordered-set aggregates with direct arguments. */
		return false;
	}

	if (aggref->aggorder != NIL)
	{
		/* Can't process aggregates with an ORDER BY clause. */
		return false;
	}

	if (aggref->aggdistinct != NIL)
	{
		/* Can't process aggregates with DISTINCT clause. */
		return false;
	}

	if (aggref->aggfilter != NULL)
	{
		/* Can process aggregates with filter clause if it's vectorizable. */
		Node *aggfilter_vectorized = vector_qual_make((Node *) aggref->aggfilter, vqi);
		if (aggfilter_vectorized == NULL)
		{
			return false;
		}
		aggref->aggfilter = (Expr *) aggfilter_vectorized;
	}

	if (get_vector_aggregate(aggref->aggfnoid) == NULL)
	{
		/*
		 * We don't have a vectorized implementation for this particular
		 * aggregate function.
		 */
		return false;
	}

	if (aggref->args == NIL)
	{
		/* This must be count(*), we can vectorize it. */
		return true;
	}

	/* The function must have one argument, check it. */
	Assert(list_length(aggref->args) == 1);
	TargetEntry *argument = castNode(TargetEntry, linitial(aggref->args));

	return is_vector_var(vqi, argument->expr);
}

/*
 * What vectorized grouping strategy we can use for the given grouping columns.
 */
static VectorAggGroupingType
get_vectorized_grouping_type(const VectorQualInfo *vqinfo, Agg *agg, List *resolved_targetlist)
{
	/*
	 * The Agg->numCols value can be less than the number of the non-aggregated
	 * vars in the aggregated targetlist, if some of them are equated to a
	 * constant. This behavior started with PG 16. This case is not very
	 * important, so we treat all non-aggregated columns as grouping columns to
	 * keep the vectorized aggregation node simple.
	 */
	int num_grouping_columns = 0;
	bool all_segmentby = true;

	Oid single_grouping_var_type = InvalidOid;
	int16 typlen = 0;
	bool typbyval = false;

	ListCell *lc;
	foreach (lc, resolved_targetlist)
	{
		TargetEntry *target_entry = lfirst_node(TargetEntry, lc);
		if (IsA(target_entry->expr, Aggref))
		{
			continue;
		}

		num_grouping_columns++;

		if (!is_vector_var(vqinfo, target_entry->expr))
			return VAGT_Invalid;

		/*
		 * Detect whether we're only grouping by segmentby columns, in which
		 * case we can use the whole-batch grouping strategy. Probably this
		 * could be extended to allow arbitrary expressions referencing only the
		 * segmentby columns.
		 */
		if (IsA(target_entry->expr, Var))
		{
			Var *var = castNode(Var, target_entry->expr);
			all_segmentby &= vqinfo->segmentby_attrs[var->varattno];
		}
		else
		{
			all_segmentby = false;
		}

		/*
		 * If we have a single grouping column, record it for the additional
		 * checks later.
		 */
		if (num_grouping_columns != 1)
		{
			continue;
		}

		TupleDesc tdesc = NULL;
		TypeFuncClass type_class =
			get_expr_result_type((Node *) target_entry->expr, &single_grouping_var_type, &tdesc);
		if (type_class != TYPEFUNC_SCALAR)
		{
			continue;
		}

		get_typlenbyval(single_grouping_var_type, &typlen, &typbyval);
		Ensure(typlen != 0, "invalid zero typlen for type %d", single_grouping_var_type);
	}

	Assert(num_grouping_columns >= agg->numCols);

	/*
	 * We support vectorized aggregation without grouping.
	 */
	if (num_grouping_columns == 0)
	{
		return VAGT_Batch;
	}

	/*
	 * We support grouping by any number of columns if all of them are segmentby.
	 */
	if (all_segmentby)
	{
		return VAGT_Batch;
	}

	/*
	 * We support hashed vectorized grouping by one fixed-size by-value
	 * compressed column.
	 * We can use our hash table for GroupAggregate as well, because it preserves
	 * the input order of the keys, but only for the direct order, not reverse.
	 */
	if (num_grouping_columns == 1 && typlen != 0)
	{
		if (typbyval)
		{
			switch (typlen)
			{
				case 1:
#ifdef TS_USE_UMASH
					Assert(single_grouping_var_type == BOOLOID);
					return VAGT_HashSerialized;
#else
					return VAGT_Invalid;
#endif
				case 2:
					return VAGT_HashSingleFixed2;
				case 4:
					return VAGT_HashSingleFixed4;
				case 8:
					return VAGT_HashSingleFixed8;
				default:
					Ensure(false, "invalid fixed size %d of a vector type", typlen);
					break;
			}
		}
#ifdef TS_USE_UMASH
		/*
		 * We also have the UUID type which is by-reference and has a
		 * columnar in-memory representation, but no specialized single-column
		 * vectorized grouping support. It can use the serialized grouping
		 * strategy.
		 */
		else if (single_grouping_var_type == TEXTOID)
		{
			return VAGT_HashSingleText;
		}
#endif
	}

#ifdef TS_USE_UMASH
	/*
	 * Use hashing of serialized keys when we have many grouping columns.
	 */
	return VAGT_HashSerialized;
#else
	return VAGT_Invalid;
#endif
}

/*
 * Check if we have a vectorized aggregation node and the usual Postgres
 * aggregation node in the plan tree. This is used for testing.
 */
bool
has_vector_agg_node(Plan *plan, bool *has_postgres_partial_agg)
{
	if (IsA(plan, Agg) && castNode(Agg, plan)->aggsplit == AGGSPLIT_INITIAL_SERIAL)
	{
		*has_postgres_partial_agg = true;
		return false;
	}

	if (plan->lefttree && has_vector_agg_node(plan->lefttree, has_postgres_partial_agg))
	{
		return true;
	}

	if (plan->righttree && has_vector_agg_node(plan->righttree, has_postgres_partial_agg))
	{
		return true;
	}

	CustomScan *custom = NULL;
	List *append_plans = NIL;
	if (IsA(plan, Append))
	{
		append_plans = castNode(Append, plan)->appendplans;
	}
	if (IsA(plan, MergeAppend))
	{
		append_plans = castNode(MergeAppend, plan)->mergeplans;
	}
	else if (IsA(plan, CustomScan))
	{
		custom = castNode(CustomScan, plan);
		if (strcmp("ChunkAppend", custom->methods->CustomName) == 0)
		{
			append_plans = custom->custom_plans;
		}
	}
	else if (IsA(plan, SubqueryScan))
	{
		SubqueryScan *subquery = castNode(SubqueryScan, plan);
		append_plans = list_make1(subquery->subplan);
	}

	if (append_plans)
	{
		ListCell *lc;
		foreach (lc, append_plans)
		{
			if (has_vector_agg_node(lfirst(lc), has_postgres_partial_agg))
			{
				return true;
			}
		}
		return false;
	}

	if (custom == NULL)
	{
		return false;
	}

	if (strcmp(VECTOR_AGG_NODE_NAME, custom->methods->CustomName) == 0)
	{
		return true;
	}

	return false;
}

/*
 * Check if a VectorAgg is possible on top of the given child plan.
 *
 * If the child plan is compatible, also initialize the VectorQualInfo struct
 * for aggregation FILTER clauses.
 *
 * Returns true if the scan node is a supported child, otherwise false.
 */
static bool
vectoragg_plan_possible(Plan *childplan, const List *rtable, VectorQualInfo *vqi)
{
	if (!IsA(childplan, CustomScan))
		return false;

	if (childplan->qual != NIL)
	{
		/* Can't do vectorized aggregation if we have Postgres quals. */
		return false;
	}

	CustomScan *customscan = castNode(CustomScan, childplan);

	if (strcmp(customscan->methods->CustomName, "ColumnarScan") == 0)
	{
		vectoragg_plan_columnar_scan(childplan, vqi);
		return true;
	}

	return false;
}

/*
 * Where possible, replace the partial aggregation plan nodes with our own
 * vectorized aggregation node. The replacement is done in-place.
 */
Plan *
try_insert_vector_agg_node(Plan *plan, List *rtable)
{
	if (plan->lefttree)
	{
		plan->lefttree = try_insert_vector_agg_node(plan->lefttree, rtable);
	}

	if (plan->righttree)
	{
		plan->righttree = try_insert_vector_agg_node(plan->righttree, rtable);
	}

	List *append_plans = NIL;
	if (IsA(plan, Append))
	{
		append_plans = castNode(Append, plan)->appendplans;
	}
	else if (IsA(plan, MergeAppend))
	{
		append_plans = castNode(MergeAppend, plan)->mergeplans;
	}
	else if (IsA(plan, CustomScan))
	{
		CustomScan *custom = castNode(CustomScan, plan);
		if (strcmp("ChunkAppend", custom->methods->CustomName) == 0)
		{
			append_plans = custom->custom_plans;
		}
	}
	else if (IsA(plan, SubqueryScan))
	{
		SubqueryScan *subquery = castNode(SubqueryScan, plan);
		append_plans = list_make1(subquery->subplan);
	}

	if (append_plans)
	{
		ListCell *lc;
		foreach (lc, append_plans)
		{
			lfirst(lc) = try_insert_vector_agg_node(lfirst(lc), rtable);
		}
		return plan;
	}

	if (plan->type != T_Agg)
	{
		return plan;
	}

	Agg *agg = castNode(Agg, plan);

	if (agg->aggsplit != AGGSPLIT_INITIAL_SERIAL)
	{
		/* Can only vectorize partial aggregation node. */
		return plan;
	}

	if (agg->groupingSets != NIL)
	{
		/* No GROUPING SETS support. */
		return plan;
	}

	if (agg->plan.qual != NIL)
	{
		/*
		 * No HAVING support. Probably we can't have it in this node in any case,
		 * because we only replace the partial aggregation nodes which can't
		 * check the HAVING clause.
		 */
		return plan;
	}

	if (agg->plan.lefttree == NULL)
	{
		/*
		 * Not sure what this would mean, but check for it just to be on the
		 * safe side because we can effectively see any possible plan here.
		 */
		return plan;
	}

	Plan *childplan = agg->plan.lefttree;
	VectorQualInfo vqi;
	MemSet(&vqi, 0, sizeof(VectorQualInfo));

	/*
	 * Build supplementary info to determine whether we can vectorize the
	 * aggregate FILTER clauses.
	 */
	if (!vectoragg_plan_possible(childplan, rtable, &vqi))
	{
		/* Not a compatible vectoragg child node */
		return plan;
	}

	/*
	 * To make it easier to examine the variables participating in the aggregation,
	 * the subsequent checks are performed on the aggregated targetlist with
	 * all variables resolved to uncompressed chunk variables.
	 */
	List *resolved_targetlist = resolve_outer_special_vars(agg->plan.targetlist, childplan);

	const VectorAggGroupingType grouping_type =
		get_vectorized_grouping_type(&vqi, agg, resolved_targetlist);
	if (grouping_type == VAGT_Invalid)
	{
		/* The grouping is not vectorizable. */
		return plan;
	}

	/*
	 * The hash grouping strategies do not preserve the input key order when the
	 * reverse ordering is requested, so in this case they cannot work in
	 * GroupAggregate mode.
	 */
	if (grouping_type != VAGT_Batch && agg->aggstrategy != AGG_HASHED)
	{
		if (vqi.reverse)
		{
			return plan;
		}
	}

	/* Now check the output targetlist. */
	ListCell *lc;
	foreach (lc, resolved_targetlist)
	{
		TargetEntry *target_entry = lfirst_node(TargetEntry, lc);
		if (IsA(target_entry->expr, Aggref))
		{
			Aggref *aggref = castNode(Aggref, target_entry->expr);
			if (!can_vectorize_aggref(&vqi, aggref))
			{
				/* Aggregate function not vectorizable. */
				return plan;
			}
		}
	}

	/*
	 * Finally, all requirements are satisfied and we can vectorize this partial
	 * aggregation node.
	 */
	return vector_agg_plan_create(childplan, agg, resolved_targetlist, grouping_type);
}
