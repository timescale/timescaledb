/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <commands/explain.h>
#include <executor/executor.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <utils/fmgroids.h>

#include "plan.h"

#include "exec.h"
#include "import/list.h"
#include "nodes/decompress_chunk/vector_quals.h"
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
		TargetEntry *decompress_chunk_tentry =
			castNode(TargetEntry, list_nth(custom->scan.plan.targetlist, var->varattno - 1));

		return resolve_outer_special_vars_mutator((Node *) decompress_chunk_tentry->expr, context);
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
vector_agg_plan_create(VectorAggPlanInfo *vpinfo, Agg *agg, List *resolved_targetlist,
					   VectorAggGroupingType grouping_type)
{
	CustomScan *vector_agg = (CustomScan *) makeNode(CustomScan);
	vector_agg->custom_plans = list_make1(vpinfo->childplan);
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
	vector_agg->scan.plan.parallel_safe = vpinfo->childplan->plan.parallel_safe;
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

/*
 * Whether the expression can be used for vectorized processing: must be a Var
 * that refers to either a bulk-decompressed or a segmentby column.
 */
static bool
is_vector_var(VectorAggPlanInfo *vpinfo, Expr *expr, bool *out_is_segmentby)
{
	if (!IsA(expr, Var))
	{
		/* Can aggregate only a bare decompressed column, not an expression. */
		return false;
	}

	Var *var = castNode(Var, expr);

	/*
	 * This must be called after resolve_outer_special_vars(), so we should only
	 * see the uncompressed chunk variables here.
	 */
	Ensure((Index) var->varno == (Index) vpinfo->childplan->scanrelid,
		   "expected scan varno %d got %d",
		   vpinfo->childplan->scanrelid,
		   var->varno);

	if (var->varattno <= 0)
	{
		/* Can't work with special attributes like tableoid. */
		if (out_is_segmentby)
		{
			*out_is_segmentby = false;
		}
		return false;
	}

	return vpinfo->is_vector_var(vpinfo, var, out_is_segmentby);
}

/*
 * Whether we can vectorize this particular aggregate.
 */
static bool
can_vectorize_aggref(VectorAggPlanInfo *vpinfo, Aggref *aggref, VectorQualInfo *vqi)
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
	if (!is_vector_var(vpinfo, argument->expr, NULL))
	{
		return false;
	}

	return true;
}

/*
 * What vectorized grouping strategy we can use for the given grouping columns.
 */
static VectorAggGroupingType
get_vectorized_grouping_type(VectorAggPlanInfo *vpinfo, Agg *agg, List *resolved_targetlist)
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
	Var *single_grouping_var = NULL;

	ListCell *lc;
	foreach (lc, resolved_targetlist)
	{
		TargetEntry *target_entry = lfirst_node(TargetEntry, lc);
		if (IsA(target_entry->expr, Aggref))
		{
			continue;
		}

		if (!IsA(target_entry->expr, Var))
		{
			/*
			 * We shouldn't see anything except Vars or Aggrefs in the
			 * aggregated targetlists. Just say it's not vectorizable, because
			 * here we are working with arbitrary plans that we don't control.
			 */
			return VAGT_Invalid;
		}

		num_grouping_columns++;

		Var *var = castNode(Var, target_entry->expr);
		bool is_segmentby;
		if (!is_vector_var(vpinfo, (Expr *) var, &is_segmentby))
		{
			return VAGT_Invalid;
		}

		all_segmentby &= is_segmentby;

		/*
		 * If we have a single grouping column, record it for the additional
		 * checks later.
		 */
		single_grouping_var = var;
	}

	if (num_grouping_columns != 1)
	{
		single_grouping_var = NULL;
	}

	Assert(num_grouping_columns == 1 || single_grouping_var == NULL);
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
	 */
	if (num_grouping_columns == 1)
	{
		int16 typlen;
		bool typbyval;

		get_typlenbyval(single_grouping_var->vartype, &typlen, &typbyval);
		if (typbyval)
		{
			switch (typlen)
			{
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
	}

	return VAGT_Invalid;
}

/*
 * Check if we have a vectorized aggregation node and the usual Postgres
 * aggregation node in the plan tree. This is used for testing.
 */
bool
has_vector_agg_node(Plan *plan, bool *has_normal_agg)
{
	if (IsA(plan, Agg))
	{
		*has_normal_agg = true;
	}

	if (plan->lefttree && has_vector_agg_node(plan->lefttree, has_normal_agg))
	{
		return true;
	}

	if (plan->righttree && has_vector_agg_node(plan->righttree, has_normal_agg))
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
			if (has_vector_agg_node(lfirst(lc), has_normal_agg))
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
 * Initialize plan info context for VectorAgg.
 *
 * Returns true if the scan node is a supported child, otherwise false.
 */
static bool
vector_plan_info_init(VectorAggPlanInfo *vpinfo, Plan *childplan, const List *rtable)
{
	if (!IsA(childplan, CustomScan))
		return false;

	CustomScan *customscan = castNode(CustomScan, childplan);
	RangeTblEntry *rte = rt_fetch(customscan->scan.scanrelid, rtable);
	Relation rel = table_open(rte->relid, AccessShareLock);
	bool vectoragg_possible = false;
	vpinfo->childplan = &customscan->scan;

	if (strcmp(customscan->methods->CustomName, "DecompressChunk") == 0)
		vectoragg_possible = vectoragg_plan_info_init_decompress_chunk(vpinfo, rel);

	table_close(rel, NoLock);

	return vectoragg_possible;
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
	VectorAggPlanInfo vpinfo;

	if (!vector_plan_info_init(&vpinfo, childplan, rtable))
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
		get_vectorized_grouping_type(&vpinfo, agg, resolved_targetlist);
	if (grouping_type == VAGT_Invalid)
	{
		/* The grouping is not vectorizable. */
		return plan;
	}

	/*
	 * Build supplementary info to determine whether we can vectorize the
	 * aggregate FILTER clauses.
	 */
	VectorQualInfo vqi = vpinfo.build_aggfilter(&vpinfo);

	/* Now check the output targetlist. */
	ListCell *lc;
	foreach (lc, resolved_targetlist)
	{
		TargetEntry *target_entry = lfirst_node(TargetEntry, lc);
		if (IsA(target_entry->expr, Aggref))
		{
			Aggref *aggref = castNode(Aggref, target_entry->expr);
			if (!can_vectorize_aggref(&vpinfo, aggref, &vqi))
			{
				/* Aggregate function not vectorizable. */
				return plan;
			}
		}
		else if (IsA(target_entry->expr, Var))
		{
			if (!is_vector_var(&vpinfo, target_entry->expr, NULL))
			{
				/* Variable not vectorizable. */
				return plan;
			}
		}
		else
		{
			/*
			 * Sometimes the plan can require this node to perform a projection,
			 * e.g. we can see a nested loop param in its output targetlist. We
			 * can't handle this case currently.
			 */
			return plan;
		}
	}

	/*
	 * Finally, all requirements are satisfied and we can vectorize this partial
	 * aggregation node.
	 */
	return vector_agg_plan_create(&vpinfo, agg, resolved_targetlist, grouping_type);
}
