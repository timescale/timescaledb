/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <commands/explain.h>
#include <executor/executor.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <utils/fmgroids.h>

#include "plan.h"

#include "exec.h"
#include "nodes/decompress_chunk/planner.h"
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
		 * by expressions in the output targetlist of DecompressChunk node.
		 */
		return (Node *) copyObject(var);
	}

	if (var->varno == OUTER_VAR)
	{
		/*
		 * Reference into the output targetlist of the DecompressChunk node.
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
resolve_outer_special_vars(List *agg_tlist, CustomScan *custom)
{
	return castNode(List, resolve_outer_special_vars_mutator((Node *) agg_tlist, custom));
}

/*
 * Create a vectorized aggregation node to replace the given partial aggregation
 * node.
 */
static Plan *
vector_agg_plan_create(Agg *agg, CustomScan *decompress_chunk, List *resolved_targetlist)
{
	CustomScan *vector_agg = (CustomScan *) makeNode(CustomScan);
	vector_agg->custom_plans = list_make1(decompress_chunk);
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
	vector_agg->scan.plan.parallel_safe = decompress_chunk->scan.plan.parallel_safe;
	vector_agg->scan.plan.async_capable = false;

	vector_agg->scan.plan.plan_node_id = agg->plan.plan_node_id;

	Assert(agg->plan.qual == NIL);

	vector_agg->scan.plan.initPlan = agg->plan.initPlan;

	vector_agg->scan.plan.extParam = bms_copy(agg->plan.extParam);
	vector_agg->scan.plan.allParam = bms_copy(agg->plan.allParam);

	List *grouping_col_offsets = NIL;
	for (int i = 0; i < agg->numCols; i++)
	{
		grouping_col_offsets =
			lappend_int(grouping_col_offsets, AttrNumberGetAttrOffset(agg->grpColIdx[i]));
	}
	vector_agg->custom_private = list_make1(grouping_col_offsets);

	return (Plan *) vector_agg;
}

/*
 * Whether the expression can be used for vectorized processing: must be a Var
 * that refers to either a bulk-decompressed or a segmentby column.
 */
static bool
is_vector_var(CustomScan *custom, Expr *expr, bool *out_is_segmentby)
{
	if (!IsA(expr, Var))
	{
		/* Can aggregate only a bare decompressed column, not an expression. */
		return false;
	}

	Var *decompressed_var = castNode(Var, expr);

	/*
	 * This must be called after resolve_outer_special_vars(), so we should only
	 * see the uncompressed chunk variables here.
	 */
	Ensure((Index) decompressed_var->varno == (Index) custom->scan.scanrelid,
		   "expected scan varno %d got %d",
		   custom->scan.scanrelid,
		   decompressed_var->varno);

	if (decompressed_var->varattno <= 0)
	{
		/* Can't work with special attributes like tableoid. */
		if (out_is_segmentby)
		{
			*out_is_segmentby = false;
		}
		return false;
	}

	/*
	 * Now, we have to translate the decompressed varno into the compressed
	 * column index, to check if the column supports bulk decompression.
	 */
	List *decompression_map = list_nth(custom->custom_private, DCP_DecompressionMap);
	int compressed_column_index = 0;
	for (; compressed_column_index < list_length(decompression_map); compressed_column_index++)
	{
		const int custom_scan_attno = list_nth_int(decompression_map, compressed_column_index);
		if (custom_scan_attno <= 0)
		{
			continue;
		}

		int uncompressed_chunk_attno = 0;
		if (custom->custom_scan_tlist == NIL)
		{
			uncompressed_chunk_attno = custom_scan_attno;
		}
		else
		{
			Var *var = castNode(Var,
								castNode(TargetEntry,
										 list_nth(custom->custom_scan_tlist,
												  AttrNumberGetAttrOffset(custom_scan_attno)))
									->expr);
			uncompressed_chunk_attno = var->varattno;
		}

		if (uncompressed_chunk_attno == decompressed_var->varattno)
		{
			break;
		}
	}
	Ensure(compressed_column_index < list_length(decompression_map), "compressed column not found");

	List *bulk_decompression_column = list_nth(custom->custom_private, DCP_BulkDecompressionColumn);
	Assert(list_length(decompression_map) == list_length(bulk_decompression_column));
	const bool bulk_decompression_enabled_for_column =
		list_nth_int(bulk_decompression_column, compressed_column_index);

	/*
	 * Bulk decompression can be disabled for all columns in the DecompressChunk
	 * node settings, we can't do vectorized aggregation for compressed columns
	 * in that case. For segmentby columns it's still possible.
	 */
	List *settings = linitial(custom->custom_private);
	const bool bulk_decompression_enabled_globally =
		list_nth_int(settings, DCS_EnableBulkDecompression);

	/*
	 * Check if this column is a segmentby.
	 */
	List *is_segmentby_column = list_nth(custom->custom_private, DCP_IsSegmentbyColumn);
	Assert(list_length(is_segmentby_column) == list_length(decompression_map));
	const bool is_segmentby = list_nth_int(is_segmentby_column, compressed_column_index);
	if (out_is_segmentby)
	{
		*out_is_segmentby = is_segmentby;
	}

	/*
	 * We support vectorized aggregation either for segmentby columns or for
	 * columns with bulk decompression enabled.
	 */
	if (!is_segmentby &&
		!(bulk_decompression_enabled_for_column && bulk_decompression_enabled_globally))
	{
		/* Vectorized aggregation not possible for this particular column. */
		return false;
	}

	return true;
}

static bool
can_vectorize_aggref(Aggref *aggref, CustomScan *custom)
{
	if (aggref->aggfilter != NULL)
	{
		/* Filter clause on aggregate is not supported. */
		return false;
	}

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
		/* Can't process aggregates with filter clause. */
		return false;
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
	if (!is_vector_var(custom, argument->expr, NULL))
	{
		return false;
	}

	return true;
}

/*
 * Whether we can perform vectorized aggregation with a given grouping.
 * Currently supports either no grouping or grouping by segmentby columns.
 */
static bool
can_vectorize_grouping(Agg *agg, CustomScan *custom, List *resolved_targetlist)
{
	if (agg->numCols == 0)
	{
		return true;
	}

	for (int i = 0; i < agg->numCols; i++)
	{
		int offset = AttrNumberGetAttrOffset(agg->grpColIdx[i]);
		TargetEntry *entry = list_nth_node(TargetEntry, resolved_targetlist, offset);

		bool is_segmentby = false;
		if (!is_vector_var(custom, entry->expr, &is_segmentby))
		{
			return false;
		}

		if (!is_segmentby)
		{
			return false;
		}
	}

	return true;
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
	else if (IsA(plan, CustomScan))
	{
		custom = castNode(CustomScan, plan);
		if (strcmp("ChunkAppend", custom->methods->CustomName) == 0)
		{
			append_plans = custom->custom_plans;
		}
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
 * Where possible, replace the partial aggregation plan nodes with our own
 * vectorized aggregation node. The replacement is done in-place.
 */
Plan *
try_insert_vector_agg_node(Plan *plan)
{
	if (plan->lefttree)
	{
		plan->lefttree = try_insert_vector_agg_node(plan->lefttree);
	}

	if (plan->righttree)
	{
		plan->righttree = try_insert_vector_agg_node(plan->righttree);
	}

	List *append_plans = NIL;
	if (IsA(plan, Append))
	{
		append_plans = castNode(Append, plan)->appendplans;
	}
	else if (IsA(plan, CustomScan))
	{
		CustomScan *custom = castNode(CustomScan, plan);
		if (strcmp("ChunkAppend", custom->methods->CustomName) == 0)
		{
			append_plans = custom->custom_plans;
		}
	}

	if (append_plans)
	{
		ListCell *lc;
		foreach (lc, append_plans)
		{
			lfirst(lc) = try_insert_vector_agg_node(lfirst(lc));
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

	if (!IsA(agg->plan.lefttree, CustomScan))
	{
		/*
		 * Should have a Custom Scan under aggregation.
		 */
		return plan;
	}

	CustomScan *custom = castNode(CustomScan, agg->plan.lefttree);
	if (strcmp(custom->methods->CustomName, "DecompressChunk") != 0)
	{
		/*
		 * It should be our DecompressChunk node.
		 */
		return plan;
	}

	if (custom->scan.plan.qual != NIL)
	{
		/* Can't do vectorized aggregation if we have Postgres quals. */
		return plan;
	}

	/*
	 * To make it easier to examine the variables participating in the aggregation,
	 * the subsequent checks are performed on the aggregated targetlist with
	 * all variables resolved to uncompressed chunk variables.
	 */
	List *resolved_targetlist = resolve_outer_special_vars(agg->plan.targetlist, custom);

	if (!can_vectorize_grouping(agg, custom, resolved_targetlist))
	{
		/* No GROUP BY support for now. */
		return plan;
	}

	/* Now check the output targetlist. */
	ListCell *lc;
	foreach (lc, resolved_targetlist)
	{
		TargetEntry *target_entry = castNode(TargetEntry, lfirst(lc));
		if (IsA(target_entry->expr, Aggref))
		{
			Aggref *aggref = castNode(Aggref, target_entry->expr);
			if (!can_vectorize_aggref(aggref, custom))
			{
				/* Aggregate function not vectorizable. */
				return plan;
			}
		}
		else if (IsA(target_entry->expr, Var))
		{
			if (!is_vector_var(custom, target_entry->expr, NULL))
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
	return vector_agg_plan_create(agg, custom, resolved_targetlist);
}
