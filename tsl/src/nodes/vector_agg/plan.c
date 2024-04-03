
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
#include "functions.h"
#include "utils.h"
#include "nodes/decompress_chunk/planner.h"

static struct CustomScanMethods scan_methods = { .CustomName = "VectorAgg",
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
	if (var->varno != OUTER_VAR)
	{
		return node;
	}

	TargetEntry *decompress_chunk_tentry =
		castNode(TargetEntry, list_nth(context, var->varattno - 1));
	Var *uncompressed_var = castNode(Var, decompress_chunk_tentry->expr);
	return (Node *) copyObject(uncompressed_var);
}

/*
 * Resolve the OUTER_VAR special variables, that are used in the output
 * targetlists of aggregation nodes, replacing them with the uncompressed chunk
 * variables.
 */
static List *
resolve_outer_special_vars(List *agg_tlist, List *outer_tlist)
{
	return castNode(List, resolve_outer_special_vars_mutator((Node *) agg_tlist, outer_tlist));
}

/*
 * Create a vectorized aggregation node to replace the given partial aggregation
 * node.
 */
static Plan *
vector_agg_plan_create(Agg *agg, CustomScan *decompress_chunk)
{
	CustomScan *custom = (CustomScan *) makeNode(CustomScan);
	custom->custom_plans = list_make1(decompress_chunk);
	custom->methods = &scan_methods;

	/*
	 * Note that this is being called from the post-planning hook, and therefore
	 * after set_plan_refs(). The meaning of output targetlists is different from
	 * the previous planning stages, and they contain special varnos referencing
	 * the scan targetlists.
	 */
	custom->custom_scan_tlist =
		resolve_outer_special_vars(agg->plan.targetlist, decompress_chunk->scan.plan.targetlist);
	custom->scan.plan.targetlist =
		build_trivial_custom_output_targetlist(custom->custom_scan_tlist);

	/*
	 * Copy the costs from the normal aggregation node, so that they show up in
	 * the EXPLAIN output. They are not used for any other purposes, because
	 * this hook is called after the planning is finished.
	 */
	custom->scan.plan.plan_rows = agg->plan.plan_rows;
	custom->scan.plan.plan_width = agg->plan.plan_width;
	custom->scan.plan.startup_cost = agg->plan.startup_cost;
	custom->scan.plan.total_cost = agg->plan.total_cost;

	return (Plan *) custom;
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

	if (agg->numCols != 0)
	{
		/* No GROUP BY support for now. */
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

	if (list_length(agg->plan.targetlist) != 1)
	{
		/* We currently handle only one agg function per node. */
		return plan;
	}

	Node *expr_node = (Node *) castNode(TargetEntry, linitial(agg->plan.targetlist))->expr;
	if (!IsA(expr_node, Aggref))
	{
		return plan;
	}

	Aggref *aggref = castNode(Aggref, expr_node);

	if (aggref->aggfilter != NULL)
	{
		/* Filter clause on aggregate is not supported. */
		return plan;
	}

	if (get_vector_aggregate(aggref->aggfnoid) == NULL)
	{
		return plan;
	}

	TargetEntry *argument = castNode(TargetEntry, linitial(aggref->args));
	if (!IsA(argument->expr, Var))
	{
		/* Can aggregate only a bare decompressed column, not an expression. */
		return plan;
	}
	Var *aggregated_var = castNode(Var, argument->expr);

	/*
	 * Check if this particular column is a segmentby or has bulk decompression
	 * enabled. This hook is called after set_plan_refs, and at this stage the
	 * output targetlist of the aggregation node uses OUTER_VAR references into
	 * the child scan targetlist, so first we have to translate this.
	 */
	Assert(aggregated_var->varno == OUTER_VAR);
	TargetEntry *decompressed_target_entry =
		list_nth(custom->scan.plan.targetlist, AttrNumberGetAttrOffset(aggregated_var->varattno));

	if (!IsA(decompressed_target_entry->expr, Var))
	{
		/*
		 * Can only aggregate the plain Vars. Not sure if this is redundant with
		 * the similar check above.
		 */
		return plan;
	}
	Var *decompressed_var = castNode(Var, decompressed_target_entry->expr);

	/*
	 * Now, we have to translate the decompressed varno into the compressed
	 * column index, to check if the column supports bulk decompression.
	 */
	List *decompression_map = list_nth(custom->custom_private, DCP_DecompressionMap);
	List *is_segmentby_column = list_nth(custom->custom_private, DCP_IsSegmentbyColumn);
	List *bulk_decompression_column = list_nth(custom->custom_private, DCP_BulkDecompressionColumn);
	int compressed_column_index = 0;
	for (; compressed_column_index < list_length(decompression_map); compressed_column_index++)
	{
		if (list_nth_int(decompression_map, compressed_column_index) == decompressed_var->varattno)
		{
			break;
		}
	}
	Ensure(compressed_column_index < list_length(decompression_map), "compressed column not found");
	Assert(list_length(decompression_map) == list_length(bulk_decompression_column));
	const bool bulk_decompression_enabled_for_column =
		list_nth_int(bulk_decompression_column, compressed_column_index);

	/* Bulk decompression can also be disabled globally. */
	List *settings = linitial(custom->custom_private);
	const bool bulk_decompression_enabled_globally =
		list_nth_int(settings, DCS_EnableBulkDecompression);

	/*
	 * We support vectorized aggregation either for segmentby columns or for
	 * columns with bulk decompression enabled.
	 */
	if (!list_nth_int(is_segmentby_column, compressed_column_index) &&
		!(bulk_decompression_enabled_for_column && bulk_decompression_enabled_globally))
	{
		/* Vectorized aggregation not possible for this particular column. */
		return plan;
	}

	/*
	 * Finally, all requirements are satisfied and we can vectorize this partial
	 * aggregation node.
	 */
	return vector_agg_plan_create(agg, custom);
}
