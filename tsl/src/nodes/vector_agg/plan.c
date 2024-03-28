
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
#include "utils.h"

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
replace_outer_special_vars_mutator(Node *node, void *context)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (!IsA(node, Var))
	{
		return expression_tree_mutator(node, replace_outer_special_vars_mutator, context);
	}

	Var *var = castNode(Var, node);
	if (var->varno != OUTER_VAR)
	{
		return node;
	}

	var = copyObject(var);
	var->varno = DatumGetInt32(PointerGetDatum(context));
	return (Node *) var;
}

/*
 * Replace the OUTER_VAR special variables, that are used in the output
 * targetlists of aggregation nodes, with the given other varno.
 */
static List *
replace_outer_special_vars(List *input, int target_varno)
{
	return castNode(List,
					replace_outer_special_vars_mutator((Node *) input,
													   DatumGetPointer(
														   Int32GetDatum(target_varno))));
}

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
		replace_outer_special_vars(agg->plan.targetlist, decompress_chunk->scan.scanrelid);
	custom->scan.plan.targetlist =
		build_trivial_custom_output_targetlist(custom->custom_scan_tlist);

	return (Plan *) custom;
}

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

	if (IsA(plan, Append))
	{
		List *plans = castNode(Append, plan)->appendplans;
		ListCell *lc;
		foreach (lc, plans)
		{
			lfirst(lc) = try_insert_vector_agg_node(lfirst(lc));
		}
	}

	if (plan->type != T_Agg)
	{
		return plan;
	}

	fprintf(stderr, "found agg!\n");

	Agg *agg = castNode(Agg, plan);

	if (agg->aggsplit != AGGSPLIT_INITIAL_SERIAL)
	{
		fprintf(stderr, "wrong split %d\n", agg->aggsplit);
		return plan;
	}

	if (agg->plan.lefttree == NULL)
	{
		fprintf(stderr, "no leftnode?\n");
		return plan;
	}

	if (!IsA(agg->plan.lefttree, CustomScan))
	{
		fprintf(stderr, "not custom\n");
		// my_print(agg->plan.lefttree);
		return plan;
	}

	CustomScan *custom = castNode(CustomScan, agg->plan.lefttree);
	if (strcmp(custom->methods->CustomName, "DecompressChunk") != 0)
	{
		fprintf(stderr, "not decompress chunk\n");
		return plan;
	}

	if (custom->scan.plan.qual != NIL)
	{
		/* Can't do vectorized aggregation if we have Postgres quals. */
		return plan;
	}

	if (linitial(custom->custom_exprs) != NIL)
	{
		/* Even the vectorized filters are not supported at the moment. */
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

	if (aggref->aggfnoid != F_SUM_INT4)
	{
		/* We only support sum(int4) at the moment. */
		return plan;
	}

	TargetEntry *argument = castNode(TargetEntry, linitial(aggref->args));
	if (!IsA(argument->expr, Var))
	{
		/* Can aggregate only a bare decompressed column, not an expression. */
		return plan;
	}
	Var *aggregated_var = castNode(Var, argument->expr);
	// my_print(aggregated_var);

	/*
	 * Check if this particular column is a segmentby or has bulk decompression
	 * enabled. This hook is called after set_plan_refs, and at this stage the
	 * output targetlist of the aggregation node uses OUTER_VAR references into
	 * the child scan targetlist, so first we have to translate this.
	 */
	Assert(aggregated_var->varno == OUTER_VAR);
	TargetEntry *decompressed_target_entry =
		list_nth(custom->scan.plan.targetlist, AttrNumberGetAttrOffset(aggregated_var->varattno));
	// my_print(decompressed_target_entry);

	if (!IsA(decompressed_target_entry->expr, Var))
	{
		/*
		 * Can only aggregate the plain Vars. Not sure if this is redundant with
		 * the similar check above.
		 */
		return plan;
	}
	Var *decompressed_var = castNode(Var, decompressed_target_entry->expr);
	// my_print(decompressed_var);

	/*
	 * Now, we have to translate the decompressed varno into the compressed
	 * column index, to check if the column supports bulk decompression.
	 */
	List *decompression_map = list_nth(custom->custom_private, 1);
	List *is_segmentby_column = list_nth(custom->custom_private, 2);
	List *bulk_decompression_column = list_nth(custom->custom_private, 3);
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
	const bool bulk_decompression_enabled_globally = list_nth_int(settings, 4);

	/*
	 * We support vectorized aggregation either for segmentby columns or for
	 * columns with bulk decompression enabled.
	 */
	if (!list_nth_int(is_segmentby_column, compressed_column_index) &&
		!(bulk_decompression_enabled_for_column && bulk_decompression_enabled_globally))
	{
		/* Vectorized aggregation not possible for this particular column. */
		fprintf(stderr, "compressed column index %d\n", compressed_column_index);
		// my_print(bulk_decompression_column);
		return plan;
	}

	fprintf(stderr, "found!!!\n");
	// my_print(plan);
	// mybt();

	return vector_agg_plan_create(agg, custom);
}
