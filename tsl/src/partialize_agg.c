/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_operator.h>
#include <miscadmin.h>
#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/typcache.h>
#include <utils/fmgroids.h>

#include <planner.h>

#include "compression/compression.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "partialize_agg.h"
#include "utils.h"
#include "debug_assert.h"

#include "nodes/vector_agg/vector_agg.h"

/*
 * Are we able to optimize the path by applying vectorized aggregation?
 */
static bool
is_vectorizable_agg_path(PlannerInfo *root, AggPath *agg_path, Path *path)
{
	Assert(agg_path->aggstrategy == AGG_SORTED || agg_path->aggstrategy == AGG_PLAIN ||
		   agg_path->aggstrategy == AGG_HASHED);

	/* Having is not supported at the moment */
	if (root->hasHavingQual)
		return false;

	/* Only vectorizing within the decompress node is supported so far */
	bool is_decompress_chunk = ts_is_decompress_chunk_path(path);
	if (!is_decompress_chunk)
		return false;

#ifdef USE_ASSERT_CHECKING
	DecompressChunkPath *decompress_path = (DecompressChunkPath *) path;
	Assert(decompress_path->custom_path.custom_paths != NIL);

	/* Hypertable compression info is already fetched from the catalog */
	Assert(decompress_path->info != NULL);
#endif

	/* No filters on the compressed attributes are supported at the moment */
	if ((list_length(path->parent->baserestrictinfo) > 0 || path->parent->joininfo != NULL))
		return false;

	/* We currently handle only one agg function per node */
	if (list_length(agg_path->path.pathtarget->exprs) != 1)
		return false;

	/* Only sum on int 4 is supported at the moment */
	Node *expr_node = linitial(agg_path->path.pathtarget->exprs);
	if (!IsA(expr_node, Aggref))
		return false;

	Aggref *aggref = castNode(Aggref, expr_node);

	/* Filter expressions in the aggregate are not supported */
	if (aggref->aggfilter != NULL)
		return false;

	if (aggref->aggfnoid != F_SUM_INT4)
		return false;

	/* Can aggregate only a bare decompressed column, not an expression. */
	TargetEntry *argument = castNode(TargetEntry, linitial(aggref->args));
	if (!IsA(argument->expr, Var))
	{
		return false;
	}

	return true;
}

/*
 * Check if we can perform the computation of the aggregate in a vectorized manner directly inside
 * of the decompress chunk node. If this is possible, the decompress chunk node will emit partial
 * aggregates directly, and there is no need for the PostgreSQL aggregation node on top.
 */
bool
apply_vectorized_agg_optimization(PlannerInfo *root, AggPath *aggregation_path, Path *path)
{
	return false;
//
//	if (!ts_guc_enable_vectorized_aggregation || !ts_guc_enable_bulk_decompression)
//		return false;
//
//	Assert(path != NULL);
//	Assert(aggregation_path->aggsplit == AGGSPLIT_INITIAL_SERIAL);
//
//	if (is_vectorizable_agg_path(root, aggregation_path, path))
//	{
//		Assert(ts_is_decompress_chunk_path(path));
//		DecompressChunkPath *decompress_path = (DecompressChunkPath *) castNode(CustomPath, path);
//
//		/* Change the output of the path and let the decompress chunk node emit partial aggregates
//		 * directly */
//		decompress_path->perform_vectorized_aggregation = true;
//
//		decompress_path->custom_path.path.pathtarget = aggregation_path->path.pathtarget;
//
//		/* The decompress chunk node can perform the aggregation directly. No need for a
//dedicated
//		 * agg node on top. */
//		return true;
//	}
//
//	/* PostgreSQL should handle the aggregation. Regular agg node on top is required. */
//	return false;
}

static Plan *
insert_vector_agg_node(Plan *plan)
{
	if (plan->lefttree)
	{
		plan->lefttree = insert_vector_agg_node(plan->lefttree);
	}

	if (plan->righttree)
	{
		plan->righttree = insert_vector_agg_node(plan->righttree);
	}

	if (IsA(plan, Append))
	{
		List *plans = castNode(Append, plan)->appendplans;
		ListCell *lc;
		foreach (lc, plans)
		{
			lfirst(lc) = insert_vector_agg_node(lfirst(lc));
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
		 * chech HAVING.
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
	 * aggregation node uses OUTER_VAR references into the child scan targetlist,
	 * so first we have to translate this.
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
	 * columns wiht bulk decompression enabled.
	 */
	if (!list_nth_int(is_segmentby_column, compressed_column_index) &&
		!(bulk_decompression_enabled_for_column && bulk_decompression_enabled_globally))
	{
		/* Vectorized aggregation not possible for this particular column. */
		fprintf(stderr, "compressed column index %d\n", compressed_column_index);
		// my_print(bulk_decompression_column);
		return plan;
	}

	//	bool perform_vectorized_aggregation = list_nth_int(linitial(custom->custom_private), 5);
	//	if (!perform_vectorized_aggregation)
	//	{
	//		fprintf(stderr, "no vectorized aggregation\n");
	//		return plan;
	//	}

	fprintf(stderr, "found!!!\n");
	// my_print(plan);
	// mybt();

	return vector_agg_plan_create(agg, custom);
}

void
tsl_postprocess_plan(PlannedStmt *stmt)
{
	// mybt();
	// my_print(stmt);

	if (ts_guc_enable_vectorized_aggregation)
	{
		stmt->planTree = insert_vector_agg_node(stmt->planTree);
	}

	// fprintf(stderr, "postprocessed:\n");
	// my_print(stmt->planTree);
}
