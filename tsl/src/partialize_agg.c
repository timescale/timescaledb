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

	DecompressChunkPath *decompress_path = (DecompressChunkPath *) path;
	Assert(decompress_path->custom_path.custom_paths != NIL);

	/* Hypertable compression info is already fetched from the catalog */
	Assert(decompress_path->info != NULL);
	Assert(decompress_path->info->hypertable_compression_info != NULL);

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

	/*
	 * Check that the input columns of the aggregate can be processed by our vectorized
	 * implementation. This is possible for (1) segment_by columns and (2) for columns which allow
	 * bulk decompression.
	 *
	 * Bulk decompression is needed to produce the ArrowArray and perform the vectorized operations.
	 * Bulk decompression should always be possible in the current implementation since we check for
	 * the data type above. However, when we lift the restriction, the check becomes necessary.
	 *
	 * Note: decompress_path->bulk_decompression_column is not populated at this point. So, we have
	 * to get this data from hypertable_compression_info.
	 */
	ListCell *lc;
	foreach (lc, aggref->args)
	{
		Node *agg_arg = lfirst(lc);

		if (!IsA(agg_arg, TargetEntry))
			return false;

		TargetEntry *target_entry = castNode(TargetEntry, agg_arg);

		if (!IsA(target_entry->expr, Var))
			return false;

		Var *var = castNode(Var, target_entry->expr);

		/* Agg input var is on the compressed relation */
		Assert((Index) var->varno == path->parent->relid);
		Assert((Index) var->varno == decompress_path->info->chunk_rel->relid);

		char *column_name =
			get_attname(decompress_path->info->chunk_rte->relid, var->varattno, false);

		FormData_hypertable_compression *ci =
			get_column_compressioninfo(decompress_path->info->hypertable_compression_info,
									   column_name);
		Assert(ci);

		/* If this is a segment_by value, allow vectorization for sum */
		if (ci->segmentby_column_index > 0)
			continue;

		bool bulk_decompression_possible = (tsl_get_decompress_all_function(ci->algo_id) != NULL);

		if (!bulk_decompression_possible)
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
	if (!ts_guc_enable_vectorized_aggregation || !ts_guc_enable_bulk_decompression)
		return false;

	Assert(path != NULL);
	Assert(aggregation_path->aggsplit == AGGSPLIT_INITIAL_SERIAL);

	if (is_vectorizable_agg_path(root, aggregation_path, path))
	{
		Assert(ts_is_decompress_chunk_path(path));
		DecompressChunkPath *decompress_path = (DecompressChunkPath *) castNode(CustomPath, path);

		/* Change the output of the path and let the decompress chunk node emit partial aggregates
		 * directly */
		decompress_path->perform_vectorized_aggregation = true;
		decompress_path->custom_path.path.pathtarget = aggregation_path->path.pathtarget;

		/* The decompress chunk node can perform the aggregation directly. No need for a dedicated
		 * agg node on top. */
		return true;
	}

	/* PostgreSQL should handle the aggregation. Regular agg node on top is required. */
	return false;
}
