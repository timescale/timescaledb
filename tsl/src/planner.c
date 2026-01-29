/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <catalog/pg_trigger.h>
#include <commands/extension.h>
#include <foreign/fdwapi.h>
#include <nodes/nodeFuncs.h>
#include <nodes/parsenodes.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>

#include "compat/compat.h"
#include "chunk.h"
#include "chunkwise_agg.h"
#include "continuous_aggs/planner.h"
#include "guc.h"
#include "hypertable.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/columnar_index_scan/columnar_index_scan.h"
#include "nodes/columnar_scan/columnar_scan.h"
#include "nodes/gapfill/gapfill.h"
#include "nodes/skip_scan/skip_scan.h"
#include "nodes/vector_agg/plan.h"
#include "planner.h"

#include <math.h>

#define OSM_EXTENSION_NAME "timescaledb_osm"

static bool
involves_hypertable(PlannerInfo *root, RelOptInfo *parent)
{
	for (int relid = bms_next_member(parent->relids, -1); relid > 0;
		 relid = bms_next_member(parent->relids, relid))
	{
		Hypertable *ht;
		RelOptInfo *child = root->simple_rel_array[relid];
		/*
		 * RelOptInfo can be null here for join RTEs on PG >= 16. This doesn't
		 * matter because we'll have all the baserels in relids bitmap as well.
		 */
		if (child != NULL && ts_classify_relation(root, child, &ht) == TS_REL_HYPERTABLE)
		{
			return true;
		}
	}
	return false;
}

/*
 * Try to disable bulk decompression on ColumnarScanPath, skipping the above
 * Projection path and also handling Lists.
 */
static Node *
try_disable_bulk_decompression(PlannerInfo *root, Node *node, List *required_pathkeys)
{
	if (node == NULL)
	{
		/*
		 * We can have e.g. AppendPath.subpaths == NULL in case of relations
		 * proven empty, so handle NULLs for simplicity here.
		 */
		return NULL;
	}

	if (IsA(node, List))
	{
		ListCell *lc;
		foreach (lc, (List *) node)
		{
			lfirst(lc) =
				try_disable_bulk_decompression(root, (Node *) lfirst(lc), required_pathkeys);
		}
		return node;
	}

	if (IsA(node, ProjectionPath))
	{
		ProjectionPath *path = castNode(ProjectionPath, node);
		path->subpath = (Path *) try_disable_bulk_decompression(root,
																(Node *) path->subpath,
																required_pathkeys);
		return node;
	}

	if (IsA(node, AppendPath))
	{
		try_disable_bulk_decompression(root,
									   (Node *) castNode(AppendPath, node)->subpaths,
									   required_pathkeys);
		return node;
	}

	if (IsA(node, MergeAppendPath))
	{
		try_disable_bulk_decompression(root,
									   (Node *) castNode(MergeAppendPath, node)->subpaths,
									   required_pathkeys);
		return node;
	}

	if (!IsA(node, CustomPath))
	{
		return node;
	}

	CustomPath *custom_child = castNode(CustomPath, node);
	if (ts_is_chunk_append_path(&custom_child->path))
	{
		try_disable_bulk_decompression(root,
									   (Node *) custom_child->custom_paths,
									   required_pathkeys);
		return node;
	}

	if (!ts_is_columnar_scan_path(&custom_child->path))
	{
		return node;
	}

	/*
	 * At the Path level, a Sort is implied anywhere in the Path tree where the
	 * pathkeys differ. All subplan's rows will be read in this case, so the
	 * optimization does not apply.
	 */
	if (!pathkeys_contained_in(required_pathkeys, custom_child->path.pathkeys))
	{
		return node;
	}

	ColumnarScanPath *dcpath = (ColumnarScanPath *) custom_child;
	if (!dcpath->enable_bulk_decompression)
	{
		return node;
	}

	ColumnarScanPath *path_copy = copy_columnar_scan_path(dcpath);
	path_copy->enable_bulk_decompression = false;
	return (Node *) path_copy;
}

/*
 * When we have a small limit above chunk decompression, it is more efficient to
 * use the row-by-row decompression iterators than the bulk decompression. Since
 * bulk decompression is about 10x faster than row-by-row, this advantage goes
 * away on limits > 100. This hook disables bulk decompression under small limits.
 */
static void
check_limit_bulk_decompression(PlannerInfo *root, Node *node)
{
	ListCell *lc;
	switch (node->type)
	{
		case T_List:
			foreach (lc, (List *) node)
			{
				check_limit_bulk_decompression(root, lfirst(lc));
			}
			break;
		case T_LimitPath:
		{
			double limit = -1;
			LimitPath *path = castNode(LimitPath, node);

			if (path->limitCount != NULL && IsA(path->limitCount, Const))
			{
				Const *count = castNode(Const, path->limitCount);
				Assert(count->consttype == INT8OID);
				int64 count_value = DatumGetInt64(count->constvalue);
				if (count_value < 0)
				{
					/*
					 * The negative LIMIT values produce an error only at the
					 * execution stage, so we have to handle them here. Just
					 * skip the processing in this case, because the query will
					 * fail anyway.
					 */
					break;
				}
				limit = count_value;
			}

			if (path->limitOffset != NULL && IsA(path->limitOffset, Const))
			{
				Const *offset = castNode(Const, path->limitOffset);
				Assert(offset->consttype == INT8OID);
				int64 offset_value = DatumGetInt64(offset->constvalue);
				if (offset_value < 0)
				{
					/* See the comment for LIMIT handling above. */
					break;
				}
				limit += offset_value;
			}

			if (limit > 0 && limit < 100)
			{
				path->subpath = (Path *) try_disable_bulk_decompression(root,
																		(Node *) path->subpath,
																		path->subpath->pathkeys);
			}

			break;
		}
		case T_MemoizePath:
			check_limit_bulk_decompression(root, (Node *) castNode(MemoizePath, node)->subpath);
			break;
		case T_ProjectionPath:
			check_limit_bulk_decompression(root, (Node *) castNode(ProjectionPath, node)->subpath);
			break;
		case T_SubqueryScanPath:
			check_limit_bulk_decompression(root,
										   (Node *) castNode(SubqueryScanPath, node)->subpath);
			break;
		case T_NestPath:
		case T_MergePath:
		case T_HashPath:
			check_limit_bulk_decompression(root, (Node *) ((JoinPath *) node)->outerjoinpath);
			check_limit_bulk_decompression(root, (Node *) ((JoinPath *) node)->innerjoinpath);
			break;
		default:
			break;
	}
}

void
tsl_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
							RelOptInfo *output_rel, TsRelType input_reltype, Hypertable *ht,
							void *extra)
{
	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			if (input_reltype != TS_REL_HYPERTABLE_CHILD)
			{
				plan_add_gapfill(root, output_rel);
			}

			if (ts_guc_enable_chunkwise_aggregation && input_rel != NULL &&
				!IS_DUMMY_REL(input_rel) && output_rel != NULL &&
				involves_hypertable(root, input_rel))
			{
				tsl_pushdown_partial_agg(root, ht, input_rel, output_rel, extra);
			}

			if (root->numOrderedAggs && !IS_DUMMY_REL(input_rel) && output_rel != NULL)
			{
				tsl_skip_scan_paths_add(root, input_rel, output_rel, stage);
			}
			break;
		case UPPERREL_WINDOW:
			if (IsA(linitial(input_rel->pathlist), CustomPath))
				gapfill_adjust_window_targetlist(root, input_rel, output_rel);
			break;
		case UPPERREL_DISTINCT:
			tsl_skip_scan_paths_add(root, input_rel, output_rel, stage);
			break;
		case UPPERREL_FINAL:
			check_limit_bulk_decompression(root, (Node *) output_rel->pathlist);
			break;
		default:
			break;
	}
}

/*
 * Check if a chunk should be decompressed via a ColumnarScan plan.
 *
 * Check first that it is a compressed chunk. Then, decompress unless it is
 * SELECT * FROM ONLY <chunk>. We check if it is the ONLY case by calling
 * ts_rte_is_marked_for_expansion. Respecting ONLY here is important to not
 * break postgres tools like pg_dump.
 */
static inline bool
use_columnar_scan(const RelOptInfo *rel, const RangeTblEntry *rte, const Chunk *chunk)
{
	if (!ts_guc_enable_columnarscan)
		return false;

	/* Check that the chunk is actually compressed */
	return chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID &&
		   /* Check that it is _not_ SELECT FROM ONLY <chunk> */
		   (rel->reloptkind != RELOPT_BASEREL || ts_rte_is_marked_for_expansion(rte));
}

void
tsl_set_rel_pathlist_query(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						   Hypertable *ht)
{
	/* Only interested in queries on relations that are part of hypertables
	 * with compression enabled, so quick exit if not this case. */
	if (ht == NULL || !TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
		return;

	/*
	 * For a chunk, we can get here via a query on the hypertable that expands
	 * to the chunk or by direct query on the chunk. In the former case,
	 * reloptkind will be RELOPT_OTHER_MEMBER_REL (nember of hypertable) or in
	 * the latter case reloptkind will be RELOPT_BASEREL (standalone rel).
	 *
	 * These two cases are checked in ts_planner_chunk_fetch().
	 */
	const Chunk *chunk = ts_planner_chunk_fetch(root, rel);

	if (chunk == NULL)
		return;

	if (use_columnar_scan(rel, rte, chunk))
	{
		ts_columnar_scan_generate_paths(root, rel, ht, chunk);
	}
}

void
tsl_set_rel_pathlist_dml(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						 Hypertable *ht)
{
	/*
	 * We do not support MERGE command with UPDATE/DELETE merge actions on
	 * compressed hypertables, because Custom Scan (ModifyHypertable) node is
	 * not generated in the plan for MERGE command on compressed hypertables
	 */
	if (ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		if (root->parse->commandType == CMD_MERGE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("The MERGE command with UPDATE/DELETE merge actions is not support on "
							"compressed hypertables")));

#if !PG17_GE
		/*
		 * PG16 and earlier: Remove BitmapHeapScan paths for DML on partial chunks.
		 *
		 * On PG16, BitmapHeapScan eagerly initializes its heap scan descriptor
		 * with the original snapshot during plan initialization. When we
		 * decompress rows and call CommandCounterIncrement(), the stale
		 * snapshot cannot see the newly decompressed rows.
		 *
		 * This bug only affects partial chunks because:
		 * - Fully compressed chunks have rel->indexlist = NIL (set in
		 *   timescaledb_get_relation_info_hook), so BitmapHeapScan is not
		 *   available anyway.
		 * - Partial chunks have indexes available, so BitmapHeapScan can be
		 *   chosen, and decompression will add rows that it cannot see.
		 *
		 * PG17+ fixed this via commit 1577081e961 which lazily initializes
		 * the scan descriptor in BitmapHeapNext(), using the current
		 * estate->es_snapshot after CommandCounterIncrement().
		 *
		 * IMPORTANT: PostgreSQL's add_path() prunes dominated paths. If
		 * BitmapHeapPath has lower cost than SeqScan (common with adjusted
		 * cost parameters), SeqScan may have been pruned from the pathlist.
		 * If removing BitmapHeapPath would leave no paths, we must add a
		 * another path as fallback to ensure a valid plan exists.
		 */
		const Chunk *chunk = ts_planner_chunk_fetch(root, rel);
		if (chunk && ts_chunk_is_partial(chunk))
		{
			ListCell *lc;
			List *filtered_paths = NIL;

			foreach (lc, rel->pathlist)
			{
				Path *path = lfirst(lc);
				if (!IsA(path, BitmapHeapPath))
					filtered_paths = lappend(filtered_paths, path);
			}

			/*
			 * If removing BitmapHeapPath left us with no paths, try to add
			 * alternative scan paths. This can happen when BitmapHeapPath
			 * dominated and pruned other paths due to cost calculations.
			 *
			 * Prefer IndexScan if available, fall back to SeqScan.
			 */
			if (filtered_paths == NIL && rel->pathlist != NIL)
			{
				/*
				 * Try to create index paths. create_index_paths() adds paths
				 * to rel->pathlist, but it also creates BitmapHeapPath entries
				 * which we must filter out again.
				 */
				rel->pathlist = NIL; /* Clear the BitmapHeapPath */
				create_index_paths(root, rel);

				/* Filter out any BitmapHeapPath that create_index_paths added */
				foreach (lc, rel->pathlist)
				{
					Path *path = lfirst(lc);
					if (!IsA(path, BitmapHeapPath))
						filtered_paths = lappend(filtered_paths, path);
				}

				/*
				 * If no non-bitmap index paths were created (e.g., enable_indexscan=off),
				 * add SeqScan as the final fallback.
				 */
				if (filtered_paths == NIL)
				{
					Relids required_outer = rel->lateral_relids;
					Path *seqpath = create_seqscan_path(root, rel, required_outer, 0);
					filtered_paths = lappend(filtered_paths, seqpath);
				}
			}

			rel->pathlist = filtered_paths;

			/* Also filter partial_pathlist for parallel plans */
			filtered_paths = NIL;
			foreach (lc, rel->partial_pathlist)
			{
				Path *path = lfirst(lc);
				if (!IsA(path, BitmapHeapPath))
					filtered_paths = lappend(filtered_paths, path);
			}
			rel->partial_pathlist = filtered_paths;
		}

#endif /* !PG17_GE */
	}
}

/*
 * Run preprocess query optimizations
 */
void
tsl_preprocess_query(Query *parse, int *cursor_opts)
{
	Assert(parse != NULL);

	/* Check if constification of watermark values is enabled */
	if (ts_guc_enable_cagg_watermark_constify)
	{
		constify_cagg_watermark(parse);
	}

#if PG16_GE
	/* Push down ORDER BY and LIMIT for realtime cagg (PG16+ only) */
	if (ts_guc_enable_cagg_sort_pushdown)
	{
		cagg_sort_pushdown(parse, cursor_opts);
	}
#endif
}

/*
 * Replaces pathkeys in tsl-specific custom path types during sort transformation.
 *
 * This hook is called from ts_sort_transform_replace_pathkeys() in sort_transform.c
 * after the basic pathkey replacement has been performed. It handles tsl-specific
 * path types (such as ColumnarScan) that contain additional pathkey fields beyond
 * the standard path.pathkeys field.
 */
void
tsl_sort_transform_replace_pathkeys(void *path, List *transformed_pathkeys, List *original_pathkeys)
{
	if (!path)
		return;
	if (ts_is_columnar_scan_path(path))
	{
		ColumnarScanPath *dcpath = (ColumnarScanPath *) path;
		if (compare_pathkeys(dcpath->required_compressed_pathkeys, transformed_pathkeys) ==
			PATHKEYS_EQUAL)
		{
			dcpath->required_compressed_pathkeys = original_pathkeys;
		}
	}
}

/*
 * Run plan postprocessing optimizations.
 */
void
tsl_postprocess_plan(PlannedStmt *stmt)
{
	if (ts_guc_enable_columnarindexscan)
	{
		ts_columnar_index_scan_fix_aggrefs(stmt->planTree);
	}

	if (ts_guc_enable_vectorized_aggregation)
	{
		stmt->planTree = try_insert_vector_agg_node(stmt->planTree, stmt->rtable);
	}

#ifdef TS_DEBUG
	if (ts_guc_debug_require_vector_agg != DRO_Allow)
	{
		bool has_some_agg = false;
		const bool has_vector_partial_agg = has_vector_agg_node(stmt->planTree, &has_some_agg);

		/*
		 * For convenience of using this in the tests, we don't complain about
		 * queries that don't have aggregation at all.
		 */
		if (has_some_agg)
		{
			if (!has_vector_partial_agg && ts_guc_debug_require_vector_agg == DRO_Require)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("vectorized aggregation node not found when required by the "
								"debug_require_vector_agg GUC")));
			}

			if (has_vector_partial_agg && ts_guc_debug_require_vector_agg == DRO_Forbid)
			{
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("vectorized aggregation node found when forbidden by the "
								"debug_require_vector_agg GUC")));
			}
		}
	}
#endif
}
