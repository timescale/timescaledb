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
#include <parser/parsetree.h>

#include "compat/compat.h"
#include "chunk.h"
#include "chunkwise_agg.h"
#include "continuous_aggs/planner.h"
#include "guc.h"
#include "hypertable.h"
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

void
tsl_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
							RelOptInfo *output_rel, TsRelType input_reltype, Hypertable *ht,
							void *extra)
{
	/*
	 * Gapfill node cannot be disabled if the query specifies it, so it runs
	 * regardless of the timescaledb.enable_optimizations GUC.
	 */
	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
			if (input_reltype != TS_REL_HYPERTABLE_CHILD)
			{
				plan_add_gapfill(root, output_rel);
			}
			break;
		case UPPERREL_WINDOW:
			if (IsA(linitial(input_rel->pathlist), CustomPath))
			{
				gapfill_adjust_window_targetlist(root, input_rel, output_rel);
			}
			break;
		default:
			break;
	}

	if (!ts_guc_enable_optimizations)
	{
		return;
	}

	switch (stage)
	{
		case UPPERREL_GROUP_AGG:
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
		case UPPERREL_DISTINCT:
			tsl_skip_scan_paths_add(root, input_rel, output_rel, stage);
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
	{
		return false;
	}

	/* Check that the chunk is actually compressed */
	return ts_chunk_is_compressed(chunk) &&
		   /* Check that it is _not_ SELECT FROM ONLY <chunk> */
		   (rel->reloptkind != RELOPT_BASEREL || ts_rte_is_marked_for_expansion(rte));
}

void
tsl_set_rel_pathlist_query(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						   Hypertable *ht)
{
	/* Only interested in queries on relations that are part of hypertables
	 * with compression enabled, so quick exit if not this case. */
	if (ht == NULL || !TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
	{
		return;
	}

	/*
	 * For a chunk, we can get here via a query on the hypertable that expands
	 * to the chunk or by direct query on the chunk. In the former case,
	 * reloptkind will be RELOPT_OTHER_MEMBER_REL (member of hypertable) or in
	 * the latter case reloptkind will be RELOPT_BASEREL (standalone rel).
	 *
	 * These two cases are checked in ts_planner_chunk_fetch().
	 */
	const Chunk *chunk = ts_planner_chunk_fetch(root, rel);

	if (chunk == NULL)
	{
		return;
	}

	if (use_columnar_scan(rel, rte, chunk))
	{
		ts_columnar_scan_generate_paths(root, rel, ht, chunk);
	}
}

void
tsl_set_rel_pathlist_dml(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						 Hypertable *ht)
{
	if (ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
	{
		if (!ts_guc_enable_compressed_merge && root->parse->commandType == CMD_MERGE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("The MERGE command with UPDATE/DELETE merge actions is not support on "
							"compressed hypertables")));
		}
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

	/* Push down ORDER BY and LIMIT for realtime cagg (PG16+ only) */
	if (ts_guc_enable_cagg_sort_pushdown)
	{
		cagg_sort_pushdown(parse, cursor_opts);
	}
}

/*
 * Run plan postprocessing optimizations.
 */
void
tsl_postprocess_plan(PlannedStmt *stmt)
{
	if (!ts_guc_enable_optimizations)
	{
		return;
	}

	if (ts_guc_enable_columnarindexscan)
	{
		stmt->planTree = try_insert_columnar_index_scan_node(stmt->planTree, stmt->rtable);
		stmt->subplans =
			(List *) try_insert_columnar_index_scan_node((Plan *) stmt->subplans, stmt->rtable);
	}

	if (ts_guc_enable_vectorized_aggregation)
	{
		stmt->planTree = try_insert_vector_agg_node(stmt->planTree);
		stmt->subplans = (List *) try_insert_vector_agg_node((Plan *) stmt->subplans);
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
