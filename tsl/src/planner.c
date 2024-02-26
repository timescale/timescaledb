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
#include <optimizer/paths.h>
#include <parser/parsetree.h>

#include "compat/compat.h"
#include "chunk.h"
#include "chunkwise_agg.h"
#include "continuous_aggs/planner.h"
#include "guc.h"
#include "hyperstore/hyperstore_handler.h"
#include "hypertable.h"
#include "nodes/columnar_scan/columnar_scan.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/frozen_chunk_dml/frozen_chunk_dml.h"
#include "nodes/gapfill/gapfill.h"
#include "nodes/skip_scan/skip_scan.h"
#include "nodes/vector_agg/plan.h"
#include "planner.h"
#include "planner/partialize.h"

#include <math.h>

#define OSM_EXTENSION_NAME "timescaledb_osm"

static int osm_present = -1;

static bool
is_osm_present()
{
	if (osm_present == -1)
	{
		Oid osm_oid = get_extension_oid(OSM_EXTENSION_NAME, true);
		osm_present = OidIsValid(osm_oid);
	}
	return osm_present;
}

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
			break;
		case UPPERREL_WINDOW:
			if (IsA(linitial(input_rel->pathlist), CustomPath))
				gapfill_adjust_window_targetlist(root, input_rel, output_rel);
			break;
		case UPPERREL_DISTINCT:
			tsl_skip_scan_paths_add(root, input_rel, output_rel);
			break;
		default:
			break;
	}
}

/*
 * Check if a chunk should be decompressed via a DecompressChunk plan.
 *
 * Check first that it is a compressed chunk. Then, decompress unless it is
 * SELECT * FROM ONLY <chunk>. We check if it is the ONLY case by calling
 * ts_rte_is_marked_for_expansion. Respecting ONLY here is important to not
 * break postgres tools like pg_dump.
 */
static inline bool
use_decompress_chunk_node(const RelOptInfo *rel, const RangeTblEntry *rte, const Chunk *chunk)
{
	/*
	 * The transparent_decompression GUC settings:
	 *
	 * 0 = Disabled.
	 * 1 = Use only with "regular" compressed chunks.
	 * 2 = Use with both "regular" compressed chunks and hypercore chunks.
	 */
	if (ts_guc_enable_transparent_decompression == 0)
		return false;

	if (ts_is_hypercore_am(chunk->amoid) && ts_guc_enable_transparent_decompression != 2)
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

	if (use_decompress_chunk_node(rel, rte, chunk))
	{
		ts_decompress_chunk_generate_paths(root, rel, ht, chunk);
	}
	/*
	 * If using our own access method on the chunk, we might want to add
	 * alternative paths. This should not be compatible with transparent
	 * decompression, so only add if we didn't add decompression paths above.
	 */
	else if (ts_is_hypercore_am(chunk->amoid))
	{
		if (ts_guc_enable_columnarscan)
			columnar_scan_set_rel_pathlist(root, rel, ht);

		hyperstore_set_rel_pathlist(root, rel, ht);
	}
}

void
tsl_set_rel_pathlist_dml(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						 Hypertable *ht)
{
	if (is_osm_present())
	{
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, false);
		if (chunk && ts_chunk_is_frozen(chunk))
		{
			ListCell *lc;
			foreach (lc, rel->pathlist)
			{
				Path **pathptr = (Path **) &lfirst(lc);
				*pathptr = frozen_chunk_dml_generate_path(*pathptr, chunk);
			}
			return;
		}
	}
#if PG15_GE
	/*
	 * We do not support MERGE command with UPDATE/DELETE merge actions on
	 * compressed hypertables, because Custom Scan (HypertableModify) node is
	 * not generated in the plan for MERGE command on compressed hypertables
	 */
	if (ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		if (root->parse->commandType == CMD_MERGE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("The MERGE command with UPDATE/DELETE merge actions is not support on "
							"compressed hypertables")));
	}
#endif
}

/*
 * The fdw needs to expand a distributed hypertable inside the `GetForeignPath`
 * callback. But, since the hypertable base table is not a foreign table, that
 * callback would not normally be called. Thus, we call it manually in this hook.
 */
void
tsl_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	if (is_dummy_rel(rel))
	{
		/*
		 * Don't have to create any other path if the relation is already proven
		 * to be empty.
		 */
		return;
	}
}

/*
 * Run preprocess query optimizations
 */
void
tsl_preprocess_query(Query *parse)
{
	Assert(parse != NULL);

	/* Check if constification of watermark values is enabled */
	if (ts_guc_enable_cagg_watermark_constify)
	{
		constify_cagg_watermark(parse);
	}
}

/*
 * Run plan postprocessing optimizations.
 */
void
tsl_postprocess_plan(PlannedStmt *stmt)
{
	if (ts_guc_enable_vectorized_aggregation)
	{
		stmt->planTree = try_insert_vector_agg_node(stmt->planTree);
	}

#ifdef TS_DEBUG
	if (ts_guc_debug_require_vector_agg != DRO_Allow)
	{
		bool has_normal_agg = false;
		const bool has_vector_agg = has_vector_agg_node(stmt->planTree, &has_normal_agg);
		const bool should_have_vector_agg = (ts_guc_debug_require_vector_agg == DRO_Require);

		/*
		 * For convenience, we don't complain about queries that don't have
		 * aggregation at all.
		 */
		if ((has_normal_agg || has_vector_agg) && (has_vector_agg != should_have_vector_agg))
		{
			elog(ERROR, "vector aggregation inconsistent with debug_require_vector_agg GUC");
		}
	}
#endif
}
