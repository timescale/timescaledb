/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_trigger.h>
#include <commands/extension.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>
#include <foreign/fdwapi.h>
#include <nodes/nodeFuncs.h>

#include "nodes/skip_scan/skip_scan.h"
#include "chunk.h"
#include "compat/compat.h"
#include "continuous_aggs/planner.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "hypertable.h"
#include "nodes/compress_dml/compress_dml.h"
#include "nodes/frozen_chunk_dml/frozen_chunk_dml.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/gapfill/gapfill.h"
#include "nodes/chunk_append/chunk_append.h"
#include "planner.h"

#include <math.h>

#define OSM_EXTENSION_NAME "timescaledb_osm"

#if PG14_GE
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
#endif

/*
 * Try to disable bulk decompression on DecompressChunkPath, skipping the above
 * Projection path and also handling Lists.
 */
static void
try_disable_bulk_decompression(PlannerInfo *root, Node *node)
{
	if (IsA(node, List))
	{
		ListCell *lc;
		foreach (lc, (List *) node)
		{
			try_disable_bulk_decompression(root, (Node *) lfirst(lc));
		}
		return;
	}

	if (IsA(node, ProjectionPath))
	{
		try_disable_bulk_decompression(root, (Node *) castNode(ProjectionPath, node)->subpath);
		return;
	}

	if (IsA(node, AppendPath))
	{
		try_disable_bulk_decompression(root, (Node *) castNode(AppendPath, node)->subpaths);
		return;
	}

	if (IsA(node, MergeAppendPath))
	{
		MergeAppendPath *mergeappend = castNode(MergeAppendPath, node);
		ListCell *lc;
		foreach (lc, mergeappend->subpaths)
		{
			Path *child = (Path *) lfirst(lc);
			if (pathkeys_contained_in(mergeappend->path.pathkeys, child->pathkeys))
			{
				try_disable_bulk_decompression(root, (Node *) child);
			}
		}
		return;
	}

	if (!IsA(node, CustomPath))
	{
		return;
	}

	CustomPath *custom_child = castNode(CustomPath, node);
	if (strcmp(custom_child->methods->CustomName, "ChunkAppend") == 0)
	{
		try_disable_bulk_decompression(root, (Node *) custom_child->custom_paths);
		return;
	}

	if (strcmp(custom_child->methods->CustomName, "DecompressChunk") != 0)
	{
		return;
	}

	DecompressChunkPath *dcpath = (DecompressChunkPath *) custom_child;
	dcpath->enable_bulk_decompression = false;
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

			if (path->limitCount != NULL)
			{
				Const *count = castNode(Const, path->limitCount);
				Assert(count->consttype == INT8OID);
				Assert(DatumGetInt64(count->constvalue) >= 0);
				limit = DatumGetInt64(count->constvalue);
			}

			if (path->limitOffset != NULL)
			{
				Const *offset = castNode(Const, path->limitOffset);
				Assert(offset->consttype == INT8OID);
				Assert(DatumGetInt64(offset->constvalue) >= 0);
				limit += DatumGetInt64(offset->constvalue);
			}

			if (limit > 0 && limit < 100)
			{
				try_disable_bulk_decompression(root, (Node *) path->subpath);
			}

			break;
		}
#if PG14_GE
		case T_MemoizePath:
			check_limit_bulk_decompression(root, (Node *) castNode(MemoizePath, node)->subpath);
			break;
#endif
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
				plan_add_gapfill(root, output_rel);
			break;
		case UPPERREL_WINDOW:
			if (IsA(linitial(input_rel->pathlist), CustomPath))
				gapfill_adjust_window_targetlist(root, input_rel, output_rel);
			break;
		case UPPERREL_DISTINCT:
			tsl_skip_scan_paths_add(root, input_rel, output_rel);
			break;
		case UPPERREL_FINAL:
			check_limit_bulk_decompression(root, (Node *) output_rel->pathlist);
			break;
		default:
			break;
	}
}

void
tsl_set_rel_pathlist_query(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						   Hypertable *ht)
{
	/* We can get here via query on hypertable in that case reloptkind
	 * will be RELOPT_OTHER_MEMBER_REL or via direct query on chunk
	 * in that case reloptkind will be RELOPT_BASEREL.
	 * If we get here via SELECT * FROM <chunk>, we decompress the chunk,
	 * unless the query was SELECT * FROM ONLY <chunk>.
	 * We check if it is the ONLY case by calling ts_rte_is_marked_for_expansion.
	 * Respecting ONLY here is important to not break postgres tools like pg_dump.
	 */
	TimescaleDBPrivate *fdw_private = (TimescaleDBPrivate *) rel->fdw_private;
	if (ts_guc_enable_transparent_decompression && ht &&
		(rel->reloptkind == RELOPT_OTHER_MEMBER_REL ||
		 (rel->reloptkind == RELOPT_BASEREL && ts_rte_is_marked_for_expansion(rte))) &&
		TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		if (fdw_private->cached_chunk_struct == NULL)
		{
			/*
			 * We can not have the cached Chunk struct,
			 * 1) if it was a direct query on the chunk;
			 * 2) if it is not a SELECT QUERY.
			 * Caching is done by our hypertable expansion, which doesn't run in
			 * these cases.
			 *
			 * Also on PG13 when a DELETE query runs through SPI, its command
			 * type is CMD_SELECT. Apparently it goes into inheritance_planner,
			 * which uses a hack to pretend it's actually a SELECT query, but
			 * for some reason for non-SPI queries the query type is still
			 * correct. You can observe it in the continuous_aggs-13 test.
			 * Just ignore this assertion on 13 and look up the chunk.
			 */
#if PG14_GE
			Assert(rel->reloptkind == RELOPT_BASEREL || root->parse->commandType != CMD_SELECT);
#endif
			fdw_private->cached_chunk_struct =
				ts_chunk_get_by_relid(rte->relid, /* fail_if_not_found = */ true);
		}

		if (fdw_private->cached_chunk_struct->fd.compressed_chunk_id != INVALID_CHUNK_ID)
		{
			ts_decompress_chunk_generate_paths(root, rel, ht, fdw_private->cached_chunk_struct);
		}
	}
}

void
tsl_set_rel_pathlist_dml(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						 Hypertable *ht)
{
#if PG14_GE
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
#else
	/*
	 * We do not support UPDATE/DELETE operations on compressed hypertables
	 * on PG versions < 14, because Custom Scan (HypertableModify) node is
	 * not generated in the plan for UPDATE/DELETE operations on hypertables
	 */
	if (ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		ListCell *lc;
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, true);
		if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
		{
			foreach (lc, rel->pathlist)
			{
				Path **pathptr = (Path **) &lfirst(lc);
				*pathptr = compress_chunk_dml_generate_paths(*pathptr, chunk);
			}
		}
	}
#endif
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
