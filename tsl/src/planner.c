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

#include "nodes/async_append.h"
#include "nodes/skip_scan/skip_scan.h"
#include "chunk.h"
#include "compat/compat.h"
#include "debug_guc.h"
#include "debug.h"
#include "fdw/data_node_scan_plan.h"
#include "fdw/fdw.h"
#include "fdw/relinfo.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "hypertable.h"
#include "nodes/compress_dml/compress_dml.h"
#include "nodes/frozen_chunk_dml/frozen_chunk_dml.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/gapfill/gapfill.h"
#include "planner.h"

#include <math.h>

#define OSM_EXTENSION_NAME "timescaledb_osm"

static bool
is_dist_hypertable_involved(PlannerInfo *root)
{
	int rti;

	for (rti = 1; rti < root->simple_rel_array_size; rti++)
	{
		RangeTblEntry *rte = root->simple_rte_array[rti];
		bool distributed = false;

		if (ts_rte_is_hypertable(rte, &distributed) && distributed)
			return true;
	}

	return false;
}

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

void
tsl_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
							RelOptInfo *output_rel, TsRelType input_reltype, Hypertable *ht,
							void *extra)
{
	bool dist_ht = false;
	switch (input_reltype)
	{
		case TS_REL_HYPERTABLE:
		case TS_REL_HYPERTABLE_CHILD:
			dist_ht = hypertable_is_distributed(ht);
			if (dist_ht)
				data_node_scan_create_upper_paths(root, stage, input_rel, output_rel, extra);
			break;
		default:
			break;
	}

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
			if (ts_guc_enable_async_append && root->parse->resultRelation == 0 &&
				is_dist_hypertable_involved(root))
				async_append_add_paths(root, output_rel);
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

	Cache *hcache;
	Hypertable *ht =
		ts_hypertable_cache_get_cache_and_entry(rte->relid, CACHE_FLAG_MISSING_OK, &hcache);

	if (rel->fdw_private != NULL && ht != NULL && hypertable_is_distributed(ht))
	{
		FdwRoutine *fdw = (FdwRoutine *) DatumGetPointer(
			DirectFunctionCall1(timescaledb_fdw_handler, PointerGetDatum(NULL)));

		fdw->GetForeignRelSize(root, rel, rte->relid);
		fdw->GetForeignPaths(root, rel, rte->relid);

#ifdef TS_DEBUG
		if (ts_debug_optimizer_flags.show_rel)
			tsl_debug_log_rel_with_paths(root, rel, (UpperRelationKind *) NULL);
#endif
	}

	ts_cache_release(hcache);
}
