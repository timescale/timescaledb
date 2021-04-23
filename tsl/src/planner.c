/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_trigger.h>
#include <optimizer/paths.h>
#include <parser/parsetree.h>
#include <foreign/fdwapi.h>

#include "async_append.h"
#include "nodes/skip_scan/skip_scan.h"
#include "chunk.h"
#include "compat.h"
#include "debug_guc.h"
#include "debug.h"
#include "fdw/data_node_scan_plan.h"
#include "fdw/fdw.h"
#include "fdw/relinfo.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "hypertable_compression.h"
#include "hypertable.h"
#include "nodes/compress_dml/compress_dml.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "nodes/data_node_dispatch.h"
#include "nodes/data_node_copy.h"
#include "nodes/gapfill/planner.h"
#include "planner.h"

#include <math.h>

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
	if (ts_guc_enable_transparent_decompression && ht != NULL &&
		rel->reloptkind == RELOPT_OTHER_MEMBER_REL && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht) &&
		rel->fdw_private != NULL && ((TimescaleDBPrivate *) rel->fdw_private)->compressed)
	{
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, true);

		if (chunk->fd.compressed_chunk_id > 0)
			ts_decompress_chunk_generate_paths(root, rel, ht, chunk);
	}
}
void
tsl_set_rel_pathlist_dml(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						 Hypertable *ht)
{
	if (ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		ListCell *lc;
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, true);
		if (chunk->fd.compressed_chunk_id > 0)
		{
			foreach (lc, rel->pathlist)
			{
				Path **pathptr = (Path **) &lfirst(lc);
				*pathptr = compress_chunk_dml_generate_paths(*pathptr, chunk);
			}
		}
	}
}

/* The fdw needs to expand a distributed hypertable inside the `GetForeignPath` callback. But, since
 * the hypertable base table is not a foreign table, that callback would not normally be called.
 * Thus, we call it manually in this hook.
 */
void
tsl_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
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

/*
 * Check session variable to disable distributed insert with COPY.
 *
 * Allows testing and comparing different insert plans. Not a full GUC, since
 * in most cases this is not something that users are expected to disable.
 */
static bool
copy_mode_enabled(void)
{
	const char *enable_copy =
		GetConfigOption("timescaledb.enable_distributed_insert_with_copy", true, false);

	/* Default to enabled */
	if (NULL == enable_copy)
		return true;

	return strcmp(enable_copy, "true") == 0;
}

/*
 * Decide on a plan to use for distributed inserts.
 */
Path *
tsl_create_distributed_insert_path(PlannerInfo *root, ModifyTablePath *mtpath, Index hypertable_rti,
								   int subplan_index)
{
	bool copy_possible = copy_mode_enabled();

	/* Check if it is possible to use COPY in the backend.
	 *
	 * There are two cases where we cannot use COPY:
	 *
	 * 1. ON CONFLICT clause exists
	 *
	 * 2. RETURNING clause exists and tuples are expected to be modified on
	 *    INSERT by a trigger.
	 *
	 * For case (2), we assume that we can return the original tuples if there
	 * are no triggers on the root hypertable (we also assume that chunks on
	 * data nodes aren't modified with their own triggers). Conversely, if
	 * there are any non-known triggers, we cannot use COPY in the backend
	 * when there is a RETURNING clause.
	 */
	if (copy_possible)
	{
		if (NULL != mtpath->onconflict)
			copy_possible = false;
		else if (NIL != mtpath->returningLists)
		{
			const RangeTblEntry *rte = planner_rt_fetch(hypertable_rti, root);
			const Relation rel = table_open(rte->relid, AccessShareLock);
			int i;

			for (i = 0; i < rel->trigdesc->numtriggers; i++)
			{
				const Trigger *trig = &rel->trigdesc->triggers[i];

				/*
				 * Check for BEFORE INSERT triggers as those are the ones that can
				 * modify a tuple.
				 */
				if (strcmp(trig->tgname, INSERT_BLOCKER_NAME) != 0 &&
					TRIGGER_FOR_INSERT(trig->tgtype) && TRIGGER_FOR_BEFORE(trig->tgtype))
				{
					copy_possible = false;
					break;
				}
			}

			table_close(rel, AccessShareLock);
		}
	}

	if (copy_possible)
		return data_node_copy_path_create(root, mtpath, hypertable_rti, subplan_index);

	return data_node_dispatch_path_create(root, mtpath, hypertable_rti, subplan_index);
}
