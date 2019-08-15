/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>

#include "planner.h"
#include "nodes/gapfill/planner.h"
#include "decompress_chunk/decompress_chunk.h"
#include "chunk.h"
#include "hypertable.h"
#include "guc.h"

void
tsl_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
							RelOptInfo *output_rel)
{
	if (UPPERREL_GROUP_AGG == stage)
		plan_add_gapfill(root, output_rel);
	if (UPPERREL_WINDOW == stage)
	{
		if (IsA(linitial(input_rel->pathlist), CustomPath))
			gapfill_adjust_window_targetlist(root, input_rel, output_rel);
	}
}

void
tsl_set_rel_pathlist_hook(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						  Hypertable *ht)
{
	if (ts_guc_enable_transparent_decompression && ht != NULL &&
		rel->reloptkind == RELOPT_OTHER_MEMBER_REL && ht->fd.compressed_hypertable_id > 0)
	{
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, 0, true);

		if (chunk->fd.compressed_chunk_id > 0)
		{
			rel->pathlist = list_make1(
				ts_decompress_chunk_path_create(root, rel, ht, chunk, linitial(rel->pathlist)));
			rel->partial_pathlist = NIL;
		}
	}
}
