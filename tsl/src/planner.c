/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>

#include <planner.h>
#include "planner.h"
#include "nodes/gapfill/planner.h"
#include "nodes/compress_dml/compress_dml.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "chunk.h"
#include "hypertable.h"
#include "hypertable_compression.h"
#include "guc.h"

#include <math.h>

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
tsl_set_rel_pathlist_query(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						   Hypertable *ht)
{
	if (ts_guc_enable_transparent_decompression && ht != NULL &&
		rel->reloptkind == RELOPT_OTHER_MEMBER_REL && TS_HYPERTABLE_HAS_COMPRESSION(ht) &&
		rel->fdw_private != NULL && ((TimescaleDBPrivate *) rel->fdw_private)->compressed)
	{
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, 0, true);

		if (chunk->fd.compressed_chunk_id > 0)
			ts_decompress_chunk_generate_paths(root, rel, ht, chunk);
	}
}
void
tsl_set_rel_pathlist_dml(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						 Hypertable *ht)
{
	if (ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION(ht))
	{
		ListCell *lc;
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, 0, true);
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
