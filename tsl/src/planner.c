/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>

#include "planner.h"
#include "nodes/gapfill/planner.h"
#include "nodes/compress_dml/compress_dml.h"
#include "chunk.h"
#include "decompress_chunk/decompress_chunk.h"
#include "hypertable.h"
#include "hypertable_compression.h"
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
tsl_set_rel_pathlist_query(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte,
						   Hypertable *ht)
{
	if (ts_guc_enable_transparent_decompression && ht != NULL &&
		rel->reloptkind == RELOPT_OTHER_MEMBER_REL && ht->fd.compressed_hypertable_id > 0)
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
	if (ht != NULL && TS_HYPERTABLE_HAS_COMPRESSION_ON(ht))
	{
		ListCell *lc;
		/* is this a chunk under compressed hypertable ? */
		AppendRelInfo *appinfo = ts_get_appendrelinfo(root, rti, false);
		Oid PG_USED_FOR_ASSERTS_ONLY parent_oid = appinfo->parent_reloid;
		Chunk *chunk = ts_chunk_get_by_relid(rte->relid, 0, true);
		Assert(parent_oid == ht->main_table_relid && (parent_oid == chunk->hypertable_relid));
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
