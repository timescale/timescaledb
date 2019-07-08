/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include "planner.h"
#include "nodes/gapfill/planner.h"

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
}
