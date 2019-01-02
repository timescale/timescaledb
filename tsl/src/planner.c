/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include "planner.h"
#include "gapfill/planner.h"

void
tsl_create_upper_paths_hook(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	if (UPPERREL_GROUP_AGG == stage)
		plan_add_gapfill(root, output_rel);
}
