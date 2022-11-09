/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_NODES_GAPFILL_H
#define TIMESCALEDB_TSL_NODES_GAPFILL_H

#include <postgres.h>
#include <nodes/pathnodes.h>
#include <nodes/primnodes.h>

#define GAPFILL_FUNCTION "time_bucket_gapfill"
#define GAPFILL_LOCF_FUNCTION "locf"
#define GAPFILL_INTERPOLATE_FUNCTION "interpolate"

bool gapfill_in_expression(Expr *node);
void plan_add_gapfill(PlannerInfo *root, RelOptInfo *group_rel);
void gapfill_adjust_window_targetlist(PlannerInfo *root, RelOptInfo *input_rel,
									  RelOptInfo *output_rel);

typedef struct GapFillPath
{
	CustomPath cpath;
	FuncExpr *func; /* time_bucket_gapfill function call */
} GapFillPath;

#endif /* TIMESCALEDB_TSL_NODES_GAPFILL_H */
