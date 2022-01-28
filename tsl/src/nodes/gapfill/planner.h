/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_NODES_GAPFILL_PLANNER_H
#define TIMESCALEDB_TSL_NODES_GAPFILL_PLANNER_H

#include <postgres.h>

#include "fdw/data_node_scan_plan.h"
#include <hypertable_cache.h>
#include <planner.h>
#include <import/allpaths.h>
#include <import/planner.h>
#include <func_cache.h>
#include <dimension.h>
#include <compat/compat.h>
#include <debug_guc.h>
#include <debug.h>

#include "fdw/data_node_chunk_assignment.h"
#include "fdw/scan_plan.h"
#include "fdw/data_node_scan_plan.h"
#include "fdw/data_node_scan_exec.h"

bool gapfill_in_expression(Expr *node);
void plan_add_gapfill(PlannerInfo *root, RelOptInfo *group_rel);
void gapfill_adjust_window_targetlist(PlannerInfo *root, RelOptInfo *input_rel,
									  RelOptInfo *output_rel);
bool pushdown_gapfill(PlannerInfo *root, RelOptInfo *hyper_rel, Hyperspace *hs,
					  DataNodeChunkAssignments *scas);

typedef struct GapFillPath
{
	CustomPath cpath;
	FuncExpr *func; /* time_bucket_gapfill function call */
} GapFillPath;

#endif /* TIMESCALEDB_TSL_NODES_GAPFILL_PLANNER_H */
