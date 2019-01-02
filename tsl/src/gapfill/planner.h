/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_GAPFILL_PLANNER_H
#define TIMESCALEDB_GAPFILL_PLANNER_H

#include <postgres.h>
#include <optimizer/planner.h>

void		plan_add_gapfill(PlannerInfo *, RelOptInfo *);

typedef struct GapFillPath
{
	CustomPath	cpath;
	FuncExpr   *func;			/* time_bucket_gapfill function call */
} GapFillPath;

#endif							/* TIMESCALEDB_GAPFILL_PLANNER_H */
