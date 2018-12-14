/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
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
