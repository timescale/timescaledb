/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PLAN_PARTIALIZE_H
#define TIMESCALEDB_PLAN_PARTIALIZE_H
#include <postgres.h>
#include <optimizer/planner.h>

typedef enum PartializeAggFixAggref
{
	TS_DO_NOT_FIX_AGGREF = 0,
	TS_FIX_AGGREF = 1
} PartializeAggFixAggref;

bool has_partialize_function(Query *parse, PartializeAggFixAggref fix_aggref);
bool ts_plan_process_partialize_agg(PlannerInfo *root, RelOptInfo *output_rel);

#endif /* TIMESCALEDB_PLAN_PARTIALIZE_H */
