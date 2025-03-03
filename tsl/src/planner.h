/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <optimizer/planner.h>

#include "hypertable.h"
#include <planner/planner.h>

void tsl_create_upper_paths_hook(PlannerInfo *, UpperRelationKind, RelOptInfo *, RelOptInfo *,
								 TsRelType, Hypertable *, void *);
void tsl_set_rel_pathlist_query(PlannerInfo *, RelOptInfo *, Index, RangeTblEntry *, Hypertable *);
void tsl_set_rel_pathlist_dml(PlannerInfo *, RelOptInfo *, Index, RangeTblEntry *, Hypertable *);
void tsl_preprocess_query(Query *parse, int *cursor_opts);
void tsl_postprocess_plan(PlannedStmt *stmt);
