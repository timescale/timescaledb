/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_PLANNER_H
#define TIMESCALEDB_TSL_PLANNER_H

#include <postgres.h>
#include <optimizer/planner.h>
#include "hypertable.h"
#include "plan_expand_hypertable.h"

void tsl_create_upper_paths_hook(PlannerInfo *, UpperRelationKind, RelOptInfo *, RelOptInfo *);
void tsl_set_rel_pathlist_query(PlannerInfo *, RelOptInfo *, Index, RangeTblEntry *, Hypertable *);
void tsl_set_rel_pathlist_dml(PlannerInfo *, RelOptInfo *, Index, RangeTblEntry *, Hypertable *);

#if !PG96
void tsl_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
bool tsl_hypertable_should_be_expanded(RelOptInfo *rel, RangeTblEntry *rte, Hypertable *ht,
									   List *chunk_oids);
#endif

#endif /* TIMESCALEDB_TSL_PLANNER_H */
