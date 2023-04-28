/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_COMPRESSED_SKIP_SCAN_PLANNER_H
#define TIMESCALEDB_COMPRESSED_SKIP_SCAN_PLANNER_H

#include <postgres.h>
#include "nodes/nodes_common.h"

extern Plan *compressed_skip_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
											  List *tlist, List *clauses, List *custom_plans);

extern void _compressed_skip_scan_init(void);

extern Node *compressed_skip_scan_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_COMPRESSED_SKIP_SCAN_PLANNER_H */
