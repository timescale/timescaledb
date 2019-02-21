/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <optimizer/planner.h>

void plan_process_partialize_agg(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel);
