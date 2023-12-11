/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/pathnodes.h>
#include <optimizer/cost.h>

extern void fdw_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *rel, List *pathkeys,
										double *p_rows, int *p_width, Cost *p_startup_cost,
										Cost *p_total_cost);
