/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

typedef struct PlannerInfo PlannerInfo;

typedef struct Sort Sort;

void
ts_label_sort_with_costsize(PlannerInfo *root, Sort *plan, double limit_tuples);


