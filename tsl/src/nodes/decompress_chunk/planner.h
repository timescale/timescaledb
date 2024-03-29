/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

extern Plan *decompress_chunk_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
										  List *tlist, List *clauses, List *custom_plans);

extern void _decompress_chunk_init(void);
