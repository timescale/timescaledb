/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "hypertable.h"

extern Expr *ts_sort_transform_expr(Expr *expr);

extern List *ts_sort_transform_get_pathkeys(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte,
											Hypertable *ht);

extern void ts_sort_transform_replace_pathkeys(void *node, List *transformed_pathkeys,
											   List *original_pathkeys);
