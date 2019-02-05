/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PLAN_ORDERED_APPEND_H
#define TIMESCALEDB_PLAN_ORDERED_APPEND_H

#include <postgres.h>
#include <nodes/relation.h>

#include "hypertable.h"
#include "hypertable_restrict_info.h"

bool ts_ordered_append_should_optimize(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
									   bool *reverse);
Path *ts_ordered_append_path_create(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
									MergeAppendPath *merge);

#endif /* TIMESCALEDB_PLAN_ORDERED_APPEND_H */
