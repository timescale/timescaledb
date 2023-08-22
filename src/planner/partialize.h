/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PLAN_PARTIALIZE_H
#define TIMESCALEDB_PLAN_PARTIALIZE_H
#include <postgres.h>
#include <optimizer/planner.h>

#include "chunk.h"

void ts_pushdown_partial_agg(PlannerInfo *root, Hypertable *ht, RelOptInfo *input_rel,
							 RelOptInfo *output_rel, void *extra);

#endif /* TIMESCALEDB_PLAN_PARTIALIZE_H */
