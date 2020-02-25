/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PLAN_EXPAND_HYPERTABLE_H
#define TIMESCALEDB_PLAN_EXPAND_HYPERTABLE_H

#include "hypertable.h"

/* This planner optimization reduces planning times when a hypertable has many chunks.
 * It does this by expanding hypertable chunks manually, eliding the `expand_inherited_tables`
 * logic used by PG.
 *
 * Slow planning time were previously seen because `expand_inherited_tables` expands all chunks of
 * a hypertable, without regard to constraints present in the query. Then, `get_relation_info` is
 * called on all chunks before constraint exclusion. Getting the statistics on many chunks ends
 * up being expensive because RelationGetNumberOfBlocks has to open the file for each relation.
 * This gets even worse under high concurrency.
 *
 * This logic solves this by expanding only the chunks needed to fulfil the query instead of all
 * chunks. In effect, it moves chunk exclusion up in the planning process. But, we actually don't
 * use constraint exclusion here, but rather a variant of range exclusion implemented by
 * HypertableRestrictInfo.
 * */

void ts_plan_expand_timebucket_annotate(PlannerInfo *root, RelOptInfo *rel);

/* Do the expansion */
extern void ts_plan_expand_hypertable_chunks(Hypertable *ht, PlannerInfo *root, RelOptInfo *rel);

#endif /* TIMESCALEDB_PLAN_EXPAND_HYPERTABLE_H */
