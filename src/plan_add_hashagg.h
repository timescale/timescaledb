#ifndef TIMESCALEDB_PLAN_ADD_HASHAGG_H
#define TIMESCALEDB_PLAN_ADD_HASHAGG_H

#include <nodes/relation.h>

/* This optimization adds a HashAggregate plan to many group by queries.
 * In plain postgres, many time-series queries will not use a the hash aggregate
 * because the planner will incorrectly assume that the number of rows is much larger than
 * it actually is and will use the less efficient GroupAggregate instead of a HashAggregate
 * to prevent running out of memory.
 *
 * The planner will assume a large number of rows because the statistics planner for grouping assumes that the number of
 * distinct items produced by a function is the same as the number of distinct items going in. This is not true for functions
 * like time_bucket and date_trunc. This optimization fixes the statistics and adds the HashAggregate plan if appropriate.
 * */

extern void plan_add_hashagg(PlannerInfo *root,
				 RelOptInfo *input_rel,
				 RelOptInfo *output_rel);
#endif							/* TIMESCALEDB_PLAN_ADD_HASHAGG_H */
