/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

typedef struct DecompressBatchState DecompressBatchState;

typedef struct GroupingPolicy GroupingPolicy;

/*
 * This is a common interface for grouping policies which define how the rows
 * are grouped for aggregation -- e.g. there can be an implementation for no
 * grouping, grouping by compression segmentby columns, grouping over sorted
 * input (GroupAggregate), grouping using a hash table, and so on.
 */
typedef struct GroupingPolicy
{
	/*
	 * Used for rescans in the Postgres sense.
	 */
	void (*gp_reset)(GroupingPolicy *gp);

	void (*gp_add_batch)(GroupingPolicy *gp, DecompressBatchState *batch_state);

	/*
	 * Is a partial aggregation result ready?
	 */
	bool (*gp_should_emit)(GroupingPolicy *gp);

	/*
	 * Emit a partial aggregation result into the result slot.
	 */
	bool (*gp_do_emit)(GroupingPolicy *gp, TupleTableSlot *aggregated_slot);

	void (*gp_destroy)(GroupingPolicy *gp);
} GroupingPolicy;

extern GroupingPolicy *create_grouping_policy_batch(List *agg_defs, List *grouping_columns,
													bool partial_per_batch);
