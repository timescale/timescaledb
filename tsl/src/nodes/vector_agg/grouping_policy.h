/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

typedef struct DecompressBatchState DecompressBatchState;

typedef struct GroupingPolicy GroupingPolicy;

typedef struct GroupingPolicy
{
	void (*gp_reset)(GroupingPolicy *gp);
	void (*gp_add_batch)(GroupingPolicy *gp, DecompressBatchState *batch_state);
	bool (*gp_should_emit)(GroupingPolicy *gp);
	void (*gp_do_emit)(GroupingPolicy *gp, TupleTableSlot *aggregated_slot);
	void (*gp_destroy)(GroupingPolicy *gp);
} GroupingPolicy;

extern GroupingPolicy *create_grouping_policy_all(List *agg_defs);

extern GroupingPolicy *create_grouping_policy_segmentby(List *agg_defs, List *grouping_columns);
