/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <executor/tuptable.h>
#include <nodes/pg_list.h>

#include "grouping_policy.h"

#include "nodes/vector_agg/exec.h"
#include "nodes/decompress_chunk/compressed_batch.h"

typedef struct
{
	GroupingPolicy funcs;
	List *agg_defs;
	List *agg_states;
} GroupingPolicyAll;

static const GroupingPolicy grouping_policy_all_functions;

static void
gp_all_reset(GroupingPolicy *obj)
{
	GroupingPolicyAll *policy = (GroupingPolicyAll *) obj;
	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_state = (void *) list_nth(policy->agg_states, i);
		agg_def->func->agg_init(agg_state);
	}
}

GroupingPolicy *
create_grouping_policy_all(List *agg_defs)
{
	GroupingPolicyAll *policy = palloc0(sizeof(GroupingPolicyAll));
	policy->funcs = grouping_policy_all_functions;
	policy->agg_defs = agg_defs;
	ListCell *lc;
	foreach (lc, agg_defs)
	{
		VectorAggDef *def = lfirst(lc);
		policy->agg_states = lappend(policy->agg_states, palloc0(def->func->state_bytes));
	}
	gp_all_reset(&policy->funcs);
	return &policy->funcs;
}

static void
compute_single_aggregate(DecompressBatchState *batch_state, VectorAggDef *agg_def, void *agg_state)
{
	/*
	 * To calculate the sum for a segment by value or default compressed
	 * column value, we need to multiply this value with the number of
	 * passing decompressed tuples in this batch.
	 */
	int n = batch_state->total_batch_rows;
	if (batch_state->vector_qual_result)
	{
		n = arrow_num_valid(batch_state->vector_qual_result, n);
		Assert(n > 0);
	}

	if (agg_def->input_offset >= 0)
	{
		CompressedColumnValues *values = &batch_state->compressed_columns[agg_def->input_offset];
		Assert(values->decompression_type != DT_Invalid);
		Assert(values->decompression_type != DT_Iterator);

		if (values->arrow == NULL)
		{
			Assert(values->decompression_type == DT_Scalar);
			agg_def->func->agg_const(agg_state, *values->output_value, *values->output_isnull, n);
		}
		else
		{
			agg_def->func->agg_vector(agg_state, values->arrow, batch_state->vector_qual_result);
		}
	}
	else
	{
		/*
		 * We have only one function w/o arguments -- count(*). Unfortunately
		 * it has to have a special code path everywhere.
		 */
		agg_def->func->agg_const(agg_state, 0, true, n);
	}
}

static void
gp_all_add_batch(GroupingPolicy *gp, DecompressBatchState *batch_state)
{
	GroupingPolicyAll *policy = (GroupingPolicyAll *) gp;
	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_state = (void *) list_nth(policy->agg_states, i);
		compute_single_aggregate(batch_state, agg_def, agg_state);
	}
}

static bool
gp_all_should_emit(GroupingPolicy *gp)
{
	return false;
}

static void
gp_all_do_emit(GroupingPolicy *gp, TupleTableSlot *aggregated_slot)
{
	GroupingPolicyAll *policy = (GroupingPolicyAll *) gp;
	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_state = (void *) list_nth(policy->agg_states, i);
		agg_def->func->agg_emit(agg_state,
								&aggregated_slot->tts_values[agg_def->output_offset],
								&aggregated_slot->tts_isnull[agg_def->output_offset]);
	}

	gp_all_reset(gp);
}

static const GroupingPolicy grouping_policy_all_functions = {
	.gp_reset = gp_all_reset,
	.gp_add_batch = gp_all_add_batch,
	.gp_should_emit = gp_all_should_emit,
	.gp_do_emit = gp_all_do_emit,
};
