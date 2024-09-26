/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This grouping policy aggregates entire compressed batches. It can be used to
 * aggregate with no grouping, or to produce partial aggregates per each batch
 * to group by segmentby columns.
 */

#include <postgres.h>

#include <executor/tuptable.h>
#include <nodes/pg_list.h>

#include "grouping_policy.h"

#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

typedef struct
{
	GroupingPolicy funcs;
	List *agg_defs;
	List *agg_states;
	List *output_grouping_columns;
	Datum *output_grouping_values;
	bool *output_grouping_isnull;
	bool partial_per_batch;
	bool have_results;

	/*
	 * A memory context for aggregate functions to allocate additional data,
	 * i.e. if they store strings or float8 datum on 32-bit systems. Valid until
	 * the grouping policy is reset.
	 */
	MemoryContext agg_extra_mctx;
} GroupingPolicyBatch;

static const GroupingPolicy grouping_policy_batch_functions;

GroupingPolicy *
create_grouping_policy_batch(List *agg_defs, List *output_grouping_columns, bool partial_per_batch)
{
	GroupingPolicyBatch *policy = palloc0(sizeof(GroupingPolicyBatch));
	policy->partial_per_batch = partial_per_batch;
	policy->funcs = grouping_policy_batch_functions;
	policy->output_grouping_columns = output_grouping_columns;
	policy->agg_defs = agg_defs;
	policy->agg_extra_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "agg extra", ALLOCSET_DEFAULT_SIZES);
	ListCell *lc;
	foreach (lc, agg_defs)
	{
		VectorAggDef *def = lfirst(lc);
		policy->agg_states = lappend(policy->agg_states, palloc0(def->func->state_bytes));
	}
	policy->output_grouping_values =
		(Datum *) palloc0(MAXALIGN(list_length(output_grouping_columns) * sizeof(Datum)) +
						  MAXALIGN(list_length(output_grouping_columns) * sizeof(bool)));
	policy->output_grouping_isnull =
		(bool *) ((char *) policy->output_grouping_values +
				  MAXALIGN(list_length(output_grouping_columns) * sizeof(Datum)));

	return &policy->funcs;
}

static void
gp_batch_reset(GroupingPolicy *obj)
{
	GroupingPolicyBatch *policy = (GroupingPolicyBatch *) obj;

	MemoryContextReset(policy->agg_extra_mctx);

	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_state = (void *) list_nth(policy->agg_states, i);
		agg_def->func->agg_init(agg_state);
	}

	const int ngrp = list_length(policy->output_grouping_columns);
	for (int i = 0; i < ngrp; i++)
	{
		policy->output_grouping_values[i] = 0;
		policy->output_grouping_isnull[i] = true;
	}

	policy->have_results = false;
}

static void
compute_single_aggregate(DecompressBatchState *batch_state, VectorAggDef *agg_def, void *agg_state,
						 MemoryContext agg_extra_mctx)
{
	ArrowArray *arg_arrow = NULL;
	Datum arg_datum = 0;
	bool arg_isnull = true;

	/*
	 * We have functions with one argument, and one function with no arguments
	 * (count(*)). Collect the arguments.
	 */
	if (agg_def->input_offset >= 0)
	{
		CompressedColumnValues *values = &batch_state->compressed_columns[agg_def->input_offset];
		Assert(values->decompression_type != DT_Invalid);
		Assert(values->decompression_type != DT_Iterator);

		if (values->arrow != NULL)
		{
			arg_arrow = values->arrow;
		}
		else
		{
			Assert(values->decompression_type == DT_Scalar);
			arg_datum = *values->output_value;
			arg_isnull = *values->output_isnull;
		}
	}

	/*
	 * Now call the function.
	 */
	if (arg_arrow != NULL)
	{
		/* Arrow argument. */
		agg_def->func->agg_vector(agg_state,
								  arg_arrow,
								  batch_state->vector_qual_result,
								  agg_extra_mctx);
	}
	else
	{
		/*
		 * Scalar argument, or count(*). Have to also count the valid rows in
		 * the batch.
		 */
		const int n =
			arrow_num_valid(batch_state->vector_qual_result, batch_state->total_batch_rows);

		/*
		 * The batches that are fully filtered out by vectorized quals should
		 * have been skipped by the caller.
		 */
		Assert(n > 0);

		agg_def->func->agg_const(agg_state, arg_datum, arg_isnull, n, agg_extra_mctx);
	}
}

static void
gp_batch_add_batch(GroupingPolicy *gp, DecompressBatchState *batch_state)
{
	GroupingPolicyBatch *policy = (GroupingPolicyBatch *) gp;
	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_state = (void *) list_nth(policy->agg_states, i);
		compute_single_aggregate(batch_state, agg_def, agg_state, policy->agg_extra_mctx);
	}

	const int ngrp = list_length(policy->output_grouping_columns);
	for (int i = 0; i < ngrp; i++)
	{
		GroupingColumn *col = list_nth(policy->output_grouping_columns, i);
		Assert(col->input_offset >= 0);
		Assert(col->output_offset >= 0);

		CompressedColumnValues *values = &batch_state->compressed_columns[col->input_offset];
		Assert(values->decompression_type == DT_Scalar);

		/*
		 * By sheer luck, we can avoid generically copying the Datum here,
		 * because if we have any output grouping columns in this policy, it
		 * means we're grouping by segmentby, and these values will be valid
		 * until the next call to the vector agg node.
		 */
		Assert(policy->partial_per_batch);
		policy->output_grouping_values[i] = *values->output_value;
		policy->output_grouping_isnull[i] = *values->output_isnull;
	}

	policy->have_results = true;
}

static bool
gp_batch_should_emit(GroupingPolicy *gp)
{
	GroupingPolicyBatch *policy = (GroupingPolicyBatch *) gp;
	return policy->partial_per_batch && policy->have_results;
}

static bool
gp_batch_do_emit(GroupingPolicy *gp, TupleTableSlot *aggregated_slot)
{
	GroupingPolicyBatch *policy = (GroupingPolicyBatch *) gp;

	if (!policy->have_results)
	{
		return false;
	}

	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_state = (void *) list_nth(policy->agg_states, i);
		agg_def->func->agg_emit(agg_state,
								&aggregated_slot->tts_values[agg_def->output_offset],
								&aggregated_slot->tts_isnull[agg_def->output_offset]);
	}

	const int ngrp = list_length(policy->output_grouping_columns);
	for (int i = 0; i < ngrp; i++)
	{
		GroupingColumn *col = list_nth(policy->output_grouping_columns, i);
		Assert(col->input_offset >= 0);
		Assert(col->output_offset >= 0);

		aggregated_slot->tts_values[col->output_offset] = policy->output_grouping_values[i];
		aggregated_slot->tts_isnull[col->output_offset] = policy->output_grouping_isnull[i];
	}

	/*
	 * We only have one partial aggregation result for this policy.
	 */
	policy->have_results = false;

	return true;
}

static const GroupingPolicy grouping_policy_batch_functions = {
	.gp_reset = gp_batch_reset,
	.gp_add_batch = gp_batch_add_batch,
	.gp_should_emit = gp_batch_should_emit,
	.gp_do_emit = gp_batch_do_emit,
};
