/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This grouping policy groups the rows using a hash table. Currently it only
 * supports a single fixed-size by-value compressed column that fits into a Datum.
 */

#include <postgres.h>

#include <executor/tuptable.h>
#include <nodes/pg_list.h>

#include "grouping_policy.h"

#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

#include "grouping_policy_hash.h"

#ifdef USE_FLOAT8_BYVAL
#define DEBUG_LOG(MSG, ...) elog(DEBUG3, MSG, __VA_ARGS__)
#else
/*
 * On 32-bit platforms we'd have to use the cross-platform int width printf
 * specifiers which are really unreadable.
 */
#define DEBUG_LOG(...)
#endif

extern HashTableFunctions single_fixed_2_functions;
extern HashTableFunctions single_fixed_4_functions;
extern HashTableFunctions single_fixed_8_functions;
extern HashTableFunctions single_text_functions;

static const GroupingPolicy grouping_policy_hash_functions;

GroupingPolicy *
create_grouping_policy_hash(List *agg_defs, List *output_grouping_columns)
{
	GroupingPolicyHash *policy = palloc0(sizeof(GroupingPolicyHash));
	policy->funcs = grouping_policy_hash_functions;
	policy->output_grouping_columns = output_grouping_columns;
	policy->agg_defs = agg_defs;
	policy->agg_extra_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "agg extra", ALLOCSET_DEFAULT_SIZES);
	policy->allocated_aggstate_rows = TARGET_COMPRESSED_BATCH_SIZE;
	ListCell *lc;
	foreach (lc, agg_defs)
	{
		VectorAggDef *agg_def = lfirst(lc);
		policy->per_agg_states =
			lappend(policy->per_agg_states,
					palloc0(agg_def->func.state_bytes * policy->allocated_aggstate_rows));
	}

	Assert(list_length(policy->output_grouping_columns) == 1);
	GroupingColumn *g = linitial(policy->output_grouping_columns);
	//  policy->key_bytes = g->value_bytes;
	policy->key_bytes = sizeof(Datum);
	Assert(policy->key_bytes > 0);

	policy->num_allocated_keys = policy->allocated_aggstate_rows;
	policy->keys = palloc(policy->key_bytes * policy->num_allocated_keys);
	policy->key_body_mctx = policy->agg_extra_mctx;

	switch (g->value_bytes)
	{
		case 8:
			policy->functions = single_fixed_8_functions;
			break;
		case 4:
			policy->functions = single_fixed_4_functions;
			break;
		case 2:
			policy->functions = single_fixed_2_functions;
			break;
		case -1:
			Assert(g->typid == TEXTOID);
			policy->functions = single_text_functions;
			break;
		default:
			Assert(false);
			break;
	}

	policy->table =
		policy->functions.create(CurrentMemoryContext, policy->allocated_aggstate_rows, NULL);
	policy->have_null_key = false;

	policy->returning_results = false;

	return &policy->funcs;
}

static void
gp_hash_reset(GroupingPolicy *obj)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) obj;

	MemoryContextReset(policy->agg_extra_mctx);

	policy->returning_results = false;

	policy->functions.reset(policy->table);
	policy->have_null_key = false;

	policy->stat_input_valid_rows = 0;
	policy->stat_input_total_rows = 0;
	policy->stat_bulk_filtered_rows = 0;
}

static void
compute_single_aggregate(DecompressBatchState *batch_state, int start_row, int end_row,
						 VectorAggDef *agg_def, void *agg_states, uint32 *offsets,
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
		agg_def->func
			.agg_many_vector(agg_states, offsets, start_row, end_row, arg_arrow, agg_extra_mctx);
	}
	else
	{
		/*
		 * Scalar argument, or count(*). The latter has an optimized
		 * implementation.
		 */
		if (agg_def->func.agg_many_scalar != NULL)
		{
			agg_def->func.agg_many_scalar(agg_states,
										  offsets,
										  start_row,
										  end_row,
										  arg_datum,
										  arg_isnull,
										  agg_extra_mctx);
		}
		else
		{
			for (int i = start_row; i < end_row; i++)
			{
				if (offsets[i] == 0)
				{
					continue;
				}

				void *state = (offsets[i] * agg_def->func.state_bytes + (char *) agg_states);
				agg_def->func.agg_scalar(state, arg_datum, arg_isnull, 1, agg_extra_mctx);
			}
		}
	}
}

static void
add_one_range(GroupingPolicyHash *policy, DecompressBatchState *batch_state, const int start_row,
			  const int end_row)
{
	const int num_fns = list_length(policy->agg_defs);
	Assert(list_length(policy->per_agg_states) == num_fns);

	const int n = batch_state->total_batch_rows;

	Assert(start_row < end_row);
	Assert(end_row <= n);

	/*
	 * Remember which aggregation states have already existed, and which we
	 * have to initialize. State index zero is invalid, and state index one
	 * is for null key. We have to initialize the null key state at the
	 * first run.
	 */
	const uint32 hash_keys = policy->functions.get_num_keys(policy->table);
	const uint32 first_uninitialized_state_index = hash_keys ? hash_keys + 2 : 1;

	/*
	 * Allocate enough storage for keys, given that each bach row might be
	 * a separate new key.
	 */
	const uint32 num_possible_keys = first_uninitialized_state_index + (end_row - start_row);
	if (num_possible_keys > policy->num_allocated_keys)
	{
		policy->num_allocated_keys = num_possible_keys;
		policy->keys = repalloc(policy->keys, policy->key_bytes * num_possible_keys);
	}

	/*
	 * Match rows to aggregation states using a hash table.
	 */
	Assert((size_t) end_row <= policy->num_allocated_offsets);
	uint32 next_unused_state_index = hash_keys + 2;
	next_unused_state_index = policy->functions.fill_offsets(policy,
															 batch_state,
															 next_unused_state_index,
															 start_row,
															 end_row);

	const uint64 new_aggstate_rows = policy->allocated_aggstate_rows * 2 + 1;
	for (int i = 0; i < num_fns; i++)
	{
		VectorAggDef *agg_def = list_nth(policy->agg_defs, i);
		if (next_unused_state_index > first_uninitialized_state_index)
		{
			if (next_unused_state_index > policy->allocated_aggstate_rows)
			{
				lfirst(list_nth_cell(policy->per_agg_states, i)) =
					repalloc(list_nth(policy->per_agg_states, i),
							 new_aggstate_rows * agg_def->func.state_bytes);
			}

			/*
			 * Initialize the aggregate function states for the newly added keys.
			 */
			agg_def->func.agg_init(agg_def->func.state_bytes * first_uninitialized_state_index +
									   (char *) list_nth(policy->per_agg_states, i),
								   next_unused_state_index - first_uninitialized_state_index);
		}

		/*
		 * Update the aggregate function states.
		 */
		compute_single_aggregate(batch_state,
								 start_row,
								 end_row,
								 agg_def,
								 list_nth(policy->per_agg_states, i),
								 policy->offsets,
								 policy->agg_extra_mctx);
	}

	/*
	 * Record the newly allocated number of rows in case we had to reallocate.
	 */
	if (next_unused_state_index > policy->allocated_aggstate_rows)
	{
		Assert(new_aggstate_rows >= policy->allocated_aggstate_rows);
		policy->allocated_aggstate_rows = new_aggstate_rows;
	}
}

static void
gp_hash_add_batch(GroupingPolicy *gp, DecompressBatchState *batch_state)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	Assert(!policy->returning_results);

	const int n = batch_state->total_batch_rows;

	/*
	 * Initialize the array for storing the aggregate state offsets corresponding
	 * to a given batch row.
	 */
	if ((size_t) n > policy->num_allocated_offsets)
	{
		policy->num_allocated_offsets = n;
		policy->offsets = palloc(sizeof(policy->offsets[0]) * policy->num_allocated_offsets);
	}
	memset(policy->offsets, 0, n * sizeof(policy->offsets[0]));

	const uint64_t *restrict filter = batch_state->vector_qual_result;
	if (filter == NULL)
	{
		add_one_range(policy, batch_state, 0, n);
	}
	else
	{
		/*
		 * If we have a filter, skip the rows for which the entire words of the
		 * filter bitmap are zero. This improves performance for highly
		 * selective filters.
		 */
		int start_word = 0;
		int end_word = 0;
		int past_the_end_word = (n - 1) / 64 + 1;
		for (;;)
		{
			for (start_word = end_word; start_word < past_the_end_word && filter[start_word] == 0;
				 start_word++)
				;

			if (start_word >= past_the_end_word)
			{
				break;
			}

			for (end_word = start_word + 1; end_word < past_the_end_word && filter[end_word] != 0;
				 end_word++)
				;

			const int start_row = start_word * 64 + pg_rightmost_one_pos64(filter[start_word]);
			/*
			 * The bits for past-the-end rows must be set to zero, so this
			 * calculation should yield no more than n.
			 */
			int end_row = (end_word - 1) * 64 + pg_leftmost_one_pos64(filter[end_word - 1]) + 1;
			add_one_range(policy, batch_state, start_row, end_row);
		}
	}
}

static bool
gp_hash_should_emit(GroupingPolicy *gp)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	/*
	 * Don't grow the hash table cardinality too much, otherwise we become bound
	 * by memory reads. In general, when this first stage of grouping doesn't
	 * significantly reduce the cardinality, it becomes pure overhead and the
	 * work will be done by the final Postgres aggregation, so we should bail
	 * out early here.
	 */
	return policy->functions.get_size_bytes(policy->table) > 128 * 1024;
}

static bool
gp_hash_do_emit(GroupingPolicy *gp, TupleTableSlot *aggregated_slot)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	if (!policy->returning_results)
	{
		policy->returning_results = true;
		policy->last_returned_key = 1 + !policy->have_null_key;

		const float keys = policy->functions.get_num_keys(policy->table) + policy->have_null_key;
		if (keys > 0)
		{
			DEBUG_LOG("spill after %ld input %ld valid %ld bulk filtered %.0f keys %f ratio %ld "
					  "curctx bytes %ld aggstate bytes",
					  policy->stat_input_total_rows,
					  policy->stat_input_valid_rows,
					  policy->stat_bulk_filtered_rows,
					  keys,
					  policy->stat_input_valid_rows / keys,
					  MemoryContextMemAllocated(CurrentMemoryContext, false),
					  MemoryContextMemAllocated(policy->agg_extra_mctx, false));
		}
	}
	else
	{
		policy->last_returned_key++;
	}

	const uint32 current_key = policy->last_returned_key;
	const uint32 keys_end = policy->functions.get_num_keys(policy->table) + 2;
	if (current_key >= keys_end)
	{
		policy->returning_results = false;
		return false;
	}

	const int naggs = list_length(policy->agg_defs);
	for (int i = 0; i < naggs; i++)
	{
		VectorAggDef *agg_def = (VectorAggDef *) list_nth(policy->agg_defs, i);
		void *agg_states = list_nth(policy->per_agg_states, i);
		void *agg_state = current_key * agg_def->func.state_bytes + (char *) agg_states;
		agg_def->func.agg_emit(agg_state,
							   &aggregated_slot->tts_values[agg_def->output_offset],
							   &aggregated_slot->tts_isnull[agg_def->output_offset]);
	}

	Assert(list_length(policy->output_grouping_columns) == 1);
	GroupingColumn *col = linitial(policy->output_grouping_columns);
	aggregated_slot->tts_values[col->output_offset] = ((Datum *) policy->keys)[current_key];
	aggregated_slot->tts_isnull[col->output_offset] = current_key == 1;

	return true;
}

static const GroupingPolicy grouping_policy_hash_functions = {
	.gp_reset = gp_hash_reset,
	.gp_add_batch = gp_hash_add_batch,
	.gp_should_emit = gp_hash_should_emit,
	.gp_do_emit = gp_hash_do_emit,
};
