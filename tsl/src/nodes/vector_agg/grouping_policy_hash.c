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
extern HashTableFunctions serialized_functions;

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

	policy->num_allocated_keys = policy->allocated_aggstate_rows;
	policy->output_keys =
		palloc((sizeof(uint64) + list_length(policy->output_grouping_columns) * sizeof(Datum)) *
			   policy->num_allocated_keys);
	policy->key_body_mctx = policy->agg_extra_mctx;

	if (false && list_length(policy->output_grouping_columns) == 1)
	{
		GroupingColumn *g = linitial(policy->output_grouping_columns);
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
	}
	else
	{
		policy->functions = serialized_functions;
	}

	policy->table =
		policy->functions.create(CurrentMemoryContext, policy->allocated_aggstate_rows, NULL);
	policy->null_key_index = 0;
	policy->next_unused_key_index = 1;

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

	policy->null_key_index = 0;
	policy->next_unused_key_index = 1;

	policy->stat_input_valid_rows = 0;
	policy->stat_input_total_rows = 0;
	policy->stat_bulk_filtered_rows = 0;
	policy->stat_consecutive_keys = 0;
}

static void
compute_single_aggregate(GroupingPolicyHash *policy, const DecompressBatchState *batch_state,
						 int start_row, int end_row, VectorAggDef *agg_def, void *agg_states,
						 const uint32 *offsets, MemoryContext agg_extra_mctx)
{
	const ArrowArray *arg_arrow = NULL;
	Datum arg_datum = 0;
	bool arg_isnull = true;

	/*
	 * We have functions with one argument, and one function with no arguments
	 * (count(*)). Collect the arguments.
	 */
	if (agg_def->input_offset >= 0)
	{
		const CompressedColumnValues *values =
			&batch_state->compressed_columns[agg_def->input_offset];
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
	 * Compute the unified validity bitmap.
	 */
	const size_t num_words = (batch_state->total_batch_rows + 63) / 64;
	uint64 *restrict filter =
		arrow_combine_validity(num_words,
							   policy->tmp_filter,
							   batch_state->vector_qual_result,
							   agg_def->filter_result,
							   arg_arrow != NULL ? arg_arrow->buffers[0] : NULL);

	/*
	 * Now call the function.
	 */
	if (arg_arrow != NULL)
	{
		/* Arrow argument. */
		agg_def->func.agg_many_vector(agg_states,
									  offsets,
									  filter,
									  start_row,
									  end_row,
									  arg_arrow,
									  agg_extra_mctx);
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
										  filter,
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
				if (!arrow_row_is_valid(filter, i))
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

	Assert(start_row < end_row);
	Assert(end_row <= batch_state->total_batch_rows);

	/*
	 * Remember which aggregation states have already existed, and which we
	 * have to initialize. State index zero is invalid.
	 */
	const uint32 first_uninitialized_state_index = policy->next_unused_key_index;

	/*
	 * Allocate enough storage for keys, given that each bach row might be
	 * a separate new key.
	 */
	const uint32 num_possible_keys = first_uninitialized_state_index + (end_row - start_row);
	if (num_possible_keys > policy->num_allocated_keys)
	{
		policy->num_allocated_keys = num_possible_keys;
		policy->output_keys =
			repalloc(policy->output_keys,
					 (sizeof(uint64) +
					  list_length(policy->output_grouping_columns) * sizeof(Datum)) *
						 num_possible_keys);
	}

	/*
	 * Match rows to aggregation states using a hash table.
	 */
	Assert((size_t) end_row <= policy->num_allocated_offsets);
	uint32 next_unused_key_index = policy->next_unused_key_index;
	next_unused_key_index = policy->functions.fill_offsets(policy,
														   batch_state,
														   next_unused_key_index,
														   start_row,
														   end_row);

	const uint64 new_aggstate_rows = policy->allocated_aggstate_rows * 2 + 1;
	for (int i = 0; i < num_fns; i++)
	{
		VectorAggDef *agg_def = list_nth(policy->agg_defs, i);
		if (next_unused_key_index > first_uninitialized_state_index)
		{
			if (next_unused_key_index > policy->allocated_aggstate_rows)
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
								   next_unused_key_index - first_uninitialized_state_index);
		}

		/*
		 * Update the aggregate function states.
		 */
		compute_single_aggregate(policy,
								 batch_state,
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
	if (next_unused_key_index > policy->allocated_aggstate_rows)
	{
		Assert(new_aggstate_rows >= policy->allocated_aggstate_rows);
		policy->allocated_aggstate_rows = new_aggstate_rows;
	}

	policy->next_unused_key_index = next_unused_key_index;
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

	/*
	 * Allocate the temporary filter array for computing the combined results of
	 * batch filter, aggregate filter and column validity.
	 */
	const size_t num_words = (n + 63) / 64;
	if (num_words > policy->num_tmp_filter_words)
	{
		policy->tmp_filter = palloc(sizeof(*policy->tmp_filter) * (num_words * 2 + 1));
		policy->num_tmp_filter_words = (num_words * 2 + 1);
	}

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
		int stat_range_rows = 0;
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

			stat_range_rows += end_row - start_row;
		}
		policy->stat_bulk_filtered_rows += batch_state->total_batch_rows - stat_range_rows;
	}

	policy->stat_input_total_rows += batch_state->total_batch_rows;
	policy->stat_input_valid_rows += arrow_num_valid(filter, batch_state->total_batch_rows);
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
		policy->last_returned_key = 1;

		const float keys = policy->next_unused_key_index - 1;
		if (keys > 0)
		{
			DEBUG_LOG("spill after %ld input, %ld valid, %ld bulk filtered, %ld cons, %.0f keys, "
					  "%f ratio, %ld curctx bytes, %ld aggstate bytes",
					  policy->stat_input_total_rows,
					  policy->stat_input_valid_rows,
					  policy->stat_bulk_filtered_rows,
					  policy->stat_consecutive_keys,
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
	const uint32 keys_end = policy->next_unused_key_index;
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

	const int num_keys = list_length(policy->output_grouping_columns);
	for (int i = 0; i < num_keys; i++)
	{
		GroupingColumn *col = list_nth(policy->output_grouping_columns, i);
		aggregated_slot->tts_values[col->output_offset] =
			gp_hash_output_keys(policy, current_key)[i];
		aggregated_slot->tts_isnull[col->output_offset] =
			!arrow_row_is_valid(gp_hash_key_validity_bitmap(policy, current_key), i);
	}

	DEBUG_PRINT("%p: output key index %d valid %lx\n",
				policy,
				current_key,
				gp_hash_key_validity_bitmap(policy, current_key)[0]);

	return true;
}

static const GroupingPolicy grouping_policy_hash_functions = {
	.gp_reset = gp_hash_reset,
	.gp_add_batch = gp_hash_add_batch,
	.gp_should_emit = gp_hash_should_emit,
	.gp_do_emit = gp_hash_do_emit,
};
