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

#include <access/attnum.h>
#include <access/tupdesc.h>
#include <executor/tuptable.h>
#include <nodes/pg_list.h>

#include "grouping_policy.h"

#include "nodes/vector_agg/exec.h"
#include "nodes/vector_agg/vector_slot.h"

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

extern HashingStrategy single_fixed_2_strategy;
extern HashingStrategy single_fixed_4_strategy;
extern HashingStrategy single_fixed_8_strategy;
#ifdef TS_USE_UMASH
extern HashingStrategy single_text_strategy;
extern HashingStrategy serialized_strategy;
#endif

static const GroupingPolicy grouping_policy_hash_functions;

GroupingPolicy *
create_grouping_policy_hash(int num_agg_defs, VectorAggDef *agg_defs, int num_grouping_columns,
							GroupingColumn *grouping_columns, VectorAggGroupingType grouping_type)
{
	GroupingPolicyHash *policy = palloc0(sizeof(GroupingPolicyHash));
	policy->funcs = grouping_policy_hash_functions;

	policy->num_grouping_columns = num_grouping_columns;
	policy->grouping_columns = grouping_columns;

	policy->agg_extra_mctx =
		AllocSetContextCreate(CurrentMemoryContext, "agg extra", ALLOCSET_DEFAULT_SIZES);
	policy->num_allocated_per_key_agg_states = TARGET_COMPRESSED_BATCH_SIZE;

	policy->num_agg_defs = num_agg_defs;
	policy->agg_defs = agg_defs;

	policy->per_agg_per_key_states =
		palloc(sizeof(*policy->per_agg_per_key_states) * policy->num_agg_defs);
	for (int i = 0; i < policy->num_agg_defs; i++)
	{
		const VectorAggDef *agg_def = &policy->agg_defs[i];
		policy->per_agg_per_key_states[i] =
			palloc(agg_def->func.state_bytes * policy->num_allocated_per_key_agg_states);
	}

	policy->current_batch_grouping_column_values =
		palloc(sizeof(CompressedColumnValues) * num_grouping_columns);

	switch (grouping_type)
	{
#ifdef TS_USE_UMASH
		case VAGT_HashSerialized:
			policy->hashing = serialized_strategy;
			break;
		case VAGT_HashSingleText:
			policy->hashing = single_text_strategy;
			break;
#endif
		case VAGT_HashSingleFixed8:
			policy->hashing = single_fixed_8_strategy;
			break;
		case VAGT_HashSingleFixed4:
			policy->hashing = single_fixed_4_strategy;
			break;
		case VAGT_HashSingleFixed2:
			policy->hashing = single_fixed_2_strategy;
			break;
		default:
			Ensure(false, "failed to determine the hashing strategy");
			break;
	}

	policy->hashing.key_body_mctx = policy->agg_extra_mctx;

	policy->hashing.init(&policy->hashing, policy);

	return &policy->funcs;
}

static void
gp_hash_reset(GroupingPolicy *obj)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) obj;

	MemoryContextReset(policy->agg_extra_mctx);

	policy->returning_results = false;

	policy->hashing.reset(&policy->hashing);

	policy->stat_input_valid_rows = 0;
	policy->stat_input_total_rows = 0;
	policy->stat_bulk_filtered_rows = 0;
	policy->stat_consecutive_keys = 0;
}

static void
compute_single_aggregate(GroupingPolicyHash *policy, DecompressContext *dcontext,
						 TupleTableSlot *vector_slot, int start_row, int end_row,
						 const VectorAggDef *agg_def, void *agg_states)
{
	const uint32 *offsets = policy->key_index_for_row;
	MemoryContext agg_extra_mctx = policy->agg_extra_mctx;

	/*
	 * We have functions with one argument, and one function with no arguments
	 * (count(*)). Collect the arguments.
	 */
	const ArrowArray *arg_arrow = NULL;
	const uint64 *arg_validity_bitmap = NULL;
	Datum arg_datum = 0;
	bool arg_isnull = true;
	if (agg_def->argument != NULL)
	{
		const CompressedColumnValues values =
			vector_slot_get_compressed_column_values(dcontext,
													 vector_slot,
													 agg_def->effective_batch_filter,
													 agg_def->argument);

		Assert(values.decompression_type != DT_Invalid);
		Ensure(values.decompression_type != DT_Iterator, "expected arrow array but got iterator");

		if (values.arrow != NULL)
		{
			arg_arrow = values.arrow;
			arg_validity_bitmap = values.buffers[0];
		}
		else
		{
			Assert(values.decompression_type == DT_Scalar);
			arg_isnull = *values.output_isnull;
			if (!arg_isnull)
			{
				arg_datum = *values.output_value;
			}
		}
	}

	/*
	 * Compute the combined validity bitmap that includes the argument validity.
	 */
	DecompressBatchState *batch_state = (DecompressBatchState *) vector_slot;
	const size_t num_words = (batch_state->total_batch_rows + 63) / 64;
	const uint64 *combined_validity = arrow_combine_validity(num_words,
															 policy->tmp_filter,
															 agg_def->effective_batch_filter,
															 arg_validity_bitmap,
															 NULL);

	/*
	 * Now call the function.
	 */
	if (arg_arrow != NULL)
	{
		/* Arrow argument. */
		agg_def->func.agg_many_vector(agg_states,
									  offsets,
									  combined_validity,
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
										  combined_validity,
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
				if (!arrow_row_is_valid(combined_validity, i))
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
add_one_range(GroupingPolicyHash *policy, DecompressContext *dcontext, TupleTableSlot *vector_slot,
			  const int start_row, const int end_row)
{
	const int num_fns = policy->num_agg_defs;
	Assert(start_row < end_row);

	/*
	 * Remember which aggregation states have already existed, and which we
	 * have to initialize. State index zero is invalid.
	 */
	const uint32 last_initialized_key_index = policy->hashing.last_used_key_index;
	Assert(last_initialized_key_index <= policy->num_allocated_per_key_agg_states);

	/*
	 * Match rows to aggregation states using a hash table.
	 */
	Assert((size_t) end_row <= policy->num_key_index_for_row);
	policy->hashing.fill_offsets(policy, vector_slot, start_row, end_row);

	/*
	 * Process the aggregate function states. We are processing single aggregate
	 * function for the entire batch to improve the memory locality.
	 */
	const uint64 new_aggstate_rows = policy->num_allocated_per_key_agg_states * 2 + 1;
	for (int agg_index = 0; agg_index < num_fns; agg_index++)
	{
		const VectorAggDef *agg_def = &policy->agg_defs[agg_index];
		/*
		 * If we added new keys, initialize the aggregate function states for
		 * them.
		 */
		if (policy->hashing.last_used_key_index > last_initialized_key_index)
		{
			/*
			 * If the aggregate function states don't fit into the existing
			 * storage, reallocate it.
			 */
			if (policy->hashing.last_used_key_index >= policy->num_allocated_per_key_agg_states)
			{
				policy->per_agg_per_key_states[agg_index] =
					repalloc(policy->per_agg_per_key_states[agg_index],
							 new_aggstate_rows * agg_def->func.state_bytes);
			}

			void *first_uninitialized_state =
				agg_def->func.state_bytes * (last_initialized_key_index + 1) +
				(char *) policy->per_agg_per_key_states[agg_index];
			agg_def->func.agg_init(first_uninitialized_state,
								   policy->hashing.last_used_key_index -
									   last_initialized_key_index);
		}

		/*
		 * Add this batch to the states of this aggregate function.
		 */
		compute_single_aggregate(policy,
								 dcontext,
								 vector_slot,
								 start_row,
								 end_row,
								 agg_def,
								 policy->per_agg_per_key_states[agg_index]);
	}

	/*
	 * Record the newly allocated number of aggregate function states in case we
	 * had to reallocate.
	 */
	if (policy->hashing.last_used_key_index >= policy->num_allocated_per_key_agg_states)
	{
		Assert(new_aggstate_rows > policy->num_allocated_per_key_agg_states);
		policy->num_allocated_per_key_agg_states = new_aggstate_rows;
	}
}

static void
gp_hash_add_batch(GroupingPolicy *gp, DecompressContext *dcontext, TupleTableSlot *vector_slot)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;
	uint16 n;
	const uint64 *restrict filter = vector_slot_get_qual_result(vector_slot, &n);

	Assert(!policy->returning_results);

	/*
	 * Initialize the array for storing the aggregate state offsets corresponding
	 * to a given batch row. We don't need the offsets for the previous batch
	 * that are currently stored there, so we don't need to use repalloc.
	 */
	if ((size_t) n > policy->num_key_index_for_row)
	{
		if (policy->key_index_for_row != NULL)
		{
			pfree(policy->key_index_for_row);
		}
		policy->num_key_index_for_row = n;
		policy->key_index_for_row =
			palloc(sizeof(policy->key_index_for_row[0]) * policy->num_key_index_for_row);
	}
	memset(policy->key_index_for_row, 0, n * sizeof(policy->key_index_for_row[0]));

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

	/*
	 * Arrange the input compressed columns in the order of grouping columns.
	 */
	for (int i = 0; i < policy->num_grouping_columns; i++)
	{
		const GroupingColumn *def = &policy->grouping_columns[i];

		policy->current_batch_grouping_column_values[i] =
			vector_slot_get_compressed_column_values(dcontext, vector_slot, filter, def->expr);
	}

	/*
	 * Call the per-batch initialization function of the hashing strategy.
	 */
	policy->hashing.prepare_for_batch(policy, vector_slot);

	/*
	 * Add the batch rows to aggregate function states.
	 */
	if (filter == NULL)
	{
		/*
		 * We don't have a filter on this batch, so aggregate it entirely in one
		 * go.
		 */
		add_one_range(policy, dcontext, vector_slot, 0, n);
	}
	else
	{
		/*
		 * If we have a filter, skip the rows for which the entire words of the
		 * filter bitmap are zero. This improves performance for highly
		 * selective filters.
		 */
		int statistics_range_row = 0;
		int start_word = 0;
		int end_word = 0;
		int past_the_end_word = (n - 1) / 64 + 1;
		for (;;)
		{
			/*
			 * Skip the bitmap words which are zero.
			 */
			for (start_word = end_word; start_word < past_the_end_word && filter[start_word] == 0;
				 start_word++)
				;

			if (start_word >= past_the_end_word)
			{
				break;
			}

			/*
			 * Collect the consecutive bitmap words which are nonzero.
			 */
			for (end_word = start_word + 1; end_word < past_the_end_word && filter[end_word] != 0;
				 end_word++)
				;

			/*
			 * Now we have the [start, end] range of bitmap words that are
			 * nonzero.
			 *
			 * Determine starting and ending rows, also skipping the starting
			 * and trailing zero bits at the ends of the range.
			 */
			const int start_row = start_word * 64 + pg_rightmost_one_pos64(filter[start_word]);
			Assert(start_row <= n);

			/*
			 * The bits for past-the-end rows must be set to zero, so this
			 * calculation should yield no more than n.
			 */
			Assert(end_word > start_word);
			const int end_row =
				(end_word - 1) * 64 + pg_leftmost_one_pos64(filter[end_word - 1]) + 1;
			Assert(end_row <= n);

			statistics_range_row += end_row - start_row;

			add_one_range(policy, dcontext, vector_slot, start_row, end_row);
		}

		policy->stat_bulk_filtered_rows += n - statistics_range_row;
	}

	policy->stat_input_total_rows += n;
	policy->stat_input_valid_rows += arrow_num_valid(filter, n);
}

static bool
gp_hash_should_emit(GroupingPolicy *gp)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	if (policy->hashing.last_used_key_index > UINT32_MAX - GLOBAL_MAX_ROWS_PER_COMPRESSION)
	{
		/*
		 * The max valid key index is UINT32_MAX, so we have to spill if the next
		 * batch can possibly lead to key index overflow.
		 */
		return true;
	}

	/*
	 * Don't grow the hash table cardinality too much, otherwise we become bound
	 * by memory reads. In general, when this first stage of grouping doesn't
	 * significantly reduce the cardinality, it becomes pure overhead and the
	 * work will be done by the final Postgres aggregation, so we should bail
	 * out early here.
	 */
	return policy->hashing.get_size_bytes(&policy->hashing) > 512 * 1024;
}

static bool
gp_hash_do_emit(GroupingPolicy *gp, TupleTableSlot *aggregated_slot)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	if (!policy->returning_results)
	{
		policy->returning_results = true;
		policy->last_returned_key = 1;

		const float keys = policy->hashing.last_used_key_index;
		if (keys > 0)
		{
			DEBUG_LOG("spill after " UINT64_FORMAT " input, " UINT64_FORMAT " valid, " UINT64_FORMAT
					  " bulk filtered, " UINT64_FORMAT " cons, %.0f keys, "
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
	const uint32 keys_end = policy->hashing.last_used_key_index + 1;
	if (current_key >= keys_end)
	{
		policy->returning_results = false;
		return false;
	}

	const int naggs = policy->num_agg_defs;
	for (int i = 0; i < naggs; i++)
	{
		const VectorAggDef *agg_def = &policy->agg_defs[i];
		void *agg_states = policy->per_agg_per_key_states[i];
		void *agg_state = current_key * agg_def->func.state_bytes + (char *) agg_states;
		agg_def->func.agg_emit(agg_state,
							   &aggregated_slot->tts_values[agg_def->output_offset],
							   &aggregated_slot->tts_isnull[agg_def->output_offset]);
	}

	policy->hashing.emit_key(policy, current_key, aggregated_slot);

	DEBUG_PRINT("%p: output key index %d\n", policy, current_key);

	return true;
}

static char *
gp_hash_explain(GroupingPolicy *gp)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;
	return psprintf("hashed with %s key", policy->hashing.explain_name);
}

static const GroupingPolicy grouping_policy_hash_functions = {
	.gp_reset = gp_hash_reset,
	.gp_add_batch = gp_hash_add_batch,
	.gp_should_emit = gp_hash_should_emit,
	.gp_do_emit = gp_hash_do_emit,
	.gp_explain = gp_hash_explain,
};
