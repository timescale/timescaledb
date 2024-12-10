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

extern HashingStrategy single_fixed_2_strategy;
extern HashingStrategy single_fixed_4_strategy;
extern HashingStrategy single_fixed_8_strategy;

static const GroupingPolicy grouping_policy_hash_functions;

GroupingPolicy *
create_grouping_policy_hash(int num_agg_defs, VectorAggDef *agg_defs, int num_grouping_columns,
							GroupingColumn *grouping_columns)
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

	Assert(num_grouping_columns == 1);
	const GroupingColumn *g = &policy->grouping_columns[0];
	switch (g->value_bytes)
	{
		case 8:
			policy->hashing = single_fixed_8_strategy;
			break;
		case 4:
			policy->hashing = single_fixed_4_strategy;
			break;
		case 2:
			policy->hashing = single_fixed_2_strategy;
			break;
		default:
			Assert(false);
			break;
	}

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

	policy->last_used_key_index = 0;

	policy->stat_input_valid_rows = 0;
	policy->stat_input_total_rows = 0;
	policy->stat_consecutive_keys = 0;
}

static void
compute_single_aggregate(GroupingPolicyHash *policy, const DecompressBatchState *batch_state,
						 int start_row, int end_row, const VectorAggDef *agg_def, void *agg_states)
{
	const ArrowArray *arg_arrow = NULL;
	const uint64 *arg_validity_bitmap = NULL;
	Datum arg_datum = 0;
	bool arg_isnull = true;

	const uint32 *offsets = policy->key_index_for_row;
	MemoryContext agg_extra_mctx = policy->agg_extra_mctx;

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
			arg_validity_bitmap = values->buffers[0];
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
	const uint64 *filter = arrow_combine_validity(num_words,
												  policy->tmp_filter,
												  batch_state->vector_qual_result,
												  arg_validity_bitmap);

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
	const int num_fns = policy->num_agg_defs;

	Assert(start_row < end_row);
	Assert(end_row <= batch_state->total_batch_rows);

	/*
	 * Remember which aggregation states have already existed, and which we
	 * have to initialize. State index zero is invalid.
	 */
	const uint32 last_initialized_key_index = policy->last_used_key_index;
	Assert(last_initialized_key_index <= policy->num_allocated_per_key_agg_states);

	/*
	 * Match rows to aggregation states using a hash table.
	 */
	Assert((size_t) end_row <= policy->num_key_index_for_row);
	policy->hashing.fill_offsets(policy, batch_state, start_row, end_row);

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
		if (policy->last_used_key_index > last_initialized_key_index)
		{
			/*
			 * If the aggregate function states don't fit into the existing
			 * storage, reallocate it.
			 */
			if (policy->last_used_key_index >= policy->num_allocated_per_key_agg_states)
			{
				policy->per_agg_per_key_states[agg_index] =
					repalloc(policy->per_agg_per_key_states[agg_index],
							 new_aggstate_rows * agg_def->func.state_bytes);
			}

			void *first_uninitialized_state =
				agg_def->func.state_bytes * (last_initialized_key_index + 1) +
				(char *) policy->per_agg_per_key_states[agg_index];
			agg_def->func.agg_init(first_uninitialized_state,
								   policy->last_used_key_index - last_initialized_key_index);
		}

		/*
		 * Add this batch to the states of this aggregate function.
		 */
		compute_single_aggregate(policy,
								 batch_state,
								 start_row,
								 end_row,
								 agg_def,
								 policy->per_agg_per_key_states[agg_index]);
	}

	/*
	 * Record the newly allocated number of aggregate function states in case we
	 * had to reallocate.
	 */
	if (policy->last_used_key_index >= policy->num_allocated_per_key_agg_states)
	{
		Assert(new_aggstate_rows > policy->num_allocated_per_key_agg_states);
		policy->num_allocated_per_key_agg_states = new_aggstate_rows;
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
		const CompressedColumnValues *values = &batch_state->compressed_columns[def->input_offset];
		policy->current_batch_grouping_column_values[i] = *values;
	}

	/*
	 * Call the per-batch initialization function of the hashing strategy.
	 */

	policy->hashing.prepare_for_batch(policy, batch_state);

	/*
	 * Add the batch rows to aggregate function states.
	 */
	const uint64_t *restrict filter = batch_state->vector_qual_result;
	add_one_range(policy, batch_state, 0, n);

	policy->stat_input_total_rows += batch_state->total_batch_rows;
	policy->stat_input_valid_rows += arrow_num_valid(filter, batch_state->total_batch_rows);
}

static bool
gp_hash_should_emit(GroupingPolicy *gp)
{
	GroupingPolicyHash *policy = (GroupingPolicyHash *) gp;

	if (policy->last_used_key_index > UINT32_MAX - GLOBAL_MAX_ROWS_PER_COMPRESSION)
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

		const float keys = policy->last_used_key_index;
		if (keys > 0)
		{
			DEBUG_LOG("spill after %ld input, %ld valid, %ld bulk filtered, %ld cons, %.0f keys, "
					  "%f ratio, %ld curctx bytes, %ld aggstate bytes",
					  policy->stat_input_total_rows,
					  policy->stat_input_valid_rows,
					  0UL,
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
	const uint32 keys_end = policy->last_used_key_index + 1;
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
