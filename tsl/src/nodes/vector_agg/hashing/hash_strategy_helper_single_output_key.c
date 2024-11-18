/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME_HELPER(X, Y) FUNCTION_NAME_HELPER2(X, Y)
#define FUNCTION_NAME(Y) FUNCTION_NAME_HELPER(KEY_VARIANT, Y)

static void
FUNCTION_NAME(alloc_output_keys)(GroupingPolicyHash *policy, DecompressBatchState *batch_state)
{
	/*
	 * Allocate enough storage for keys, given that each row of the new
	 * compressed batch might turn out to be a new grouping key.
	 * We do this separately to avoid allocations in the hot loop that fills the hash
	 * table.
	 */
	HashingStrategy *hashing = &policy->strategy;
	const int n = batch_state->total_batch_rows;
	const uint32 num_possible_keys = policy->last_used_key_index + 1 + n;
	if (num_possible_keys > hashing->num_output_keys)
	{
		hashing->num_output_keys = num_possible_keys * 2 + 1;
		const size_t new_bytes = sizeof(Datum) * hashing->num_output_keys;
		if (hashing->output_keys == NULL)
		{
			hashing->output_keys = palloc(new_bytes);
		}
		else
		{
			hashing->output_keys = repalloc(hashing->output_keys, new_bytes);
		}
	}
}

static void
FUNCTION_NAME(emit_key)(GroupingPolicyHash *policy, uint32 current_key,
						TupleTableSlot *aggregated_slot)
{
	HashingStrategy *hashing = &policy->strategy;
	Assert(policy->num_grouping_columns == 1);

	const GroupingColumn *col = &policy->grouping_columns[0];
	aggregated_slot->tts_values[col->output_offset] = hashing->output_keys[current_key];
	aggregated_slot->tts_isnull[col->output_offset] = current_key == hashing->null_key_index;
}

#undef FUNCTION_NAME_HELPER2
#undef FUNCTION_NAME_HELPER
#undef FUNCTION_NAME
