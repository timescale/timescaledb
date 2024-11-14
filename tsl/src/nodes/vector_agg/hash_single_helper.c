/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME_HELPER(X, Y) FUNCTION_NAME_HELPER2(X, Y)
#define FUNCTION_NAME(Y) FUNCTION_NAME_HELPER(KEY_VARIANT, Y)

static void
FUNCTION_NAME(emit_key)(GroupingPolicyHash *policy, uint32 current_key,
						TupleTableSlot *aggregated_slot)
{
	Assert(policy->num_grouping_columns == 1);

	const GroupingColumn *col = &policy->grouping_columns[0];
	aggregated_slot->tts_values[col->output_offset] = gp_hash_output_keys(policy, current_key)[0];
	aggregated_slot->tts_isnull[col->output_offset] = current_key == policy->null_key_index;
}

#undef FUNCTION_NAME_HELPER2
#undef FUNCTION_NAME_HELPER
#undef FUNCTION_NAME
