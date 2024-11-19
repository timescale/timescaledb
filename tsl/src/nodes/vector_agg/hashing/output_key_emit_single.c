/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

static void
FUNCTION_NAME(emit_key)(GroupingPolicyHash *policy, uint32 current_key,
						TupleTableSlot *aggregated_slot)
{
	HashingStrategy *hashing = &policy->hashing;
	Assert(policy->num_grouping_columns == 1);

	const GroupingColumn *col = &policy->grouping_columns[0];
	aggregated_slot->tts_values[col->output_offset] = hashing->output_keys[current_key];
	aggregated_slot->tts_isnull[col->output_offset] = current_key == hashing->null_key_index;
}
