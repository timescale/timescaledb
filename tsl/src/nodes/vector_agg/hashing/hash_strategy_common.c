/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "hashing_strategy.h"

#include "nodes/vector_agg/exec.h"
#include "nodes/vector_agg/grouping_policy_hash.h"

/*
 * Allocate enough storage for keys, given that each row of the new compressed
 * batch might turn out to be a new grouping key. We do this separately to avoid
 * allocations in the hot loop that fills the hash table.
 */
void
hash_strategy_output_key_alloc(GroupingPolicyHash *policy, uint16 nrows)
{
	HashingStrategy *hashing = &policy->hashing;
	const uint32 num_possible_keys = hashing->last_used_key_index + 1 + nrows;

	if (num_possible_keys > hashing->num_allocated_output_keys)
	{
		hashing->num_allocated_output_keys = num_possible_keys * 2 + 1;
		const size_t new_bytes = sizeof(Datum) * hashing->num_allocated_output_keys;
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

/*
 * Emit a single-column grouping key with the given index into the aggregated
 * slot.
 */
void
hash_strategy_output_key_single_emit(GroupingPolicyHash *policy, uint32 current_key,
									 TupleTableSlot *aggregated_slot)
{
	HashingStrategy *hashing = &policy->hashing;
	Assert(policy->num_grouping_columns == 1);

	const GroupingColumn *col = &policy->grouping_columns[0];
	aggregated_slot->tts_values[col->output_offset] = hashing->output_keys[current_key];
	aggregated_slot->tts_isnull[col->output_offset] = current_key == hashing->null_key_index;
}
