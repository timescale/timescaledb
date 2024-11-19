/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

static void
FUNCTION_NAME(alloc_output_keys)(GroupingPolicyHash *policy, DecompressBatchState *batch_state)
{
	/*
	 * Allocate enough storage for keys, given that each row of the new
	 * compressed batch might turn out to be a new grouping key.
	 * We do this separately to avoid allocations in the hot loop that fills the hash
	 * table.
	 */
	HashingStrategy *hashing = &policy->hashing;
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
