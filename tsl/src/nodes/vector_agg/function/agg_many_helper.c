/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * A generic implementation of adding the given batch to many aggregate function
 * states with given offsets. Used for hash aggregation, and builds on the
 * FUNCTION_NAME(one) function, which adds one passing non-null row to the given
 * aggregate function state.
 */
static void
FUNCTION_NAME(many)(void *restrict agg_states, uint32 *restrict offsets, int start_row, int end_row,
					const ArrowArray *vector, MemoryContext agg_extra_mctx)
{
	MemoryContext old = MemoryContextSwitchTo(agg_extra_mctx);
	const CTYPE *values = vector->buffers[1];
	const uint64 *valid = vector->buffers[0];
	for (int row = start_row; row < end_row; row++)
	{
		FUNCTION_NAME(state) *state = (offsets[row] + (FUNCTION_NAME(state) *) agg_states);
		const CTYPE value = values[row];
		const bool row_passes = (offsets[row] != 0);
		const bool value_notnull = arrow_row_is_valid(valid, row);

		if (row_passes && value_notnull)
		{
			FUNCTION_NAME(one)(state, value);
		}
	}
	MemoryContextSwitchTo(old);
}
