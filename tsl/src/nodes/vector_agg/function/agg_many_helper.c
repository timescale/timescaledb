/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

static void
FUNCTION_NAME(many)(void *restrict agg_states, int32 *restrict offsets, const ArrowArray *vector,
					MemoryContext agg_extra_mctx)
{
	MemoryContext old = MemoryContextSwitchTo(agg_extra_mctx);
	const int n = vector->length;
	const CTYPE *values = vector->buffers[1];
	const uint64 *valid = vector->buffers[0];
	for (int row = 0; row < n; row++)
	{
		if (offsets[row] == 0)
		{
			continue;
		}

		if (!arrow_row_is_valid(valid, row))
		{
			continue;
		}

		FUNCTION_NAME(state) *state = (offsets[row] + (FUNCTION_NAME(state) *) agg_states);
		FUNCTION_NAME(one)(state, values[row]);
	}
	MemoryContextSwitchTo(old);
}
