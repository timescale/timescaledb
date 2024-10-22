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
static pg_attribute_always_inline void
FUNCTION_NAME(many_vector_impl)(void *restrict agg_states, const uint32 *offsets,
								const uint64 *filter, int start_row, int end_row,
								const ArrowArray *vector, MemoryContext agg_extra_mctx)
{
	FUNCTION_NAME(state) *restrict states = (FUNCTION_NAME(state) *) agg_states;
	const CTYPE *values = vector->buffers[1];
	MemoryContext old = MemoryContextSwitchTo(agg_extra_mctx);
	for (int row = start_row; row < end_row; row++)
	{
		const CTYPE value = values[row];
		FUNCTION_NAME(state) *restrict state = &states[offsets[row]];
		if (arrow_row_is_valid(filter, row))
		{
			Assert(offsets[row] != 0);
			FUNCTION_NAME(one)(state, value);
		}
	}
	MemoryContextSwitchTo(old);
}

static void
FUNCTION_NAME(many_vector)(void *restrict agg_states, const uint32 *offsets, const uint64 *filter,
						   int start_row, int end_row, const ArrowArray *vector,
						   MemoryContext agg_extra_mctx)
{
	if (filter == NULL)
	{
		FUNCTION_NAME(many_vector_impl)
		(agg_states, offsets, NULL, start_row, end_row, vector, agg_extra_mctx);
	}
	else
	{
		FUNCTION_NAME(many_vector_impl)
		(agg_states, offsets, filter, start_row, end_row, vector, agg_extra_mctx);
	}
}
