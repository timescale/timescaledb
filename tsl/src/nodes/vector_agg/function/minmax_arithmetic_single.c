/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifdef GENERATE_DISPATCH_TABLE
extern VectorAggFunctions FUNCTION_NAME(argdef);
case PG_AGG_OID_HELPER(AGG_NAME, PG_TYPE):
	return &FUNCTION_NAME(argdef);
#else
static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl)(void *agg_state, int n, const CTYPE *values, const uint64 *filter,
						   MemoryContext agg_extra_mctx)
{
	MinMaxState *state = (MinMaxState *) agg_state;

	CTYPE outer_result = state->isvalid ? DATUM_TO_CTYPE(state->value) : 0;
	bool outer_isvalid = state->isvalid;
	for (int row = 0; row < n; row++)
	{
		const CTYPE new_value = values[row];
		const bool new_value_ok = arrow_row_is_valid(filter, row);

		/*
		 * Note that we have to properly handle NaNs and Infinities for floats.
		 */
		const bool do_replace =
			new_value_ok && (unlikely(!outer_isvalid) || PREDICATE(outer_result, new_value) ||
							 isnan((double) new_value));

		outer_result = do_replace ? new_value : outer_result;
		outer_isvalid = outer_isvalid || do_replace;
	}

	state->isvalid = outer_isvalid;

	/* Note that float8 Datum is by-reference on 32-bit systems. */
	MemoryContext old = MemoryContextSwitchTo(agg_extra_mctx);
	state->value = CTYPE_TO_DATUM(outer_result);
	MemoryContextSwitchTo(old);
}

typedef MinMaxState FUNCTION_NAME(state);

static pg_attribute_always_inline void
FUNCTION_NAME(one)(void *restrict agg_state, const CTYPE value)
{
	FUNCTION_NAME(state) *state = (FUNCTION_NAME(state) *) agg_state;
	if (!state->isvalid || PREDICATE(DATUM_TO_CTYPE(state->value), value) || isnan((double) value))
	{
		/*
		 * Note that float8 Datum is by-reference on 32-bit systems, and this
		 * function is called in the extra aggregate data memory context.
		 */
		state->value = CTYPE_TO_DATUM(value);
		state->isvalid = true;
	}
}

#include "agg_many_vector_helper.c"
#include "agg_scalar_helper.c"
#include "agg_vector_validity_helper.c"

VectorAggFunctions FUNCTION_NAME(argdef) = {
	.state_bytes = sizeof(MinMaxState),
	.agg_init = minmax_init,
	.agg_emit = minmax_emit,
	.agg_scalar = FUNCTION_NAME(scalar),
	.agg_vector = FUNCTION_NAME(vector),
	.agg_many_vector = FUNCTION_NAME(many_vector),
};
#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM
