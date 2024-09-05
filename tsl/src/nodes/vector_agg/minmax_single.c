/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifdef GENERATE_DISPATCH_TABLE
case PG_AGG_OID_HELPER(AGG_NAME, PG_TYPE):
	return &FUNCTION_NAME(argdef);
#else
static void
FUNCTION_NAME(const)(void *agg_state, Datum constvalue, bool constisnull, int n)
{
	MinMaxState *state = (MinMaxState *) agg_state;
	if (constisnull)
	{
		return;
	}

	if (!state->isvalid)
	{
		state->isvalid = true;
		state->value = constvalue;
		return;
	}

	const CTYPE new = DATUM_TO_CTYPE(constvalue);
	const CTYPE current = DATUM_TO_CTYPE(state->value);

	/*
	 * Note that we have to properly handle NaNs and Infinities for floats.
	 */
	const bool do_replace = PREDICATE(current, new) || isnan((double) new);
	state->value = CTYPE_TO_DATUM(do_replace ? new : current);
}

static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl)(void *agg_state, const ArrowArray *vector, const uint64 *valid1,
						   const uint64 *valid2)
{
	MinMaxState *state = (MinMaxState *) agg_state;
	const int n = vector->length;
	const CTYPE *values = (CTYPE *) vector->buffers[1];

	CTYPE outer_result = DATUM_TO_CTYPE(state->value);
	bool outer_isvalid = state->isvalid;
	for (int row = 0; row < n; row++)
	{
		const CTYPE new_value = values[row];
		const bool new_value_ok = arrow_both_valid(valid1, valid2, row);

		/*
		 * Note that we have to properly handle NaNs and Infinities for floats.
		 */
		const bool do_replace =
			new_value_ok && (unlikely(!outer_isvalid) || PREDICATE(outer_result, new_value) ||
							 isnan((double) new_value));

		outer_result = do_replace ? new_value : outer_result;
		outer_isvalid |= do_replace;
	}

	state->value = CTYPE_TO_DATUM(outer_result);
	state->isvalid = outer_isvalid;
}

#include "agg_vector_validity_helper.c"

static VectorAggFunctions FUNCTION_NAME(argdef) = { .state_bytes = sizeof(MinMaxState),
													.agg_init = minmax_init,
													.agg_emit = minmax_emit,
													.agg_const = FUNCTION_NAME(const),
													.agg_vector = FUNCTION_NAME(vector) };
#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM
