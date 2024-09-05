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

static void
FUNCTION_NAME(vector)(void *agg_state, const ArrowArray *vector, const uint64 *filter)
{
	MinMaxState *state = (MinMaxState *) agg_state;
	const int n = vector->length;
	const CTYPE *restrict values = (CTYPE *) vector->buffers[1];
	CTYPE result = DATUM_TO_CTYPE(state->value);
	bool result_isvalid = state->isvalid;
	int i = 0;
	if (!result_isvalid)
	{
		for (; i < n; i++)
		{
			const CTYPE new_value = values[i];
			const bool new_isvalid = arrow_row_is_valid(vector->buffers[0], i);
			const bool passes = arrow_row_is_valid(filter, i);

			/*
			 * Note that we have to properly handle NaNs and Infinities for floats.
			 */
			if (passes && new_isvalid)
			{
				result_isvalid = true;
				result = new_value;
				break;
			}
		}
	}

	/* We either found a valid value or ran out of input. */
	Assert(result_isvalid || i == n);

	for (; i < n; i++)
	{
		const CTYPE new_value = values[i];
		const bool new_isvalid = arrow_row_is_valid(vector->buffers[0], i);
		const bool passes = arrow_row_is_valid(filter, i);

		/*
		 * Note that we have to properly handle NaNs and Infinities for floats.
		 */
		const bool do_replace =
			passes && new_isvalid && (PREDICATE(result, new_value) || isnan((double) new_value));

		result = do_replace ? new_value : result;
	}

	state->value = CTYPE_TO_DATUM(result);
	state->isvalid = result_isvalid;
}

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
