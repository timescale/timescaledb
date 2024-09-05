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

	CTYPE batch_result = { 0 };
	bool batch_isvalid = false;
	for (int i = 0; i < n; i++)
	{
		const CTYPE new_value = values[i];
		const bool new_isvalid = arrow_row_is_valid(vector->buffers[0], i);
		const bool passes = arrow_row_is_valid(filter, i);

		/*
		 * Note that we have to properly handle NaNs and Infinities for floats.
		 */
		const bool do_replace =
			passes && new_isvalid &&
			(!batch_isvalid || PREDICATE(batch_result, new_value) || isnan((double) new_value));

		batch_result = do_replace ? new_value : batch_result;
		batch_isvalid |= do_replace;
	}

	if (batch_isvalid &&
		(!state->isvalid || PREDICATE(DATUM_TO_CTYPE(state->value), batch_result) ||
		 isnan((double) batch_result)))
	{
		state->value = CTYPE_TO_DATUM(batch_result);
		state->isvalid = true;
	}
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
