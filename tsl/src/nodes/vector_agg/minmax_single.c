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

	const CTYPE new = DATUM_TO_CTYPE(constvalue);
	if (!isfinite((double) new))
	{
		/* Skip NaNs. */
		return;
	}

	if (!state->isvalid)
	{
		state->isvalid = true;
		state->value = constvalue;
	}

	const CTYPE current = DATUM_TO_CTYPE(state->value);
	if (!(PREDICATE(current, new)))
	{
		state->value = CTYPE_TO_DATUM(new);
	}
}

static void
FUNCTION_NAME(vector)(void *agg_state, ArrowArray *vector, uint64 *filter)
{
	MinMaxState *state = (MinMaxState *) agg_state;
	const int n = vector->length;
	const CTYPE *restrict values = (CTYPE *) vector->buffers[1];
	CTYPE result = DATUM_TO_CTYPE(state->value);
	for (int i = 0; i < n; i++)
	{
		if (!arrow_row_is_valid(vector->buffers[0], i))
		{
			continue;
		}

		if (!isfinite((double) values[i]))
		{
			/* Skip NaNs. */
			continue;
		}

		if (!state->isvalid)
		{
			state->isvalid = true;
			result = values[i];
		}

		if (!(PREDICATE(result, values[i])))
		{
			result = values[i];
		}
	}
	state->value = CTYPE_TO_DATUM(result);
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
