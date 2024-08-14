#define PG_AGG_OID_HELPER(X, Y) PG_AGG_OID(X, Y)
#define FUNCTION_NAME_HELPER(X, Y, Z) X##_##Y##_##Z
#define FUNCTION_NAME(X, Y, Z) FUNCTION_NAME_HELPER(X, Y, Z)

#ifdef GENERATE_DISPATCH_TABLE
case PG_AGG_OID_HELPER(AGG_NAME, PG_TYPE):
	return &FUNCTION_NAME(AGG_NAME, PG_TYPE, argdef);
#else
static void
FUNCTION_NAME(AGG_NAME, PG_TYPE, const)(void *agg_state, Datum constvalue, bool constisnull, int n)
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
	}

	const CTYPE current = DATUM_TO_CTYPE(state->value);
	const CTYPE new = DATUM_TO_CTYPE(constvalue);
	if (PREDICATE(current, new))
	{
		state->value = CTYPE_TO_DATUM(new);
	}
}

static void
FUNCTION_NAME(AGG_NAME, PG_TYPE, vector)(void *agg_state, ArrowArray *vector, uint64 *filter)
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

		if (!state->isvalid)
		{
			state->isvalid = true;
			result = values[i];
		}

		if (PREDICATE(result, values[i]))
		{
			result = values[i];
		}
	}
	state->value = CTYPE_TO_DATUM(result);
}

static VectorAggFunctions FUNCTION_NAME(AGG_NAME, PG_TYPE, argdef) = {
	.state_bytes = sizeof(MinMaxState),
	.agg_init = minmax_init,
	.agg_emit = minmax_emit,
	.agg_const = FUNCTION_NAME(AGG_NAME, PG_TYPE, const),
	.agg_vector = FUNCTION_NAME(AGG_NAME, PG_TYPE, vector)
};
#endif

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
#undef PG_AGG_OID_HELPER

#undef PG_AGG_OID
#undef CTYPE
#undef PG_TYPE
#undef AGG_NAME
