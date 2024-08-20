/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifdef GENERATE_DISPATCH_TABLE
AVG_CASE(PG_TYPE)
return &FUNCTION_NAME(argdef);
#else
static void
FUNCTION_NAME(const)(void *agg_state, Datum constvalue, bool constisnull, int n)
{
	if (constisnull)
	{
		return;
	}

	FloatAvgState *state = (FloatAvgState *) agg_state;
	const double newN = n;
	const double newSx = n * DATUM_TO_CTYPE(constvalue);
	/* Sxx = sum((X - sum(X) / N)^2) = 0 for equal values. */

	/*
	 * See float8_combine() for the formula for combining two Youngs-Cramer
	 * states.
	 */
	if (state->N == 0.0)
	{
		state->N = newN;
		state->Sx = newSx;
		state->Sxx = 0;
		return;
	}

	Assert(newN != 0);

	const double combinedN = state->N + newN;
	const double combinedSx = state->Sx + newSx;
	const double tmp = state->Sx / state->N - newSx / newN;
	const double combinedSxx = state->Sxx + state->N * newN * tmp * tmp / combinedN;

	state->N = combinedN;
	state->Sx = combinedSx;
	state->Sxx = combinedSxx;
}

static void
FUNCTION_NAME(vector)(void *agg_state, ArrowArray *vector, uint64 *filter)
{
	FloatAvgState *state = (FloatAvgState *) agg_state;
	const int rows = vector->length;
	const uint64 *validity = vector->buffers[0];
	const CTYPE *values = vector->buffers[1];

	double N = state->N;
	double Sx = state->Sx;
	double Sxx = state->Sxx;
	for (int i = 0; i < rows; i++)
	{
		const CTYPE newval = values[i];
		const bool passes = arrow_row_is_valid(filter, i);
		const bool isvalid = arrow_row_is_valid(validity, i);

		if (!passes || !isvalid)
		{
			continue;
		}

		/*
		 * This code follows float8_accum(), see the comments there.
		 */
		N += 1.0;
		Sx += newval;
		if (likely(N > 1.0))
		{
			double tmp = newval * N - Sx;
			Sxx += tmp * tmp / (N * (N - 1));
		}

		/*
		 * Sxx should be NaN if any of the inputs are infinite or NaN.
		 */
		Sxx += 0.0 * newval;
	}

	state->N = N;
	state->Sx = Sx;
	state->Sxx = Sxx;
}

static VectorAggFunctions FUNCTION_NAME(argdef) = { .state_bytes = sizeof(FloatAvgState),
													.agg_init = avg_float_init,
													.agg_emit = avg_float_emit,
													.agg_const = FUNCTION_NAME(const),
													.agg_vector = FUNCTION_NAME(vector) };
#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM
