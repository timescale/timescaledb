/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Functions handled by *accum() aggregate functions states, implementation
 * for a single type. They use the same Youngs-Cramer state, but for AVG we can
 * skip calculating the Sxx variable.
 */

#ifdef NEED_SXX
#define ACCUM_CASE_HELPER(PG_TYPE)                                                                 \
	case F_STDDEV_##PG_TYPE:                                                                       \
	case F_STDDEV_SAMP_##PG_TYPE:                                                                  \
	case F_STDDEV_POP_##PG_TYPE:                                                                   \
	case F_VARIANCE_##PG_TYPE:                                                                     \
	case F_VAR_SAMP_##PG_TYPE:                                                                     \
	case F_VAR_POP_##PG_TYPE:
#else
#define ACCUM_CASE_HELPER(PG_TYPE) case F_AVG_##PG_TYPE:
#endif

#define ACCUM_CASE(PG_TYPE) ACCUM_CASE_HELPER(PG_TYPE)

#ifdef GENERATE_DISPATCH_TABLE
ACCUM_CASE(PG_TYPE)
return &FUNCTION_NAME(argdef);
#else
static void
FUNCTION_NAME(const)(void *agg_state, Datum constvalue, bool constisnull, int n)
{
	if (constisnull)
	{
		return;
	}
	Assert(n != 0);

	FloatAvgState *state = (FloatAvgState *) agg_state;
	const double newN = n;
	const double newSx = n * DATUM_TO_CTYPE(constvalue);

	/*
	 * Sxx = sum((X - sum(X) / N)^2) = 0 for equal values. Note that it should
	 * be NaN if any of the inputs are infinite or NaN. This is checked by
	 * float8_combine() even if it's not used for the actual calculations (e.g.
	 * for avg()).
	 */
	const double newSxx = 0 * DATUM_TO_CTYPE(constvalue);

	youngs_cramer_combine(&state->N, &state->Sx, &state->Sxx, newN, newSx, newSxx);
}

/*
 * Youngs-Cramer update for rows after the first.
 */
static pg_attribute_always_inline void
FUNCTION_NAME(inner_update)(const uint64 *filter, const uint64 *validity, const CTYPE *values,
							int row, double *N, double *Sx, double *Sxx)
{
	const CTYPE newval = values[row];
	const bool passes = arrow_row_is_valid(filter, row);
	const bool isvalid = arrow_row_is_valid(validity, row);
	if (!passes || !isvalid)
	{
		return;
	}

	/*
	 * This code follows float8_accum(), see the comments there.
	 */
#ifdef NEED_SXX
	if (*N > 0.0)
	{
		const double tmp = newval * (*N + 1.0) - (*Sx + newval);
		*Sxx += tmp * tmp / (*N * (*N + 1.0));
	}
#endif

	*N = *N + 1.0;
	*Sx = *Sx + newval;

	/*
	 * Sxx should be NaN if any of the inputs are infinite or NaN. This is
	 * checked by float8_combine even if it's not used for the actual
	 * calculations.
	 */
	*Sxx += 0.0 * newval;
}

static void
FUNCTION_NAME(vector)(void *agg_state, ArrowArray *vector, uint64 *filter)
{
	FloatAvgState *state = (FloatAvgState *) agg_state;
	const int rows = vector->length;
	const uint64 *validity = vector->buffers[0];
	const CTYPE *values = vector->buffers[1];

	/*
	 * Vector registers can be up to 512 bits wide.
	 */
#define UNROLL_SIZE ((int) (512 / 8 / sizeof(CTYPE)))

	double Narray[UNROLL_SIZE] = { 0 };
	double Sxarray[UNROLL_SIZE] = { 0 };
	double Sxxarray[UNROLL_SIZE] = { 0 };

	int row = 0;

	/*
	 * Unrolled loop.
	 */
	Assert(row % UNROLL_SIZE == 0 || row == rows);
	for (; row < UNROLL_SIZE * (rows / UNROLL_SIZE); row += UNROLL_SIZE)
	{
		for (int inner = 0; inner < UNROLL_SIZE; inner++)
		{
			FUNCTION_NAME(inner_update)
			(filter,
			 validity,
			 values,
			 row + inner,
			 &Narray[inner],
			 &Sxarray[inner],
			 &Sxxarray[inner]);
		}
	}

	/*
	 * Process the odd tail.
	 */
	for (; row < rows; row++)
	{
		FUNCTION_NAME(inner_update)
		(filter, validity, values, row, &Narray[0], &Sxarray[0], &Sxxarray[0]);
	}

	/*
	 * Merge all intermediate states into the first one.
	 */
	for (int i = 1; i < UNROLL_SIZE; i++)
	{
		youngs_cramer_combine(&Narray[0],
							  &Sxarray[0],
							  &Sxxarray[0],
							  Narray[i],
							  Sxarray[i],
							  Sxxarray[i]);
	}
#undef UNROLL_SIZE

	/*
	 * Merge the total computed state into the aggregate function state.
	 */
	youngs_cramer_combine(&state->N, &state->Sx, &state->Sxx, Narray[0], Sxarray[0], Sxxarray[0]);
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

#undef ACCUM_CASE
#undef ACCUM_CASE_HELPER
