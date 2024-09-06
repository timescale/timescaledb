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
/*
 * Youngs-Cramer update for rows after the first.
 */
static pg_attribute_always_inline void
FUNCTION_NAME(update)(const uint64 *valid1, const uint64 *valid2, const CTYPE *values, int row,
					  double *N, double *Sx, double *Sxx)
{
	const CTYPE newval = values[row];
	if (!arrow_both_valid(valid1, valid2, row))
	{
		return;
	}

	/*
	 * This code follows float8_accum(), see the comments there.
	 */
#ifdef NEED_SXX
	Assert(*N > 0.0);
	const double tmp = newval * (*N + 1.0) - (*Sx + newval);
	*Sxx += tmp * tmp / (*N * (*N + 1.0));
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

static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl)(void *agg_state, int n, const CTYPE *values, const uint64 *valid1,
						   const uint64 *valid2)
{
	FloatAvgState *state = (FloatAvgState *) agg_state;

	int row = 0;

	/*
	 * Vector registers can be up to 512 bits wide.
	 */
#define UNROLL_SIZE ((int) (512 / 8 / sizeof(CTYPE)))

	/*
	 * Each inner iteration works with its own accumulators to avoid data
	 * dependencies.
	 */
	double Narray[UNROLL_SIZE] = { 0 };
	double Sxarray[UNROLL_SIZE] = { 0 };
	double Sxxarray[UNROLL_SIZE] = { 0 };

#ifdef NEED_SXX
	/*
	 * Initialize each state with the first matching row. We do this separately
	 * to make the actual update function branchless, namely the computation of
	 * Sxx which works differently for the first row.
	 */
	for (int inner = 0; inner < UNROLL_SIZE; inner++)
	{
		for (; row < n; row++)
		{
			const CTYPE newval = values[row];
			if (arrow_both_valid(valid1, valid2, row))
			{
				Narray[inner] = 1;
				Sxarray[inner] = newval;
				Sxxarray[inner] = 0 * newval;
				row++;
				break;
			}
		}
	}

	/*
	 * Scroll to the row that is a multiple of UNROLL_SIZE. This is the correct
	 * row at which to enter the unrolled loop below.
	 */
	for (int inner = row % UNROLL_SIZE; inner > 0 && inner < UNROLL_SIZE && row < n; inner++, row++)
	{
		FUNCTION_NAME(update)
		(valid1, valid2, values, row, &Narray[inner], &Sxarray[inner], &Sxxarray[inner]);
	}
#endif

	/*
	 * Unrolled loop.
	 */
	Assert(row % UNROLL_SIZE == 0 || row == n);
	for (; row < UNROLL_SIZE * (n / UNROLL_SIZE); row += UNROLL_SIZE)
	{
		for (int inner = 0; inner < UNROLL_SIZE; inner++)
		{
			FUNCTION_NAME(update)
			(valid1,
			 valid2,
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
	for (; row < n; row++)
	{
		const int inner = row % UNROLL_SIZE;
		FUNCTION_NAME(update)
		(valid1, valid2, values, row, &Narray[inner], &Sxarray[inner], &Sxxarray[inner]);
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

#include "agg_const_helper.c"
#include "agg_vector_validity_helper.c"

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
