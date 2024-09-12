/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Vectorized implementation of a float{4,8}_accum() transition function for a
 * single type. They use the same Youngs-Cramer state, but for AVG we can skip
 * calculating the Sxx variable.
 */

#ifdef GENERATE_DISPATCH_TABLE
/*
 * Forward declaration for the vectorized aggregate function definition.
 */
extern VectorAggFunctions FUNCTION_NAME(argdef);

/*
 * Helper macros to generate the cases for the given argument type. We support
 * different aggregate functions based on whether we calculate the Sxx variable.
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

/*
 * The actual case label.
 */
ACCUM_CASE(PG_TYPE)
return &FUNCTION_NAME(argdef);
#else

/*
 * State of Youngs-Cramer algorithm, see the comments for float8_accum().
 */
typedef struct
{
	double N;
	double Sx;
#ifdef NEED_SXX
	double Sxx;
#endif
} FUNCTION_NAME(state);

static void
FUNCTION_NAME(init)(void *agg_state)
{
	FUNCTION_NAME(state) *state = (FUNCTION_NAME(state) *) agg_state;
	*state = (FUNCTION_NAME(state)){ 0 };
}

static void
FUNCTION_NAME(emit)(void *agg_state, Datum *out_result, bool *out_isnull)
{
	FUNCTION_NAME(state) *state = (FUNCTION_NAME(state) *) agg_state;

	Datum transdatums[3] = {
		Float8GetDatumFast(state->N),
		Float8GetDatumFast(state->Sx),
		/*
		 * Sxx should be NaN if any of the inputs are infinite or NaN. This is
		 * checked by float8_combine even if it's not used for the actual
		 * calculations.
		 */
		Float8GetDatumFast(0. * state->Sx
#ifdef NEED_SXX
						   + state->Sxx
#endif
						   ),
	};

	ArrayType *result = construct_array(transdatums,
										3,
										FLOAT8OID,
										sizeof(float8),
										FLOAT8PASSBYVAL,
										TYPALIGN_DOUBLE);

	*out_result = PointerGetDatum(result);
	*out_isnull = false;
}

/*
 * Youngs-Cramer update for rows after the first.
 */
static pg_attribute_always_inline void
FUNCTION_NAME(update)(const uint64 *valid1, const uint64 *valid2, const CTYPE *values, int row,
					  double *N, double *Sx
#ifdef NEED_SXX
					  ,
					  double *Sxx
#endif
)
{
	const CTYPE newval = values[row];
	if (!arrow_row_both_valid(valid1, valid2, row))
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
}

/*
 * Combine two Youngs-Cramer states following the float8_combine() function.
 */
static pg_attribute_always_inline void
FUNCTION_NAME(combine)(double *inout_N, double *inout_Sx,
#ifdef NEED_SXX
					   double *inout_Sxx,
#endif
					   double N2, double Sx2
#ifdef NEED_SXX
					   ,
					   double Sxx2
#endif
)
{
	const double N1 = *inout_N;
	const double Sx1 = *inout_Sx;
#ifdef NEED_SXX
	const double Sxx1 = *inout_Sxx;
#endif

	if (unlikely(N1 == 0))
	{
		*inout_N = N2;
		*inout_Sx = Sx2;
#ifdef NEED_SXX
		*inout_Sxx = Sxx2;
#endif
		return;
	}

	if (unlikely(N2 == 0))
	{
		*inout_N = N1;
		*inout_Sx = Sx1;
#ifdef NEED_SXX
		*inout_Sxx = Sxx1;
#endif
		return;
	}

	const double combinedN = N1 + N2;
	const double combinedSx = Sx1 + Sx2;
#ifdef NEED_SXX
	const double tmp = Sx1 / N1 - Sx2 / N2;
	const double combinedSxx = Sxx1 + Sxx2 + N1 * N2 * tmp * tmp / combinedN;
#endif

	*inout_N = combinedN;
	*inout_Sx = combinedSx;
#ifdef NEED_SXX
	*inout_Sxx = combinedSxx;
#endif
}

#ifdef NEED_SXX
#define UPDATE(valid1, valid2, values, row, N, Sx, Sxx)                                            \
	FUNCTION_NAME(update)(valid1, valid2, values, row, N, Sx, Sxx)
#define COMBINE(inout_N, inout_Sx, inout_Sxx, N2, Sx2, Sxx2)                                       \
	FUNCTION_NAME(combine)(inout_N, inout_Sx, inout_Sxx, N2, Sx2, Sxx2)
#else
#define UPDATE(valid1, valid2, values, row, N, Sx, Sxx)                                            \
	FUNCTION_NAME(update)(valid1, valid2, values, row, N, Sx)
#define COMBINE(inout_N, inout_Sx, inout_Sxx, N2, Sx2, Sxx2)                                       \
	FUNCTION_NAME(combine)(inout_N, inout_Sx, N2, Sx2)
#endif

static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl)(void *agg_state, int n, const CTYPE *values, const uint64 *valid1,
						   const uint64 *valid2, MemoryContext agg_extra_mctx)
{
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
#ifdef NEED_SXX
	double Sxxarray[UNROLL_SIZE] = { 0 };
#endif

	int row = 0;

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
			if (arrow_row_both_valid(valid1, valid2, row))
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
		UPDATE(valid1, valid2, values, row, &Narray[inner], &Sxarray[inner], &Sxxarray[inner]);
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
			UPDATE(valid1,
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
		UPDATE(valid1, valid2, values, row, &Narray[inner], &Sxarray[inner], &Sxxarray[inner]);
	}

	/*
	 * Merge all intermediate states into the first one.
	 */
	for (int i = 1; i < UNROLL_SIZE; i++)
	{
		COMBINE(&Narray[0], &Sxarray[0], &Sxxarray[0], Narray[i], Sxarray[i], Sxxarray[i]);
	}
#undef UNROLL_SIZE

	/*
	 * Merge the total computed state into the aggregate function state.
	 */
	FUNCTION_NAME(state) *state = (FUNCTION_NAME(state) *) agg_state;
	COMBINE(&state->N, &state->Sx, &state->Sxx, Narray[0], Sxarray[0], Sxxarray[0]);
}

#include "agg_const_helper.c"
#include "agg_vector_validity_helper.c"

VectorAggFunctions FUNCTION_NAME(argdef) = { .state_bytes = sizeof(FUNCTION_NAME(state)),
											 .agg_init = FUNCTION_NAME(init),
											 .agg_emit = FUNCTION_NAME(emit),
											 .agg_const = FUNCTION_NAME(const),
											 .agg_vector = FUNCTION_NAME(vector) };
#undef UPDATE
#undef COMBINE

#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM

#undef ACCUM_CASE
#undef ACCUM_CASE_HELPER
