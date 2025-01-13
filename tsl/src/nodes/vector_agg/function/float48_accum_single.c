/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Vectorized implementation of a Postgres float{4,8}_accum() transition
 * function for a single type. They use the same Youngs-Cramer state, but for
 * AVG we can skip calculating the Sxx variable.
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
 * State of Youngs-Cramer algorithm, see the comments for float8_accum()
 * Postgres function.
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
FUNCTION_NAME(init)(void *restrict agg_states, int n)
{
	FUNCTION_NAME(state) *states = (FUNCTION_NAME(state) *) agg_states;
	for (int i = 0; i < n; i++)
	{
		states[i] = (FUNCTION_NAME(state)){ 0 };
	}
}

static void
FUNCTION_NAME(emit)(void *agg_state, Datum *out_result, bool *out_isnull)
{
	FUNCTION_NAME(state) *state = (FUNCTION_NAME(state) *) agg_state;

	const size_t nbytes = 3 * sizeof(float8) + ARR_OVERHEAD_NONULLS(/* ndims = */ 1);
	ArrayType *result = palloc(nbytes);
	SET_VARSIZE(result, nbytes);
	result->ndim = 1;
	result->dataoffset = 0;
	result->elemtype = FLOAT8OID;
	ARR_DIMS(result)[0] = 3;
	ARR_LBOUND(result)[0] = 1;

	/*
	 * The array elements are stored by value, regardless of if the float8
	 * itself is by-value on this platform.
	 */
	((float8 *) ARR_DATA_PTR(result))[0] = state->N;
	((float8 *) ARR_DATA_PTR(result))[1] = state->Sx;
	((float8 *) ARR_DATA_PTR(result))[2] =
		/*
		 * Sxx should be NaN if any of the inputs are infinite or NaN. This is
		 * checked by float8_combine even if it's not used for the actual
		 * calculations.
		 */
		0. * state->Sx
#ifdef NEED_SXX
		+ state->Sxx
#endif
		;

	*out_result = PointerGetDatum(result);
	*out_isnull = false;
}

/*
 * Youngs-Cramer update for rows after the first.
 */
static pg_attribute_always_inline void
FUNCTION_NAME(update)(const uint64 *filter, const CTYPE *values, int row, double *N, double *Sx
#ifdef NEED_SXX
					  ,
					  double *Sxx
#endif
)
{
	const CTYPE newval = values[row];
	if (!arrow_row_is_valid(filter, row))
	{
		return;
	}

	/*
	 * This code follows the Postgres float8_accum() transition function, see
	 * the comments there.
	 */
	const double newN = *N + 1.0;
	const double newSx = *Sx + newval;
#ifdef NEED_SXX
	Assert(*N > 0.0);
	const double tmp = newval * newN - newSx;
	*Sxx += tmp * tmp / (*N * newN);
#endif

	*N = newN;
	*Sx = newSx;
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
#define UPDATE(filter, values, row, N, Sx, Sxx)                                                    \
	FUNCTION_NAME(update)(filter, values, row, N, Sx, Sxx)
#define COMBINE(inout_N, inout_Sx, inout_Sxx, N2, Sx2, Sxx2)                                       \
	FUNCTION_NAME(combine)(inout_N, inout_Sx, inout_Sxx, N2, Sx2, Sxx2)
#else
#define UPDATE(filter, values, row, N, Sx, Sxx) FUNCTION_NAME(update)(filter, values, row, N, Sx)
#define COMBINE(inout_N, inout_Sx, inout_Sxx, N2, Sx2, Sxx2)                                       \
	FUNCTION_NAME(combine)(inout_N, inout_Sx, N2, Sx2)
#endif

static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl)(void *agg_state, size_t n, const CTYPE *values, const uint64 *filter,
						   MemoryContext agg_extra_mctx)
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

	size_t row = 0;

#ifdef NEED_SXX
	/*
	 * Initialize each state with the first matching row. We do this separately
	 * to make the actual update function branchless, namely the computation of
	 * Sxx which works differently for the first row.
	 */
	for (size_t inner = 0; inner < UNROLL_SIZE; inner++)
	{
		for (; row < n; row++)
		{
			const CTYPE newval = values[row];
			if (arrow_row_is_valid(filter, row))
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
	for (size_t inner = row % UNROLL_SIZE; inner > 0 && inner < UNROLL_SIZE && row < n;
		 inner++, row++)
	{
		UPDATE(filter, values, row, &Narray[inner], &Sxarray[inner], &Sxxarray[inner]);
	}
#endif

	/*
	 * Unrolled loop.
	 */
	Assert(row % UNROLL_SIZE == 0 || row == n);
	for (; row < UNROLL_SIZE * (n / UNROLL_SIZE); row += UNROLL_SIZE)
	{
		for (size_t inner = 0; inner < UNROLL_SIZE; inner++)
		{
			UPDATE(filter, values, row + inner, &Narray[inner], &Sxarray[inner], &Sxxarray[inner]);
		}
	}

	/*
	 * Process the odd tail.
	 */
	for (; row < n; row++)
	{
		const size_t inner = row % UNROLL_SIZE;
		UPDATE(filter, values, row, &Narray[inner], &Sxarray[inner], &Sxxarray[inner]);
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

static pg_attribute_always_inline void
FUNCTION_NAME(one)(void *restrict agg_state, const CTYPE value)
{
	FUNCTION_NAME(state) *state = (FUNCTION_NAME(state) *) agg_state;
	/*
	 * This code follows the Postgres float8_accum() transition function, see
	 * the comments there.
	 */
	const double newN = state->N + 1.0;
	const double newSx = state->Sx + value;
#ifdef NEED_SXX
	if (state->N > 0.0)
	{
		const double tmp = value * newN - newSx;
		state->Sxx += tmp * tmp / (state->N * newN);
	}
	else
	{
		state->Sxx = 0 * value;
	}
#endif

	state->N = newN;
	state->Sx = newSx;
}

#include "agg_many_vector_helper.c"
#include "agg_scalar_helper.c"
#include "agg_vector_validity_helper.c"

VectorAggFunctions FUNCTION_NAME(argdef) = {
	.state_bytes = sizeof(FUNCTION_NAME(state)),
	.agg_init = FUNCTION_NAME(init),
	.agg_emit = FUNCTION_NAME(emit),
	.agg_scalar = FUNCTION_NAME(scalar),
	.agg_vector = FUNCTION_NAME(vector),
	.agg_many_vector = FUNCTION_NAME(many_vector),
};
#undef UPDATE
#undef COMBINE

#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM

#undef ACCUM_CASE
#undef ACCUM_CASE_HELPER
