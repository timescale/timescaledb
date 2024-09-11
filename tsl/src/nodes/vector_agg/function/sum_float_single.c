/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifdef GENERATE_DISPATCH_TABLE
extern VectorAggFunctions FUNCTION_NAME(argdef);
case PG_AGG_OID_HELPER(AGG_NAME, PG_TYPE):
	return &FUNCTION_NAME(argdef);
#else

StaticAssertDecl(sizeof(CTYPE) == sizeof(MASKTYPE), "CTYPE and MASKTYPE must be the same size");

static void
FUNCTION_NAME(emit)(void *agg_state, Datum *out_result, bool *out_isnull)
{
	FloatSumState *state = (FloatSumState *) agg_state;
	*out_result = CTYPE_TO_DATUM((CTYPE) state->result);
	*out_isnull = state->isnull;
}

static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl)(void *agg_state, int n, const CTYPE *values, const uint64 *valid1,
						   const uint64 *valid2, MemoryContext agg_extra_mctx)
{
	/*
	 * Vector registers can be up to 512 bits wide.
	 */
#define UNROLL_SIZE ((int) (512 / 8 / sizeof(CTYPE)))

	bool have_result_accu[UNROLL_SIZE] = { 0 };
	double sum_accu[UNROLL_SIZE] = { 0 };
	for (int outer = 0; outer < UNROLL_SIZE * (n / UNROLL_SIZE); outer += UNROLL_SIZE)
	{
		for (int inner = 0; inner < UNROLL_SIZE; inner++)
		{
			const int row = outer + inner;
			double *dest = &sum_accu[inner];
			bool *have_result = &have_result_accu[inner];
			/*
			 * We're using a trick with bitmasking the numbers that don't
			 * pass the filter, to allow for branchless code generation. This is
			 * analogous to integer version where we just multiply the integers
			 * by bool, but for floats we can't use multiplication because of
			 * infinities and NaNs.
			 */
#define INNER_LOOP                                                                                 \
	const bool valid = arrow_both_valid(valid1, valid2, row);                                      \
	union                                                                                          \
	{                                                                                              \
		CTYPE f;                                                                                   \
		MASKTYPE m;                                                                                \
	} u = { .f = values[row] };                                                                    \
	u.m &= valid ? ~(MASKTYPE) 0 : (MASKTYPE) 0;                                                   \
	*dest += u.f;                                                                                  \
	*have_result |= valid;

			INNER_LOOP
		}
	}

	for (int row = UNROLL_SIZE * (n / UNROLL_SIZE); row < n; row++)
	{
		double *dest = &sum_accu[0];
		bool *have_result = &have_result_accu[0];
		INNER_LOOP
	}

	for (int i = 1; i < UNROLL_SIZE; i++)
	{
		sum_accu[0] += sum_accu[i];
		have_result_accu[0] |= have_result_accu[i];
	}
#undef UNROLL_SIZE
#undef INNER_LOOP

	FloatSumState *state = (FloatSumState *) agg_state;
	state->isnull &= !have_result_accu[0];
	state->result += sum_accu[0];
}

#include "agg_const_helper.c"
#include "agg_vector_validity_helper.c"

VectorAggFunctions FUNCTION_NAME(argdef) = {
	.state_bytes = sizeof(FloatSumState),
	.agg_init = float_sum_init,
	.agg_emit = FUNCTION_NAME(emit),
	.agg_const = FUNCTION_NAME(const),
	.agg_vector = FUNCTION_NAME(vector),
};

#endif

#undef PG_TYPE
#undef CTYPE
#undef MASKTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM
