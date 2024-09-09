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

	bool have_result = false;
	double accu[UNROLL_SIZE] = { 0 };
	for (int outer = 0; outer < UNROLL_SIZE * (n / UNROLL_SIZE); outer += UNROLL_SIZE)
	{
		for (int inner = 0; inner < UNROLL_SIZE; inner++)
		{
			const int row = outer + inner;
			double *dest = &accu[inner];
#define INNER_LOOP                                                                                 \
	const CTYPE value = values[row];                                                               \
	if (!arrow_both_valid(valid1, valid2, row))                                                    \
	{                                                                                              \
		continue;                                                                                  \
	}                                                                                              \
                                                                                                   \
	*dest += value;                                                                                \
	have_result = true;

			INNER_LOOP
		}
	}

	for (int row = UNROLL_SIZE * (n / UNROLL_SIZE); row < n; row++)
	{
		double *dest = &accu[0];
		INNER_LOOP
	}

	for (int i = 1; i < UNROLL_SIZE; i++)
	{
		accu[0] += accu[i];
	}
#undef UNROLL_SIZE
#undef INNER_LOOP

	FloatSumState *state = (FloatSumState *) agg_state;
	state->isnull &= !have_result;
	state->result += accu[0];
}

#include "agg_const_helper.c"
#include "agg_vector_validity_helper.c"

static VectorAggFunctions FUNCTION_NAME(argdef) = {
	.state_bytes = sizeof(FloatSumState),
	.agg_init = float_sum_init,
	.agg_emit = FUNCTION_NAME(emit),
	.agg_const = FUNCTION_NAME(const),
	.agg_vector = FUNCTION_NAME(vector),
};

#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM
