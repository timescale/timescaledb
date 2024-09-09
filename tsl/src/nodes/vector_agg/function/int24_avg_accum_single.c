/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifdef GENERATE_DISPATCH_TABLE
case PG_AGG_OID_HELPER(AGG_NAME, PG_TYPE):
	return &FUNCTION_NAME(argdef);
#else

static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl)(void *agg_state, int n, const CTYPE *values, const uint64 *valid1,
						   const uint64 *valid2, MemoryContext agg_extra_mctx)
{
	int64 batch_count = 0;
	int64 batch_sum = 0;
	for (int row = 0; row < n; row++)
	{
		const bool row_ok = arrow_both_valid(valid1, valid2, row);
		batch_count += row_ok;
		batch_sum += values[row] * row_ok;
	}

	Int24AvgAccumState *state = (Int24AvgAccumState *) agg_state;
	state->count += batch_count;
	state->sum += batch_sum;
}

#include "agg_const_helper.c"
#include "agg_vector_validity_helper.c"

static VectorAggFunctions FUNCTION_NAME(argdef) = {
	.state_bytes = sizeof(Int24AvgAccumState),
	.agg_init = int24_avg_accum_init,
	.agg_emit = int24_avg_accum_emit,
	.agg_const = FUNCTION_NAME(const),
	.agg_vector = FUNCTION_NAME(vector),
};

#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM
