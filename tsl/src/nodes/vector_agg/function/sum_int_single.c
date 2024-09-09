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
	IntSumState *state = (IntSumState *) agg_state;

	/*
	 * We accumulate the sum as int64, so we can sum INT_MAX = 2^31 - 1
	 * at least 2^31 times without incurring an overflow of the int64
	 * accumulator. The same is true for negative numbers. The
	 * compressed batch size is currently capped at 1000 rows, but even
	 * if it's changed in the future, it's unlikely that we support
	 * batches larger than 65536 rows, not to mention 2^31. Therefore,
	 * we don't need to check for overflows within the loop, which would
	 * slow down the calculation.
	 */
	Assert(n <= INT_MAX);

	/*
	 * Note that we use a simplest loop here, there are many possibilities of
	 * optimizing this function (for example, this loop is not unrolled by
	 * clang-16).
	 */
	int64 batch_sum = 0;
	bool have_result = false;
	for (int row = 0; row < n; row++)
	{
		const bool row_ok = arrow_both_valid(valid1, valid2, row);
		batch_sum += values[row] * row_ok;
		have_result |= row_ok;
	}

	if (unlikely(pg_add_s64_overflow(state->result, batch_sum, &state->result)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}

	state->isnull &= !have_result;
}

#include "agg_vector_validity_helper.c"

#include "agg_const_helper.c"

static VectorAggFunctions FUNCTION_NAME(argdef) = {
	.state_bytes = sizeof(IntSumState),
	.agg_init = int_sum_init,
	.agg_emit = int_sum_emit,
	.agg_const = FUNCTION_NAME(const),
	.agg_vector = FUNCTION_NAME(vector),
};
#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM
