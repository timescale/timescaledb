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
FUNCTION_NAME(vector_impl)(void *agg_state, const ArrowArray *vector, const uint64 *valid1,
						   const uint64 *valid2)
{
	IntSumState *state = (IntSumState *) agg_state;

	Assert(vector != NULL);
	Assert(vector->length > 0);

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
	Assert(vector->length <= INT_MAX);

	/*
	 * Note that we use a simplest loop here, there are many possibilities of
	 * optimizing this function (for example, this loop is not unrolled by
	 * clang-16).
	 */
	const int n = vector->length;
	const CTYPE *values = (CTYPE *) vector->buffers[1];
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

static void
FUNCTION_NAME(const)(void *agg_state, Datum constvalue, bool constisnull, int n)
{
	IntSumState *state = (IntSumState *) agg_state;

	if (constisnull)
	{
		return;
	}

	const CTYPE intvalue = DATUM_TO_CTYPE(constvalue);
	int64 batch_sum = 0;

	/* Multiply the number of tuples with the actual value */
	Assert(n > 0);
	if (unlikely(pg_mul_s64_overflow(intvalue, n, &batch_sum)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}

	/* Add the value to our sum */
	if (unlikely(pg_add_s64_overflow(state->result, batch_sum, &state->result)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}
	state->isnull = false;
}

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
