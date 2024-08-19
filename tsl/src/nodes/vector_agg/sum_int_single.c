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
FUNCTION_NAME(vector)(void *agg_state, ArrowArray *vector, uint64 *filter)
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

	const uint64 *validity = (uint64 *) vector->buffers[0];
	const CTYPE *values = (CTYPE *) vector->buffers[1];
	int64 batch_sum = 0;
	bool have_result = false;
	for (int row = 0; row < vector->length; row++)
	{
		const bool passes = arrow_row_is_valid(filter, row);
		const bool isvalid = arrow_row_is_valid(validity, row);
		batch_sum += values[row] * passes * isvalid;
		have_result |= passes && isvalid;
	}

	if (unlikely(pg_add_s64_overflow(state->result, batch_sum, &state->result)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}

	state->isnull &= !have_result;
}

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
