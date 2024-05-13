/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <limits.h>

#include <postgres.h>

#include <common/int.h>
#include <utils/fmgroids.h>

#include "functions.h"

#include "compat/compat.h"

/*
 * Vectorized implementation of int4_sum.
 */
typedef struct
{
	int64 result;
	bool isnull;
} Int4SumState;

static void
int4_sum_init(void *agg_state)
{
	Int4SumState *state = (Int4SumState *) agg_state;
	state->result = 0;
	state->isnull = true;
}

static void
int4_sum_vector(void *agg_state, ArrowArray *vector, uint64 *filter)
{
	Int4SumState *state = (Int4SumState *) agg_state;

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

	int64 batch_sum = 0;

	/*
	 * This loop is not unrolled automatically, so do it manually as usual.
	 * The value buffer is padded to an even multiple of 64 bytes, i.e. to
	 * 64 / 4 = 16 elements. The bitmap is an even multiple of 64 elements.
	 * The number of elements in the inner loop must be less than both these
	 * values so that we don't go out of bounds. The particular value was
	 * chosen because it gives some speedup, and the larger values blow up
	 * the generated code with no performance benefit (checked on clang 16).
	 */
#define INNER_LOOP_SIZE 4
	const int outer_boundary = pad_to_multiple(INNER_LOOP_SIZE, vector->length);
	for (int outer = 0; outer < outer_boundary; outer += INNER_LOOP_SIZE)
	{
		for (int inner = 0; inner < INNER_LOOP_SIZE; inner++)
		{
			const int row = outer + inner;
			const int32 arrow_value = ((int32 *) vector->buffers[1])[row];
			const bool passes_filter = filter ? arrow_row_is_valid(filter, row) : true;
			batch_sum += passes_filter * arrow_value * arrow_row_is_valid(vector->buffers[0], row);
		}
	}
#undef INNER_LOOP_SIZE

	if (unlikely(pg_add_s64_overflow(state->result, batch_sum, &state->result)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}

	state->isnull = false;
}

static void
int4_sum_const(void *agg_state, Datum constvalue, bool constisnull, int n)
{
	Int4SumState *state = (Int4SumState *) agg_state;

	if (constisnull)
	{
		return;
	}

	int32 intvalue = DatumGetInt32(constvalue);
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

static void
int4_sum_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	Int4SumState *state = (Int4SumState *) agg_state;
	*out_result = Int64GetDatum(state->result);
	*out_isnull = state->isnull;
}

static VectorAggFunctions int4_sum_agg = {
	.state_bytes = sizeof(Int4SumState),
	.agg_init = int4_sum_init,
	.agg_const = int4_sum_const,
	.agg_vector = int4_sum_vector,
	.agg_emit = int4_sum_emit,
};

typedef struct
{
	int64 count;
} CountState;

static void
count_init(void *agg_state)
{
	CountState *state = (CountState *) agg_state;
	state->count = 0;
}

static void
count_emit(void *agg_state, Datum *out_result, bool *out_isnull)
{
	CountState *state = (CountState *) agg_state;
	*out_result = Int64GetDatum(state->count);
	*out_isnull = false;
}

static void
count_star_const(void *agg_state, Datum constvalue, bool constisnull, int n)
{
	CountState *state = (CountState *) agg_state;
	state->count += n;
}

VectorAggFunctions count_star_agg = {
	.state_bytes = sizeof(CountState),
	.agg_init = count_init,
	.agg_const = count_star_const,
	.agg_emit = count_emit,
};

VectorAggFunctions *
get_vector_aggregate(Oid aggfnoid)
{
	switch (aggfnoid)
	{
		case F_SUM_INT4:
			return &int4_sum_agg;
		case F_COUNT_:
			return &count_star_agg;
		default:
			return NULL;
	}
}
