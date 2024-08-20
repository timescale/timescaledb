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

static void
int4_sum_init(Datum *agg_value, bool *agg_isnull)
{
	*agg_value = Int64GetDatum(0);
	*agg_isnull = true;
}

static void
int4_sum_vector(ArrowArray *vector, uint64 *filter, Datum *agg_value, bool *agg_isnull)
{
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
	int64 batch_sum = 0;
	for (int row = 0; row < vector->length; row++)
	{
		const int32 arrow_value = ((int32 *) vector->buffers[1])[row];
		batch_sum += arrow_value * arrow_row_is_valid(filter, row) *
					 arrow_row_is_valid(vector->buffers[0], row);
	}

	int64 tmp = DatumGetInt64(*agg_value);
	if (unlikely(pg_add_s64_overflow(tmp, batch_sum, &tmp)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}

	/*
	 * Use Int64GetDatum to store the result since a 64-bit value is not
	 * pass-by-value on 32-bit systems.
	 */
	*agg_value = Int64GetDatum(tmp);
	*agg_isnull = false;
}

static void
int4_sum_const(Datum constvalue, bool constisnull, int n, Datum *agg_value, bool *agg_isnull)
{
	Assert(n > 0);

	if (constisnull)
	{
		return;
	}

	int32 intvalue = DatumGetInt32(constvalue);
	int64 batch_sum = 0;

	/* Multiply the number of tuples with the actual value */
	if (unlikely(pg_mul_s64_overflow(intvalue, n, &batch_sum)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}

	/* Add the value to our sum */
	int64 tmp = DatumGetInt64(*agg_value);
	if (unlikely(pg_add_s64_overflow(tmp, batch_sum, &tmp)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of range")));
	}

	/*
	 * Use Int64GetDatum to store the result since a 64-bit value is not
	 * pass-by-value on 32-bit systems.
	 */
	*agg_value = Int64GetDatum(tmp);
	*agg_isnull = false;
}

static VectorAggregate int4_sum_agg = {
	.agg_init = int4_sum_init,
	.agg_const = int4_sum_const,
	.agg_vector = int4_sum_vector,
};

VectorAggregate *
get_vector_aggregate(Oid aggfnoid)
{
	switch (aggfnoid)
	{
		case F_SUM_INT4:
			return &int4_sum_agg;
		default:
			return NULL;
	}
}
