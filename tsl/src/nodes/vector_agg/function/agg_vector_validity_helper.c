/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Generate a separate implementation of aggregating an ArrowArray for the
 * common cases where we have no nulls and/or all rows pass the filter. It
 * avoids branches so can be more easily vectorized.
 */

static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl_arrow)(void *agg_state, const ArrowArray *vector, const uint64 *valid1,
								 const uint64 *valid2, MemoryContext agg_extra_mctx)
{
	const int n = vector->length;
	const CTYPE *values = vector->buffers[1];
	FUNCTION_NAME(vector_impl)(agg_state, n, values, valid1, valid2, agg_extra_mctx);
}

static pg_noinline void
FUNCTION_NAME(vector_no_validity)(void *agg_state, const ArrowArray *vector,
								  MemoryContext agg_extra_mctx)
{
	FUNCTION_NAME(vector_impl_arrow)(agg_state, vector, NULL, NULL, agg_extra_mctx);
}

static pg_noinline void
FUNCTION_NAME(vector_one_validity)(void *agg_state, const ArrowArray *vector, const uint64 *valid,
								   MemoryContext agg_extra_mctx)
{
	FUNCTION_NAME(vector_impl_arrow)(agg_state, vector, valid, NULL, agg_extra_mctx);
}

static pg_noinline void
FUNCTION_NAME(vector_two_validity)(void *agg_state, const ArrowArray *vector, const uint64 *valid1,
								   const uint64 *valid2, MemoryContext agg_extra_mctx)
{
	FUNCTION_NAME(vector_impl_arrow)(agg_state, vector, valid1, valid2, agg_extra_mctx);
}

static void
FUNCTION_NAME(vector)(void *agg_state, const ArrowArray *vector, const uint64 *filter,
					  MemoryContext agg_extra_mctx)
{
	const uint64 *valid1 = vector->buffers[0];
	const uint64 *valid2 = filter;

	if (valid1 == NULL && valid2 == NULL)
	{
		FUNCTION_NAME(vector_no_validity)(agg_state, vector, agg_extra_mctx);
	}
	else if (valid1 != NULL && valid2 == NULL)
	{
		FUNCTION_NAME(vector_one_validity)(agg_state, vector, valid1, agg_extra_mctx);
	}
	else if (valid2 != NULL && valid1 == NULL)
	{
		FUNCTION_NAME(vector_one_validity)(agg_state, vector, valid2, agg_extra_mctx);
	}
	else
	{
		FUNCTION_NAME(vector_two_validity)(agg_state, vector, valid1, valid2, agg_extra_mctx);
	}
}
