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

static pg_noinline void
FUNCTION_NAME(vector_no_validity)(void *agg_state, const ArrowArray *vector)
{
	FUNCTION_NAME(vector_impl)(agg_state, vector, NULL, NULL);
}

static pg_noinline void
FUNCTION_NAME(vector_one_validity)(void *agg_state, const ArrowArray *vector, const uint64 *valid)
{
	FUNCTION_NAME(vector_impl)(agg_state, vector, valid, NULL);
}

static pg_noinline void
FUNCTION_NAME(vector_two_validity)(void *agg_state, const ArrowArray *vector, const uint64 *valid1,
								   const uint64 *valid2)
{
	FUNCTION_NAME(vector_impl)(agg_state, vector, valid1, valid2);
}

static pg_attribute_always_inline void
FUNCTION_NAME(vector)(void *agg_state, const ArrowArray *vector, const uint64 *filter)
{
	const uint64 *valid1 = vector->buffers[0];
	const uint64 *valid2 = filter;

	if (valid1 == NULL && valid2 == NULL)
	{
		FUNCTION_NAME(vector_no_validity)(agg_state, vector);
	}
	else if (valid1 != NULL && valid2 == NULL)
	{
		FUNCTION_NAME(vector_one_validity)(agg_state, vector, valid1);
	}
	else if (valid2 != NULL && valid1 == NULL)
	{
		FUNCTION_NAME(vector_one_validity)(agg_state, vector, valid2);
	}
	else
	{
		FUNCTION_NAME(vector_two_validity)(agg_state, vector, valid1, valid2);
	}
}
