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
FUNCTION_NAME(vector_impl_arrow)(void *agg_state, const ArrowArray *vector, const uint64 *filter,
								 MemoryContext agg_extra_mctx)
{
	const int n = vector->length;
	const CTYPE *values = vector->buffers[1];
	FUNCTION_NAME(vector_impl)(agg_state, n, values, filter, agg_extra_mctx);
}

static pg_noinline void
FUNCTION_NAME(vector_all_valid)(void *agg_state, const ArrowArray *vector,
								MemoryContext agg_extra_mctx)
{
	FUNCTION_NAME(vector_impl_arrow)(agg_state, vector, NULL, agg_extra_mctx);
}

static pg_noinline void
FUNCTION_NAME(vector_one_validity)(void *agg_state, const ArrowArray *vector, const uint64 *filter,
								   MemoryContext agg_extra_mctx)
{
	FUNCTION_NAME(vector_impl_arrow)(agg_state, vector, filter, agg_extra_mctx);
}

static void
FUNCTION_NAME(vector)(void *agg_state, const ArrowArray *vector, const uint64 *filter,
					  MemoryContext agg_extra_mctx)
{
	if (filter == NULL)
	{
		/* All rows are valid and we don't have to check any validity bitmaps. */
		FUNCTION_NAME(vector_all_valid)(agg_state, vector, agg_extra_mctx);
	}
	else
	{
		/* Have to check only one combined validity bitmap. */
		FUNCTION_NAME(vector_one_validity)(agg_state, vector, filter, agg_extra_mctx);
	}
}
