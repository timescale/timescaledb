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
FUNCTION_NAME(vector_all_valid)(void *agg_state, const ArrowArray *vector,
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
	const uint64 *row_validity = vector->buffers[0];

	if (row_validity == NULL && filter == NULL)
	{
		/* All rows are valid and we don't have to check any validity bitmaps. */
		FUNCTION_NAME(vector_all_valid)(agg_state, vector, agg_extra_mctx);
	}
	else if (row_validity != NULL && filter == NULL)
	{
		/* Have to check only one bitmap -- row validity bitmap. */
		FUNCTION_NAME(vector_one_validity)(agg_state, vector, row_validity, agg_extra_mctx);
	}
	else if (filter != NULL && row_validity == NULL)
	{
		/* Have to check only one bitmap -- results of the vectorized filter. */
		FUNCTION_NAME(vector_one_validity)(agg_state, vector, filter, agg_extra_mctx);
	}
	else
	{
		/*
		 * Have to check both the row validity bitmap and the results of the
		 * vectorized filter.
		 */
		FUNCTION_NAME(vector_two_validity)(agg_state, vector, row_validity, filter, agg_extra_mctx);
	}
}
