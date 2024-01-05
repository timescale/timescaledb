/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include "compression/arrow_c_data_interface.h"

#include "vector_predicates.h"

#include "compression/compression.h"

/*
 * Vectorized implementation of ScalarArrayOpExpr. Applies scalar_predicate for
 * vector and each element of array, combines the result according to "is_or"
 * flag. Written along the lines of ExecEvalScalarArrayOp().
 */
static inline void
vector_array_predicate_impl(VectorPredicate *vector_const_predicate, bool is_or,
							const ArrowArray *vector, Datum array, uint64 *restrict final_result)
{
	const size_t result_bits = vector->length;
	const size_t result_words = (result_bits + 63) / 64;

	uint64 *restrict array_result = NULL;
	/*
	 * For OR, we need an intermediate storage to accumulate the results
	 * from all elements.
	 * For AND, we can apply predicate for each element to the final result.
	 */
	uint64 array_result_storage[(GLOBAL_MAX_ROWS_PER_COMPRESSION + 63) / 64];
	if (is_or)
	{
		array_result = array_result_storage;
		for (size_t i = 0; i < result_words; i++)
		{
			array_result_storage[i] = 0;
		}

		if (vector->length % 64 != 0)
		{
			/*
			 * Set the bits for past-the-end elements to 1. This way it's more
			 * convenient to check for early exit, and the final result should
			 * have them already set to 0 so it doesn't matter.
			 */
			const uint64 mask = ((uint64) -1) << (vector->length % 64);
			array_result[vector->length / 64] = mask;
		}
	}

	ArrayType *arr = DatumGetArrayTypeP(array);

	int16 typlen;
	bool typbyval;
	char typalign;
	get_typlenbyvalalign(ARR_ELEMTYPE(arr), &typlen, &typbyval, &typalign);

	const char *array_data = (const char *) ARR_DATA_PTR(arr);
	const size_t nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
	const uint64 *restrict array_null_bitmap = (uint64 *) ARR_NULLBITMAP(arr);

	for (size_t array_index = 0; array_index < nitems; array_index++)
	{
		if (array_null_bitmap != NULL && !arrow_row_is_valid(array_null_bitmap, array_index))
		{
			/*
			 * This array element is NULL. We can't avoid NULLS when evaluating
			 * the stable functions at run time, so we have to support them.
			 * This is a predicate, not a generic scalar array operation, so
			 * thankfully we return a non-nullable bool.
			 * For ANY: null | true = true, null | false = null, so this means
			 * we can skip the null element and continue evaluation.
			 * For ALL: null & true = null, null & false = false, so this means
			 * that for each row the condition goes to false, and we don't have
			 * to evaluate the next elements.
			 */
			if (is_or)
			{
				continue;
			}

			for (size_t word = 0; word < result_words; word++)
			{
				final_result[word] = 0;
			}
			return;
		}
		Datum constvalue = fetch_att(array_data, typbyval, typlen);
		array_data = att_addlength_pointer(array_data, typlen, array_data);
		array_data = (char *) att_align_nominal(array_data, typalign);

		/*
		 * For OR, we also need an intermediate storage for predicate result
		 * for each array element, since the predicates AND their result.
		 *
		 * For AND, we can and apply predicate for each array element to the
		 * final result.
		 */
		uint64 single_result_storage[(GLOBAL_MAX_ROWS_PER_COMPRESSION + 63) / 64];
		uint64 *restrict single_result;
		if (is_or)
		{
			single_result = single_result_storage;
			for (size_t outer = 0; outer < result_words; outer++)
			{
				single_result[outer] = -1;
			}
		}
		else
		{
			single_result = final_result;
		}

		vector_const_predicate(vector, constvalue, single_result);

		if (is_or)
		{
			for (size_t outer = 0; outer < result_words; outer++)
			{
				array_result[outer] |= single_result[outer];
			}
		}

		/*
		 * On big arrays, we want to sometimes check if we can exit early,
		 * to avoid being slower than the non-vectorized version which exits
		 * at first possibility. The frequency is chosen by benchmarking.
		 * In debug mode, do this more frequently to simplify testing.
		 */
#ifdef NDEBUG
		if (array_index > 0 && array_index % 16 == 0)
#else
		if (array_index > 0 && array_index % 3 == 0)
#endif
		{
			if (is_or)
			{
				bool all_rows_match = true;
				for (size_t word = 0; word < result_words; word++)
				{
					/*
					 * Note that we have set the bits for past-the-end rows in
					 * array_result to 1, so we can use simple comparison to
					 * zero here.
					 */
					all_rows_match &= (~array_result[word] == 0);
				}
				if (all_rows_match)
				{
					return;
				}
			}
			else
			{
				bool any_rows_match = false;
				for (size_t word = 0; word < result_words; word++)
				{
					any_rows_match |= (final_result[word] != 0);
				}
				if (!any_rows_match)
				{
					return;
				}
			}
		}
	}

	if (is_or)
	{
		for (size_t outer = 0; outer < result_words; outer++)
		{
			/*
			 * The tail bits corresponding to past-the-end rows when n % 64 != 0
			 * should be already zeroed out in the final_result.
			 */
			final_result[outer] &= array_result[outer];
		}
	}
}

/*
 * This is a thin wrapper to nudge the compiler to specialize the AND version
 * which is much simpler than the OR version.
 */
static pg_noinline void
vector_array_predicate_and(VectorPredicate *scalar_predicate, const ArrowArray *vector, Datum array,
						   uint64 *restrict result)
{
	vector_array_predicate_impl(scalar_predicate, /* is_or = */ false, vector, array, result);
}

void
vector_array_predicate(VectorPredicate *scalar_predicate, bool is_or, const ArrowArray *vector,
					   Datum array, uint64 *restrict result)
{
	if (is_or)
	{
		vector_array_predicate_impl(scalar_predicate, /* is_or = */ true, vector, array, result);
	}
	else
	{
		vector_array_predicate_and(scalar_predicate, vector, array, result);
	}
}
