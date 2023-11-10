/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Functions for working with vectorized predicates.
 */

#include <postgres.h>

#include <utils/date.h>
#include <utils/fmgroids.h>

#include "compression/arrow_c_data_interface.h"

#include "vector_predicates.h"

#include "compat/compat.h"
#include "compression/compression.h"
#include "debug_assert.h"

/*
 * We include all implementations of vector-const predicates here. No separate
 * declarations for them to reduce the amount of macro template magic.
 */
#include "pred_vector_const_arithmetic_all.c"

/*
 * Look up the vectorized implementation for a Postgres predicate, specified by
 * its Oid in pg_proc. Note that this Oid is different from the opcode.
 */
VectorPredicate *
get_vector_const_predicate(Oid pg_predicate)
{
	switch (pg_predicate)
	{
#define GENERATE_DISPATCH_TABLE
#include "pred_vector_const_arithmetic_all.c"
#undef GENERATE_DISPATCH_TABLE
	}
	return NULL;
}

/*
 * Vectorized implementation of ScalarArrayOpExpr. Applies scalar_predicate for
 * vector and each element of array, combines the result according to "is_or"
 * flag. Written along the lines of ExecEvalScalarArrayOp().
 */
static inline void
vector_array_operator_impl(VectorPredicate *vector_const_predicate, bool is_or,
						   const ArrowArray *vector, Datum array, uint64 *restrict final_result)
{
	const size_t result_bits = vector->length;
	const size_t result_words = (result_bits + 63) / 64;

	uint64 *restrict array_result;
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
	}
	else
	{
		array_result = final_result;
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
 * This is a thin wrapper to nudge the compiler to inline the AND version which
 * is much simpler than OR version.
 */
void
vector_array_operator(VectorPredicate *scalar_predicate, bool is_or, const ArrowArray *vector,
					  Datum array, uint64 *restrict result)
{
	if (is_or)
	{
		vector_array_operator_impl(scalar_predicate, /* is_or = */ true, vector, array, result);
	}
	else
	{
		vector_array_operator_impl(scalar_predicate, /* is_or = */ false, vector, array, result);
	}
}
