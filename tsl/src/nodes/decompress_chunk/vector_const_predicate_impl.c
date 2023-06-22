/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Decompress the entire batch of gorilla-compressed rows into an Arrow array.
 * Specialized for each supported data type.
 */

#define FUNCTION_NAME_HELPER(X, Y, Z) predicate_##X##_##Y##_vector_##Z##_const
#define FUNCTION_NAME(X, Y, Z) FUNCTION_NAME_HELPER(X, Y, Z)

static void
FUNCTION_NAME(PREDICATE_NAME, VECTOR_TYPE, CONST_TYPE)(const VECTOR_TYPE *restrict vector,
													   const size_t n, const CONST_TYPE constvalue,
													   uint64 *restrict result)
{
	for (size_t outer = 0; outer < n / 64; outer++)
	{
		uint64 word = 0;
		for (size_t inner = 0; inner < 64; inner++)
		{
			const bool valid = PREDICATE(vector[outer * 64 + inner], constvalue);
			word |= ((uint64) valid) << inner;
		}
		result[outer] &= word;
	}

	if (n % 64)
	{
		uint64 tail_word = 0;
		for (size_t i = (n / 64) * 64; i < n; i++)
		{
			const bool valid = PREDICATE(vector[i], constvalue);
			tail_word |= ((uint64) valid) << (i % 64);
		}
		result[n / 64] &= tail_word;
	}
}

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER

#undef PREDICATE_NAME
#undef PREDICATE
#undef VECTOR_TYPE
#undef CONST_TYPE
