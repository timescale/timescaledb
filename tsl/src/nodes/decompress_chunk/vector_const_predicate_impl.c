/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Compute a vector-const predicate and AND it to the filter bitmap.
 * Specialized for particular data types and predicates.
 * Marked as noinline for the ease of debugging. Inlining it shouldn't be
 * beneficial because it's a big loop.
 */

#define FUNCTION_NAME_HELPER(X, Y, Z) predicate_##X##_##Y##_vector_##Z##_const
#define FUNCTION_NAME(X, Y, Z) FUNCTION_NAME_HELPER(X, Y, Z)

static pg_noinline void
FUNCTION_NAME(PREDICATE_NAME, VECTOR_CTYPE,
			  CONST_CTYPE)(const ArrowArray *arrow, const Datum constdatum, uint64 *restrict result)
{
	const size_t n = arrow->length;

	/* Account for nulls which shouldn't pass the predicate. */
	const size_t n_words = (n + 63) / 64;
	const uint64 *restrict validity = (uint64 *restrict) arrow->buffers[0];
	for (size_t i = 0; i < n_words; i++)
	{
		result[i] &= validity[i];
	}

	/* Now run the predicate itself. */
	const CONST_CTYPE constvalue = CONST_CONVERSION(constdatum);
	const VECTOR_CTYPE *restrict vector = (VECTOR_CTYPE *restrict) arrow->buffers[1];

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

#undef PREDICATE
#undef PREDICATE_NAME
