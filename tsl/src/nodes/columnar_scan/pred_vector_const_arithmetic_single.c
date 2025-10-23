/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Compute a vector-const predicate and AND it to the filter bitmap.
 * Specialized for particular arithmetic data types and predicate.
 * Marked as noinline for the ease of debugging. Inlining it shouldn't be
 * beneficial because it's a big self-contained loop.
 */

#define PG_PREDICATE_HELPER(X) PG_PREDICATE(X)

#define FUNCTION_NAME_HELPER(X, Y, Z) predicate_##X##_##Y##_vector_##Z##_const
#define FUNCTION_NAME(X, Y, Z) FUNCTION_NAME_HELPER(X, Y, Z)

#ifdef GENERATE_DISPATCH_TABLE
case PG_PREDICATE_HELPER(PREDICATE_NAME):
	return FUNCTION_NAME(PREDICATE_NAME, VECTOR_CTYPE, CONST_CTYPE);
#else

static pg_noinline void
FUNCTION_NAME(PREDICATE_NAME, VECTOR_CTYPE,
			  CONST_CTYPE)(const ArrowArray *arrow, const Datum constdatum, uint64 *restrict result)
{
	const size_t n = arrow->length;

	/* Now run the predicate itself. */
	const CONST_CTYPE constvalue = CONST_CONVERSION(constdatum);
	const VECTOR_CTYPE *vector = (const VECTOR_CTYPE *) arrow->buffers[1];

	for (size_t outer = 0; outer < n / 64; outer++)
	{
		/* no need to check the values if the result is already invalid */
		if (result[outer] == 0)
			continue;

		uint64 word = 0;
		for (size_t inner = 0; inner < 64; inner++)
		{
			const bool valid = PREDICATE_EXPRESSION(vector[outer * 64 + inner], constvalue);
			word |= ((uint64) valid) << inner;
		}
		result[outer] &= word;
	}

	if (n % 64)
	{
		uint64 tail_word = 0;
		for (size_t i = (n / 64) * 64; i < n; i++)
		{
			const bool valid = PREDICATE_EXPRESSION(vector[i], constvalue);
			tail_word |= ((uint64) valid) << (i % 64);
		}
		result[n / 64] &= tail_word;
	}
}

#endif

#undef PG_PREDICATE_HELPER

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER

#undef PREDICATE_EXPRESSION
#undef PREDICATE_NAME
