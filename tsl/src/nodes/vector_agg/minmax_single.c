/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifdef GENERATE_DISPATCH_TABLE
case PG_AGG_OID_HELPER(AGG_NAME, PG_TYPE):
	return &FUNCTION_NAME(argdef);
#else
static void
FUNCTION_NAME(const)(void *agg_state, Datum constvalue, bool constisnull, int n)
{
	MinMaxState *state = (MinMaxState *) agg_state;
	if (constisnull)
	{
		return;
	}

	if (!state->isvalid)
	{
		state->isvalid = true;
		state->value = constvalue;
		return;
	}

	const CTYPE new = DATUM_TO_CTYPE(constvalue);
	const CTYPE current = DATUM_TO_CTYPE(state->value);

	/*
	 * Note that we have to properly handle NaNs and Infinities for floats.
	 */
	const bool do_replace = PREDICATE(current, new) || isnan((double) new);
	state->value = CTYPE_TO_DATUM(do_replace ? new : current);
}

static pg_attribute_always_inline void
FUNCTION_NAME(vector_impl)(MinMaxState *state, const int n, const CTYPE *values,
						   const uint64 *valid1, const uint64 *valid2)
{
#define UNROLL_SIZE ((int) (512 / 8 / sizeof(CTYPE)))
	CTYPE outer_result = DATUM_TO_CTYPE(state->value);
	bool outer_isvalid = state->isvalid;
	for (int outer = 0; outer < (n / UNROLL_SIZE) * UNROLL_SIZE; outer += UNROLL_SIZE)
	{
		bool inner_isvalid = false;
		CTYPE inner_result = { 0 };
		for (int inner = 0; inner < UNROLL_SIZE; inner++)
		{
			const int row = outer + inner;
			const CTYPE new_value = values[row];
			const bool new_value_ok = arrow_both_valid(valid1, valid2, row);

			/*
			 * Note that we have to properly handle NaNs and Infinities for floats.
			 */
			const bool do_replace =
				new_value_ok &&
				(!inner_isvalid || PREDICATE(inner_result, new_value) || isnan((double) new_value));

			inner_result = do_replace ? new_value : inner_result;
			inner_isvalid |= do_replace;
		}

		if (inner_isvalid && (!outer_isvalid || PREDICATE(outer_result, inner_result) ||
							  isnan((double) inner_result)))
		{
			outer_result = inner_result;
			outer_isvalid = true;
		}
	}

	for (int row = (n / UNROLL_SIZE) * UNROLL_SIZE; row < n; row++)
	{
		const CTYPE new_value = values[row];
		const bool new_value_ok = arrow_both_valid(valid1, valid2, row);

		/*
		 * Note that we have to properly handle NaNs and Infinities for floats.
		 */
		const bool do_replace =
			new_value_ok && (unlikely(!outer_isvalid) || PREDICATE(outer_result, new_value) ||
							 isnan((double) new_value));

		outer_result = do_replace ? new_value : outer_result;
		outer_isvalid |= do_replace;
	}

	state->value = CTYPE_TO_DATUM(outer_result);
	state->isvalid = outer_isvalid;
#undef UNROLL_SIZE
}

static pg_noinline void
FUNCTION_NAME(vector_no_validity)(MinMaxState *state, const int n, const CTYPE *values)
{
	FUNCTION_NAME(vector_impl)(state, n, values, NULL, NULL);
}

static pg_noinline void
FUNCTION_NAME(vector_one_validity)(MinMaxState *state, const int n, const CTYPE *values,
								   const uint64 *valid)
{
	FUNCTION_NAME(vector_impl)(state, n, values, valid, NULL);
}

static pg_noinline void
FUNCTION_NAME(vector_two_validity)(MinMaxState *state, const int n, const CTYPE *values,
								   const uint64 *valid1, const uint64 *valid2)
{
	FUNCTION_NAME(vector_impl)(state, n, values, valid1, valid2);
}

static pg_attribute_always_inline void
FUNCTION_NAME(vector)(void *agg_state, const ArrowArray *vector, const uint64 *filter)
{
	MinMaxState *state = (MinMaxState *) agg_state;
	const int n = vector->length;
	const CTYPE *values = (CTYPE *) vector->buffers[1];
	const uint64 *valid1 = vector->buffers[0];
	const uint64 *valid2 = filter;

	if (valid1 == NULL && valid2 == NULL)
	{
		FUNCTION_NAME(vector_no_validity)(state, n, values);
	}
	else if (valid1 != NULL && valid2 == NULL)
	{
		FUNCTION_NAME(vector_one_validity)(state, n, values, valid1);
	}
	else if (valid2 != NULL && valid1 == NULL)
	{
		FUNCTION_NAME(vector_one_validity)(state, n, values, valid2);
	}
	else
	{
		FUNCTION_NAME(vector_two_validity)(state, n, values, valid1, valid2);
	}
}

static VectorAggFunctions FUNCTION_NAME(argdef) = { .state_bytes = sizeof(MinMaxState),
													.agg_init = minmax_init,
													.agg_emit = minmax_emit,
													.agg_const = FUNCTION_NAME(const),
													.agg_vector = FUNCTION_NAME(vector) };
#endif

#undef PG_TYPE
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM
