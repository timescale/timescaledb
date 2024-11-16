/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME_HELPER(X, Y) FUNCTION_NAME_HELPER2(X, Y)
#define FUNCTION_NAME(Y) FUNCTION_NAME_HELPER(KEY_VARIANT, Y)

static pg_attribute_always_inline void
FUNCTION_NAME(get_key)(HashingConfig config, int row, void *restrict full_key_ptr,
					   void *restrict abbrev_key_ptr, bool *restrict valid)
{
	FULL_KEY_TYPE *restrict full_key = (FULL_KEY_TYPE *) full_key_ptr;
	ABBREV_KEY_TYPE *restrict abbrev_key = (ABBREV_KEY_TYPE *) abbrev_key_ptr;

	if (unlikely(config.single_key.decompression_type == DT_Scalar))
	{
		*full_key = DATUM_TO_FULL_KEY(*config.single_key.output_value);
		*valid = !*config.single_key.output_isnull;
	}
	else if (config.single_key.decompression_type == sizeof(FULL_KEY_TYPE))
	{
		const FULL_KEY_TYPE *values = config.single_key.buffers[1];
		*valid = arrow_row_is_valid(config.single_key.buffers[0], row);
		*full_key = values[row];
	}
	else
	{
		pg_unreachable();
	}

	*abbrev_key = ABBREVIATE(*full_key);
}

static pg_attribute_always_inline FULL_KEY_TYPE
FUNCTION_NAME(store_key)(HashingConfig config, int row, FULL_KEY_TYPE full_key,
						 ABBREV_KEY_TYPE abbrev_key)
{
	gp_hash_output_keys(config.policy, config.policy->last_used_key_index)[0] =
		FULL_KEY_TO_DATUM(full_key);
	return abbrev_key;
}

static pg_attribute_always_inline void
FUNCTION_NAME(destroy_key)(FULL_KEY_TYPE key)
{
	/* Noop for fixed-size keys. */
}

#undef FUNCTION_NAME_HELPER2
#undef FUNCTION_NAME_HELPER
#undef FUNCTION_NAME

#undef ABBREVIATE

#include "hash_single_helper.c"
