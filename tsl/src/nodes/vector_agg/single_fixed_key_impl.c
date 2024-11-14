/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME_HELPER(X, Y) FUNCTION_NAME_HELPER2(X, Y)
#define FUNCTION_NAME(Y) FUNCTION_NAME_HELPER(KEY_VARIANT, Y)

static pg_attribute_always_inline void
FUNCTION_NAME(get_key)(HashingConfig config, int row, void *restrict key_ptr, bool *restrict valid)
{
	GroupingPolicyHash *policy = config.policy;

	CTYPE *restrict key = (CTYPE *) key_ptr;

	if (unlikely(config.single_key.decompression_type == DT_Scalar))
	{
		*key = DATUM_TO_CTYPE(*config.single_key.output_value);
		*valid = !*config.single_key.output_isnull;
	}
	else if (config.single_key.decompression_type == sizeof(CTYPE))
	{
		const CTYPE *values = config.single_key.buffers[1];
		*valid = arrow_row_is_valid(config.single_key.buffers[0], row);
		*key = values[row];
	}
	else
	{
		pg_unreachable();
	}

	gp_hash_output_keys(policy, policy->last_used_key_index + 1)[0] = CTYPE_TO_DATUM(*key);
}

static pg_attribute_always_inline CTYPE
FUNCTION_NAME(store_key)(GroupingPolicyHash *restrict policy, CTYPE key)
{
	return key;
}

static pg_attribute_always_inline void
FUNCTION_NAME(destroy_key)(CTYPE key)
{
	/* Noop for fixed-size keys. */
}

#undef FUNCTION_NAME_HELPER2
#undef FUNCTION_NAME_HELPER
#undef FUNCTION_NAME

#include "hash_single_helper.c"
