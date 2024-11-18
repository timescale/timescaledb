/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "hash_single_output_key_helper.c"

#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME_HELPER(X, Y) FUNCTION_NAME_HELPER2(X, Y)
#define FUNCTION_NAME(Y) FUNCTION_NAME_HELPER(KEY_VARIANT, Y)

static pg_attribute_always_inline void
FUNCTION_NAME(get_key)(HashingConfig config, int row, void *restrict output_key_ptr,
					   void *restrict hash_table_key_ptr, bool *restrict valid)
{
	OUTPUT_KEY_TYPE *restrict output_key = (OUTPUT_KEY_TYPE *) output_key_ptr;
	HASH_TABLE_KEY_TYPE *restrict hash_table_key = (HASH_TABLE_KEY_TYPE *) hash_table_key_ptr;

	if (unlikely(config.single_key.decompression_type == DT_Scalar))
	{
		*output_key = DATUM_TO_output_key(*config.single_key.output_value);
		*valid = !*config.single_key.output_isnull;
	}
	else if (config.single_key.decompression_type == sizeof(OUTPUT_KEY_TYPE))
	{
		const OUTPUT_KEY_TYPE *values = config.single_key.buffers[1];
		*valid = arrow_row_is_valid(config.single_key.buffers[0], row);
		*output_key = values[row];
	}
	else
	{
		pg_unreachable();
	}

	*hash_table_key = *output_key;
}

static pg_attribute_always_inline OUTPUT_KEY_TYPE
FUNCTION_NAME(store_output_key)(GroupingPolicyHash *restrict policy, uint32 new_key_index,
								OUTPUT_KEY_TYPE output_key, HASH_TABLE_KEY_TYPE hash_table_key)
{
	policy->strategy.output_keys[new_key_index] = output_key_TO_DATUM(output_key);
	return hash_table_key;
}

static pg_attribute_always_inline void
FUNCTION_NAME(destroy_key)(OUTPUT_KEY_TYPE key)
{
	/* Noop for fixed-size keys. */
}

static void
FUNCTION_NAME(prepare_for_batch)(GroupingPolicyHash *policy, DecompressBatchState *batch_state)
{
	FUNCTION_NAME(alloc_output_keys)(policy, batch_state);
}

#undef FUNCTION_NAME_HELPER2
#undef FUNCTION_NAME_HELPER
#undef FUNCTION_NAME

