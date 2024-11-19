/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include "batch_hashing_params.h"

#include "output_key_alloc.c"
#include "output_key_emit_single.c"

static pg_attribute_always_inline void
FUNCTION_NAME(get_key)(BatchHashingParams params, int row, void *restrict output_key_ptr,
					   void *restrict hash_table_key_ptr, bool *restrict valid)
{
	OUTPUT_KEY_TYPE *restrict output_key = (OUTPUT_KEY_TYPE *) output_key_ptr;
	HASH_TABLE_KEY_TYPE *restrict hash_table_key = (HASH_TABLE_KEY_TYPE *) hash_table_key_ptr;

	if (unlikely(params.single_key.decompression_type == DT_Scalar))
	{
		*output_key = DATUM_TO_OUTPUT_KEY(*params.single_key.output_value);
		*valid = !*params.single_key.output_isnull;
	}
	else if (params.single_key.decompression_type == sizeof(OUTPUT_KEY_TYPE))
	{
		const OUTPUT_KEY_TYPE *values = params.single_key.buffers[1];
		*valid = arrow_row_is_valid(params.single_key.buffers[0], row);
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
	policy->hashing.output_keys[new_key_index] = OUTPUT_KEY_TO_DATUM(output_key);
	return hash_table_key;
}

static void
FUNCTION_NAME(prepare_for_batch)(GroupingPolicyHash *policy, DecompressBatchState *batch_state)
{
	FUNCTION_NAME(alloc_output_keys)(policy, batch_state);
}
