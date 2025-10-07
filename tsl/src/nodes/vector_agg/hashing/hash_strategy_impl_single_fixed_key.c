/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Key handling function for a single fixed-size grouping key.
 */

#include "batch_hashing_params.h"

static void
FUNCTION_NAME(key_hashing_init)(HashingStrategy *hashing)
{
}

static void
FUNCTION_NAME(key_hashing_prepare_for_batch)(GroupingPolicyHash *policy,
											 TupleTableSlot *vector_slot)
{
}

static pg_attribute_always_inline void
FUNCTION_NAME(key_hashing_get_key)(BatchHashingParams params, int row,
								   void *restrict output_key_ptr, void *restrict hash_table_key_ptr,
								   bool *restrict valid)
{
	OUTPUT_KEY_TYPE *restrict output_key = (OUTPUT_KEY_TYPE *) output_key_ptr;
	HASH_TABLE_KEY_TYPE *restrict hash_table_key = (HASH_TABLE_KEY_TYPE *) hash_table_key_ptr;

	if (unlikely(params.single_grouping_column.decompression_type == DT_Scalar))
	{
		*output_key = DATUM_TO_OUTPUT_KEY(*params.single_grouping_column.output_value);
		*valid = !*params.single_grouping_column.output_isnull;
	}
	else if (params.single_grouping_column.decompression_type == sizeof(OUTPUT_KEY_TYPE))
	{
		const OUTPUT_KEY_TYPE *values = params.single_grouping_column.buffers[1];
		*valid = arrow_row_is_valid(params.single_grouping_column.buffers[0], row);
		*output_key = values[row];
	}
	else
	{
		pg_unreachable();
	}

	/*
	 * For the fixed-size hash grouping, we use the output key as the hash table
	 * key as well.
	 */
	*hash_table_key = *output_key;
}

static pg_attribute_always_inline void
FUNCTION_NAME(key_hashing_store_new)(HashingStrategy *restrict hashing, uint32 new_key_index,
									 OUTPUT_KEY_TYPE output_key)
{
	hashing->output_keys[new_key_index] = OUTPUT_KEY_TO_DATUM(output_key);
}

static void
FUNCTION_NAME(emit_key)(GroupingPolicyHash *policy, uint32 current_key,
						TupleTableSlot *aggregated_slot)
{
	hash_strategy_output_key_single_emit(policy, current_key, aggregated_slot);
}

#undef DATUM_TO_OUTPUT_KEY
#undef OUTPUT_KEY_TO_DATUM
