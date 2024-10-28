/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME_HELPER(X, Y) FUNCTION_NAME_HELPER2(X, Y)
#define FUNCTION_NAME(Y) FUNCTION_NAME_HELPER(KEY_VARIANT, Y)

static pg_attribute_always_inline void
FUNCTION_NAME(get_key)(GroupingPolicyHash *restrict policy,
						DecompressBatchState *restrict batch_state,
					CompressedColumnValues *single_key_column,
					   int row, CTYPE *restrict key,
					   bool *restrict valid)
{
	Assert(policy->num_grouping_columns == 1);

	if (unlikely(single_key_column->decompression_type == DT_Scalar))
	{
		*key = DATUM_TO_CTYPE(*single_key_column->output_value);
		*valid = !*single_key_column->output_isnull;
	}
	else if (single_key_column->decompression_type == sizeof(CTYPE))
	{
		const CTYPE *values = single_key_column->buffers[1];
		const uint64 *key_validity = single_key_column->buffers[0];
		*valid = arrow_row_is_valid(key_validity, row);
		*key = values[row];
	}
	else
	{
		pg_unreachable();
	}

	gp_hash_output_keys(policy, policy->last_used_key_index + 1)[0] = CTYPE_TO_DATUM(*key);
	gp_hash_key_validity_bitmap(policy, policy->last_used_key_index + 1)[0] = *valid;
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
