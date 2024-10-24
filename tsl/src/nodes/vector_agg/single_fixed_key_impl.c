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
	DecompressBatchState *restrict batch_state, int row, int next_key_index, CTYPE *restrict key,
					   bool *restrict valid)
{
	if (list_length(policy->output_grouping_columns) != 1)
	{
		Assert(false);
		pg_unreachable();
	}

	GroupingColumn *g = linitial(policy->output_grouping_columns);
	CompressedColumnValues column = batch_state->compressed_columns[g->input_offset];

	if (unlikely(column.decompression_type == DT_Scalar))
	{
		*key = DATUM_TO_CTYPE(*column.output_value);
		*valid = !*column.output_isnull;
	}
	else if (column.decompression_type == sizeof(CTYPE))
	{
		const CTYPE *values = column.buffers[1];
		const uint64 *key_validity = column.buffers[0];
		*valid = arrow_row_is_valid(key_validity, row);
		*key = values[row];
	}
	else
	{
		pg_unreachable();
	}
}


static pg_attribute_always_inline CTYPE
FUNCTION_NAME(store_key)(GroupingPolicyHash *restrict policy, CTYPE key, uint32 key_index)
{
	((Datum *restrict) policy->keys)[key_index] = CTYPE_TO_DATUM(key);
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
