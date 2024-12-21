/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

/*
 * The data required to map the rows of the given compressed batch to the unique
 * indexes of grouping keys, using a hash table.
 */
typedef struct BatchHashingParams
{
	const uint64 *batch_filter;
	CompressedColumnValues single_grouping_column;

	int num_grouping_columns;
	const CompressedColumnValues *grouping_column_values;

	/*
	 * Whether we have any scalar or nullable grouping columns in the current
	 * batch. This is used to select the more efficient implementation when we
	 * have none.
	 */
	bool have_scalar_or_nullable_columns;

	GroupingPolicyHash *restrict policy;

	uint32 *restrict result_key_indexes;
} BatchHashingParams;

static pg_attribute_always_inline BatchHashingParams
build_batch_hashing_params(GroupingPolicyHash *policy, DecompressBatchState *batch_state)
{
	BatchHashingParams params = {
		.policy = policy,
		.batch_filter = batch_state->vector_qual_result,
		.num_grouping_columns = policy->num_grouping_columns,
		.grouping_column_values = policy->current_batch_grouping_column_values,
		.result_key_indexes = policy->key_index_for_row,
	};

	Assert(policy->num_grouping_columns > 0);
	if (policy->num_grouping_columns == 1)
	{
		params.single_grouping_column = policy->current_batch_grouping_column_values[0];
	}

	for (int i = 0; i < policy->num_grouping_columns; i++)
	{
		params.have_scalar_or_nullable_columns =
			params.have_scalar_or_nullable_columns ||
			(policy->current_batch_grouping_column_values[i].decompression_type == DT_Scalar ||
			 policy->current_batch_grouping_column_values[i].buffers[0] != NULL);
	}

	return params;
}
