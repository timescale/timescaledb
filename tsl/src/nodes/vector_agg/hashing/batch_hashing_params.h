/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include "nodes/vector_agg/grouping_policy_hash.h"
#include "nodes/vector_agg/vector_slot.h"

/*
 * The data required to map the rows of the given compressed batch to the unique
 * indexes of grouping keys, using a hash table.
 */
typedef struct BatchHashingParams
{
	const uint64 *batch_filter;
	CompressedColumnValues single_grouping_column;

	GroupingPolicyHash *restrict policy;

	uint32 *restrict result_key_indexes;
} BatchHashingParams;

static pg_attribute_always_inline BatchHashingParams
build_batch_hashing_params(GroupingPolicyHash *policy, TupleTableSlot *vector_slot)
{
	uint16 nrows;
	BatchHashingParams params = {
		.policy = policy,
		.batch_filter = vector_slot_get_qual_result(vector_slot, &nrows),
		.result_key_indexes = policy->key_index_for_row,
	};

	Assert(policy->num_grouping_columns == 1);
	params.single_grouping_column = policy->current_batch_grouping_column_values[0];

	return params;
}
