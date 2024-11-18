/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <nodes/pg_list.h>

#include "grouping_policy.h"

#include "nodes/decompress_chunk/compressed_batch.h"

typedef struct GroupingPolicyHash GroupingPolicyHash;

typedef struct HashingStrategy HashingStrategy;

struct umash_params;

typedef struct HashingStrategy
{
	char *explain_name;
	void (*init)(HashingStrategy *strategy, GroupingPolicyHash *policy);
	void (*reset)(void *table);
	uint32 (*get_num_keys)(void *table);
	uint64 (*get_size_bytes)(void *table);
	void (*prepare_for_batch)(GroupingPolicyHash *policy, DecompressBatchState *batch_state);
	void (*fill_offsets)(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
						 int start_row, int end_row);
	void (*emit_key)(GroupingPolicyHash *policy, uint32 current_key,
					 TupleTableSlot *aggregated_slot);

	/*
	 * For each unique grouping key, we store the values of the grouping columns.
	 * This is stored separately from hash table keys, because they might not
	 * have the full column values, and also storing them contiguously here
	 * leads to better memory access patterns when emitting the results.
	 * The details of the key storage are managed by the hashing strategy.
	 */
	Datum *restrict output_keys;
	uint64 num_output_keys;
	MemoryContext key_body_mctx;

	/*
	 * In single-column grouping, we store the null key outside of the hash
	 * table, and its index is given by this value. Key index 0 is invalid.
	 * This is done to avoid having an "is null" flag in the hash table entries,
	 * to reduce the hash table size.
	 */
	uint32 null_key_index;

} HashingStrategy;

/*
 * Hash grouping policy.
 *
 * The grouping and aggregation is performed as follows:
 *
 * 0) The grouping policy keeps track of the unique grouping keys seen in
 * the input rows, and the states of aggregate functions for each key. This
 * spans multiple input compressed batches, and is reset after the partial
 * aggregation results are emitted.
 *
 * 1) For each row of the new compressed batch, we obtain an integer index that
 * uniquely identifies its grouping key. This is done by matching the row's
 * grouping columns to the hash table recording the unique grouping keys and
 * their respective indexes. It is performed in bulk for all rows of the batch,
 * to improve memory locality.
 *
 * 2) The key indexes are used to locate the aggregate function states
 * corresponding to a given row, and update it. This is done in bulk for all
 * rows of the batch, and for each aggregate function separately, to generate
 * simpler and potentially vectorizable code, and improve memory locality.
 *
 * 3) After the input have ended, or if the memory limit is reached, the partial
 * results are emitted into the output slot. This is done in the order of unique
 * grouping key indexes, thereby preserving the incoming key order. This
 * guarantees that this policy works correctly even in a Partial GroupAggregate
 * node, even though it's not optimal performance-wise.
 */
typedef struct GroupingPolicyHash
{
	/*
	 * We're using data inheritance from the GroupingPolicy.
	 */
	GroupingPolicy funcs;

	int num_agg_defs;
	const VectorAggDef *restrict agg_defs;

	int num_grouping_columns;
	const GroupingColumn *restrict grouping_columns;

	/*
	 * The values of the grouping columns picked from the compressed batch and
	 * arranged in the order of grouping column definitions.
	 */
	CompressedColumnValues *restrict current_batch_grouping_column_values;

	/*
	 * The hash table we use for grouping. It matches each grouping key to its
	 * unique integer index.
	 */
	void *table;
	HashingStrategy strategy;
	struct umash_params *umash_params;

	/*
	 * Temporary key storages. Some hashing strategies need to put the key in a
	 * separate memory area, we don't want to alloc/free it on each row.
	 */
	uint8 *tmp_key_storage;
	uint64 num_tmp_key_storage_bytes;

	/*
	 * The last used index of an unique grouping key. Key index 0 is invalid.
	 */
	uint32 last_used_key_index;

	/*
	 * In single-column grouping, we store the null key outside of the hash
	 * table, and its index is given by this value. Key index 0 is invalid.
	 * This is done to avoid having an "is null" flag in the hash table entries,
	 * to reduce the hash table size.
	 */
	uint32 null_key_index;

	/*
	 * Temporary storage of unique key indexes corresponding to a given row of
	 * the compressed batch that is currently being aggregated. We keep it in
	 * the policy because it is potentially too big to keep on stack, and we
	 * don't want to reallocate it each batch.
	 */
	uint32 *restrict key_index_for_row;
	uint64 num_key_index_for_row;

	/*
	 * For single text key that uses dictionary encoding, in some cases we first
	 * calculate the key indexes for the dictionary entries, and then translate
	 * it to the actual rows.
	 */
	uint32 *restrict key_index_for_dict;
	uint64 num_key_index_for_dict;
	bool use_key_index_for_dict;

	/*
	 * The temporary filter bitmap we use to combine the results of the
	 * vectorized filters in WHERE, validity of the aggregate function argument,
	 * and the aggregate FILTER clause. It is then used by the aggregate
	 * function implementation to filter out the rows that don't pass.
	 */
	uint64 *tmp_filter;
	uint64 num_tmp_filter_words;

	/*
	 * Aggregate function states. Each element is an array of states for the
	 * respective function from agg_defs. These arrays are indexed by the unique
	 * grouping key indexes. The state index 0 is invalid, so the corresponding
	 * states are unused.
	 * The states of each aggregate function are stored separately and
	 * contiguously, to achieve better memory locality when updating them.
	 */
	void **per_agg_states;
	uint64 num_agg_state_rows;

	/*
	 * A memory context for aggregate functions to allocate additional data,
	 * i.e. if they store strings or float8 datum on 32-bit systems. Valid until
	 * the grouping policy is reset.
	 */
	MemoryContext agg_extra_mctx;

	/*
	 * Whether we are in the mode of returning the partial aggregation results.
	 * If we are, track the index of the last returned grouping key.
	 */
	bool returning_results;
	uint32 last_returned_key;

	/*
	 * Some statistics for debugging.
	 */
	uint64 stat_input_total_rows;
	uint64 stat_input_valid_rows;
	uint64 stat_bulk_filtered_rows;
	uint64 stat_consecutive_keys;
} GroupingPolicyHash;

static pg_attribute_always_inline bool
byte_bitmap_row_is_valid(const uint8 *bitmap, size_t row_number)
{
	if (likely(bitmap == NULL))
	{
		return true;
	}

	const size_t byte_index = row_number / 8;
	const size_t bit_index = row_number % 8;
	const uint8 mask = ((uint8) 1) << bit_index;
	return bitmap[byte_index] & mask;
}

static pg_attribute_always_inline void
byte_bitmap_set_row_validity(uint8 *bitmap, size_t row_number, bool value)
{
	const size_t byte_index = row_number / 8;
	const size_t bit_index = row_number % 8;
	const uint8 mask = ((uint8) 1) << bit_index;
	const uint8 new_bit = ((uint8) value) << bit_index;

	bitmap[byte_index] = (bitmap[byte_index] & ~mask) | new_bit;

	Assert(byte_bitmap_row_is_valid(bitmap, row_number) == value);
}

//#define DEBUG_PRINT(...) fprintf(stderr, __VA_ARGS__)
#ifndef DEBUG_PRINT
#define DEBUG_PRINT(...)
#endif

typedef struct HashingConfig
{
	const uint64 *batch_filter;
	CompressedColumnValues single_key;

	int num_grouping_columns;
	const CompressedColumnValues *grouping_column_values;
	bool have_scalar_columns;

	GroupingPolicyHash *restrict policy;

	uint32 *restrict result_key_indexes;
} HashingConfig;

static pg_attribute_always_inline HashingConfig
build_hashing_config(GroupingPolicyHash *policy, DecompressBatchState *batch_state)
{
	HashingConfig config = {
		.policy = policy,
		.batch_filter = batch_state->vector_qual_result,
		.num_grouping_columns = policy->num_grouping_columns,
		.grouping_column_values = policy->current_batch_grouping_column_values,
		.result_key_indexes = policy->key_index_for_row,
	};

	Assert(policy->num_grouping_columns > 0);
	if (policy->num_grouping_columns == 1)
	{
		config.single_key = policy->current_batch_grouping_column_values[0];
	}

	for (int i = 0; i < policy->num_grouping_columns; i++)
	{
		config.have_scalar_columns =
			config.have_scalar_columns ||
			(policy->current_batch_grouping_column_values[i].decompression_type == DT_Scalar ||
			 policy->current_batch_grouping_column_values[i].buffers[0] != NULL);
	}

	return config;
}