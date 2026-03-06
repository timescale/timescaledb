/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <nodes/pg_list.h>

#include "grouping_policy.h"

#include "nodes/columnar_scan/compressed_batch.h"

#include "hashing/hashing_strategy.h"

typedef struct GroupingPolicyHash GroupingPolicyHash;

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
 * 1) For each row of the new compressed batch, we obtain an index that
 * uniquely identifies its grouping key. This is done by matching the row's
 * grouping columns to the hash table recording the unique grouping keys and
 * their respective indexes. It is performed in bulk for all rows of the batch,
 * to improve memory locality. The details of this are managed by the hashing
 * strategy.
 *
 * 2) The key indexes are used to locate the aggregate function states
 * corresponding to a given row's key, and update it. This is done in bulk for all
 * rows of the batch, and for each aggregate function separately, to generate
 * simpler and potentially vectorizable code, and improve memory locality.
 *
 * 3) After the input has ended, or if the memory limit is reached, the partial
 * results are emitted into the output slot. This is done in the order of unique
 * grouping key indexes, thereby preserving the incoming key order. This
 * guarantees that this policy works correctly even in a Partial GroupAggregate
 * node, even though it's not optimal performance-wise. We only support the
 * direct order of records in batch though, not reverse. This is checked at
 * planning time.
 */
typedef struct GroupingPolicyHash
{
	/*
	 * We're using data inheritance from the GroupingPolicy.
	 */
	GroupingPolicy funcs;

	/*
	 * Aggregate function definitions.
	 */
	int num_agg_defs;
	const VectorAggDef *restrict agg_defs;

	/*
	 * Grouping column definitions.
	 */
	int num_grouping_columns;
	const GroupingColumn *restrict grouping_columns;

	/*
	 * The values of the grouping columns picked from the compressed batch and
	 * arranged in the order of grouping column definitions.
	 */
	CompressedColumnValues *restrict current_batch_grouping_column_values;

	/*
	 * Hashing strategy that is responsible for mapping the rows to the unique
	 * indexes of their grouping keys.
	 */
	HashingStrategy hashing;

	/*
	 * Temporary storage of unique indexes of keys corresponding to a given row
	 * of the compressed batch that is currently being aggregated. We keep it in
	 * the policy because it is potentially too big to keep on stack, and we
	 * don't want to reallocate it for each batch.
	 */
	uint32 *restrict key_index_for_row;
	uint64 num_key_index_for_row;

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
	 * grouping key indexes. The key index 0 is invalid, so the corresponding
	 * states are unused.
	 * The states of each aggregate function are stored separately and
	 * contiguously, to achieve better memory locality when updating them.
	 */
	void **per_agg_per_key_states;
	uint64 num_allocated_per_key_agg_states;

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

// #define DEBUG_PRINT(...) fprintf(stderr, __VA_ARGS__)
#ifndef DEBUG_PRINT
#define DEBUG_PRINT(...)
#endif
