/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <nodes/pg_list.h>

#include "grouping_policy.h"

typedef struct DecompressBatchState DecompressBatchState;

typedef struct GroupingPolicyHash GroupingPolicyHash;

typedef struct
{
	void *(*create)(MemoryContext context, uint32 initial_rows, void *data);
	void (*reset)(void *table);
	uint32 (*get_num_keys)(void *table);
	uint64 (*get_size_bytes)(void *table);
	uint32 (*fill_offsets)(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
						   uint32 next_unused_state_index, int start_row, int end_row);
} HashTableFunctions;

/*
 * Hash grouping policy.
 */
typedef struct GroupingPolicyHash
{
	/*
	 * We're using data inheritance from the GroupingPolicy.
	 */
	GroupingPolicy funcs;

	List *agg_defs;
	List *output_grouping_columns;

	/*
	 * The hash table we use for grouping.
	 */
	void *table;
	HashTableFunctions functions;

	/*
	 * Whether we are in the mode of returning the partial aggregation results.
	 * Track the last returned aggregate state offset if we are.
	 */
	bool returning_results;
	uint32 last_returned_key;

	/*
	 * In single-column grouping, we store the null key outside of the hash
	 * table, and it has a reserved aggregate state index 1. We also reset this
	 * flag after we output the null key during iteration.
	 */
	bool have_null_key;

	/*
	 * A memory context for aggregate functions to allocate additional data,
	 * i.e. if they store strings or float8 datum on 32-bit systems. Valid until
	 * the grouping policy is reset.
	 */
	MemoryContext agg_extra_mctx;

	/*
	 * Temporary storage of aggregate state offsets for a given batch. We keep
	 * it in the policy because it is potentially too big to keep on stack, and
	 * we don't want to reallocate it each batch.
	 */
	uint32 *restrict offsets;
	uint64 num_allocated_offsets;

	/*
	 * Storage of aggregate function states, each List entry is the array of
	 * states for the respective function from agg_defs. The state index 0 is
	 * invalid, and the state index 1 is reserved for a null key.
	 */
	List *per_agg_states;
	uint64 allocated_aggstate_rows;

	uint64 key_bytes;
	uint64 num_allocated_keys;
	void *restrict keys;
	MemoryContext key_body_mctx;

	uint64 *tmp_filter;
	uint64 num_tmp_filter_words;

	/*
	 * Some statistics for debugging.
	 */
	uint64 stat_input_total_rows;
	uint64 stat_input_valid_rows;
	uint64 stat_bulk_filtered_rows;
	uint64 stat_consecutive_keys;
} GroupingPolicyHash;
