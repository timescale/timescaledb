/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <nodes/pg_list.h>

#include "grouping_policy.h"

typedef struct GroupingPolicyHash GroupingPolicyHash;

typedef struct
{
	char *explain_name;
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

	int num_agg_defs;
	VectorAggDef *agg_defs;

	int num_grouping_columns;
	GroupingColumn *grouping_columns;

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
	uint32 null_key_index;

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
	 * Storage of aggregate function states, each entry is the array of
	 * states for the respective function from agg_defs. The state index 0 is
	 * invalid.
	 */
	void **per_agg_states;
	uint64 allocated_aggstate_rows;

	uint64 num_allocated_keys;
	void *restrict output_keys;
	MemoryContext key_body_mctx;
	uint32 next_unused_key_index;

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

static inline uint64 *
gp_hash_key_validity_bitmap(GroupingPolicyHash *policy, int key_index)
{
	return (uint64 *) ((char *) policy->output_keys +
					   (sizeof(uint64) + sizeof(Datum) * policy->num_grouping_columns) * key_index);
}

static inline Datum *
gp_hash_output_keys(GroupingPolicyHash *policy, int key_index)
{
	Assert(key_index != 0);
	return (Datum *) &gp_hash_key_validity_bitmap(policy, key_index)[1];
}

// #define DEBUG_PRINT(...) fprintf(stderr, __VA_ARGS__)
#ifndef DEBUG_PRINT
#define DEBUG_PRINT(...)
#endif
