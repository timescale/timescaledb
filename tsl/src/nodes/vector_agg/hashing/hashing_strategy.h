/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

typedef struct GroupingPolicyHash GroupingPolicyHash;

typedef struct HashingStrategy HashingStrategy;

typedef struct DecompressBatchState DecompressBatchState;

typedef struct TupleTableSlot TupleTableSlot;

/*
 * The hashing strategy manages the details of how the grouping keys are stored
 * in a hash table.
 */
typedef struct HashingStrategy
{
	char *explain_name;
	void (*init)(HashingStrategy *hashing, GroupingPolicyHash *policy);
	void (*reset)(HashingStrategy *hashing);
	uint64 (*get_size_bytes)(HashingStrategy *hashing);
	void (*prepare_for_batch)(GroupingPolicyHash *policy, DecompressBatchState *batch_state);
	void (*fill_offsets)(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
						 int start_row, int end_row);
	void (*emit_key)(GroupingPolicyHash *policy, uint32 current_key,
					 TupleTableSlot *aggregated_slot);

	/*
	 * The hash table we use for grouping. It matches each grouping key to its
	 * unique integer index.
	 */
	void *table;

	/*
	 * For each unique grouping key, we store the values of the grouping columns.
	 * This is stored separately from hash table keys, because they might not
	 * have the full column values, and also storing them contiguously here
	 * leads to better memory access patterns when emitting the results.
	 * The details of the key storage are managed by the hashing strategy.
	 */
	Datum *restrict output_keys;
	uint64 num_allocated_output_keys;

	/*
	 * In single-column grouping, we store the null key outside of the hash
	 * table, and its index is given by this value. Key index 0 is invalid.
	 * This is done to avoid having an "is null" flag in the hash table entries,
	 * to reduce the hash table size.
	 */
	uint32 null_key_index;
} HashingStrategy;

void hash_strategy_output_key_alloc(GroupingPolicyHash *policy, DecompressBatchState *batch_state);
void hash_strategy_output_key_single_emit(GroupingPolicyHash *policy, uint32 current_key,
										  TupleTableSlot *aggregated_slot);
