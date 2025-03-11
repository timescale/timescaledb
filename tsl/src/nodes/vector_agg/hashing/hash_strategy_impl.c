/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "batch_hashing_params.h"
#include "nodes/vector_agg/vector_slot.h"

/*
 * The hash table maps the value of the grouping key to its unique index.
 * We don't store any extra information here, because we're accessing the memory
 * of the hash table randomly, and want it to be as small as possible to fit the
 * caches.
 */
typedef struct FUNCTION_NAME(entry)
{
	/* Key index 0 is invalid. */
	uint32 key_index;

	uint8 status;

	HASH_TABLE_KEY_TYPE hash_table_key;
} FUNCTION_NAME(entry);

#define SH_PREFIX KEY_VARIANT
#define SH_ELEMENT_TYPE FUNCTION_NAME(entry)
#define SH_KEY_TYPE HASH_TABLE_KEY_TYPE
#define SH_KEY hash_table_key
#define SH_HASH_KEY(tb, key) KEY_HASH(key)
#define SH_EQUAL(tb, a, b) KEY_EQUAL(a, b)
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#include <lib/simplehash.h>

struct FUNCTION_NAME(hash);

static uint64
FUNCTION_NAME(get_size_bytes)(HashingStrategy *hashing)
{
	struct FUNCTION_NAME(hash) *hash = (struct FUNCTION_NAME(hash) *) hashing->table;
	return hash->members * sizeof(FUNCTION_NAME(entry));
}

static void
FUNCTION_NAME(hash_strategy_init)(HashingStrategy *hashing, GroupingPolicyHash *policy)
{
	hashing->table =
		FUNCTION_NAME(create)(CurrentMemoryContext, policy->num_allocated_per_key_agg_states, NULL);

	FUNCTION_NAME(key_hashing_init)(hashing);
}

static void
FUNCTION_NAME(hash_strategy_reset)(HashingStrategy *hashing)
{
	struct FUNCTION_NAME(hash) *table = (struct FUNCTION_NAME(hash) *) hashing->table;
	FUNCTION_NAME(reset)(table);

	hashing->last_used_key_index = 0;

	hashing->null_key_index = 0;

	/*
	 * Have to reset this because it's in the key body context which is also
	 * reset here.
	 */
	hashing->tmp_key_storage = NULL;
	hashing->num_tmp_key_storage_bytes = 0;
}

static void
FUNCTION_NAME(hash_strategy_prepare_for_batch)(GroupingPolicyHash *policy,
											   TupleTableSlot *vector_slot)
{
	uint16 nrows = 0;
	vector_slot_get_qual_result(vector_slot, &nrows);
	hash_strategy_output_key_alloc(policy, nrows);
	FUNCTION_NAME(key_hashing_prepare_for_batch)(policy, vector_slot);
}

/*
 * Fill the unique key indexes for all rows of the batch, using a hash table.
 */
static pg_attribute_always_inline void
FUNCTION_NAME(fill_offsets_impl)(BatchHashingParams params, int start_row, int end_row)
{
	HashingStrategy *restrict hashing = params.hashing;

	uint32 *restrict indexes = params.result_key_indexes;

	struct FUNCTION_NAME(hash) *restrict table = hashing->table;

	HASH_TABLE_KEY_TYPE prev_hash_table_key = { 0 };
	uint32 previous_key_index = 0;
	for (int row = start_row; row < end_row; row++)
	{
		if (!arrow_row_is_valid(params.batch_filter, row))
		{
			/* The row doesn't pass the filter. */
			DEBUG_PRINT("%p: row %d doesn't pass batch filter\n", hashing, row);
			continue;
		}

		/*
		 * Get the key for the given row. For some hashing strategies, the key
		 * that is used for the hash table is different from actual values of
		 * the grouping columns, termed "output key" here.
		 */
		bool key_valid = false;
		OUTPUT_KEY_TYPE output_key = { 0 };
		HASH_TABLE_KEY_TYPE hash_table_key = { 0 };
		FUNCTION_NAME(key_hashing_get_key)(params, row, &output_key, &hash_table_key, &key_valid);

		if (unlikely(!key_valid))
		{
			/* The key is null. */
			if (hashing->null_key_index == 0)
			{
				hashing->null_key_index = ++hashing->last_used_key_index;
			}
			indexes[row] = hashing->null_key_index;
			DEBUG_PRINT("%p: row %d null key index %d\n", hashing, row, hashing->null_key_index);
			continue;
		}

		if (likely(previous_key_index != 0) && KEY_EQUAL(hash_table_key, prev_hash_table_key))
		{
			/*
			 * In real data sets, we often see consecutive rows with the
			 * same value of a grouping column, so checking for this case
			 * improves performance. For multi-column keys, this is unlikely,
			 * but we currently often have suboptimal plans that use this policy
			 * as a GroupAggregate, so we still use this as an easy optimization
			 * for that case.
			 */
			indexes[row] = previous_key_index;
#ifndef NDEBUG
			params.policy->stat_consecutive_keys++;
#endif
			DEBUG_PRINT("%p: row %d consecutive key index %d\n", hashing, row, previous_key_index);
			continue;
		}

		/*
		 * Find the key using the hash table.
		 */
		bool found = false;
		FUNCTION_NAME(entry) *restrict entry = FUNCTION_NAME(insert)(table, hash_table_key, &found);
		if (!found)
		{
			/*
			 * New key, have to store it persistently.
			 */
			const uint32 index = ++hashing->last_used_key_index;
			entry->key_index = index;
			FUNCTION_NAME(key_hashing_store_new)(hashing, index, output_key);
			DEBUG_PRINT("%p: row %d new key index %d\n", hashing, row, index);
		}
		else
		{
			DEBUG_PRINT("%p: row %d old key index %d\n", hashing, row, entry->key_index);
		}
		indexes[row] = entry->key_index;

		previous_key_index = entry->key_index;
		prev_hash_table_key = entry->hash_table_key;
	}
}

static void
FUNCTION_NAME(fill_offsets)(GroupingPolicyHash *policy, TupleTableSlot *vector_slot, int start_row,
							int end_row)
{
	Assert((size_t) end_row <= policy->num_key_index_for_row);

	BatchHashingParams params = build_batch_hashing_params(policy, vector_slot);

	FUNCTION_NAME(fill_offsets_impl)(params, start_row, end_row);
}

HashingStrategy FUNCTION_NAME(strategy) = {
	.emit_key = FUNCTION_NAME(emit_key),
	.explain_name = EXPLAIN_NAME,
	.fill_offsets = FUNCTION_NAME(fill_offsets),
	.get_size_bytes = FUNCTION_NAME(get_size_bytes),
	.init = FUNCTION_NAME(hash_strategy_init),
	.prepare_for_batch = FUNCTION_NAME(hash_strategy_prepare_for_batch),
	.reset = FUNCTION_NAME(hash_strategy_reset),
};

#undef EXPLAIN_NAME
#undef KEY_VARIANT
#undef KEY_EQUAL
#undef OUTPUT_KEY_TYPE
#undef HASH_TABLE_KEY_TYPE
#undef USE_DICT_HASHING
