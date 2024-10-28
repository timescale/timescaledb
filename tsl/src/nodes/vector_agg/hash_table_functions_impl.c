/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME_HELPER(X, Y) FUNCTION_NAME_HELPER2(X, Y)
#define FUNCTION_NAME(Y) FUNCTION_NAME_HELPER(KEY_VARIANT, Y)

/*
 * The hash table maps the value of the grouping key to its unique index.
 * We don't store any extra information here, because we're accessing the memory
 * of the hash table randomly, and want it to be as small as possible to fit the
 * caches.
 */
typedef struct
{
	CTYPE key;

#ifdef STORE_HASH
	/*
	 * We store hash for non-POD types, because it's slow to recalculate it
	 * on inserts for the existing values.
	 */
	uint32 hash;
#endif

	/* Key index 0 is invalid. */
	uint32 key_index;
} FUNCTION_NAME(entry);

#define SH_PREFIX KEY_VARIANT
#define SH_ELEMENT_TYPE FUNCTION_NAME(entry)
#define SH_KEY_TYPE CTYPE
#define SH_KEY key
#define SH_HASH_KEY(tb, key) KEY_HASH(key)
#define SH_EQUAL(tb, a, b) KEY_EQUAL(a, b)
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#define SH_ENTRY_EMPTY(entry) ((entry)->key_index == 0)
#ifdef STORE_HASH
#define SH_GET_HASH(tb, entry) entry->hash
#define SH_STORE_HASH
#endif
#include "import/ts_simplehash.h"

struct FUNCTION_NAME(hash);

static uint32
FUNCTION_NAME(get_num_keys)(void *table)
{
	struct FUNCTION_NAME(hash) *hash = (struct FUNCTION_NAME(hash) *) table;
	return hash->members;
}

static uint64
FUNCTION_NAME(get_size_bytes)(void *table)
{
	struct FUNCTION_NAME(hash) *hash = (struct FUNCTION_NAME(hash) *) table;
	return hash->members * sizeof(FUNCTION_NAME(entry));
}

/*
 * Fill the unique key indexes for all rows using a hash table.
 */
static pg_attribute_always_inline void
FUNCTION_NAME(impl)(GroupingPolicyHash *restrict policy, DecompressBatchState *restrict batch_state,
					CompressedColumnValues *single_key_column,
					int start_row, int end_row)
{
	uint32 *restrict indexes = policy->key_index_for_row;
	Assert((size_t) end_row <= policy->num_key_index_for_row);

	struct FUNCTION_NAME(hash) *restrict table = policy->table;

#ifdef CHECK_PREVIOUS_KEY
	CTYPE previous_key;
	uint32 previous_key_index = 0;
#endif
	for (int row = start_row; row < end_row; row++)
	{
		bool key_valid = false;
		CTYPE key = { 0 };
		FUNCTION_NAME(get_key)(policy, batch_state, single_key_column, row, &key, &key_valid);

		if (!arrow_row_is_valid(batch_state->vector_qual_result, row))
		{
			DEBUG_PRINT("%p: row %d doesn't pass batch filter\n", policy, row);
			/* The row doesn't pass the filter. */
			FUNCTION_NAME(destroy_key)(key);
			continue;
		}

		if (unlikely(!key_valid))
		{
			/* The key is null. */
			if (policy->null_key_index == 0)
			{
				policy->null_key_index = ++policy->last_used_key_index;
			}
			indexes[row] = policy->null_key_index;
			DEBUG_PRINT("%p: row %d null key index %d\n", policy, row, policy->null_key_index);
			FUNCTION_NAME(destroy_key)(key);
			continue;
		}

#ifdef CHECK_PREVIOUS_KEY
		if (likely(previous_key_index != 0) && KEY_EQUAL(key, previous_key))
		{
			/*
			 * In real data sets, we often see consecutive rows with the
			 * same value of a grouping column, so checking for this case
			 * improves performance. For multi-column keys, this is unlikely and
			 * so this check is disabled.
			 */
			indexes[row] = previous_key_index;
			FUNCTION_NAME(destroy_key)(key);
#ifndef NDEBUG
			policy->stat_consecutive_keys++;
#endif
			DEBUG_PRINT("%p: row %d consecutive key index %d\n", policy, row, previous_key_index);
			continue;
		}
#endif

		/*
		 * Find the key using the hash table.
		 */
		bool found = false;
		FUNCTION_NAME(entry) *restrict entry = FUNCTION_NAME(insert)(table, key, &found);
		if (!found)
		{
			/*
			 * New key, have to store it persistently.
			 */
			const int index = ++policy->last_used_key_index;
			entry->key = FUNCTION_NAME(store_key)(policy, key);
			entry->key_index = index;
			DEBUG_PRINT("%p: row %d new key index %d\n", policy, row, index);
		}
		else
		{
			DEBUG_PRINT("%p: row %d old key index %d\n", policy, row, entry->key_index);
			FUNCTION_NAME(destroy_key)(key);
		}
		indexes[row] = entry->key_index;

#ifdef CHECK_PREVIOUS_KEY
		previous_key_index = entry->key_index;
		previous_key = entry->key;
#endif
	}
}

/*
 * Nudge the compiler to generate separate implementations for different key
 * decompression types.
 */
static pg_attribute_always_inline void
FUNCTION_NAME(dispatch_type)(GroupingPolicyHash *restrict policy,
							 DecompressBatchState *restrict batch_state, int start_row, int end_row)
{
	if (policy->num_grouping_columns == 1)
	{
		GroupingColumn *g = &policy->grouping_columns[0];
		CompressedColumnValues *restrict single_key_column = &batch_state->compressed_columns[g->input_offset];

		if (unlikely(single_key_column->decompression_type == DT_Scalar))
		{
			FUNCTION_NAME(impl)(policy, batch_state, single_key_column, start_row, end_row);
		}
		else if (single_key_column->decompression_type == DT_ArrowText)
		{
			FUNCTION_NAME(impl)(policy, batch_state, single_key_column, start_row, end_row);
		}
		else if (single_key_column->decompression_type == DT_ArrowTextDict)
		{
			FUNCTION_NAME(impl)(policy, batch_state, single_key_column, start_row, end_row);
		}
		else
		{
			FUNCTION_NAME(impl)(policy, batch_state, single_key_column, start_row, end_row);
		}
	}
	else
	{
		FUNCTION_NAME(impl)(policy, batch_state, NULL, start_row, end_row);
	}
}

/*
 * Nudge the compiler to generate separate implementation for the important case
 * where the entire batch matches and the key has no null values, and the
 * unimportant corner case when we have a scalar column.
 */
static void
FUNCTION_NAME(fill_offsets)(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
							int start_row, int end_row)
{
	if (batch_state->vector_qual_result == NULL)
	{
		FUNCTION_NAME(dispatch_type)(policy, batch_state, start_row, end_row);
	}
	else
	{
		FUNCTION_NAME(dispatch_type)(policy, batch_state, start_row, end_row);
	}
}

HashTableFunctions FUNCTION_NAME(functions) = {
	.create = (void *(*) (MemoryContext, uint32, void *) ) FUNCTION_NAME(create),
	.reset = (void (*)(void *)) FUNCTION_NAME(reset),
	.get_num_keys = FUNCTION_NAME(get_num_keys),
	.get_size_bytes = FUNCTION_NAME(get_size_bytes),
	.fill_offsets = FUNCTION_NAME(fill_offsets),
	.explain_name = EXPLAIN_NAME,
};

#undef EXPLAIN_NAME
#undef KEY_VARIANT
#undef KEY_BYTES
#undef KEY_HASH
#undef KEY_EQUAL
#undef STORE_HASH
#undef CHECK_PREVIOUS_KEY
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM

#undef FUNCTION_NAME_HELPER2
#undef FUNCTION_NAME_HELPER
#undef FUNCTION_NAME
