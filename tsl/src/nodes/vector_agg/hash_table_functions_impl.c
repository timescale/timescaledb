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
FUNCTION_NAME(fill_offsets_impl)(GroupingPolicyHash *restrict policy,
								 DecompressBatchState *restrict batch_state, HashingConfig config,
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
		if (!arrow_row_is_valid(config.batch_filter, row))
		{
			/* The row doesn't pass the filter. */
			DEBUG_PRINT("%p: row %d doesn't pass batch filter\n", policy, row);
			continue;
		}

		bool key_valid = false;
		CTYPE key = { 0 };
		FUNCTION_NAME(get_key)(policy, batch_state, config, row, &key, &key_valid);

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
 * For some configurations of hashing, we want to generate dedicated
 * implementations that will be more efficient. For example, for 2-byte keys
 * when all the batch and key rows are valid.
 */
#define APPLY_FOR_BATCH_FILTER(X, NAME, COND)                                                      \
	X(NAME##_all, (COND) && (config.batch_filter == NULL))                                         \
	X(NAME##_filter, (COND) && (config.batch_filter != NULL))

#define APPLY_FOR_TYPE(X, NAME, COND)                                                              \
	APPLY_FOR_BATCH_FILTER(X,                                                                      \
						   NAME##_fixed,                                                           \
						   (COND) && (config.single_key.decompression_type == sizeof(CTYPE)))      \
	APPLY_FOR_BATCH_FILTER(X,                                                                      \
						   NAME##_text,                                                            \
						   (COND) && (config.single_key.decompression_type == DT_ArrowText))       \
	APPLY_FOR_BATCH_FILTER(X,                                                                      \
						   NAME##_dict,                                                            \
						   (COND) && (config.single_key.decompression_type == DT_ArrowTextDict))

#define APPLY_FOR_VALIDITY(X, NAME, COND)                                                          \
	APPLY_FOR_TYPE(X, NAME, (COND) && (config.single_key.buffers[0] == NULL))                      \
	APPLY_FOR_TYPE(X, NAME##_nullable, (COND) && (config.single_key.buffers[0] != NULL))

#define APPLY_FOR_SPECIALIZATIONS(X) APPLY_FOR_VALIDITY(X, , true)

#define DEFINE(NAME, CONDITION)                                                                    \
	static pg_noinline void FUNCTION_NAME(NAME)(GroupingPolicyHash *restrict policy,               \
												DecompressBatchState *restrict batch_state,        \
												HashingConfig config,                              \
												int start_row,                                     \
												int end_row)                                       \
	{                                                                                              \
		if (!(CONDITION))                                                                          \
		{                                                                                          \
			pg_unreachable();                                                                      \
		}                                                                                          \
                                                                                                   \
		FUNCTION_NAME(fill_offsets_impl)(policy, batch_state, config, start_row, end_row);         \
	}

APPLY_FOR_SPECIALIZATIONS(DEFINE)

#undef DEFINE

/*
 * In some special cases we call a more efficient specialization of the grouping
 * function.
 */
static void
FUNCTION_NAME(fill_offsets)(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
							int start_row, int end_row)
{
	HashingConfig config = {
		.batch_filter = batch_state->vector_qual_result,
	};

	if (policy->num_grouping_columns == 1)
	{
		const GroupingColumn *g = &policy->grouping_columns[0];
		CompressedColumnValues *restrict single_key_column =
			&batch_state->compressed_columns[g->input_offset];

		config.single_key = *single_key_column;
	}

#define DISPATCH(NAME, CONDITION)                                                                  \
	if (CONDITION)                                                                                 \
	{                                                                                              \
		FUNCTION_NAME(NAME)(policy, batch_state, config, start_row, end_row);                      \
	}                                                                                              \
	else

	APPLY_FOR_SPECIALIZATIONS(DISPATCH)
	{
		/* Use a generic implementation if no specializations matched. */
		FUNCTION_NAME(fill_offsets_impl)(policy, batch_state, config, start_row, end_row);
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
