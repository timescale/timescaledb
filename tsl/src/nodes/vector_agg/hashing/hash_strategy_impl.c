/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "batch_hashing_params.h"

#include "import/umash.h"

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

	HASH_TABLE_KEY_TYPE hash_table_key;
} FUNCTION_NAME(entry);

// #define SH_FILLFACTOR (0.5)
#define SH_PREFIX KEY_VARIANT
#define SH_ELEMENT_TYPE FUNCTION_NAME(entry)
#define SH_KEY_TYPE HASH_TABLE_KEY_TYPE
#define SH_KEY hash_table_key
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

static uint64
FUNCTION_NAME(get_size_bytes)(HashingStrategy *hashing)
{
	struct FUNCTION_NAME(hash) *hash = (struct FUNCTION_NAME(hash) *) hashing->table;
	return hash->members * sizeof(FUNCTION_NAME(entry));
}

static void
FUNCTION_NAME(init)(HashingStrategy *hashing, GroupingPolicyHash *policy)
{
	hashing->table = FUNCTION_NAME(create)(CurrentMemoryContext, policy->num_agg_state_rows, NULL);
#ifdef UMASH
	hashing->umash_params = palloc0(sizeof(struct umash_params));
	umash_params_derive(hashing->umash_params, 0xabcdef1234567890ull, NULL);
#endif
}

static void
FUNCTION_NAME(reset_strategy)(HashingStrategy *hashing)
{
	struct FUNCTION_NAME(hash) *table = (struct FUNCTION_NAME(hash) *) hashing->table;
	FUNCTION_NAME(reset)(table);
	hashing->null_key_index = 0;
}

/*
 * Fill the unique key indexes for all rows using a hash table.
 */
static pg_attribute_always_inline void
FUNCTION_NAME(fill_offsets_impl)(BatchHashingParams params, int start_row, int end_row)
{
	GroupingPolicyHash *policy = params.policy;
	HashingStrategy *hashing = &policy->hashing;

	uint32 *restrict indexes = params.result_key_indexes;

	struct FUNCTION_NAME(hash) *restrict table = hashing->table;

	HASH_TABLE_KEY_TYPE prev_hash_table_key;
	uint32 previous_key_index = 0;
	for (int row = start_row; row < end_row; row++)
	{
		if (!arrow_row_is_valid(params.batch_filter, row))
		{
			/* The row doesn't pass the filter. */
			DEBUG_PRINT("%p: row %d doesn't pass batch filter\n", policy, row);
			continue;
		}

		bool key_valid = false;
		OUTPUT_KEY_TYPE output_key = { 0 };
		HASH_TABLE_KEY_TYPE hash_table_key = { 0 };
		FUNCTION_NAME(get_key)(params, row, &output_key, &hash_table_key, &key_valid);

		if (unlikely(!key_valid))
		{
			/* The key is null. */
			if (hashing->null_key_index == 0)
			{
				hashing->null_key_index = ++policy->last_used_key_index;
			}
			indexes[row] = hashing->null_key_index;
			DEBUG_PRINT("%p: row %d null key index %d\n", policy, row, hashing->null_key_index);
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
			policy->stat_consecutive_keys++;
			DEBUG_PRINT("%p: row %d consecutive key index %d\n", policy, row, previous_key_index);
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
			const uint32 index = ++policy->last_used_key_index;
			entry->key_index = index;
			FUNCTION_NAME(store_new_output_key)(policy, index, output_key);
			DEBUG_PRINT("%p: row %d new key index %d\n", policy, row, index);
		}
		else
		{
			DEBUG_PRINT("%p: row %d old key index %d\n", policy, row, entry->key_index);
		}
		indexes[row] = entry->key_index;

		previous_key_index = entry->key_index;
		prev_hash_table_key = entry->hash_table_key;
	}
}

/*
 * For some configurations of hashing, we want to generate dedicated
 * implementations that will be more efficient. For example, for 2-byte keys
 * when all the batch and key rows are valid.
 */
#define APPLY_FOR_BATCH_FILTER(X, NAME, COND)                                                      \
	X(NAME##_nofilter, (COND) && (params.batch_filter == NULL))                                    \
	X(NAME##_filter, (COND) && (params.batch_filter != NULL))

#define APPLY_FOR_NULLABILITY(X, NAME, COND)                                                       \
	APPLY_FOR_BATCH_FILTER(X, NAME##_notnull, (COND) && params.single_key.buffers[0] == NULL)      \
	APPLY_FOR_BATCH_FILTER(X, NAME##_nullable, (COND) && params.single_key.buffers[0] != NULL)

#define APPLY_FOR_SCALARS(X, NAME, COND)                                                           \
	APPLY_FOR_BATCH_FILTER(X,                                                                      \
						   NAME##_noscalar_notnull,                                                \
						   (COND) && !params.have_scalar_or_nullable_columns)                      \
	APPLY_FOR_BATCH_FILTER(X,                                                                      \
						   NAME##_scalar_or_nullable,                                              \
						   (COND) && params.have_scalar_or_nullable_columns)

#define APPLY_FOR_TYPE(X, NAME, COND)                                                              \
	APPLY_FOR_NULLABILITY(X,                                                                       \
						  NAME##_byval,                                                            \
						  (COND) &&                                                                \
							  params.single_key.decompression_type == sizeof(OUTPUT_KEY_TYPE))     \
	APPLY_FOR_NULLABILITY(X,                                                                       \
						  NAME##_text,                                                             \
						  (COND) && params.single_key.decompression_type == DT_ArrowText)          \
	APPLY_FOR_NULLABILITY(X,                                                                       \
						  NAME##_dict,                                                             \
						  (COND) && params.single_key.decompression_type == DT_ArrowTextDict)      \
	APPLY_FOR_SCALARS(X, NAME##_multi, (COND) && params.single_key.decompression_type == DT_Invalid)

#define APPLY_FOR_SPECIALIZATIONS(X) APPLY_FOR_TYPE(X, index, true)

#define DEFINE(NAME, CONDITION)                                                                    \
	static pg_noinline void FUNCTION_NAME(                                                         \
		NAME)(BatchHashingParams params, int start_row, int end_row)                               \
	{                                                                                              \
		if (!(CONDITION))                                                                          \
		{                                                                                          \
			pg_unreachable();                                                                      \
		}                                                                                          \
                                                                                                   \
		FUNCTION_NAME(fill_offsets_impl)(params, start_row, end_row);                              \
	}

APPLY_FOR_SPECIALIZATIONS(DEFINE)

#undef DEFINE

static void
FUNCTION_NAME(dispatch_for_params)(BatchHashingParams params, int start_row, int end_row)
{
	if (params.num_grouping_columns == 0)
	{
		pg_unreachable();
	}

	if ((params.num_grouping_columns == 1) != (params.single_key.decompression_type != DT_Invalid))
	{
		pg_unreachable();
	}

#define DISPATCH(NAME, CONDITION)                                                                  \
	if (CONDITION)                                                                                 \
	{                                                                                              \
		FUNCTION_NAME(NAME)(params, start_row, end_row);                                           \
	}                                                                                              \
	else

	APPLY_FOR_SPECIALIZATIONS(DISPATCH)
	{
		/* Use a generic implementation if no specializations matched. */
		FUNCTION_NAME(fill_offsets_impl)(params, start_row, end_row);
	}
#undef DISPATCH
}

#undef APPLY_FOR_SPECIALIZATIONS

/*
 * In some special cases we call a more efficient specialization of the grouping
 * function.
 */
static void
FUNCTION_NAME(fill_offsets)(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
							int start_row, int end_row)
{
	Assert((size_t) end_row <= policy->num_key_index_for_row);

	BatchHashingParams params = build_batch_hashing_params(policy, batch_state);

#ifdef USE_DICT_HASHING
	if (policy->use_key_index_for_dict)
	{
		Assert(params.single_key.decompression_type == DT_ArrowTextDict);
		single_text_offsets_translate(params, start_row, end_row);
		return;
	}
#endif

	FUNCTION_NAME(dispatch_for_params)(params, start_row, end_row);
}

HashingStrategy FUNCTION_NAME(strategy) = {
	.emit_key = FUNCTION_NAME(emit_key),
	.explain_name = EXPLAIN_NAME,
	.fill_offsets = FUNCTION_NAME(fill_offsets),
	.get_size_bytes = FUNCTION_NAME(get_size_bytes),
	.init = FUNCTION_NAME(init),
	.prepare_for_batch = FUNCTION_NAME(prepare_for_batch),
	.reset = FUNCTION_NAME(reset_strategy),
};

#undef EXPLAIN_NAME
#undef KEY_VARIANT
#undef KEY_EQUAL
#undef STORE_HASH
#undef OUTPUT_KEY_TYPE
#undef HASH_TABLE_KEY_TYPE
#undef DATUM_TO_OUTPUT_KEY
#undef OUTPUT_KEY_TO_DATUM
#undef UMASH
#undef USE_DICT_HASHING
