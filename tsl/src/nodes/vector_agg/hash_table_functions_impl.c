/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER2(X, Y) X##_##Y
#define FUNCTION_NAME_HELPER(X, Y) FUNCTION_NAME_HELPER2(X, Y)
#define FUNCTION_NAME(Y) FUNCTION_NAME_HELPER(KEY_VARIANT, Y)

/*
 * For the hash table, use the generic Datum key that is mapped to the aggregate
 * state index.
 */
typedef struct
{
	CTYPE key;
#ifdef SH_STORE_HASH
	uint32 hash;
#endif
	uint32 agg_state_index;
} FUNCTION_NAME(entry);

#ifdef SH_STORE_HASH
#define SH_GET_HASH(tb, entry) entry->hash
#endif

#define SH_PREFIX KEY_VARIANT
#define SH_ELEMENT_TYPE FUNCTION_NAME(entry)
#define SH_KEY_TYPE CTYPE
#define SH_KEY key
#define SH_HASH_KEY(tb, key) KEY_HASH(key)
#define SH_EQUAL(tb, a, b) KEY_EQUAL(a, b)
#define SH_SCOPE static inline
#define SH_DECLARE
#define SH_DEFINE
#define SH_ENTRY_EMPTY(entry) ((entry)->agg_state_index == 0)
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
 * Fill the aggregation state offsets for all rows using a hash table.
 */
static pg_attribute_always_inline uint32
FUNCTION_NAME(impl)(GroupingPolicyHash *policy, CompressedColumnValues column,
					const uint64 *restrict filter, uint32 next_unused_state_index, int start_row,
					int end_row)
{
	uint32 *restrict offsets = policy->offsets;
	Assert((size_t) end_row <= policy->num_allocated_offsets);

	struct FUNCTION_NAME(hash) *restrict table = policy->table;

	CTYPE last_key;
	uint32 last_key_index = 0;
	for (int row = start_row; row < end_row; row++)
	{
		bool key_valid = false;
		CTYPE key = { 0 };
		FUNCTION_NAME(get_key)(column, row, &key, &key_valid);

		if (!arrow_row_is_valid(filter, row))
		{
			/* The row doesn't pass the filter. */
			continue;
		}

		if (unlikely(!key_valid))
		{
			/* The key is null. */
			policy->have_null_key = true;
			offsets[row] = 1;
			continue;
		}

		if (likely(last_key_index != 0) && KEY_EQUAL(key, last_key))
		{
			/*
			 * In real data sets, we often see consecutive rows with the
			 * same key, so checking for this case improves performance.
			 */
			Assert(last_key_index >= 2);
			offsets[row] = last_key_index;
#ifndef NDEBUG
			policy->stat_consecutive_keys++;
#endif
			continue;
		}

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
			const int index = next_unused_state_index++;
			entry->key = FUNCTION_NAME(
				store_key)(key, &((Datum *restrict) policy->keys)[index], policy->key_body_mctx);
			entry->agg_state_index = index;
		}
		offsets[row] = entry->agg_state_index;

		last_key_index = entry->agg_state_index;
		last_key = key;
	}

	return next_unused_state_index;
}

static pg_attribute_always_inline uint32
FUNCTION_NAME(dispatch_type)(GroupingPolicyHash *policy, CompressedColumnValues column,
							 const uint64 *restrict filter, uint32 next_unused_state_index,
							 int start_row, int end_row)
{
	if (unlikely(column.decompression_type == DT_Scalar))
	{
		return FUNCTION_NAME(
			impl)(policy, column, filter, next_unused_state_index, start_row, end_row);
	}
	else if (column.decompression_type == DT_ArrowText)
	{
		return FUNCTION_NAME(
			impl)(policy, column, filter, next_unused_state_index, start_row, end_row);
	}
	else if (column.decompression_type == DT_ArrowTextDict)
	{
		return FUNCTION_NAME(
			impl)(policy, column, filter, next_unused_state_index, start_row, end_row);
	}
	else
	{
		return FUNCTION_NAME(
			impl)(policy, column, filter, next_unused_state_index, start_row, end_row);
	}
}

/*
 * This function exists just to nudge the compiler to generate separate
 * implementation for the important case where the entire batch matches and the
 * key has no null values, and the unimportant corner case when we have a scalar
 * column.
 */
static uint32
FUNCTION_NAME(fill_offsets)(GroupingPolicyHash *policy, DecompressBatchState *batch_state,
							uint32 next_unused_state_index, int start_row, int end_row)
{
	Assert(list_length(policy->output_grouping_columns) == 1);
	GroupingColumn *g = linitial(policy->output_grouping_columns);
	CompressedColumnValues column = batch_state->compressed_columns[g->input_offset];
	const uint64 *restrict filter = batch_state->vector_qual_result;

	if (filter == NULL && column.buffers[0] == NULL)
	{
		next_unused_state_index = FUNCTION_NAME(
			dispatch_type)(policy, column, filter, next_unused_state_index, start_row, end_row);
	}
	else if (filter != NULL && column.buffers[0] == NULL)
	{
		next_unused_state_index = FUNCTION_NAME(
			dispatch_type)(policy, column, filter, next_unused_state_index, start_row, end_row);
	}
	else if (filter == NULL && column.buffers[0] != NULL)
	{
		next_unused_state_index = FUNCTION_NAME(
			dispatch_type)(policy, column, filter, next_unused_state_index, start_row, end_row);
	}
	else if (filter != NULL && column.buffers[0] != NULL)
	{
		next_unused_state_index = FUNCTION_NAME(
			dispatch_type)(policy, column, filter, next_unused_state_index, start_row, end_row);
	}
	else
	{
		Assert(false);
	}

	policy->stat_input_total_rows += batch_state->total_batch_rows;
	policy->stat_input_valid_rows += arrow_num_valid(filter, batch_state->total_batch_rows);
	return next_unused_state_index;
}

HashTableFunctions FUNCTION_NAME(functions) = {
	.create = (void *(*) (MemoryContext, uint32, void *) ) FUNCTION_NAME(create),
	.reset = (void (*)(void *)) FUNCTION_NAME(reset),
	.get_num_keys = FUNCTION_NAME(get_num_keys),
	.get_size_bytes = FUNCTION_NAME(get_size_bytes),
	.fill_offsets = FUNCTION_NAME(fill_offsets),
};

#undef KEY_VARIANT
#undef KEY_BYTES
#undef KEY_HASH
#undef KEY_EQUAL
#undef CTYPE
#undef DATUM_TO_CTYPE
#undef CTYPE_TO_DATUM

#undef FUNCTION_NAME_HELPER2
#undef FUNCTION_NAME_HELPER
#undef FUNCTION_NAME
