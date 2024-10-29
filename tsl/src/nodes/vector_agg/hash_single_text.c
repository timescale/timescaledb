/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Implementation of column hashing for a single text column.
 */

#include <postgres.h>

#include <common/hashfn.h>

#include "bytes_view.h"
#include "compression/arrow_c_data_interface.h"
#include "grouping_policy_hash.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

static BytesView
get_bytes_view(CompressedColumnValues *column_values, int arrow_row)
{
	const uint32 start = ((uint32 *) column_values->buffers[1])[arrow_row];
	const int32 value_bytes = ((uint32 *) column_values->buffers[1])[arrow_row + 1] - start;
	Assert(value_bytes >= 0);

	return (BytesView){ .len = value_bytes, .data = &((uint8 *) column_values->buffers[2])[start] };
}

static pg_attribute_always_inline void
single_text_get_key(HashingConfig config, int row, void *restrict key_ptr, bool *restrict valid)
{
	GroupingPolicyHash *policy = config.policy;
	Assert(policy->num_grouping_columns == 1);

	BytesView *restrict key = (BytesView *) key_ptr;

	if (unlikely(config.single_key.decompression_type == DT_Scalar))
	{
		/* Already stored. */
		key->len = VARSIZE_ANY_EXHDR(*config.single_key.output_value);
		key->data = (const uint8 *) VARDATA_ANY(*config.single_key.output_value);
		*valid = !*config.single_key.output_isnull;
	}
	else if (config.single_key.decompression_type == DT_ArrowText)
	{
		*key = get_bytes_view(&config.single_key, row);
		*valid = arrow_row_is_valid(config.single_key.buffers[0], row);
	}
	else if (config.single_key.decompression_type == DT_ArrowTextDict)
	{
		const int16 index = ((int16 *) config.single_key.buffers[3])[row];
		*key = get_bytes_view(&config.single_key, index);
		*valid = arrow_row_is_valid(config.single_key.buffers[0], row);
	}
	else
	{
		pg_unreachable();
	}

	*(uint64 *restrict) gp_hash_key_validity_bitmap(policy, policy->last_used_key_index + 1) =
		*valid;

	DEBUG_PRINT("%p consider key row %d key index %d is %d bytes: ",
				policy,
				row,
				policy->last_used_key_index + 1,
				key->len);
	for (size_t i = 0; i < key->len; i++)
	{
		DEBUG_PRINT("%.2x.", key->data[i]);
	}
	DEBUG_PRINT("\n");
}

static pg_attribute_always_inline BytesView
single_text_store_key(GroupingPolicyHash *restrict policy, BytesView key)
{
	const int total_bytes = key.len + VARHDRSZ;
	text *restrict stored = (text *) MemoryContextAlloc(policy->key_body_mctx, total_bytes);
	SET_VARSIZE(stored, total_bytes);
	memcpy(VARDATA(stored), key.data, key.len);
	key.data = (uint8 *) VARDATA(stored);
	gp_hash_output_keys(policy, policy->last_used_key_index)[0] = PointerGetDatum(stored);
	return key;
}

static pg_attribute_always_inline void
single_text_destroy_key(BytesView key)
{
	/* Noop. */
}

static pg_attribute_always_inline void single_text_fill_offsets_impl(HashingConfig config,
																	 int start_row, int end_row);

static pg_attribute_always_inline void
single_text_get_key_dict(HashingConfig config, int row, void *restrict key_ptr,
						 bool *restrict valid)
{
	GroupingPolicyHash *policy = config.policy;
	Assert(policy->num_grouping_columns == 1);

	BytesView *restrict key = (BytesView *) key_ptr;

	if (config.single_key.decompression_type == DT_ArrowTextDict)
	{
		*key = get_bytes_view(&config.single_key, row);
		*valid = true;
	}
	else
	{
		pg_unreachable();
	}

	*(uint64 *restrict) gp_hash_key_validity_bitmap(policy, policy->last_used_key_index + 1) = true;
}

static void
single_text_prepare_for_batch(GroupingPolicyHash *policy, DecompressBatchState *batch_state)
{
	policy->use_key_index_for_dict = false;

	if (policy->num_grouping_columns != 1)
	{
		return;
	}

	const GroupingColumn *g = &policy->grouping_columns[0];
	CompressedColumnValues *restrict single_key_column =
		&batch_state->compressed_columns[g->input_offset];
	if (single_key_column->decompression_type != DT_ArrowTextDict)
	{
		return;
	}

	const int dict_rows = single_key_column->arrow->dictionary->length;
	if ((size_t) dict_rows >
		arrow_num_valid(batch_state->vector_qual_result, batch_state->total_batch_rows))
	{
		return;
	}

	/*
	 * Initialize the array for storing the aggregate state offsets corresponding
	 * to a given batch row. We don't need the offsets for the previous batch
	 * that are currently stored there, so we don't need to use repalloc.
	 */
	if ((size_t) dict_rows > policy->num_key_index_for_dict)
	{
		if (policy->key_index_for_dict != NULL)
		{
			pfree(policy->key_index_for_dict);
		}
		policy->num_key_index_for_dict = dict_rows;
		policy->key_index_for_dict =
			palloc(sizeof(policy->key_index_for_dict[0]) * policy->num_key_index_for_dict);
	}

	/*
	 * Compute key indexes for the dictionary entries.
	 */
	HashingConfig config = {
		.policy = policy,
		.batch_filter = NULL,
		.single_key = *single_key_column,
		.num_grouping_columns = policy->num_grouping_columns,
		.grouping_columns = policy->grouping_columns,
		.compressed_columns = batch_state->compressed_columns,
		.get_key = single_text_get_key_dict,
		.result_key_indexes = policy->key_index_for_dict,
	};

	Assert((size_t) dict_rows <= policy->num_key_index_for_dict);

	bool have_null_key = false;
	if (batch_state->vector_qual_result != NULL)
	{
		const uint64 *row_filter = batch_state->vector_qual_result;
		uint64 *restrict dict_filter = policy->tmp_filter;
		const size_t dict_words = (dict_rows + 63) / 64;
		memset(dict_filter, 0, sizeof(*dict_filter) * dict_words);

		const int n = batch_state->total_batch_rows;
		for (int i = 0; i < n; i++)
		{
			const int16 index = ((int16 *) config.single_key.buffers[3])[i];
			const bool batch_row_valid = arrow_row_is_valid(row_filter, i);

			const size_t qword_index = index / 64;
			Assert(qword_index < dict_words);
			const size_t bit_index = index % 64;
			const uint64 mask = (batch_row_valid ? 1ull : 0ull) << bit_index;
			dict_filter[qword_index] |= mask;
		}

		config.batch_filter = dict_filter;

		if (config.single_key.arrow->null_count > 0)
		{
			Assert(config.single_key.buffers[0] != NULL);
			const size_t batch_words = (n + 63) / 64;
			for (size_t i = 0; i < batch_words; i++)
			{
				have_null_key |=
					(row_filter[i] & (~((uint64 *) config.single_key.buffers[0])[i])) != 0;
			}
		}
	}
	else
	{
		if (config.single_key.arrow->null_count > 0)
		{
			have_null_key = true;
		}
	}

	single_text_fill_offsets_impl(config, 0, dict_rows);

	/*
	 * The dictionary doesn't store nulls, so add the null key separately if we
	 * have one.
	 */
	if (have_null_key && policy->null_key_index == 0)
	{
		policy->null_key_index = ++policy->last_used_key_index;
		gp_hash_output_keys(policy, policy->null_key_index)[0] = PointerGetDatum(NULL);
		*(uint64 *restrict) gp_hash_key_validity_bitmap(policy, policy->null_key_index) = false;
	}

	policy->use_key_index_for_dict = true;

	DEBUG_PRINT("computed the dict offsets\n");
}

static pg_attribute_always_inline void
single_text_offsets_translate_impl(HashingConfig config, int start_row, int end_row)
{
	GroupingPolicyHash *policy = config.policy;
	Assert(policy->use_key_index_for_dict);

	uint32 *restrict indexes_for_rows = config.result_key_indexes;
	uint32 *restrict indexes_for_dict = policy->key_index_for_dict;

	for (int row = start_row; row < end_row; row++)
	{
		bool passes = arrow_row_is_valid(config.batch_filter, row);
		bool row_valid = arrow_row_is_valid(config.single_key.buffers[0], row);
		const int16 dict_index = ((int16 *) config.single_key.buffers[3])[row];

		if (!passes)
		{
			continue;
		}

		if (row_valid)
		{
			indexes_for_rows[row] = indexes_for_dict[dict_index];
		}
		else
		{
			indexes_for_rows[row] = policy->null_key_index;
		}

		Assert(indexes_for_rows[row] != 0);
	}
}

#define APPLY_FOR_BATCH_FILTER(X, NAME, COND)                                                      \
	X(NAME##_all, (COND) && (config.batch_filter == NULL))                                         \
	X(NAME##_filter, (COND) && (config.batch_filter != NULL))

#define APPLY_FOR_VALIDITY(X, NAME, COND)                                                          \
	APPLY_FOR_BATCH_FILTER(X, NAME##_notnull, (COND) && (config.single_key.buffers[0] == NULL))    \
	APPLY_FOR_BATCH_FILTER(X, NAME##_nullable, (COND) && (config.single_key.buffers[0] != NULL))

#define APPLY_FOR_SPECIALIZATIONS(X) APPLY_FOR_VALIDITY(X, single_text_offsets_translate, true)

#define DEFINE(NAME, CONDITION)                                                                    \
	static pg_noinline void NAME(HashingConfig config, int start_row, int end_row)                 \
	{                                                                                              \
		if (!(CONDITION))                                                                          \
		{                                                                                          \
			pg_unreachable();                                                                      \
		}                                                                                          \
                                                                                                   \
		single_text_offsets_translate_impl(config, start_row, end_row);                            \
	}

APPLY_FOR_SPECIALIZATIONS(DEFINE)

#undef DEFINE

static void
single_text_offsets_translate(HashingConfig config, int start_row, int end_row)
{
#define DISPATCH(NAME, CONDITION)                                                                  \
	if (CONDITION)                                                                                 \
	{                                                                                              \
		NAME(config, start_row, end_row);                                                          \
	}                                                                                              \
	else

	APPLY_FOR_SPECIALIZATIONS(DISPATCH) { pg_unreachable(); }
#undef DISPATCH
}

#undef APPLY_FOR_SPECIALIZATIONS
#undef APPLY_FOR_VALIDITY
#undef APPLY_FOR_BATCH_FILTER

#define EXPLAIN_NAME "single text"
#define KEY_VARIANT single_text
#define KEY_HASH(X) hash_bytes_view(X)
#define KEY_EQUAL(a, b) (a.len == b.len && memcmp(a.data, b.data, a.len) == 0)
#define STORE_HASH
#define CTYPE BytesView
#define HAVE_PREPARE_FUNCTION
#include "hash_table_functions_impl.c"
