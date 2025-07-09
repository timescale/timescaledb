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

#include "compression/arrow_c_data_interface.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"
#include "nodes/vector_agg/grouping_policy_hash.h"
#include "template_helper.h"

#include "batch_hashing_params.h"

#include "umash_fingerprint_key.h"

#define EXPLAIN_NAME "single text"
#define KEY_VARIANT single_text
#define OUTPUT_KEY_TYPE BytesView

static void
single_text_key_hashing_init(HashingStrategy *hashing)
{
	hashing->umash_params = umash_key_hashing_init();
}

typedef struct BytesView
{
	const uint8 *data;
	uint32 len;
} BytesView;

static BytesView
get_bytes_view(CompressedColumnValues *column_values, int arrow_row)
{
	const uint32 start = ((uint32 *) column_values->buffers[1])[arrow_row];
	const int32 value_bytes = ((uint32 *) column_values->buffers[1])[arrow_row + 1] - start;
	Assert(value_bytes >= 0);

	return (BytesView){ .len = value_bytes, .data = &((uint8 *) column_values->buffers[2])[start] };
}

static pg_attribute_always_inline void
single_text_key_hashing_get_key(BatchHashingParams params, int row, void *restrict output_key_ptr,
								void *restrict hash_table_key_ptr, bool *restrict valid)
{
	Assert(params.policy->num_grouping_columns == 1);

	BytesView *restrict output_key = (BytesView *) output_key_ptr;
	HASH_TABLE_KEY_TYPE *restrict hash_table_key = (HASH_TABLE_KEY_TYPE *) hash_table_key_ptr;

	if (unlikely(params.single_grouping_column.decompression_type == DT_Scalar))
	{
		*valid = !*params.single_grouping_column.output_isnull;
		if (*valid)
		{
			output_key->len = VARSIZE_ANY_EXHDR(*params.single_grouping_column.output_value);
			output_key->data =
				(const uint8 *) VARDATA_ANY(*params.single_grouping_column.output_value);
		}
		else
		{
			output_key->len = 0;
			output_key->data = NULL;
		}
	}
	else if (params.single_grouping_column.decompression_type == DT_ArrowText)
	{
		*output_key = get_bytes_view(&params.single_grouping_column, row);
		*valid = arrow_row_is_valid(params.single_grouping_column.buffers[0], row);
	}
	else if (params.single_grouping_column.decompression_type == DT_ArrowTextDict)
	{
		const int16 index = ((int16 *) params.single_grouping_column.buffers[3])[row];
		*output_key = get_bytes_view(&params.single_grouping_column, index);
		*valid = arrow_row_is_valid(params.single_grouping_column.buffers[0], row);
	}
	else
	{
		pg_unreachable();
	}

	DEBUG_PRINT("%p consider key row %d key index %d is %d bytes: ",
				hashing,
				row,
				hashing->last_used_key_index + 1,
				output_key->len);
	for (size_t i = 0; i < output_key->len; i++)
	{
		DEBUG_PRINT("%.2x.", output_key->data[i]);
	}
	DEBUG_PRINT("\n");

	const struct umash_fp fp = umash_fprint(params.policy->hashing.umash_params,
											/* seed = */ ~0ULL,
											output_key->data,
											output_key->len);
	*hash_table_key = umash_fingerprint_get_key(fp);
}

static pg_attribute_always_inline void
single_text_key_hashing_store_new(HashingStrategy *restrict hashing, uint32 new_key_index,
								  BytesView output_key)
{
	const int total_bytes = output_key.len + VARHDRSZ;
	text *restrict stored = (text *) MemoryContextAlloc(hashing->key_body_mctx, total_bytes);
	SET_VARSIZE(stored, total_bytes);
	memcpy(VARDATA(stored), output_key.data, output_key.len);
	hashing->output_keys[new_key_index] = PointerGetDatum(stored);
}

/*
 * We use the standard single-key key output functions.
 */
static void
single_text_emit_key(GroupingPolicyHash *policy, uint32 current_key,
					 TupleTableSlot *aggregated_slot)
{
	return hash_strategy_output_key_single_emit(policy, current_key, aggregated_slot);
}

/*
 * We use a special batch preparation function to sometimes hash the dictionary-
 * encoded column using the dictionary.
 */

#define USE_DICT_HASHING

static pg_attribute_always_inline void single_text_dispatch_for_params(BatchHashingParams params,
																	   int start_row, int end_row);

static void
single_text_key_hashing_prepare_for_batch(GroupingPolicyHash *policy, TupleTableSlot *vector_slot)
{
	/*
	 * Determine whether we're going to use the dictionary for hashing.
	 */
	policy->use_key_index_for_dict = false;

	BatchHashingParams params = build_batch_hashing_params(policy, vector_slot);
	if (params.single_grouping_column.decompression_type != DT_ArrowTextDict)
	{
		return;
	}

	uint16 batch_rows;
	const uint64 *row_filter = vector_slot_get_qual_result(vector_slot, &batch_rows);

	const int dict_rows = params.single_grouping_column.arrow->dictionary->length;
	if ((size_t) dict_rows > arrow_num_valid(row_filter, batch_rows))
	{
		return;
	}

	/*
	 * Remember which aggregation states have already existed, and which we have
	 * to initialize. State index zero is invalid.
	 */
	const uint32 last_initialized_key_index = params.hashing->last_used_key_index;
	Assert(last_initialized_key_index <= policy->num_allocated_per_key_agg_states);

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
	 * We shouldn't add the dictionary entries that are not used by any matching
	 * rows. Translate the batch filter bitmap to dictionary rows.
	 */
	if (row_filter != NULL)
	{
		uint64 *restrict dict_filter = policy->tmp_filter;
		const size_t dict_words = (dict_rows + 63) / 64;
		memset(dict_filter, 0, sizeof(*dict_filter) * dict_words);

		bool *restrict tmp = (bool *) policy->key_index_for_dict;
		Assert(sizeof(*tmp) <= sizeof(*policy->key_index_for_dict));
		memset(tmp, 0, sizeof(*tmp) * dict_rows);

		int outer;
		for (outer = 0; outer < batch_rows / 64; outer++)
		{
#define INNER_LOOP(INNER_MAX)                                                                      \
	const uint64 word = row_filter[outer];                                                         \
	for (int inner = 0; inner < INNER_MAX; inner++)                                                \
	{                                                                                              \
		const int16 index =                                                                        \
			((int16 *) params.single_grouping_column.buffers[3])[outer * 64 + inner];              \
		tmp[index] = tmp[index] || (word & (1ull << inner));                                       \
	}

			INNER_LOOP(64)
		}

		if (batch_rows % 64)
		{
			INNER_LOOP(batch_rows % 64)
		}
#undef INNER_LOOP

		for (outer = 0; outer < dict_rows / 64; outer++)
		{
#define INNER_LOOP(INNER_MAX)                                                                      \
	uint64 word = 0;                                                                               \
	for (int inner = 0; inner < INNER_MAX; inner++)                                                \
	{                                                                                              \
		word |= (tmp[outer * 64 + inner] ? 1ull : 0ull) << inner;                                  \
	}                                                                                              \
	dict_filter[outer] = word;

			INNER_LOOP(64)
		}
		if (dict_rows % 64)
		{
			INNER_LOOP(dict_rows % 64)
		}
#undef INNER_LOOP

		params.batch_filter = dict_filter;
	}
	else
	{
		params.batch_filter = NULL;
	}

	/*
	 * The dictionary contains no null entries, so we will be adding the null
	 * key separately. Determine if we have any null key that also passes the
	 * batch filter.
	 */
	bool have_null_key = false;
	if (row_filter != NULL)
	{
		if (params.single_grouping_column.arrow->null_count > 0)
		{
			Assert(params.single_grouping_column.buffers[0] != NULL);
			const size_t batch_words = (batch_rows + 63) / 64;
			for (size_t i = 0; i < batch_words; i++)
			{
				have_null_key = have_null_key ||
								(row_filter[i] &
								 (~((uint64 *) params.single_grouping_column.buffers[0])[i])) != 0;
			}
		}
	}
	else
	{
		if (params.single_grouping_column.arrow->null_count > 0)
		{
			Assert(params.single_grouping_column.buffers[0] != NULL);
			have_null_key = true;
		}
	}

	/*
	 * Build key indexes for the dictionary entries as for normal non-nullable
	 * text values.
	 */
	Assert(params.single_grouping_column.decompression_type = DT_ArrowTextDict);
	Assert((size_t) dict_rows <= policy->num_key_index_for_dict);
	memset(policy->key_index_for_dict, 0, sizeof(*policy->key_index_for_dict) * dict_rows);

	params.single_grouping_column.decompression_type = DT_ArrowText;
	params.single_grouping_column.buffers[0] = NULL;
	params.have_scalar_or_nullable_columns = false;
	params.result_key_indexes = policy->key_index_for_dict;

	single_text_dispatch_for_params(params, 0, dict_rows);

	/*
	 * The dictionary doesn't store nulls, so add the null key separately if we
	 * have one.
	 *
	 * FIXME doesn't respect nulls last/first in GroupAggregate. Add a test.
	 */
	if (have_null_key && policy->hashing.null_key_index == 0)
	{
		policy->hashing.null_key_index = ++params.hashing->last_used_key_index;
		policy->hashing.output_keys[policy->hashing.null_key_index] = PointerGetDatum(NULL);
	}

	policy->use_key_index_for_dict = true;

	/*
	 * Initialize the new keys if we added any.
	 */
	if (params.hashing->last_used_key_index > last_initialized_key_index)
	{
		const uint64 new_aggstate_rows = policy->num_allocated_per_key_agg_states * 2 + 1;
		const int num_fns = policy->num_agg_defs;
		for (int i = 0; i < num_fns; i++)
		{
			const VectorAggDef *agg_def = &policy->agg_defs[i];
			if (params.hashing->last_used_key_index >= policy->num_allocated_per_key_agg_states)
			{
				policy->per_agg_per_key_states[i] =
					repalloc(policy->per_agg_per_key_states[i],
							 new_aggstate_rows * agg_def->func.state_bytes);
			}

			/*
			 * Initialize the aggregate function states for the newly added keys.
			 */
			void *first_uninitialized_state =
				agg_def->func.state_bytes * (last_initialized_key_index + 1) +
				(char *) policy->per_agg_per_key_states[i];
			agg_def->func.agg_init(first_uninitialized_state,
								   params.hashing->last_used_key_index -
									   last_initialized_key_index);
		}

		/*
		 * Record the newly allocated number of rows in case we had to reallocate.
		 */
		if (params.hashing->last_used_key_index >= policy->num_allocated_per_key_agg_states)
		{
			Assert(new_aggstate_rows > policy->num_allocated_per_key_agg_states);
			policy->num_allocated_per_key_agg_states = new_aggstate_rows;
		}
	}

	DEBUG_PRINT("computed the dict offsets\n");
}

static pg_attribute_always_inline void
single_text_offsets_translate_impl(BatchHashingParams params, int start_row, int end_row)
{
	GroupingPolicyHash *policy = params.policy;
	Assert(policy->use_key_index_for_dict);

	uint32 *restrict indexes_for_rows = params.result_key_indexes;
	uint32 *restrict indexes_for_dict = policy->key_index_for_dict;

	for (int row = start_row; row < end_row; row++)
	{
		const bool row_valid = arrow_row_is_valid(params.single_grouping_column.buffers[0], row);
		const int16 dict_index = ((int16 *) params.single_grouping_column.buffers[3])[row];

		if (row_valid)
		{
			indexes_for_rows[row] = indexes_for_dict[dict_index];
		}
		else
		{
			indexes_for_rows[row] = policy->hashing.null_key_index;
		}

		Assert(indexes_for_rows[row] != 0 || !arrow_row_is_valid(params.batch_filter, row));
	}
}

#define APPLY_FOR_VALIDITY(X, NAME, COND)                                                          \
	X(NAME##_notnull, (COND) && (params.single_grouping_column.buffers[0] == NULL))                \
	X(NAME##_nullable, (COND) && (params.single_grouping_column.buffers[0] != NULL))

#define APPLY_FOR_SPECIALIZATIONS(X) APPLY_FOR_VALIDITY(X, single_text_offsets_translate, true)

#define DEFINE(NAME, CONDITION)                                                                    \
	static pg_noinline void NAME(BatchHashingParams params, int start_row, int end_row)            \
	{                                                                                              \
		if (!(CONDITION))                                                                          \
		{                                                                                          \
			pg_unreachable();                                                                      \
		}                                                                                          \
                                                                                                   \
		single_text_offsets_translate_impl(params, start_row, end_row);                            \
	}

APPLY_FOR_SPECIALIZATIONS(DEFINE)

#undef DEFINE

static void
single_text_offsets_translate(BatchHashingParams params, int start_row, int end_row)
{
#define DISPATCH(NAME, CONDITION)                                                                  \
	if (CONDITION)                                                                                 \
	{                                                                                              \
		NAME(params, start_row, end_row);                                                          \
	}                                                                                              \
	else

	APPLY_FOR_SPECIALIZATIONS(DISPATCH) { pg_unreachable(); }
#undef DISPATCH
}

#undef APPLY_FOR_SPECIALIZATIONS
#undef APPLY_FOR_VALIDITY
#undef APPLY_FOR_BATCH_FILTER

#include "hash_strategy_impl.c"
