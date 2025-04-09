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

static void
single_text_key_hashing_prepare_for_batch(GroupingPolicyHash *policy, TupleTableSlot *vector_slot)
{
}

#include "hash_strategy_impl.c"
