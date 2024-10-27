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
single_text_get_key(GroupingPolicyHash *restrict policy, DecompressBatchState *restrict batch_state,
					int row, int next_key_index, BytesView *restrict key, bool *restrict valid)
{
	if (list_length(policy->output_grouping_columns) != 1)
	{
		pg_unreachable();
	}

	GroupingColumn *g = linitial(policy->output_grouping_columns);
	CompressedColumnValues column = batch_state->compressed_columns[g->input_offset];

	if (unlikely(column.decompression_type == DT_Scalar))
	{
		/* Already stored. */
		key->len = VARSIZE_ANY_EXHDR(*column.output_value);
		key->data = (const uint8 *) VARDATA_ANY(*column.output_value);
		*valid = !*column.output_isnull;
	}
	else if (column.decompression_type == DT_ArrowText)
	{
		*key = get_bytes_view(&column, row);
		*valid = arrow_row_is_valid(column.buffers[0], row);
	}
	else if (column.decompression_type == DT_ArrowTextDict)
	{
		const int16 index = ((int16 *) column.buffers[3])[row];
		*key = get_bytes_view(&column, index);
		*valid = arrow_row_is_valid(column.buffers[0], row);
	}
	else
	{
		pg_unreachable();
	}

	gp_hash_key_validity_bitmap(policy, next_key_index)[0] = *valid;

	DEBUG_PRINT("%p consider key row %d key index %d is %d bytes: ",
				policy,
				row,
				next_key_index,
				key->len);
	for (size_t i = 0; i < key->len; i++)
	{
		DEBUG_PRINT("%.2x.", key->data[i]);
	}
	DEBUG_PRINT("\n");
}

static pg_attribute_always_inline BytesView
single_text_store_key(GroupingPolicyHash *restrict policy, BytesView key, uint32 key_index)
{
	const int total_bytes = key.len + VARHDRSZ;
	text *restrict stored = (text *) MemoryContextAlloc(policy->key_body_mctx, total_bytes);
	SET_VARSIZE(stored, total_bytes);
	memcpy(VARDATA(stored), key.data, key.len);
	key.data = (uint8 *) VARDATA(stored);
	gp_hash_output_keys(policy, key_index)[0] = PointerGetDatum(stored);
	return key;
}

static pg_attribute_always_inline void
single_text_destroy_key(BytesView key)
{
	/* Noop. */
}

#define EXPLAIN_NAME "single text"
#define KEY_VARIANT single_text
#define KEY_HASH(X) hash_bytes_view(X)
#define KEY_EQUAL(a, b) (a.len == b.len && memcmp(a.data, b.data, a.len) == 0)
#define STORE_HASH
#define CTYPE BytesView
#include "hash_table_functions_impl.c"