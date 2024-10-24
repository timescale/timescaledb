/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Implementation of column hashing for multiple serialized columns.
 */

#include <postgres.h>

#include <common/hashfn.h>

#include "bytes_view.h"
#include "compression/arrow_c_data_interface.h"
#include "grouping_policy_hash.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"

static pg_attribute_always_inline void
serialized_get_key(GroupingPolicyHash *restrict policy,
	DecompressBatchState *restrict batch_state, int row, int next_key_index, BytesView *restrict key,
				   bool *restrict valid)
{
	if (list_length(policy->output_grouping_columns) == 1)
	{
		pg_unreachable();
	}

	uint64 *restrict validity_bitmap;

	const int num_columns = list_length(policy->output_grouping_columns);
	Assert(num_columns <= 64);
	size_t num_bytes = 0;
	for (int i = 0; i < num_columns; i++)
	{
		const GroupingColumn *def = list_nth(policy->output_grouping_columns, i);
		num_bytes = att_align_nominal(num_bytes, def->typalign);
		if (def->by_value)
		{
			num_bytes += def->value_bytes;
		}
		else
		{
			const CompressedColumnValues *column = &batch_state->compressed_columns[def->input_offset];
			if (unlikely(column->decompression_type == DT_Scalar))
			{
				num_bytes += (*column->output_isnull) ? 0 : VARSIZE_ANY(*column->output_value);
			}
			else if (column->decompression_type == DT_ArrowText)
			{
				if (arrow_row_is_valid(column->buffers[0], row))
				{
					const uint32 start = ((uint32 *) column->buffers[1])[row];
					const int32 value_bytes = ((uint32 *) column->buffers[1])[row + 1] - start;
					num_bytes += value_bytes + VARHDRSZ;
				}
			}
			else if (column->decompression_type == DT_ArrowTextDict)
			{
				if (arrow_row_is_valid(column->buffers[0], row))
				{
					const int16 index = ((int16 *) column->buffers[3])[row];
					const uint32 start = ((uint32 *) column->buffers[1])[index];
					const int32 value_bytes = ((uint32 *) column->buffers[1])[index + 1] - start;
					num_bytes += value_bytes + VARHDRSZ;
				}
			}
			else
			{
				pg_unreachable();
			}
		}
	}

	/* The optional null bitmap. */
	num_bytes = att_align_nominal(num_bytes, TYPALIGN_DOUBLE);
	num_bytes += sizeof(*validity_bitmap);


	Datum *restrict key_data = &((Datum *) policy->keys)[num_columns * next_key_index];
	uint8 *restrict key_storage = MemoryContextAlloc(policy->key_body_mctx, num_bytes);
	validity_bitmap = (uint64 *) &((char *) key_storage)[num_bytes - sizeof(*validity_bitmap)];
	*validity_bitmap = ~0ULL;

	uint32 offset = 0;
	for (int i = 0; i < num_columns; i++)
	{
		const GroupingColumn *def = list_nth(policy->output_grouping_columns, i);
		offset = att_align_nominal(offset, def->typalign);

		const CompressedColumnValues *column = &batch_state->compressed_columns[def->input_offset];
		if (column->decompression_type > 0)
		{
			Assert(offset <= UINT_MAX - column->decompression_type);
			memcpy(&key_storage[offset],
				((char *) column->buffers[1]) + column->decompression_type * row,
				column->decompression_type);
			arrow_set_row_validity(validity_bitmap, i, arrow_row_is_valid(column->buffers[0], row));
			offset += column->decompression_type;

			memcpy(&key_data[i],
				((char *) column->buffers[1]) + column->decompression_type * row,
				column->decompression_type);
		}
		else if (unlikely(column->decompression_type == DT_Scalar))
		{
			const bool is_valid = !*column->output_isnull;
			arrow_set_row_validity(validity_bitmap, i, is_valid);
			if (is_valid)
			{
				if (def->by_value)
				{
					memcpy(&key_storage[offset], column->output_value, def->value_bytes);
					offset += def->value_bytes;

					memcpy(&key_data[row], column->output_value, def->value_bytes);
				}
				else
				{
					memcpy(&key_storage[offset], DatumGetPointer(*column->output_value), VARSIZE_ANY(column->output_value));

					key_data[row] = PointerGetDatum(&key_storage[offset]);

					offset += VARSIZE_ANY(column->output_value);
				}
			}
		}
		else if (column->decompression_type == DT_ArrowText)
		{
			const bool is_valid = arrow_row_is_valid(column->buffers[0], row);
			arrow_set_row_validity(validity_bitmap, i, is_valid);
			if (is_valid)
			{
				const uint32 start = ((uint32 *) column->buffers[1])[row];
				const int32 value_bytes = ((uint32 *) column->buffers[1])[row + 1] - start;
				const int32 total_bytes = value_bytes + VARHDRSZ;

				SET_VARSIZE(&key_storage[offset], total_bytes);
				memcpy(VARDATA(&key_storage[offset]), &((uint8 *) column->buffers[2])[start],
					value_bytes);

				key_data[i] = PointerGetDatum(&key_storage[offset]);

				offset += total_bytes;
			}
		}
		else if (column->decompression_type == DT_ArrowTextDict)
		{
			const bool is_valid = arrow_row_is_valid(column->buffers[0], row);
			arrow_set_row_validity(validity_bitmap, i, is_valid);
			if (is_valid)
			{
				const int16 index = ((int16 *) column->buffers[3])[row];
				const uint32 start = ((uint32 *) column->buffers[1])[index];
				const int32 value_bytes = ((uint32 *) column->buffers[1])[index + 1] - start;
				const int32 total_bytes = value_bytes + VARHDRSZ;

				SET_VARSIZE(&key_storage[offset], total_bytes);
				memcpy(VARDATA(&key_storage[offset]), &((uint8 *) column->buffers[2])[start],
					value_bytes);

				key_data[i] = PointerGetDatum(&key_storage[offset]);

				offset += total_bytes;
			}
		}
		else
		{
			pg_unreachable();
		}
	}

	Assert((void *) &key_storage[att_align_nominal(offset, TYPALIGN_DOUBLE)] == (void *) validity_bitmap);

	if (*validity_bitmap != ~0ULL)
	{
		offset = att_align_nominal(offset, TYPALIGN_DOUBLE) + sizeof(*validity_bitmap);
		Assert(offset == num_bytes);
	}

	*key = (BytesView) { .data = key_storage, .len = offset };
	*valid = true;
}

static pg_attribute_always_inline BytesView
serialized_store_key(GroupingPolicyHash *restrict policy, BytesView key, uint32 key_index)
{
	/* Noop, all done in get_key. */
	return key;
}

static pg_attribute_always_inline void
serialized_destroy_key(BytesView key)
{
	pfree((void *) key.data);
}

#define KEY_VARIANT serialized
#define KEY_HASH(X) hash_bytes_view(X)
#define KEY_EQUAL(a, b) (a.len == b.len && memcmp(a.data, b.data, a.len) == 0)
#define STORE_HASH
#define CTYPE BytesView
#include "hash_table_functions_impl.c"
