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

#include "compression/arrow_c_data_interface.h"
#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/vector_agg/exec.h"
#include "nodes/vector_agg/grouping_policy_hash.h"
#include "template_helper.h"

#include "batch_hashing_params.h"

#include "umash_fingerprint_key.h"

#define EXPLAIN_NAME "serialized"
#define KEY_VARIANT serialized
#define OUTPUT_KEY_TYPE text *

static void
serialized_key_hashing_init(HashingStrategy *hashing)
{
	hashing->umash_params = umash_key_hashing_init();
}

static void
serialized_key_hashing_prepare_for_batch(GroupingPolicyHash *policy, TupleTableSlot *vector_slot)
{
}

static pg_attribute_always_inline bool
byte_bitmap_row_is_valid(const uint8 *bitmap, size_t row_number)
{
	const size_t byte_index = row_number / 8;
	const size_t bit_index = row_number % 8;
	const uint8 mask = ((uint8) 1) << bit_index;
	return bitmap[byte_index] & mask;
}

static pg_attribute_always_inline void
byte_bitmap_set_row_validity(uint8 *bitmap, size_t row_number, bool value)
{
	const size_t byte_index = row_number / 8;
	const size_t bit_index = row_number % 8;
	const uint8 mask = ((uint8) 1) << bit_index;
	const uint8 new_bit = ((uint8) value) << bit_index;

	bitmap[byte_index] = (bitmap[byte_index] & ~mask) | new_bit;

	Assert(byte_bitmap_row_is_valid(bitmap, row_number) == value);
}

static pg_attribute_always_inline void
serialized_key_hashing_get_key(BatchHashingParams params, int row, void *restrict output_key_ptr,
							   void *restrict hash_table_key_ptr, bool *restrict valid)
{
	HashingStrategy *hashing = params.hashing;

	text **restrict output_key = (text **) output_key_ptr;
	HASH_TABLE_KEY_TYPE *restrict hash_table_key = (HASH_TABLE_KEY_TYPE *) hash_table_key_ptr;

	const int num_columns = params.num_grouping_columns;

	const size_t bitmap_bytes = (num_columns + 7) / 8;

	/*
	 * Loop through the grouping columns to determine the length of the key. We
	 * need that to allocate memory to store it.
	 *
	 * The key has the null bitmap at the beginning.
	 */
	size_t num_bytes = bitmap_bytes;
	for (int column_index = 0; column_index < num_columns; column_index++)
	{
		const CompressedColumnValues *column_values = &params.grouping_column_values[column_index];

		if (column_values->decompression_type == DT_Scalar)
		{
			if (!*column_values->output_isnull)
			{
				const GroupingColumn *def = &params.policy->grouping_columns[column_index];
				if (def->value_bytes > 0)
				{
					num_bytes += def->value_bytes;
				}
				else
				{
					/*
					 * The default value always has a long varlena header, but
					 * we are going to use short if it fits.
					 */
					const int32 value_bytes = VARSIZE_ANY_EXHDR(*column_values->output_value);
					if (value_bytes + VARHDRSZ_SHORT <= VARATT_SHORT_MAX)
					{
						/* Short varlena, unaligned. */
						const int total_bytes = value_bytes + VARHDRSZ_SHORT;
						num_bytes += total_bytes;
					}
					else
					{
						/* Long varlena, requires alignment. */
						const int total_bytes = value_bytes + VARHDRSZ;
						num_bytes = TYPEALIGN(4, num_bytes) + total_bytes;
					}
				}
			}

			continue;
		}

		const bool is_valid = arrow_row_is_valid(column_values->buffers[0], row);
		if (!is_valid)
		{
			continue;
		}

		if (column_values->decompression_type > 0)
		{
			num_bytes += column_values->decompression_type;
		}
		else
		{
			Assert(column_values->decompression_type == DT_ArrowText ||
				   column_values->decompression_type == DT_ArrowTextDict);
			Assert((column_values->decompression_type == DT_ArrowTextDict) ==
				   (column_values->buffers[3] != NULL));

			const uint32 data_row = (column_values->decompression_type == DT_ArrowTextDict) ?
										((int16 *) column_values->buffers[3])[row] :
										row;
			const uint32 start = ((uint32 *) column_values->buffers[1])[data_row];
			const int32 value_bytes = ((uint32 *) column_values->buffers[1])[data_row + 1] - start;

			if (value_bytes + VARHDRSZ_SHORT <= VARATT_SHORT_MAX)
			{
				/* Short varlena, unaligned. */
				const int total_bytes = value_bytes + VARHDRSZ_SHORT;
				num_bytes += total_bytes;
			}
			else
			{
				/* Long varlena, requires alignment. */
				const int total_bytes = value_bytes + VARHDRSZ;
				num_bytes = TYPEALIGN(4, num_bytes) + total_bytes;
			}
		}
	}

	/*
	 * The key has short or long varlena header. This is a little tricky, we
	 * decide the header length after we have counted all the columns, but we
	 * put it at the beginning. Technically it could change the length because
	 * of the alignment. In practice, we only use alignment by 4 bytes for long
	 * varlena strings, and if we have at least one long varlena string column,
	 * the key is also going to use the long varlena header which is 4 bytes, so
	 * the alignment is not affected. If we use the short varlena header for the
	 * key, it necessarily means that there were no long varlena columns and
	 * therefore no alignment is needed.
	 */
	const bool key_uses_short_header = num_bytes + VARHDRSZ_SHORT <= VARATT_SHORT_MAX;
	num_bytes += key_uses_short_header ? VARHDRSZ_SHORT : VARHDRSZ;

	/*
	 * Use temporary storage for the new key, reallocate if it's too small.
	 */
	if (num_bytes > hashing->num_tmp_key_storage_bytes)
	{
		if (hashing->tmp_key_storage != NULL)
		{
			pfree(hashing->tmp_key_storage);
		}
		hashing->tmp_key_storage = MemoryContextAlloc(hashing->key_body_mctx, num_bytes);
		hashing->num_tmp_key_storage_bytes = num_bytes;
	}
	uint8 *restrict serialized_key_storage = hashing->tmp_key_storage;

	/*
	 * Build the actual grouping key.
	 */
	uint32 offset = 0;
	offset += key_uses_short_header ? VARHDRSZ_SHORT : VARHDRSZ;

	/*
	 * We must always save the validity bitmap, even when there are no
	 * null words, so that the key is uniquely deserializable. Otherwise a key
	 * with some nulls might collide with a key with no nulls.
	 */
	uint8 *restrict serialized_key_validity_bitmap = &serialized_key_storage[offset];
	offset += bitmap_bytes;

	/*
	 * Loop through the grouping columns again and add their values to the
	 * grouping key.
	 */
	for (int column_index = 0; column_index < num_columns; column_index++)
	{
		const CompressedColumnValues *column_values = &params.grouping_column_values[column_index];

		if (column_values->decompression_type == DT_Scalar)
		{
			const bool is_valid = !*column_values->output_isnull;
			byte_bitmap_set_row_validity(serialized_key_validity_bitmap, column_index, is_valid);
			if (is_valid)
			{
				const GroupingColumn *def = &params.policy->grouping_columns[column_index];
				if (def->by_value)
				{
					memcpy(&serialized_key_storage[offset],
						   column_values->output_value,
						   def->value_bytes);

					offset += def->value_bytes;
				}
				else if (def->value_bytes > 0)
				{
					memcpy(&serialized_key_storage[offset],
						   DatumGetPointer(*column_values->output_value),
						   def->value_bytes);

					offset += def->value_bytes;
				}
				else
				{
					/*
					 * The default value always has a long varlena header, but
					 * we are going to use short if it fits.
					 */
					const int32 value_bytes = VARSIZE_ANY_EXHDR(*column_values->output_value);
					if (value_bytes + VARHDRSZ_SHORT <= VARATT_SHORT_MAX)
					{
						/* Short varlena, no alignment. */
						const int32 total_bytes = value_bytes + VARHDRSZ_SHORT;
						SET_VARSIZE_SHORT(&serialized_key_storage[offset], total_bytes);
						offset += VARHDRSZ_SHORT;
					}
					else
					{
						/* Long varlena, requires alignment. Zero out the alignment bytes. */
						memset(&serialized_key_storage[offset], 0, 4);
						offset = TYPEALIGN(4, offset);
						const int32 total_bytes = value_bytes + VARHDRSZ;
						SET_VARSIZE(&serialized_key_storage[offset], total_bytes);
						offset += VARHDRSZ;
					}

					memcpy(&serialized_key_storage[offset],
						   VARDATA_ANY(*column_values->output_value),
						   value_bytes);

					offset += value_bytes;
				}
			}
			continue;
		}

		const bool is_valid = arrow_row_is_valid(column_values->buffers[0], row);
		byte_bitmap_set_row_validity(serialized_key_validity_bitmap, column_index, is_valid);

		if (!is_valid)
		{
			continue;
		}

		if (column_values->decompression_type > 0)
		{
			Assert(offset <= UINT_MAX - column_values->decompression_type);

			switch ((int) column_values->decompression_type)
			{
				case 2:
					memcpy(&serialized_key_storage[offset],
						   row + (int16 *) column_values->buffers[1],
						   2);
					break;
				case 4:
					memcpy(&serialized_key_storage[offset],
						   row + (int32 *) column_values->buffers[1],
						   4);
					break;
				case 8:
					memcpy(&serialized_key_storage[offset],
						   row + (int64 *) column_values->buffers[1],
						   8);
					break;
				case 16:
					memcpy(&serialized_key_storage[offset],
						   (row * 2) + (int64 *) column_values->buffers[1],
						   16);
					break;
				default:
					pg_unreachable();
					break;
			}

			offset += column_values->decompression_type;

			continue;
		}

		Assert(column_values->decompression_type == DT_ArrowText ||
			   column_values->decompression_type == DT_ArrowTextDict);

		const uint32 data_row = column_values->decompression_type == DT_ArrowTextDict ?
									((int16 *) column_values->buffers[3])[row] :
									row;
		const uint32 start = ((uint32 *) column_values->buffers[1])[data_row];
		const int32 value_bytes = ((uint32 *) column_values->buffers[1])[data_row + 1] - start;

		if (value_bytes + VARHDRSZ_SHORT <= VARATT_SHORT_MAX)
		{
			/* Short varlena, unaligned. */
			const int32 total_bytes = value_bytes + VARHDRSZ_SHORT;
			SET_VARSIZE_SHORT(&serialized_key_storage[offset], total_bytes);
			offset += VARHDRSZ_SHORT;
		}
		else
		{
			/* Long varlena, requires alignment. Zero out the alignment bytes. */
			memset(&serialized_key_storage[offset], 0, 4);
			offset = TYPEALIGN(4, offset);
			const int32 total_bytes = value_bytes + VARHDRSZ;
			SET_VARSIZE(&serialized_key_storage[offset], total_bytes);
			offset += VARHDRSZ;
		}
		memcpy(&serialized_key_storage[offset],
			   &((uint8 *) column_values->buffers[2])[start],
			   value_bytes);

		offset += value_bytes;
	}

	Assert(offset == num_bytes);

	if (key_uses_short_header)
	{
		SET_VARSIZE_SHORT(serialized_key_storage, offset);
	}
	else
	{
		SET_VARSIZE(serialized_key_storage, offset);
	}

	DEBUG_PRINT("key is %d bytes: ", offset);
	for (size_t i = 0; i < offset; i++)
	{
		DEBUG_PRINT("%.2x.", serialized_key_storage[i]);
	}
	DEBUG_PRINT("\n");

	*output_key = (text *) serialized_key_storage;

	Assert(VARSIZE_ANY(*output_key) == num_bytes);

	/*
	 * The multi-column key is always considered non-null, and the null flags
	 * for the individual columns are stored in a bitmap that is part of the
	 * key.
	 */
	*valid = true;

	const struct umash_fp fp = umash_fprint(params.hashing->umash_params,
											/* seed = */ ~0ULL,
											serialized_key_storage,
											num_bytes);
	*hash_table_key = umash_fingerprint_get_key(fp);
}

static pg_attribute_always_inline void
serialized_key_hashing_store_new(HashingStrategy *restrict hashing, uint32 new_key_index,
								 text *output_key)
{
	/*
	 * We will store this key so we have to consume the temporary storage that
	 * was used for it. The subsequent keys will need to allocate new memory.
	 */
	Assert(hashing->tmp_key_storage == (void *) output_key);
	hashing->tmp_key_storage = NULL;
	hashing->num_tmp_key_storage_bytes = 0;

	hashing->output_keys[new_key_index] = PointerGetDatum(output_key);
}

static void
serialized_emit_key(GroupingPolicyHash *policy, uint32 current_key, TupleTableSlot *aggregated_slot)
{
	const HashingStrategy *hashing = &policy->hashing;
	const int num_key_columns = policy->num_grouping_columns;
	const Datum serialized_key_datum = hashing->output_keys[current_key];
	const uint8 *serialized_key = (const uint8 *) VARDATA_ANY(serialized_key_datum);
	PG_USED_FOR_ASSERTS_ONLY const int key_data_bytes = VARSIZE_ANY_EXHDR(serialized_key_datum);
	const uint8 *restrict ptr = serialized_key;

	/*
	 * We have the column validity bitmap at the beginning of the key.
	 */
	const int bitmap_bytes = (num_key_columns + 7) / 8;
	Assert(bitmap_bytes <= key_data_bytes);
	const uint8 *restrict key_validity_bitmap = serialized_key;
	ptr += bitmap_bytes;

	DEBUG_PRINT("emit key #%d, with header %ld without %d bytes: ",
				current_key,
				VARSIZE_ANY(serialized_key_datum),
				key_data_bytes);
	for (size_t i = 0; i < VARSIZE_ANY(serialized_key_datum); i++)
	{
		DEBUG_PRINT("%.2x.", ((const uint8 *) serialized_key_datum)[i]);
	}
	DEBUG_PRINT("\n");

	for (int column_index = 0; column_index < num_key_columns; column_index++)
	{
		const GroupingColumn *col = &policy->grouping_columns[column_index];
		const bool isnull = !byte_bitmap_row_is_valid(key_validity_bitmap, column_index);

		aggregated_slot->tts_isnull[col->output_offset] = isnull;

		if (isnull)
		{
			continue;
		}

		Datum *output = &aggregated_slot->tts_values[col->output_offset];
		if (col->value_bytes > 0)
		{
			if (col->by_value)
			{
				Assert(col->by_value);
				Assert((size_t) col->value_bytes <= sizeof(Datum));
				*output = 0;
				memcpy(output, ptr, col->value_bytes);
			}
			else
			{
				*output = PointerGetDatum(ptr);
			}

			ptr += col->value_bytes;
		}
		else
		{
			Assert(col->value_bytes == -1);
			Assert(!col->by_value);
			if (VARATT_IS_SHORT(ptr))
			{
				*output = PointerGetDatum(ptr);
				ptr += VARSIZE_SHORT(ptr);
			}
			else
			{
				ptr = (const uint8 *) TYPEALIGN(4, ptr);
				*output = PointerGetDatum(ptr);
				ptr += VARSIZE(ptr);
			}
		}
	}

	Assert(ptr == serialized_key + key_data_bytes);
}

#include "hash_strategy_impl.c"
