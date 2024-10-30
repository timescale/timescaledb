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
serialized_get_key(HashingConfig config, int row, void *restrict key_ptr, bool *restrict valid)
{
	GroupingPolicyHash *policy = config.policy;

	text **restrict key = (text **) key_ptr;

	const int num_columns = config.num_grouping_columns;

	size_t bitmap_bytes = (num_columns + 7) / 8;
	uint8 *restrict serialized_key_validity_bitmap;

	/*
	 * Loop through the grouping columns to determine the length of the key. We
	 * need that to allocate memory to store it.
	 */
	size_t num_bytes = 0;
	num_bytes += VARHDRSZ;
	for (int column_index = 0; column_index < num_columns; column_index++)
	{
		const CompressedColumnValues *column_values = &config.grouping_column_values[column_index];

		if (config.have_scalar_columns && column_values->decompression_type == DT_Scalar)
		{
			if (!*column_values->output_isnull)
			{
				const GroupingColumn *def = &config.policy->grouping_columns[column_index];
				if (def->by_value)
				{
					num_bytes += def->value_bytes;
				}
				else
				{
					num_bytes = TYPEALIGN(4, num_bytes) + VARSIZE_ANY(*column_values->output_value);
				}
			}

			continue;
		}

		const bool is_valid =
			!config.have_scalar_columns || arrow_row_is_valid(column_values->buffers[0], row);
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

			int32 total_bytes;
			if (value_bytes + VARHDRSZ_SHORT <= VARATT_SHORT_MAX)
			{
				/* Short varlena, unaligned. */
				total_bytes = value_bytes + VARHDRSZ_SHORT;
			}
			else
			{
				/* Long varlena, requires alignment. */
				total_bytes = value_bytes + VARHDRSZ;
				num_bytes = TYPEALIGN(4, num_bytes) + total_bytes;
			}

			num_bytes += total_bytes;
		}
	}

	/*
	 * The key has a null bitmap at the end.
	 */
	num_bytes += bitmap_bytes;

	/*
	 * Use temporary storage for the new key, reallocate if it's too small.
	 */
	if (num_bytes > policy->num_tmp_key_storage_bytes)
	{
		if (policy->tmp_key_storage != NULL)
		{
			pfree(policy->tmp_key_storage);
		}
		policy->tmp_key_storage = MemoryContextAlloc(policy->key_body_mctx, num_bytes);
		policy->num_tmp_key_storage_bytes = num_bytes;
	}
	uint8 *restrict serialized_key_storage = policy->tmp_key_storage;

	/*
	 * Have to memset the key with zeros, so that the alignment bytes are zeroed
	 * out.
	 */
	memset(serialized_key_storage, 0, num_bytes);

	serialized_key_validity_bitmap = &serialized_key_storage[num_bytes - bitmap_bytes];

	/*
	 * We will also fill the output key values in Postgres format. They can go
	 * straight into the next unused key space, if this key is discarded they
	 * will just be overwritten later.
	 */
	uint8 *restrict output_key_validity_bitmap =
		gp_hash_key_validity_bitmap(policy, policy->last_used_key_index + 1);
	Datum *restrict output_key_datums =
		gp_hash_output_keys(policy, policy->last_used_key_index + 1);

	/*
	 * Loop through the grouping columns again and build the actual key.
	 */
	uint32 offset = 0;
	offset += VARHDRSZ;
	for (int column_index = 0; column_index < num_columns; column_index++)
	{
		const CompressedColumnValues *column_values = &config.grouping_column_values[column_index];

		if (config.have_scalar_columns && column_values->decompression_type == DT_Scalar)
		{
			const bool is_valid = !*column_values->output_isnull;
			byte_bitmap_set_row_validity(serialized_key_validity_bitmap, column_index, is_valid);
			if (is_valid)
			{
				const GroupingColumn *def = &config.policy->grouping_columns[column_index];
				if (def->by_value)
				{
					memcpy(&serialized_key_storage[offset],
						   column_values->output_value,
						   def->value_bytes);

					memcpy(&output_key_datums[column_index],
						   column_values->output_value,
						   def->value_bytes);

					offset += def->value_bytes;
				}
				else
				{
					offset = TYPEALIGN(4, offset);

					memcpy(&serialized_key_storage[offset],
						   DatumGetPointer(*column_values->output_value),
						   VARSIZE_ANY(*column_values->output_value));

					output_key_datums[column_index] =
						PointerGetDatum(&serialized_key_storage[offset]);

					offset += VARSIZE_ANY(*column_values->output_value);
				}
			}
			continue;
		}

		const bool is_valid =
			!config.have_scalar_columns || arrow_row_is_valid(column_values->buffers[0], row);
		byte_bitmap_set_row_validity(serialized_key_validity_bitmap, column_index, is_valid);

		if (!is_valid)
		{
			continue;
		}

		if (column_values->decompression_type > 0)
		{
			Assert(offset <= UINT_MAX - column_values->decompression_type);

			memcpy(&serialized_key_storage[offset],
				   ((char *) column_values->buffers[1]) + column_values->decompression_type * row,
				   column_values->decompression_type);
			memcpy(&output_key_datums[column_index],
				   ((char *) column_values->buffers[1]) + column_values->decompression_type * row,
				   column_values->decompression_type);

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

		void *datum_start;
		if (value_bytes + VARHDRSZ_SHORT <= VARATT_SHORT_MAX)
		{
			/* Short varlena, unaligned. */
			datum_start = &serialized_key_storage[offset];
			const int32 total_bytes = value_bytes + VARHDRSZ_SHORT;
			SET_VARSIZE_SHORT(&serialized_key_storage[offset], total_bytes);
			offset += VARHDRSZ_SHORT;
		}
		else
		{
			/* Long varlena, requires alignment. */
			offset = TYPEALIGN(4, offset);
			datum_start = &serialized_key_storage[offset];
			const int32 total_bytes = value_bytes + VARHDRSZ;
			SET_VARSIZE(&serialized_key_storage[offset], total_bytes);
			offset += VARHDRSZ;
		}
		output_key_datums[column_index] = PointerGetDatum(datum_start);
		memcpy(&serialized_key_storage[offset],
			   &((uint8 *) column_values->buffers[2])[start],
			   value_bytes);
		offset += value_bytes;
	}

	Assert(&serialized_key_storage[offset] == (void *) serialized_key_validity_bitmap);

	/*
	 * Note that we must always save the validity bitmap, even when there are no
	 * null words, so that the key is uniquely deserializable. Otherwise a key
	 * with some nulls might collide with a key with no nulls.
	 */
	offset += bitmap_bytes;
	Assert(offset == num_bytes);

	/*
	 * The output keys also have a validity bitmap, copy it there.
	 */
	for (size_t i = 0; i < bitmap_bytes; i++)
	{
		output_key_validity_bitmap[i] = serialized_key_validity_bitmap[i];
	}

	DEBUG_PRINT("key is %d bytes: ", offset);
	for (size_t i = 0; i < offset; i++)
	{
		DEBUG_PRINT("%.2x.", serialized_key_storage[i]);
	}
	DEBUG_PRINT("\n");

	SET_VARSIZE(serialized_key_storage, offset);

	*key = (text *) serialized_key_storage;

	/*
	 * The multi-column key is always considered non-null, and the null flags
	 * for the individual columns are stored in a bitmap that is part of the
	 * key.
	 */
	*valid = true;
}

static pg_attribute_always_inline text *
serialized_store_key(GroupingPolicyHash *restrict policy, text *key)
{
	/*
	 * We will store this key so we have to consume the temporary storage that
	 * was used for it. The subsequent keys will need to allocate new memory.
	 */
	Assert(policy->tmp_key_storage == (void *) key);
	policy->tmp_key_storage = NULL;
	policy->num_tmp_key_storage_bytes = 0;
	return key;
}

static pg_attribute_always_inline void
serialized_destroy_key(text *key)
{
	/* Noop, the memory will be reused by the subsequent key. */
}

#define EXPLAIN_NAME "serialized"
#define KEY_VARIANT serialized
#define KEY_HASH(X) hash_bytes_view((BytesView){ .len = VARSIZE_4B(X), .data = (uint8 *) (X) })
#define KEY_EQUAL(a, b)                                                                            \
	(VARSIZE_4B(a) == VARSIZE_4B(b) &&                                                             \
	 memcmp(VARDATA_4B(a), VARDATA_4B(b), VARSIZE_4B(a) - VARHDRSZ) == 0)
#define STORE_HASH
#define CTYPE text *
#include "hash_table_functions_impl.c"
