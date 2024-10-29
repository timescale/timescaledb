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
serialized_get_key(HashingConfig config, int row, BytesView *restrict key, bool *restrict valid)
{
	GroupingPolicyHash *policy = config.policy;

	uint64 *restrict serialized_key_validity_word;

	const int num_columns = config.num_grouping_columns;
	Assert(num_columns <= 64);

	/*
	 * Loop through the grouping columns to determine the length of the key. We
	 * need that to allocate memory to store it.
	 */
	size_t num_bytes = 0;
	for (int column_index = 0; column_index < num_columns; column_index++)
	{
		const GroupingColumn *def = &config.grouping_columns[column_index];
		const CompressedColumnValues *column_values = &config.compressed_columns[def->input_offset];

		num_bytes = TYPEALIGN(def->alignment_bytes, num_bytes);

		if (column_values->decompression_type > 0)
		{
			Assert(def->by_value);
			num_bytes += column_values->decompression_type;
		}
		else if (unlikely(column_values->decompression_type == DT_Scalar))
		{
			if (def->by_value)
			{
				num_bytes += def->value_bytes;
			}
			else
			{
				num_bytes +=
					(*column_values->output_isnull) ? 0 : VARSIZE_ANY(*column_values->output_value);
			}
		}
		else if (column_values->decompression_type == DT_ArrowText)
		{
			if (arrow_row_is_valid(column_values->buffers[0], row))
			{
				const uint32 start = ((uint32 *) column_values->buffers[1])[row];
				const int32 value_bytes = ((uint32 *) column_values->buffers[1])[row + 1] - start;
				num_bytes += value_bytes + VARHDRSZ;
			}
		}
		else if (column_values->decompression_type == DT_ArrowTextDict)
		{
			if (arrow_row_is_valid(column_values->buffers[0], row))
			{
				const int16 index = ((int16 *) column_values->buffers[3])[row];
				const uint32 start = ((uint32 *) column_values->buffers[1])[index];
				const int32 value_bytes = ((uint32 *) column_values->buffers[1])[index + 1] - start;
				num_bytes += value_bytes + VARHDRSZ;
			}
		}
		else
		{
			pg_unreachable();
		}
	}

	/*
	 * The key can have a null bitmap if any values are null, we always allocate
	 * the storage for it.
	 */
	num_bytes = TYPEALIGN(ALIGNOF_DOUBLE, num_bytes);
	num_bytes += sizeof(*serialized_key_validity_word);

	/*
	 * Use temporary storage for the new key, reallocate if it's too small.
	 */
	if (num_bytes > policy->num_tmp_key_storage_bytes)
	{
		if (policy->tmp_key_storage != NULL)
		{
			policy->key_body_mctx->methods->free_p(policy->key_body_mctx, policy->tmp_key_storage);
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

	serialized_key_validity_word = (uint64 *) &(
		(char *) serialized_key_storage)[num_bytes - sizeof(*serialized_key_validity_word)];
	*serialized_key_validity_word = ~0ULL;

	/*
	 * We will also fill the output key values in Postgres format. They can go
	 * straight into the next unused key space, if this key is discarded they
	 * will just be overwritten later.
	 */
	uint64 *restrict output_key_validity_word =
		gp_hash_key_validity_bitmap(policy, policy->last_used_key_index + 1);
	Datum *restrict output_key_datums =
		gp_hash_output_keys(policy, policy->last_used_key_index + 1);

	/*
	 * Loop through the grouping columns again and build the actual key.
	 */
	uint32 offset = 0;
	for (int column_index = 0; column_index < num_columns; column_index++)
	{
		const GroupingColumn *def = &config.grouping_columns[column_index];
		const CompressedColumnValues *column_values = &config.compressed_columns[def->input_offset];

		offset = TYPEALIGN(def->alignment_bytes, offset);

		if (column_values->decompression_type > 0)
		{
			Assert(offset <= UINT_MAX - column_values->decompression_type);
			memcpy(&serialized_key_storage[offset],
				   ((char *) column_values->buffers[1]) + column_values->decompression_type * row,
				   column_values->decompression_type);
			arrow_set_row_validity(serialized_key_validity_word,
								   column_index,
								   arrow_row_is_valid(column_values->buffers[0], row));
			offset += column_values->decompression_type;

			memcpy(&output_key_datums[column_index],
				   ((char *) column_values->buffers[1]) + column_values->decompression_type * row,
				   column_values->decompression_type);
		}
		else if (unlikely(column_values->decompression_type == DT_Scalar))
		{
			const bool is_valid = !*column_values->output_isnull;
			arrow_set_row_validity(serialized_key_validity_word, column_index, is_valid);
			if (is_valid)
			{
				if (def->by_value)
				{
					memcpy(&serialized_key_storage[offset],
						   column_values->output_value,
						   def->value_bytes);
					offset += def->value_bytes;

					memcpy(&output_key_datums[column_index],
						   column_values->output_value,
						   def->value_bytes);
				}
				else
				{
					memcpy(&serialized_key_storage[offset],
						   DatumGetPointer(*column_values->output_value),
						   VARSIZE_ANY(*column_values->output_value));

					output_key_datums[column_index] =
						PointerGetDatum(&serialized_key_storage[offset]);

					offset += VARSIZE_ANY(*column_values->output_value);
				}
			}
			else
			{
				output_key_datums[column_index] = PointerGetDatum(NULL);
			}
		}
		else if (column_values->decompression_type == DT_ArrowText)
		{
			const bool is_valid = arrow_row_is_valid(column_values->buffers[0], row);
			arrow_set_row_validity(serialized_key_validity_word, column_index, is_valid);
			if (is_valid)
			{
				const uint32 start = ((uint32 *) column_values->buffers[1])[row];
				const int32 value_bytes = ((uint32 *) column_values->buffers[1])[row + 1] - start;
				const int32 total_bytes = value_bytes + VARHDRSZ;

				SET_VARSIZE(&serialized_key_storage[offset], total_bytes);
				memcpy(VARDATA(&serialized_key_storage[offset]),
					   &((uint8 *) column_values->buffers[2])[start],
					   value_bytes);

				output_key_datums[column_index] = PointerGetDatum(&serialized_key_storage[offset]);

				offset += total_bytes;
			}
			else
			{
				output_key_datums[column_index] = PointerGetDatum(NULL);
			}
		}
		else if (column_values->decompression_type == DT_ArrowTextDict)
		{
			const bool is_valid = arrow_row_is_valid(column_values->buffers[0], row);
			arrow_set_row_validity(serialized_key_validity_word, column_index, is_valid);
			if (is_valid)
			{
				const int16 index = ((int16 *) column_values->buffers[3])[row];
				const uint32 start = ((uint32 *) column_values->buffers[1])[index];
				const int32 value_bytes = ((uint32 *) column_values->buffers[1])[index + 1] - start;
				const int32 total_bytes = value_bytes + VARHDRSZ;

				SET_VARSIZE(&serialized_key_storage[offset], total_bytes);
				memcpy(VARDATA(&serialized_key_storage[offset]),
					   &((uint8 *) column_values->buffers[2])[start],
					   value_bytes);

				output_key_datums[column_index] = PointerGetDatum(&serialized_key_storage[offset]);

				offset += total_bytes;
			}
			else
			{
				output_key_datums[column_index] = PointerGetDatum(NULL);
			}
		}
		else
		{
			pg_unreachable();
		}
	}

	Assert((void *) &serialized_key_storage[TYPEALIGN(ALIGNOF_DOUBLE, offset)] ==
		   (void *) serialized_key_validity_word);

	if (*serialized_key_validity_word != ~0ULL)
	{
		offset = TYPEALIGN(ALIGNOF_DOUBLE, offset) + sizeof(*serialized_key_validity_word);
		Assert(offset == num_bytes);
	}

	*output_key_validity_word = *serialized_key_validity_word;

	//	fprintf(stderr, "key is %d bytes: ", offset);
	//	for (size_t i = 0; i < offset; i++)
	//	{
	//		fprintf(stderr, "%.2x.", key_storage[i]);
	//	}
	//	fprintf(stderr, "\n");

	*key = (BytesView){ .data = serialized_key_storage, .len = offset };

	/*
	 * The multi-column key is always considered non-null, and the null flags
	 * for the individual columns are stored in a bitmap that is part of the
	 * key.
	 */
	*valid = true;
}

static pg_attribute_always_inline BytesView
serialized_store_key(GroupingPolicyHash *restrict policy, BytesView key)
{
	/*
	 * We will store this key so we have to consume the temporary storage that
	 * was used for it. The subsequent keys will need to allocate new memory.
	 */
	Assert(policy->tmp_key_storage == key.data);
	policy->tmp_key_storage = NULL;
	policy->num_tmp_key_storage_bytes = 0;
	return key;
}

static pg_attribute_always_inline void
serialized_destroy_key(BytesView key)
{
	/* Noop, the memory will be reused by the subsequent key. */
}

#define EXPLAIN_NAME "serialized"
#define KEY_VARIANT serialized
#define KEY_HASH(X) hash_bytes_view(X)
#define KEY_EQUAL(a, b) (a.len == b.len && memcmp(a.data, b.data, a.len) == 0)
#define STORE_HASH
#define CTYPE BytesView
#include "hash_table_functions_impl.c"
