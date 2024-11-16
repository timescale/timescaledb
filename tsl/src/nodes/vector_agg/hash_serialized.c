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

#include "import/umash.h"

struct abbrev_key
{
	uint32 hash;
	uint64 rest;
} pg_attribute_packed;

#define UMASH
#define ABBREV_KEY_TYPE struct abbrev_key
#define KEY_HASH(X) (X.hash)
#define KEY_EQUAL(a, b) (a.hash == b.hash && a.rest == b.rest)

static pg_attribute_always_inline void
serialized_get_key(HashingConfig config, int row, void *restrict full_key_ptr,
				   void *restrict abbrev_key_ptr, bool *restrict valid)
{
	*(text **) full_key_ptr = NULL;

	ABBREV_KEY_TYPE *restrict abbrev_key = (ABBREV_KEY_TYPE *) abbrev_key_ptr;

	const int num_columns = config.num_grouping_columns;

	/*
	 * Loop through the grouping columns again and build the actual fingerprint.
	 */
	struct umash_fp_state fp_state;
	umash_fp_init(&fp_state, config.policy->umash_params, /* seed = */ -1ull);

	for (int column_index = 0; column_index < num_columns; column_index++)
	{
		const CompressedColumnValues *column_values = &config.grouping_column_values[column_index];

		if (config.have_scalar_columns && column_values->decompression_type == DT_Scalar)
		{
			const bool is_valid = !*column_values->output_isnull;
			umash_sink_update(&fp_state.sink, &is_valid, sizeof(is_valid));
			if (is_valid)
			{
				const GroupingColumn *def = &config.policy->grouping_columns[column_index];
				if (def->by_value)
				{
					umash_sink_update(&fp_state.sink,
									  column_values->output_value,
									  def->value_bytes);
				}
				else
				{
					const int32 value_bytes = VARSIZE_ANY_EXHDR(*column_values->output_value);
					umash_sink_update(&fp_state.sink,
						&value_bytes, sizeof(value_bytes));
					umash_sink_update(&fp_state.sink,
									  VARDATA_ANY(DatumGetPointer(*column_values->output_value)),
									  value_bytes);
				}
			}
			continue;
		}

		const bool is_valid =
			!config.have_scalar_columns || arrow_row_is_valid(column_values->buffers[0], row);

		umash_sink_update(&fp_state.sink, &is_valid, sizeof(is_valid));

		if (!is_valid)
		{
			continue;
		}

		if (column_values->decompression_type > 0)
		{
			umash_sink_update(&fp_state.sink,
							  ((char *) column_values->buffers[1]) +
								  column_values->decompression_type * row,
							  column_values->decompression_type);

			continue;
		}

		Assert(column_values->decompression_type == DT_ArrowText ||
			   column_values->decompression_type == DT_ArrowTextDict);

		const uint32 data_row = column_values->decompression_type == DT_ArrowTextDict ?
									((int16 *) column_values->buffers[3])[row] :
									row;
		const uint32 start = ((uint32 *) column_values->buffers[1])[data_row];
		const int32 value_bytes = ((uint32 *) column_values->buffers[1])[data_row + 1] - start;

		umash_sink_update(&fp_state.sink,
			&value_bytes,
			sizeof(value_bytes));

		umash_sink_update(&fp_state.sink,
						  &((uint8 *) column_values->buffers[2])[start],
						  value_bytes);
	}

	/*
	 * The multi-column key is always considered non-null, and the null flags
	 * for the individual columns are stored in a bitmap that is part of the
	 * key.
	 */
	*valid = true;

	struct umash_fp fp = umash_fp_digest(&fp_state);
	abbrev_key->hash = fp.hash[0] & (~(uint32) 0);
	abbrev_key->rest = fp.hash[1];
}

static pg_attribute_always_inline ABBREV_KEY_TYPE
serialized_store_key(HashingConfig config, int row, text *full_key, ABBREV_KEY_TYPE abbrev_key)
{
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
				num_bytes += total_bytes;
			}
			else
			{
				/* Long varlena, requires alignment. */
				total_bytes = value_bytes + VARHDRSZ;
				num_bytes = TYPEALIGN(4, num_bytes) + total_bytes;
			}
		}
	}

	/*
	 * The key has a null bitmap at the end.
	 */
	num_bytes += bitmap_bytes;

	uint8 *restrict serialized_key_storage =
		MemoryContextAlloc(config.policy->key_body_mctx, num_bytes);

	/*
	 * Have to memset the key with zeros, so that the alignment bytes are zeroed
	 * out.
	 * FIXME no alignment anymore, remove?
	 */
	memset(serialized_key_storage, 0, num_bytes);

	serialized_key_validity_bitmap = &serialized_key_storage[num_bytes - bitmap_bytes];

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

					offset += def->value_bytes;
				}
				else
				{
					offset = TYPEALIGN(4, offset);

					memcpy(&serialized_key_storage[offset],
						   DatumGetPointer(*column_values->output_value),
						   VARSIZE_ANY(*column_values->output_value));

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
			/* FIXME only storing new keys now, remove? */
			const int32 total_bytes = value_bytes + VARHDRSZ_SHORT;
			SET_VARSIZE_SHORT(&serialized_key_storage[offset], total_bytes);
			offset += VARHDRSZ_SHORT;
		}
		else
		{
			/* Long varlena, requires alignment. */
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

	Assert(&serialized_key_storage[offset] == (void *) serialized_key_validity_bitmap);

	/*
	 * Note that we must always save the validity bitmap, even when there are no
	 * null words, so that the key is uniquely deserializable. Otherwise a key
	 * with some nulls might collide with a key with no nulls.
	 */
	offset += bitmap_bytes;
	Assert(offset == num_bytes);

	DEBUG_PRINT("new key is %d bytes: ", offset);
	for (size_t i = 0; i < offset; i++)
	{
		DEBUG_PRINT("%.2x.", serialized_key_storage[i]);
	}
	DEBUG_PRINT("\n");

	SET_VARSIZE(serialized_key_storage, offset);

	gp_hash_output_keys(config.policy, config.policy->last_used_key_index)[0] =
		PointerGetDatum(serialized_key_storage);

	return abbrev_key;
}

static pg_attribute_always_inline void
serialized_destroy_key(const text *key)
{
	/* Noop, the memory will be reused by the subsequent key. */
}

static void
serialized_emit_key(GroupingPolicyHash *policy, uint32 current_key, TupleTableSlot *aggregated_slot)
{
	const int num_key_columns = policy->num_grouping_columns;
	const Datum serialized_key_datum = gp_hash_output_keys(policy, current_key)[0];
	const uint8 *serialized_key = (const uint8 *) VARDATA_ANY(serialized_key_datum);
	const int key_data_bytes = VARSIZE_ANY_EXHDR(serialized_key_datum);
	const uint8 *restrict ptr = serialized_key;
	const int bitmap_bytes = (num_key_columns + 7) / 8;
	Assert(bitmap_bytes <= key_data_bytes);
	const uint8 *restrict key_validity_bitmap = &serialized_key[key_data_bytes - bitmap_bytes];

	DEBUG_PRINT("emit key #%d, without header %d bytes: ", current_key, key_data_bytes);
	for (int i = 0; i < key_data_bytes; i++)
	{
		DEBUG_PRINT("%.2x.", ptr[i]);
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
		if (col->by_value)
		{
			*output = 0;
			memcpy(output, ptr, col->value_bytes);
			ptr += col->value_bytes;
		}
		else
		{
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

	Assert(ptr == key_validity_bitmap);
}

#define EXPLAIN_NAME "serialized"
#define KEY_VARIANT serialized
#define FULL_KEY_TYPE text *

#include "hash_table_functions_impl.c"
