/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Decompress the entire batch of deltadelta-compressed rows into an Arrow array.
 * Specialized for each supported data type.
 *
 * dd6 modification: replaces simple8brle_bitmap_decompress (1 byte/element
 * bool array + per-element accessor) with simple8brle_bitarray_decompress
 * (1 bit/element uint64 bitmap, directly usable as ArrowArray validity bitmap).
 *
 * This eliminates: validity bitmap allocation, memset, tail masking,
 * and per-null arrow_set_row_validity calls. The backward scatter loop
 * reads validity bits directly from the pre-built bitmap.
 *
 * The fused decode loop (dd3_fused_decode) is unchanged from dd3/dd5.
 */

#define FUNCTION_NAME_HELPER(X, Y) X##_##Y
#define FUNCTION_NAME(X, Y) FUNCTION_NAME_HELPER(X, Y)

/*
 * Fused Simple8b decode + zigzag + double prefix-sum.
 * (Identical to dd3_fused_decode — no changes needed here.)
 */
static void
FUNCTION_NAME(dd6_fused_decode, ELEMENT_TYPE)(
	Simple8bRleSerialized *compressed,
	ELEMENT_TYPE *restrict output,
	uint32 n_output_padded)
{
	const uint32 num_blocks = compressed->num_blocks;
	const uint32 num_selector_slots =
		simple8brle_num_selector_slots_for_num_blocks(num_blocks);

	static const uint8 bit_length[16] =
		{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 16, 21, 32, 64, 36 };
	static const uint8 num_elements[16] =
		{ 0, 64, 32, 21, 16, 12, 10, 9, 8, 6, 5, 4, 3, 2, 1, 0 };

	Assert(num_blocks <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	const uint64 *slots = compressed->slots;

	const uint64 *blocks = slots + num_selector_slots;
	ELEMENT_TYPE current_delta = 0;
	ELEMENT_TYPE current_element = 0;
	uint32 out_pos = 0;

	for (uint32 bi = 0; bi < num_blocks && out_pos < n_output_padded; bi++)
	{
		const uint8 sel = (slots[bi >> 4] >> ((bi & 15) << 2)) & 0xF;
		const uint64 block_data = blocks[bi];

		if (unlikely(sel == 15))
		{
			const uint32 n_rle = simple8brle_rledata_repeatcount(block_data);
			const uint64 rle_val = simple8brle_rledata_value(block_data);
			const ELEMENT_TYPE d = zig_zag_decode(rle_val);

			uint32 remaining = n_rle;
			if (out_pos + remaining > n_output_padded)
				remaining = n_output_padded - out_pos;

			if (likely(d == 0))
			{
				const ELEMENT_TYPE stride = current_delta;
				const ELEMENT_TYPE base = current_element;
				ELEMENT_TYPE *restrict dst = output + out_pos;

				for (uint32 j = 0; j < remaining; j++)
				{
					dst[j] = (ELEMENT_TYPE)(base + (ELEMENT_TYPE)(j + 1) * stride);
				}

				current_element = (ELEMENT_TYPE)(base + (ELEMENT_TYPE)remaining * stride);
				out_pos += remaining;
			}
			else
			{
				for (uint32 j = 0; j < remaining; j++, out_pos++)
				{
					current_delta += d;
					current_element += current_delta;
					output[out_pos] = current_element;
				}
			}
		}
		else if (sel == 14)
		{
			current_delta += zig_zag_decode(block_data);
			current_element += current_delta;
			output[out_pos++] = current_element;
		}
		else if (sel == 13)
		{
			current_delta += zig_zag_decode(block_data & 0xFFFFFFFF);
			current_element += current_delta;
			output[out_pos++] = current_element;

			if (likely(out_pos < n_output_padded))
			{
				current_delta += zig_zag_decode(block_data >> 32);
				current_element += current_delta;
				output[out_pos++] = current_element;
			}
		}
		else
		{
			const uint8 bpv = bit_length[sel];
			const uint16 n_block_vals = num_elements[sel];
			const uint64 bitmask = (~0ULL) >> (64 - bpv);

			CheckCompressedData(bpv <= sizeof(uint64) * 8);

			uint32 n = n_block_vals;
			if (out_pos + n > n_output_padded)
				n = n_output_padded - out_pos;

			for (uint32 j = 0; j < n; j++, out_pos++)
			{
				uint64 val = (block_data >> (bpv * j)) & bitmask;
				current_delta += zig_zag_decode(val);
				current_element += current_delta;
				output[out_pos] = current_element;
			}
		}
	}
}

static ArrowArray *
FUNCTION_NAME(delta_delta_decompress_all, ELEMENT_TYPE)(Datum compressed, MemoryContext dest_mctx)
{
	StringInfoData si = { .data = DatumGetPointer(compressed), .len = VARSIZE(compressed) };
	DeltaDeltaCompressed *header = consumeCompressedData(&si, sizeof(DeltaDeltaCompressed));
	Simple8bRleSerialized *deltas_compressed = bytes_deserialize_simple8b_and_advance(&si);

	const bool has_nulls = header->has_nulls == 1;

	Assert(header->has_nulls == 0 || header->has_nulls == 1);

	const uint32 num_deltas = deltas_compressed->num_elements;

	/*
	 * dd6: decompress null bitmap directly as a bitarray validity bitmap.
	 * With inverted=true, the result has 1=valid, 0=null — exactly what
	 * ArrowArray expects.  No separate validity bitmap allocation, memset,
	 * or tail masking needed.
	 */
	Simple8bRleBitArray validity_bits = { 0 };
	if (has_nulls)
	{
		Simple8bRleSerialized *nulls_compressed = bytes_deserialize_simple8b_and_advance(&si);
		validity_bits = simple8brle_bitarray_decompress(nulls_compressed, /* inverted */ true);
	}

#define INNER_LOOP_SIZE_LOG2 3
#define INNER_LOOP_SIZE (1 << INNER_LOOP_SIZE_LOG2)
	const uint32 n_total = has_nulls ? validity_bits.num_elements : num_deltas;
	const uint32 n_total_padded = pad_to_multiple(INNER_LOOP_SIZE, n_total);
	const uint32 n_notnull = num_deltas;
	const uint32 n_notnull_padded = pad_to_multiple(INNER_LOOP_SIZE, n_notnull);
	Assert(n_total_padded >= n_total);
	Assert(n_notnull_padded >= n_notnull);
	Assert(n_total >= n_notnull);
	Assert(n_total <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	const int buffer_bytes = n_total_padded * sizeof(ELEMENT_TYPE) + 8;
	ELEMENT_TYPE *restrict decompressed_values = MemoryContextAlloc(dest_mctx, buffer_bytes);

	FUNCTION_NAME(dd6_fused_decode, ELEMENT_TYPE)(
		deltas_compressed, decompressed_values, n_notnull_padded);

#undef INNER_LOOP_SIZE_LOG2
#undef INNER_LOOP_SIZE

	uint64 *restrict validity_bitmap = NULL;
	if (has_nulls)
	{
		/*
		 * dd6: the bitarray IS the validity bitmap — use it directly.
		 * No allocation, no memset(0xFF), no tail masking.
		 */
		validity_bitmap = validity_bits.data;

		/*
		 * Consistency check: with inverted=true, num_ones counts valid
		 * (non-null) elements — must equal the number of delta values.
		 */
		CheckCompressedData(n_notnull == validity_bits.num_ones);

		/*
		 * Backward scatter: move non-null values from their compact
		 * positions to their final positions, leaving gaps for nulls.
		 * The validity bitmap is already correct — no bit manipulation.
		 */
		int current_notnull_element = n_notnull - 1;
		for (int i = n_total - 1; i >= 0; i--)
		{
			Assert(i >= current_notnull_element);

			if (!((validity_bitmap[i / 64] >> (i % 64)) & 1))
			{
				/* Position i is null — nothing to do (bitmap already 0) */
			}
			else
			{
				Assert(current_notnull_element >= 0);
				decompressed_values[i] = decompressed_values[current_notnull_element];
				current_notnull_element--;
			}
		}

		Assert(current_notnull_element == -1);
	}

	ArrowArray *result = MemoryContextAllocZero(dest_mctx, sizeof(ArrowArray) + sizeof(void *) * 2);
	const void **buffers = (const void **) &result[1];
	buffers[0] = validity_bitmap;
	buffers[1] = decompressed_values;
	result->n_buffers = 2;
	result->buffers = buffers;
	result->length = n_total;
	result->null_count = n_total - n_notnull;
	return result;
}

#undef FUNCTION_NAME
#undef FUNCTION_NAME_HELPER
