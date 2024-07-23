/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#define FUNCTION_NAME_HELPER(X, Y) X##_##Y
#define FUNCTION_NAME(X, Y) FUNCTION_NAME_HELPER(X, Y)

/*
 * Specialization of bulk simple8brle decompression for a data type specified by
 * ELEMENT_TYPE macro.
 *
 * The buffer must have a padding of 63 elements after the last one, because
 * decompression is performed always in full blocks.
 */
static uint32
FUNCTION_NAME(simple8brle_decompress_all_buf,
			  ELEMENT_TYPE)(Simple8bRleSerialized *compressed,
							ELEMENT_TYPE *restrict decompressed_values, uint32 n_buffer_elements)
{
	const uint32 n_total_values = compressed->num_elements;

	/*
	 * Caller must have allocated a properly sized buffer, see the comment above.
	 */
	Assert(n_buffer_elements >= n_total_values + 63);

	const uint32 num_selector_slots =
		simple8brle_num_selector_slots_for_num_blocks(compressed->num_blocks);
	const uint32 num_blocks = compressed->num_blocks;

	/*
	 * Unpack the selector slots to get the selector values. Best done separately,
	 * so that this loop can be vectorized.
	 */
	Assert(num_blocks <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	uint8 selector_values[GLOBAL_MAX_ROWS_PER_COMPRESSION];
	const uint64 *slots = compressed->slots;
	for (uint32 block_index = 0; block_index < num_blocks; block_index++)
	{
		const uint32 selector_slot = block_index / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
		const uint32 selector_pos_in_slot = block_index % SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
		const uint64 slot_value = slots[selector_slot];
		const uint8 selector_shift = selector_pos_in_slot * SIMPLE8B_BITS_PER_SELECTOR;
		const uint64 selector_mask = 0xFULL << selector_shift;
		const uint8 selector_value = (slot_value & selector_mask) >> selector_shift;
		selector_values[block_index] = selector_value;
	}

	/*
	 * Now decompress the individual blocks.
	 */
	uint32 decompressed_index = 0;
	const uint64 *blocks = compressed->slots + num_selector_slots;
	for (uint32 block_index = 0; block_index < num_blocks; block_index++)
	{
		const uint8 selector_value = selector_values[block_index];
		const uint64 block_data = blocks[block_index];

		/* We don't see RLE blocks so often in the real data, <1% of blocks. */
		if (unlikely(simple8brle_selector_is_rle(selector_value)))
		{
			const uint16 n_block_values = simple8brle_rledata_repeatcount(block_data);
			CheckCompressedData(n_block_values <= n_buffer_elements);
			CheckCompressedData(decompressed_index <= n_buffer_elements - n_block_values);

			const uint64 repeated_value_raw = simple8brle_rledata_value(block_data);
			const ELEMENT_TYPE repeated_value_converted = repeated_value_raw;
			CheckCompressedData(repeated_value_raw == (uint64) repeated_value_converted);

			for (uint16 i = 0; i < n_block_values; i++)
			{
				decompressed_values[decompressed_index + i] = repeated_value_converted;
			}

			decompressed_index += n_block_values;
		}
		else
		{
			/* Bit-packed blocks. Generate separate code for each block type. */
#define UNPACK_BLOCK(X)                                                                            \
	case (X):                                                                                      \
	{                                                                                              \
		/*                                                                                         \
		 * Error out it if the bit width is higher than that of the destination                    \
		 * type. We could just skip it, but this way the result of e.g. gorilla                    \
		 * decompression will be closer to what the row-by-row decompression                       \
		 * produces, which is easier for testing.                                                  \
		 */                                                                                        \
		const uint8 bits_per_value = SIMPLE8B_BIT_LENGTH[X];                                       \
		CheckCompressedData(bits_per_value <= sizeof(ELEMENT_TYPE) * 8);                           \
                                                                                                   \
		/*                                                                                         \
		 * The last block might have less values than normal, but we have                          \
		 * padding at the end so we can unpack them all always for simpler                         \
		 * code. We still have to check if they fit, because the incoming data                     \
		 * might be incorrect.                                                                     \
		 */                                                                                        \
		const uint16 n_block_values = SIMPLE8B_NUM_ELEMENTS[X];                                    \
		CheckCompressedData(n_block_values <= n_buffer_elements);                                  \
		CheckCompressedData(decompressed_index <= n_buffer_elements - n_block_values);             \
                                                                                                   \
		const uint64 bitmask = simple8brle_selector_get_bitmask(X);                                \
                                                                                                   \
		for (uint16 i = 0; i < n_block_values; i++)                                                \
		{                                                                                          \
			const ELEMENT_TYPE value = (block_data >> (bits_per_value * i)) & bitmask;             \
			decompressed_values[decompressed_index + i] = value;                                   \
		}                                                                                          \
		decompressed_index += n_block_values;                                                      \
		break;                                                                                     \
	}

			switch (selector_value)
			{
				UNPACK_BLOCK(1);
				UNPACK_BLOCK(2);
				UNPACK_BLOCK(3);
				UNPACK_BLOCK(4);
				UNPACK_BLOCK(5);
				UNPACK_BLOCK(6);
				UNPACK_BLOCK(7);
				UNPACK_BLOCK(8);
				UNPACK_BLOCK(9);
				UNPACK_BLOCK(10);
				UNPACK_BLOCK(11);
				UNPACK_BLOCK(12);
				UNPACK_BLOCK(13);
				UNPACK_BLOCK(14);
				default:
					/*
					 * Can only get 0 here in case the data is corrupt. Doesn't
					 * harm to report it right away, because this loop can't be
					 * vectorized.
					 */
					CheckCompressedData(false);
			}
#undef UNPACK_BLOCK
		}
	}

	/*
	 * We can decompress more than expected because we work in full blocks,
	 * but if we decompressed less, this means broken data. Better to report it
	 * not to have an uninitialized tail.
	 */
	CheckCompressedData(decompressed_index >= n_total_values);
	Assert(decompressed_index <= n_buffer_elements);

	return n_total_values;
}

/*
 * The same function as above, but does palloc instead of taking the buffer as
 * an input. We mark it as possibly unused because it is used not for every
 * element type we have.
 */
static ELEMENT_TYPE *FUNCTION_NAME(simple8brle_decompress_all,
								   ELEMENT_TYPE)(Simple8bRleSerialized *compressed, uint32 *n_)
	pg_attribute_unused();

static ELEMENT_TYPE *
FUNCTION_NAME(simple8brle_decompress_all, ELEMENT_TYPE)(Simple8bRleSerialized *compressed,
														uint32 *n_)
{
	const uint32 n_total_values = compressed->num_elements;
	Assert(n_total_values <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	/*
	 * We need a quite significant padding of 63 elements, not bytes, after the
	 * last element, because we work in Simple8B blocks which can contain up to
	 * 64 elements.
	 */
	const uint32 n_buffer_elements = n_total_values + 63;

	ELEMENT_TYPE *restrict decompressed_values = palloc(sizeof(ELEMENT_TYPE) * n_buffer_elements);

	*n_ = FUNCTION_NAME(simple8brle_decompress_all_buf,
						ELEMENT_TYPE)(compressed, decompressed_values, n_buffer_elements);

	return decompressed_values;
}
