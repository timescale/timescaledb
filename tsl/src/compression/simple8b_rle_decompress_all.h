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
 */
static uint16
FUNCTION_NAME(simple8brle_decompress_all_buf,
			  ELEMENT_TYPE)(Simple8bRleSerialized *compressed,
							ELEMENT_TYPE *restrict decompressed_values, uint16 n_buffer_elements)
{
	const uint16 n_total_values = compressed->num_elements;

	const uint16 num_selector_slots =
		simple8brle_num_selector_slots_for_num_blocks(compressed->num_blocks);
	const uint16 num_blocks = compressed->num_blocks;

	/*
	 * Unpack the selector slots to get the selector values. Best done separately,
	 * so that this loop can be vectorized.
	 */
	Assert(num_blocks <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	uint8 selector_values[GLOBAL_MAX_ROWS_PER_COMPRESSION];
	const uint64 *restrict slots = compressed->slots;
	for (uint32 block_index = 0; block_index < num_blocks; block_index++)
	{
		const uint16 selector_slot = block_index / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
		const uint16 selector_pos_in_slot = block_index % SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
		const uint64 slot_value = slots[selector_slot];
		const uint8 selector_shift = selector_pos_in_slot * SIMPLE8B_BITS_PER_SELECTOR;
		const uint64 selector_mask = 0xFULL << selector_shift;
		const uint8 selector_value = (slot_value & selector_mask) >> selector_shift;
		selector_values[block_index] = selector_value;
	}

	/*
	 * Now decompress the individual blocks.
	 */
	int decompressed_index = 0;
	const uint64 *restrict blocks = compressed->slots + num_selector_slots;
	for (uint32 block_index = 0; block_index < num_blocks; block_index++)
	{
		const int selector_value = selector_values[block_index];
		const uint64 block_data = blocks[block_index];

		/* We don't see RLE blocks so often in the real data, <1% of blocks. */
		if (unlikely(simple8brle_selector_is_rle(selector_value)))
		{
			const uint16 n_block_values = simple8brle_rledata_repeatcount(block_data);
			CheckCompressedData(decompressed_index + n_block_values <= n_buffer_elements);

			const ELEMENT_TYPE repeated_value = simple8brle_rledata_value(block_data);
			for (uint16 i = 0; i < n_block_values; i++)
			{
				decompressed_values[decompressed_index + i] = repeated_value;
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
		CheckCompressedData(bits_per_value / 8 <= sizeof(ELEMENT_TYPE));                           \
                                                                                                   \
		/*                                                                                         \
		 * The last block might have less values than normal, but we have                          \
		 * padding at the end so we can unpack them all always for simpler                         \
		 * code. We still have to check if they fit, because the incoming data                     \
		 * might be incorrect.                                                                     \
		 */                                                                                        \
		const uint16 n_block_values = SIMPLE8B_NUM_ELEMENTS[X];                                    \
		CheckCompressedData(decompressed_index + n_block_values < n_buffer_elements);              \
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
								   ELEMENT_TYPE)(Simple8bRleSerialized *compressed, uint16 *n_)
	pg_attribute_unused();

static ELEMENT_TYPE *
FUNCTION_NAME(simple8brle_decompress_all, ELEMENT_TYPE)(Simple8bRleSerialized *compressed,
														uint16 *n_)
{
	const uint16 n_total_values = compressed->num_elements;
	Assert(n_total_values <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	/*
	 * We need a significant padding of 64 elements, not bytes, here, because we
	 * work in Simple8B blocks which can contain up to 64 elements.
	 */
	const uint16 n_buffer_elements = ((n_total_values + 63) / 64 + 1) * 64;

	ELEMENT_TYPE *restrict decompressed_values = palloc(sizeof(ELEMENT_TYPE) * n_buffer_elements);

	*n_ = FUNCTION_NAME(simple8brle_decompress_all_buf,
						ELEMENT_TYPE)(compressed, decompressed_values, n_buffer_elements);

	return decompressed_values;
}
