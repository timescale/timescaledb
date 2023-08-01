
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file implements calculation of prefix sums of a Simple8bRLE-encoded
 * bitmap. Used for gorilla decompression.
 */

#pragma once

#include "compression/simple8b_rle.h"

typedef struct Simple8bRlePrefixSum
{
	uint16 num_elements;
	/*
	 * Pad to next multiple of 64 bytes on the right, so that we can simplify the
	 * decompression loop and the get() function. Note that for get() we need at
	 * least one byte of padding, hence the next multiple.
	 */
#define BITMAP_NUM_ELEMENTS (((GLOBAL_MAX_ROWS_PER_COMPRESSION + 63) / 64 + 1) * 64)
	uint16 data[BITMAP_NUM_ELEMENTS];
#undef BITMAP_NUM_ELEMENTS
} Simple8bRlePrefixSum;

pg_attribute_always_inline static uint16
simple8brle_prefix_sum_get_at(Simple8bRlePrefixSum *sums, uint16 i)
{
	Assert(i < ((sums->num_elements + 63) / 64 + 1) * 64);
	return sums->data[i];
}

pg_attribute_always_inline static uint16
simple8brle_prefix_sum_total(Simple8bRlePrefixSum *sums)
{
	return sums->data[sums->num_elements - 1];
}

static void
simple8brle_prefix_sums(Simple8bRleSerialized *compressed, Simple8bRlePrefixSum *result)
{
	CheckCompressedData(compressed->num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	CheckCompressedData(compressed->num_blocks <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	const uint16 num_elements = compressed->num_elements;

	const uint16 num_selector_slots =
		simple8brle_num_selector_slots_for_num_blocks(compressed->num_blocks);
	const uint64 *compressed_data = compressed->slots + num_selector_slots;

	/*
	 * Pad to next multiple of 64 bytes on the right, so that we can simplify the
	 * decompression loop and the get() function. Note that for get() we need at
	 * least one byte of padding, hence the next multiple.
	 */
	const uint16 num_elements_padded = ((num_elements + 63) / 64 + 1) * 64;
	const uint16 num_blocks = compressed->num_blocks;

	uint16 *restrict prefix_sums = result->data;

	uint16 current_prefix_sum = 0;
	uint16 decompressed_index = 0;
	for (uint16 block_index = 0; block_index < num_blocks; block_index++)
	{
		const uint16 selector_slot = block_index / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
		const uint16 selector_pos_in_slot = block_index % SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
		const uint64 slot_value = compressed->slots[selector_slot];
		const uint8 selector_shift = selector_pos_in_slot * SIMPLE8B_BITS_PER_SELECTOR;
		const uint64 selector_mask = 0xFULL << selector_shift;
		const uint8 selector_value = (slot_value & selector_mask) >> selector_shift;
		Assert(selector_value < 16);

		uint64 block_data = compressed_data[block_index];

		if (simple8brle_selector_is_rle(selector_value))
		{
			/*
			 * RLE block.
			 */
			const size_t n_block_values = simple8brle_rledata_repeatcount(block_data);
			CheckCompressedData(n_block_values <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

			const bool repeated_value = simple8brle_rledata_value(block_data);

			CheckCompressedData(decompressed_index + n_block_values <= num_elements);

			if (repeated_value)
			{
				for (uint16 i = 0; i < n_block_values; i++)
				{
					prefix_sums[decompressed_index + i] = current_prefix_sum + i + 1;
				}
				current_prefix_sum += n_block_values;
			}
			else
			{
				for (uint16 i = 0; i < n_block_values; i++)
				{
					prefix_sums[decompressed_index + i] = current_prefix_sum;
				}
			}

			decompressed_index += n_block_values;
			Assert(decompressed_index <= num_elements);
		}
		else
		{
			/*
			 * Bit-packed block. Since this is a bitmap, this block has 64 bits
			 * packed. The last block might contain less than maximal possible
			 * number of elements, but we have 64 bytes of padding on the right
			 * so we don't care.
			 */
			CheckCompressedData(selector_value == 1);

			Assert(SIMPLE8B_BIT_LENGTH[selector_value] == 1);
			Assert(SIMPLE8B_NUM_ELEMENTS[selector_value] == 64);

			/*
			 * We should require at least one element from the block. Previous
			 * blocks might have had incorrect lengths, so this is not an
			 * assertion.
			 */
			CheckCompressedData(decompressed_index < num_elements);

			/* Have to zero out the unused bits, so that the popcnt works properly. */
			const int elements_this_block = Min(64, num_elements - decompressed_index);
			Assert(elements_this_block <= 64);
			Assert(elements_this_block > 0);
			block_data &= (-1ULL) >> (64 - elements_this_block);

			/*
			 * The number of block elements should fit within padding. Previous
			 * blocks might have had incorrect lengths, so this is not an
			 * assertion.
			 */
			CheckCompressedData(decompressed_index + 64 < num_elements_padded);

#ifdef HAVE__BUILTIN_POPCOUNT
			for (uint16 i = 0; i < 64; i++)
			{
				const uint16 word_prefix_sum =
					__builtin_popcountll(block_data & (-1ULL >> (63 - i)));
				prefix_sums[decompressed_index + i] = current_prefix_sum + word_prefix_sum;
			}
			current_prefix_sum += __builtin_popcountll(block_data);
#else
			/*
			 * Unfortunatly, we have to have this fallback for Windows.
			 */
			for (uint16 i = 0; i < 64; i++)
			{
				const bool this_bit = (block_data >> i) & 1;
				current_prefix_sum += this_bit;
				prefix_sums[decompressed_index + i] = current_prefix_sum;
			}
#endif
			decompressed_index += 64;
		}
	}

	/*
	 * We might have unpacked more because we work in full blocks, but at least
	 * we shouldn't have unpacked less.
	 */
	CheckCompressedData(decompressed_index >= num_elements);
	Assert(decompressed_index <= num_elements_padded);

	/*
	 * Might happen if we have stray ones in the higher unused bits of the last
	 * block.
	 */
	CheckCompressedData(current_prefix_sum <= num_elements);

	result->num_elements = num_elements;
}
