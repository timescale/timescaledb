/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include "simple8b_rle.h"

/*
 * This is a specialization of Simple8bRLE decoder for encoded 1 bit values
 * as they are used to store NULL flags in the compression methods as well as
 * the values for bool compression.
 *
 * Note that in the bool compression we store a validity map instead of a NULL
 * map, which is the same except the bits are inverted.
 *
 * The goal of this decoder is to support the following use cases:
 *
 *  1. Decompress the validity map of the bool compression method.
 *  2. Decompress the values of the bool compression method.
 *  3. Decompress the NULL map of the other compression methods into a validity
 *     map in the ArrowArray. In this case the bits will be inverted.
 *
 * The reason we don't use the Simple8bRleBitmap is that the end result is an
 * array of bits and not bools.
 *
 * The complication comes from the RLE encoding of Simple8b while in the Arrow
 * validity bitmaps we have a straight array of bits.
 */

typedef struct Simple8bRleBitArray
{
	uint64 *data;
	uint32 num_elements;
	uint32 num_blocks;
	uint16 num_ones;
} Simple8bRleBitArray;

static Simple8bRleBitArray
simple8brle_bitarray_decompress(Simple8bRleSerialized *compressed, bool inverted)
{
	Simple8bRleBitArray result = { 0 };
	if (!compressed)
	{
		return result;
	}

	CheckCompressedData(compressed->num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	CheckCompressedData(compressed->num_blocks <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

	const uint32 num_elements = compressed->num_elements;

	const uint32 num_selector_slots =
		simple8brle_num_selector_slots_for_num_blocks(compressed->num_blocks);
	const uint64 *compressed_data = compressed->slots + num_selector_slots;

	const uint32 num_elements_padded = ((num_elements + 63) / 64 + 1) * 64;
	const uint32 num_blocks = compressed->num_blocks;

	result.data = palloc0(num_elements_padded / 64 * sizeof(uint64));
	result.num_elements = num_elements;
	result.num_blocks = num_blocks;

	uint64 *restrict current_output_ptr = result.data;
	uint32 decompressed_index = 0;
	uint32 bit_position = 0;

	for (uint32 block_index = 0; block_index < num_blocks; block_index++)
	{
		const uint32 selector_slot = block_index / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
		const uint32 selector_pos_in_slot = block_index % SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
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
			uint32 repeat_count = simple8brle_rledata_repeatcount(block_data);
			CheckCompressedData(repeat_count <= GLOBAL_MAX_ROWS_PER_COMPRESSION);

			/*
			 * We might get an incorrect value from the corrupt data. Explicitly
			 * truncate it to 0/1 in case the bool is not a standard bool type
			 * which would have done it for us.
			 */
			const bool repeated_value = simple8brle_rledata_value(block_data) & 1;
			const bool bit_value = repeated_value ^ inverted;

			CheckCompressedData(decompressed_index + repeat_count <= num_elements);

			if (bit_value)
			{
				result.num_ones += repeat_count;

				/* Repeated 'ones' repeat_count times */
				if ((repeat_count + bit_position) >= 64)
				{
					/* Head: Fill the remaining bits in the current word if not aligned */
					if (bit_position > 0)
					{
						uint64_t head_bits = 64 - bit_position;
						uint64_t head_mask = (1ULL << head_bits) - 1;
						*current_output_ptr |= (head_mask << bit_position);
						repeat_count -= head_bits;
						decompressed_index += head_bits;
						current_output_ptr++;
						bit_position = 0;
					}

					/* Middle: Fill complete words */
					uint64_t full_words = repeat_count / 64;
					for (uint64_t j = 0; j < full_words; j++)
					{
						*current_output_ptr = 0xFFFFFFFFFFFFFFFF;
						current_output_ptr++;
					}

					decompressed_index += full_words * 64;
					repeat_count -= full_words * 64;
				}

				/* Tail: Handle remaining bits (less than 64) */
				if (repeat_count > 0)
				{
					Assert(repeat_count < 64);
					uint64_t tail_mask = (1ULL << (repeat_count & 63)) - 1;
					*current_output_ptr |= (tail_mask << bit_position);

					decompressed_index += repeat_count;
					bit_position = (bit_position + repeat_count) % 64;
					if (bit_position == 0)
					{
						current_output_ptr++;
					}
					else
					{
						current_output_ptr = result.data + (decompressed_index / 64);
					}
				}
			}
			else
			{
				decompressed_index += repeat_count;
				bit_position = decompressed_index % 64;
				current_output_ptr = result.data + (decompressed_index / 64);
			}
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
			CheckCompressedData(decompressed_index + 64 < num_elements_padded);

			/* Have to zero out the unused bits, so that the popcnt works properly. */
			const int elements_this_block = Min(64, num_elements - decompressed_index);
			Assert(elements_this_block <= 64);
			Assert(elements_this_block > 0);

			block_data = block_data ^ -(uint64_t) inverted;
			block_data &= (~0ULL) >> (64 - elements_this_block);

			if (bit_position == 0)
			{
				/* The decoding is on exact 64bit boundaries */
				*current_output_ptr = block_data;
			}
			else
			{
				/* We need to split the word */
				uint64_t bits_remaining_in_word = 64 - bit_position;

				/* First part goes at the end of the current word */
				*current_output_ptr |= (block_data << bit_position);

				/* Second part goes at the beginning of the next word */
				*(current_output_ptr + 1) |= block_data >> bits_remaining_in_word;
			}

#ifdef HAVE__BUILTIN_POPCOUNT
			result.num_ones += __builtin_popcountll(block_data);
#else
			for (uint16 i = 0; i < 64; i++)
				result.num_ones += ((block_data >> i) & 1);
#endif
			decompressed_index += 64;
			bit_position = decompressed_index % 64;
			current_output_ptr = result.data + (decompressed_index / 64);
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
	CheckCompressedData(result.num_ones <= num_elements);
	return result;
}
