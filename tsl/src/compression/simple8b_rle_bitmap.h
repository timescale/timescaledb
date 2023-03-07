#include "compression/simple8b_rle.h"
#pragma once

typedef struct Simple8bRleBitmap
{
	uint64 *bitmap;
	int current_element;
	int num_elements;
} Simple8bRleBitmap;

static pg_attribute_always_inline __attribute__((unused)) bool
bitmap_get(const uint64 *bitmap, int row_number)
{
	const int qword_index = row_number / 64;
	const int bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;
	return (bitmap[qword_index] & mask) ? 1 : 0;
}

static pg_attribute_always_inline __attribute__((unused)) void
bitmap_set(uint64 *bitmap, int row_number, bool value)
{
	const int qword_index = row_number / 64;
	const int bit_index = row_number % 64;
	const uint64 mask = 1ull << bit_index;

	if (value)
	{
		bitmap[qword_index] |= mask;
	}
	else
	{
		bitmap[qword_index] &= ~mask;
	}

	Assert(bitmap_get(bitmap, row_number) == value);
}

static Simple8bRleDecompressResult pg_attribute_always_inline __attribute__((unused))
simple8brle_bitmap_get_next(Simple8bRleBitmap *bitmap)
{
	if (bitmap->current_element >= bitmap->num_elements)
	{
		return (Simple8bRleDecompressResult) { .is_done = true };
	}

	return (Simple8bRleDecompressResult) { .val = bitmap_get(bitmap->bitmap, bitmap->current_element++) };
}

static Simple8bRleDecompressResult pg_attribute_always_inline __attribute__((unused))
simple8brle_bitmap_get_next_reverse(Simple8bRleBitmap *bitmap)
{
	if (bitmap->current_element >= bitmap->num_elements)
	{
		return (Simple8bRleDecompressResult) { .is_done = true };
	}

	return (Simple8bRleDecompressResult) { .val = bitmap_get(bitmap->bitmap,
		bitmap->num_elements - 1 - bitmap->current_element++) };
}

static Simple8bRleBitmap __attribute__((unused))
simple8brle_decompress_bitmap(Simple8bRleSerialized *compressed)
{
	Simple8bRleBitmap result;
	result.current_element = 0;
	result.num_elements = compressed->num_elements;

	const uint32 num_elements = compressed->num_elements;
	if (num_elements > GLOBAL_MAX_ROWS_PER_COMPRESSION)
	{
		/* Don't allocate too much if we got corrupt data or something. */
		elog(ERROR,
			 "the number of elements in compressed data %d is larger than the maximum allowed %d",
			 num_elements,
			 GLOBAL_MAX_ROWS_PER_COMPRESSION);
	}

	const uint32 num_selector_slots = simple8brle_num_selector_slots_for_num_blocks(compressed->num_blocks);
	const uint64 *compressed_data = compressed->slots + num_selector_slots;

	/* Decompress all the rows in one go for better throughput. */
	uint64 *restrict bitmap = palloc(sizeof(uint64) * ((num_elements + 63) / 64) * 64);
	uint32 decompressed_index = 0;
	const uint32 num_blocks = compressed->num_blocks;
	for (uint32 block_index = 0; block_index < num_blocks; block_index++)
	{
		const int selector_slot = block_index / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
		const int selector_pos_in_slot = block_index % SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT;
		const uint64 slot_value = compressed->slots[selector_slot];
		const uint8 selector_shift = selector_pos_in_slot * SIMPLE8B_BITS_PER_SELECTOR;
		const uint64 selector_mask = 0xFULL << selector_shift;
		const uint8 selector_value = (slot_value & selector_mask) >> selector_shift;
		Assert(selector_value < 16);
		Assert(selector_value == 1 || selector_value == 15);

		const uint64 block_data = compressed_data[block_index];

		if (simple8brle_selector_is_rle(selector_value))
		{
			/*
			 * RLE block.
			 */
			const int n_block_values = simple8brle_rledata_repeatcount(block_data);
			const uint64 repeated_value = simple8brle_rledata_value(block_data);
			Assert(repeated_value == 0 || repeated_value == 1);
			for (int i = 0; i < n_block_values; i++)
			{
				bitmap_set(bitmap, decompressed_index + i, repeated_value);
			}
			decompressed_index += n_block_values;
			Assert(decompressed_index <= num_elements);
		}
		else
		{
			/*
			 * Bit-packed block. The last one might contain less than maximal
			 * possible number of elements.
			 */
			const int n_block_values = (block_index == num_blocks - 1) ?
										   num_elements - decompressed_index :
										   SIMPLE8B_NUM_ELEMENTS[selector_value];

			Assert(n_block_values <= SIMPLE8B_NUM_ELEMENTS[selector_value]);
			Assert(SIMPLE8B_BIT_LENGTH[selector_value] == 1);
			Assert(selector_value == 1);

			for (int i = 0; i < n_block_values; i++)
			{
				const uint64 value = (block_data >> i) & 1;
				Assert(value == 0 || value == 1);
				bitmap_set(bitmap, decompressed_index + i, value);
			}
			decompressed_index += n_block_values;
			Assert(decompressed_index <= num_elements);
		}
	}

	result.bitmap = bitmap;
	return result;
}

