/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 *
 * This is a specialization of Simple8bRLE decoder for bitmaps, i.e. where the
 * elements are only 0 and 1.
 */
#include "compression/simple8b_rle.h"
#pragma once

typedef struct Simple8bRleBitmap
{
	char *bitmap;
	uint16 current_element;
	uint16 num_elements;
} Simple8bRleBitmap;

pg_attribute_always_inline static Simple8bRleDecompressResult
simple8brle_bitmap_get_next(Simple8bRleBitmap *bitmap)
{
	if (bitmap->current_element >= bitmap->num_elements)
	{
		return (Simple8bRleDecompressResult){ .is_done = true };
	}

	return (Simple8bRleDecompressResult){ .val = bitmap->bitmap[bitmap->current_element++] };
}

pg_attribute_always_inline static Simple8bRleDecompressResult
simple8brle_bitmap_get_next_reverse(Simple8bRleBitmap *bitmap)
{
	if (bitmap->current_element >= bitmap->num_elements)
	{
		return (Simple8bRleDecompressResult){ .is_done = true };
	}

	return (Simple8bRleDecompressResult){
		.val = bitmap->bitmap[bitmap->num_elements - 1 - bitmap->current_element++]
	};
}

pg_attribute_always_inline static void
simple8brle_bitmap_rewind(Simple8bRleBitmap *bitmap)
{
	bitmap->current_element = 0;
}

static Simple8bRleBitmap
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

	const uint32 num_selector_slots =
		simple8brle_num_selector_slots_for_num_blocks(compressed->num_blocks);
	const uint64 *compressed_data = compressed->slots + num_selector_slots;

	// int blocks[16] = {0};

	/*
	 * Decompress all the rows in one go for better throughput. Add 64 bytes of
	 * padding on the right so that we can simplify the decompression loop.
	 */
	char *restrict bitmap = palloc(((num_elements + 63) / 64 + 1) * 64);
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

		//		blocks[selector_value]++;

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
				bitmap[decompressed_index + i] = repeated_value;
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
			Assert(selector_value == 1);
			Assert(SIMPLE8B_BIT_LENGTH[selector_value] == 1);
			Assert(SIMPLE8B_NUM_ELEMENTS[selector_value] == 64);

			for (int i = 0; i < 64; i++)
			{
				const uint64 value = (block_data >> i) & 1;
				bitmap[decompressed_index + i] = value;
			}
			decompressed_index += 64;
		}
	}

	//	mybt();
	//	fprintf(stderr, "   1  15\n");
	//	fprintf(stderr, " %3d %3d\n\n", blocks[1], blocks[15]);

	result.bitmap = bitmap;
	return result;
}
