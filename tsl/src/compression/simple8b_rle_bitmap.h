/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/*
 * This is a specialization of Simple8bRLE decoder for bitmaps, i.e. where the
 * elements are only 0 and 1.
 */
#include "compression/simple8b_rle.h"
#pragma once

typedef struct Simple8bRleBitmap
{
	char *bitmap_bools_;
	uint16 current_element;
	uint16 num_elements;
	uint16 num_ones;
} Simple8bRleBitmap;

pg_attribute_always_inline static Simple8bRleDecompressResult
simple8brle_bitmap_get_next(Simple8bRleBitmap *bitmap)
{
	Simple8bRleDecompressResult res;
	/* We have at least one element of padding so it's OK to overrun. */
	res.val = bitmap->bitmap_bools_[bitmap->current_element];
	res.is_done = bitmap->current_element >= bitmap->num_elements;
	bitmap->current_element++;
	return res;
}

pg_attribute_always_inline static Simple8bRleDecompressResult
simple8brle_bitmap_get_next_reverse(Simple8bRleBitmap *bitmap)
{
	if (bitmap->current_element >= bitmap->num_elements)
	{
		return (Simple8bRleDecompressResult){ .is_done = true };
	}

	return (Simple8bRleDecompressResult){
		.val = bitmap->bitmap_bools_[bitmap->num_elements - 1 - bitmap->current_element++]
	};
}

pg_attribute_always_inline static bool
simple8brle_bitmap_get_at(Simple8bRleBitmap *bitmap, int i)
{
	Assert(i >= 0);
	// Assert(i < bitmap->num_elements);
	return bitmap->bitmap_bools_[i];
}

pg_attribute_always_inline static void
simple8brle_bitmap_rewind(Simple8bRleBitmap *bitmap)
{
	bitmap->current_element = 0;
}

pg_attribute_always_inline static uint16
simple8brle_bitmap_num_ones(Simple8bRleBitmap *bitmap)
{
	return bitmap->num_ones;
}

static Simple8bRleBitmap
simple8brle_bitmap_decompress(Simple8bRleSerialized *compressed)
{
	Simple8bRleBitmap result;
	result.current_element = 0;
	result.num_elements = compressed->num_elements;

	if (compressed->num_elements > GLOBAL_MAX_ROWS_PER_COMPRESSION)
	{
		/* Don't allocate too much if we got corrupt data or something. */
		ereport(ERROR,
				(errmsg("the number of elements in compressed data %d is larger than the maximum "
						"allowed %d",
						compressed->num_elements,
						GLOBAL_MAX_ROWS_PER_COMPRESSION)));
	}

	const uint16 num_elements = compressed->num_elements;
	uint16 num_ones = 0;


	const uint32 num_selector_slots =
		simple8brle_num_selector_slots_for_num_blocks(compressed->num_blocks);
	const uint64 *compressed_data = compressed->slots + num_selector_slots;

	// int blocks[16] = {0};

	/*
	 * Decompress all the rows in one go for better throughput. Pad to next
	 * multiple of 64 bytes on the right, so that we can simplify the
	 * decompression loop and the get() function. Note that for get() we need at
	 * least one byte of padding, hence the next multiple.
	 */
	const uint32 num_elements_padded = ((num_elements + 63) / 64 + 1) * 64;
	char *restrict bitmap_bools_ = palloc(num_elements_padded);
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
			CheckCompressedData(repeated_value <= 1);
			CheckCompressedData(decompressed_index + n_block_values <= num_elements);

			for (int i = 0; i < n_block_values; i++)
			{
				bitmap_bools_[decompressed_index + i] = repeated_value;
			}
			decompressed_index += n_block_values;
			Assert(decompressed_index <= num_elements);

			num_ones += repeated_value * n_block_values;
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

			CheckCompressedData(decompressed_index + 64 < num_elements_padded);

#ifdef HAVE__BUILTIN_POPCOUNT
			num_ones += __builtin_popcountll(block_data);
#endif
			for (int i = 0; i < 64; i++)
			{
				const uint64 value = (block_data >> i) & 1;
				bitmap_bools_[decompressed_index + i] = value;
#ifndef HAVE__BUILTIN_POPCOUNT
				num_ones += value;
#endif
			}
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
	 * Stray ones in the higher unused bits of the last block?
	 */
	CheckCompressedData(num_ones <= num_elements);

	//	mybt();
	//	fprintf(stderr, "   1  15\n");
	//	fprintf(stderr, " %3d %3d\n\n", blocks[1], blocks[15]);

	result.bitmap_bools_ = bitmap_bools_;
	result.num_ones = num_ones;
	return result;
}
