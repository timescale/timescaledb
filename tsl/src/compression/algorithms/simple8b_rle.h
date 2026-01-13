/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <c.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>

#include <adts/bit_array.h>

#include "compat/compat.h"
#include <adts/uint64_vec.h>

/* This is defined as a header file as it is expected to be used as a primitive
 * for "real" compression algorithms, not used directly on SQL data. Also, due to inlining.
 *
 *
 * From Vo Ngoc Anh, Alistair Moffat: Index compression using 64-bit words. Softw., Pract. Exper.
 * 40(2): 131-147 (2010)
 *
 * Simple 8b RLE is a block based encoding/compression scheme for integers. Each block is made up of
 * one selector and one 64-bit data value. The interpretation of the data value is based on the
 * selector values. Selectors 1-14 indicate that the data value is a bit packing of integers, where
 * each integer takes up a constant number of bits. The value of the constant-number-of-bits is set
 * according to the table below. Selector 15 indicates that the block encodes a single "run" of RLE,
 * where the data element is a bit packing of the run count and run value.
 *
 *
 *  Selector value: 0 |  1  2  3  4  5  6  7  8  9 10 11 12 13 14 | 15 (RLE)
 *  Integers coded: 0 | 64 32 21 16 12 10  9  8  6  5  4  3  2  1 | up to 2^28
 *  Bits/integer:   0 |  1  2  3  4  5  6  7  8 10 12 16 21 32 64 | 36 bits
 *  Wasted bits:    0 |  0  0  1  0  4  4  1  0  4  4  0  1  0  0 |   N/A
 *
 *  a 0 selector is currently unused
 */

/************** Constants *****************/
#define SIMPLE8B_BITSIZE 64
#define SIMPLE8B_MAXCODE 15
#define SIMPLE8B_MINCODE 1

#define SIMPLE8B_RLE_SELECTOR SIMPLE8B_MAXCODE
#define SIMPLE8B_RLE_MAX_VALUE_BITS 36
#define SIMPLE8B_RLE_MAX_COUNT_BITS (SIMPLE8B_BITSIZE - SIMPLE8B_RLE_MAX_VALUE_BITS)
#define SIMPLE8B_RLE_MAX_VALUE_MASK ((1ULL << SIMPLE8B_RLE_MAX_VALUE_BITS) - 1)
#define SIMPLE8B_RLE_MAX_COUNT_MASK ((1ULL << SIMPLE8B_RLE_MAX_COUNT_BITS) - 1)

#define SIMPLE8B_BITS_PER_SELECTOR 4
#define SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT 16
#define SIMPLE8B_MAX_BUFFERED 256

/* clang-format off */
/* selector value:                        0,  1,  2,  3,  4,  5,  6, 7, 8,  9, 10, 11, 12, 13, 14, RLE */
#define SIMPLE8B_NUM_ELEMENTS ((uint8[]){ 0, 64, 32, 21, 16, 12, 10, 9, 8,  6,  5,  4,  3,  2,  1, 0  })
#define SIMPLE8B_BIT_LENGTH   ((uint8[]){ 0,  1,  2,  3,  4,  5,  6, 7, 8, 10, 12, 16, 21, 32, 64, 36 })
/* Map bit lengths directly to selector values */
#define SIMPLE8B_SELECTOR_FOR_BIT_WIDTH ((uint8[]){                        \
/*   0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, */     \
     1,  1,  2,  3,  4,  5,  6,  7,  8,  9,  9, 10, 10, 11, 11, 11,        \
/*  16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, */     \
    11, 12, 12, 12, 12, 12, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13,        \
/*  32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, */     \
    13, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14,        \
/*  48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64 */  \
    14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14     \
})
/* clang-format on */

/* The full block selector is used when a value occupies the full 64bit block, see above. */
#define SIMPLE8B_FULL_BLOCK_SELECTOR 14

/********************
 ***  Public API  ***
 ********************/

typedef struct Simple8bRleSerialized
{
	/* the slots are padded with 0 to fill out the last slot, so there may be up
	 * to 59 extra values stored, to counteract this, we store how many values
	 * there should be on output.
	 * We currently disallow more than 2^32 values per compression, since we're
	 * going to limit the amount of rows stored per-compressed-row anyway.
	 */
	uint32 num_elements;
	/* we store nslots as a uint32 since we'll need to fit this in a varlen, and
	 * we cannot store more than 2^32 bytes anyway
	 */
	uint32 num_blocks;
	uint64 slots[FLEXIBLE_ARRAY_MEMBER];
} Simple8bRleSerialized;

static void
pg_attribute_unused() simple8brle_size_assertions(void)
{
	Simple8bRleSerialized test_val = { 0 };
	/* ensure no padding bits make it to disk */
	StaticAssertStmt(sizeof(Simple8bRleSerialized) ==
						 sizeof(test_val.num_elements) + sizeof(test_val.num_blocks),
					 "simple8b_rle_oob wrong size");
	StaticAssertStmt(sizeof(Simple8bRleSerialized) == 8, "simple8b_rle_oob wrong size");
}

typedef struct Simple8bRleBlock
{
	uint64 data;
	uint32 num_elements_compressed;
	uint8 selector;
} Simple8bRleBlock;

typedef struct Simple8bRleBuffer
{
	uint64 data;
	uint32 repcount;
	/* The saved_repcount is placed here to help cache locality when we
	 * need to do a partial flush and need to restore the repcount. */
	uint32 saved_repcount;
} Simple8bRleBuffer;

typedef struct Simple8bRleCompressor
{
	/* The total number of elements that have been compressed. */
	uint32 num_elements;

	/* The number of elements that have been buffered in the uncompressed_buffer. */
	uint32 num_buffered_elements;

	/* The last value that has been compressed. */
	uint64 last_value;

	/* The buffer of uncompressed elements. */
	Simple8bRleBuffer uncompressed_buffer[SIMPLE8B_MAX_BUFFERED];

	BitArray selectors;
	uint64_vec compressed_data;

} Simple8bRleCompressor;

typedef struct Simple8bRleDecompressionIterator
{
	BitArray selector_data;
	BitArrayIterator selectors;
	Simple8bRleBlock current_block;

	const uint64 *compressed_data;
	int32 num_blocks;
	int32 current_compressed_pos;
	int32 current_in_compressed_pos;

	uint32 num_elements;
	uint32 num_elements_returned;
} Simple8bRleDecompressionIterator;

typedef struct Simple8bRleDecompressResult
{
	uint64 val;
	bool is_done;
} Simple8bRleDecompressResult;

static inline void simple8brle_compressor_init(Simple8bRleCompressor *compressor);
static inline Simple8bRleSerialized *
simple8brle_compressor_finish(Simple8bRleCompressor *compressor);
static inline char *simple8brle_compressor_finish_into(Simple8bRleCompressor *compressor,
													   char *dest, size_t expected_size);
static inline void simple8brle_compressor_append(Simple8bRleCompressor *compressor, uint64 val);
static inline bool simple8brle_compressor_is_empty(Simple8bRleCompressor *compressor);

static inline void
simple8brle_decompression_iterator_init_forward(Simple8bRleDecompressionIterator *iter,
												Simple8bRleSerialized *compressed);
static inline void
simple8brle_decompression_iterator_init_reverse(Simple8bRleDecompressionIterator *iter,
												Simple8bRleSerialized *compressed);
static pg_attribute_always_inline Simple8bRleDecompressResult
simple8brle_decompression_iterator_try_next_forward(Simple8bRleDecompressionIterator *iter);
static pg_attribute_always_inline Simple8bRleDecompressResult
simple8brle_decompression_iterator_try_next_reverse(Simple8bRleDecompressionIterator *iter);

static inline void simple8brle_serialized_send(StringInfo buffer,
											   const Simple8bRleSerialized *data);
static inline char *bytes_serialize_simple8b_and_advance(char *dest, size_t expected_size,
														 const Simple8bRleSerialized *data);
static inline Simple8bRleSerialized *bytes_deserialize_simple8b_and_advance(StringInfo si);
static inline size_t simple8brle_serialized_slot_size(const Simple8bRleSerialized *data);
static inline size_t simple8brle_serialized_total_size(const Simple8bRleSerialized *data);

/*
 * Calculate the size of the compressed data with the assumption that all uncompressed
 * data is flushed and pushed already.
 */
static inline size_t
simple8brle_compressor_compressed_size(const Simple8bRleCompressor *compressor);

/*
 * Calculate the size of the compressed data without modifying the compressor and without
 * making assumptions about the compressor state.
 */
static inline size_t
simple8brle_compressor_compressed_const_size(const Simple8bRleCompressor *compressor);

/*********************
 ***  Private API  ***
 *********************/

typedef struct Simple8bRlePartiallyCompressedData
{
	Simple8bRleBlock block;
	const uint64 *data;
	uint32 data_size;
} Simple8bRlePartiallyCompressedData;

/* compressor */
static inline void simple8brle_compressor_partial_flush(Simple8bRleCompressor *compressor);
static inline void simple8brle_compressor_full_flush(Simple8bRleCompressor *compressor);

/* block */
static inline Simple8bRleBlock simple8brle_block_create(uint8 selector, uint64 data);
static inline uint64 simple8brle_block_get_element(Simple8bRleBlock block,
												   uint32 position_in_value);

/* utils */
static inline bool simple8brle_selector_is_rle(uint8 selector);
static inline uint64 simple8brle_selector_get_bitmask(uint8 selector);
static inline uint32 simple8brle_bits_for_value(uint64 v);
static inline uint32 simple8brle_rledata_repeatcount(uint64 rledata);
static inline uint64 simple8brle_rledata_value(uint64 rledata);
static uint32 simple8brle_num_selector_slots_for_num_blocks(uint32 num_blocks);

/*******************************
 ***  Simple8bRleSerialized  ***
 *******************************/

static inline Simple8bRleSerialized *
simple8brle_serialized_recv(StringInfo buffer)
{
	uint32 i;
	uint32 num_elements = pq_getmsgint32(buffer);
	CheckCompressedData(num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	uint32 num_blocks = pq_getmsgint32(buffer);
	CheckCompressedData(num_blocks <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	uint32 num_selector_slots = simple8brle_num_selector_slots_for_num_blocks(num_blocks);
	Simple8bRleSerialized *data;
	Size compressed_size =
		sizeof(Simple8bRleSerialized) + (num_blocks + num_selector_slots) * sizeof(uint64);
	if (!AllocSizeIsValid(compressed_size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("compressed size exceeds the maximum allowed (%d)", (int) MaxAllocSize)));

	data = palloc(compressed_size);
	data->num_elements = num_elements;
	data->num_blocks = num_blocks;

	for (i = 0; i < num_blocks + num_selector_slots; i++)
		data->slots[i] = pq_getmsgint64(buffer);

	return data;
}

static inline void *
simple8brle_serialized_recv_into(StringInfo buffer, void *dest, Simple8bRleSerialized **data_out)
{
	uint32 i;
	uint32 num_elements = pq_getmsgint32(buffer);
	CheckCompressedData(num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	uint32 num_blocks = pq_getmsgint32(buffer);
	CheckCompressedData(num_blocks <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	uint32 num_selector_slots = simple8brle_num_selector_slots_for_num_blocks(num_blocks);

	Size compressed_size =
		sizeof(Simple8bRleSerialized) + (num_blocks + num_selector_slots) * sizeof(uint64);
	if (!AllocSizeIsValid(compressed_size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("compressed size exceeds the maximum allowed (%d)", (int) MaxAllocSize)));

	*data_out = (Simple8bRleSerialized *) dest;
	(*data_out)->num_elements = num_elements;
	(*data_out)->num_blocks = num_blocks;

	for (i = 0; i < num_blocks + num_selector_slots; i++)
		(*data_out)->slots[i] = pq_getmsgint64(buffer);

	return (char *) *data_out + compressed_size;
}

static void
simple8brle_serialized_send(StringInfo buffer, const Simple8bRleSerialized *data)
{
	Assert(NULL != data);
	uint32 num_selector_slots = simple8brle_num_selector_slots_for_num_blocks(data->num_blocks);
	uint32 i;
	pq_sendint32(buffer, data->num_elements);
	pq_sendint32(buffer, data->num_blocks);
	for (i = 0; i < data->num_blocks + num_selector_slots; i++)
		pq_sendint64(buffer, data->slots[i]);
}

static char *
bytes_serialize_simple8b_and_advance(char *dest, size_t expected_size,
									 const Simple8bRleSerialized *data)
{
	size_t size = simple8brle_serialized_total_size(data);

	if (expected_size != size)
		elog(ERROR, "the size to serialize does not match simple8brle");

	memcpy(dest, data, size);
	return dest + size;
}

static Simple8bRleSerialized *
bytes_deserialize_simple8b_and_advance(StringInfo si)
{
	Simple8bRleSerialized *serialized = consumeCompressedData(si, sizeof(Simple8bRleSerialized));
	consumeCompressedData(si, simple8brle_serialized_slot_size(serialized));

	CheckCompressedData(serialized->num_elements <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	CheckCompressedData(serialized->num_elements > 0);
	CheckCompressedData(serialized->num_blocks > 0);
	CheckCompressedData(serialized->num_elements >= serialized->num_blocks);

	return serialized;
}

static size_t
simple8brle_serialized_slot_size(const Simple8bRleSerialized *data)
{
	if (data == NULL)
		return 0;

	int total_slots =
		data->num_blocks + simple8brle_num_selector_slots_for_num_blocks(data->num_blocks);

	CheckCompressedData(total_slots > 0);
	CheckCompressedData((uint32) total_slots < PG_INT32_MAX / sizeof(uint64));

	return total_slots * sizeof(uint64);
}

static size_t
simple8brle_serialized_total_size(const Simple8bRleSerialized *data)
{
	Assert(data != NULL);
	return sizeof(*data) + simple8brle_serialized_slot_size(data);
}

/*******************************
 ***  Simple8bRleCompressor  ***
 *******************************/

static void
simple8brle_compressor_init(Simple8bRleCompressor *compressor)
{
	*compressor = (Simple8bRleCompressor){
		.num_elements = 0,
		.num_buffered_elements = 0,
		.last_value = 0,
	};

	/* Allocate the size according to the batch target */
	uint64_vec_init(&compressor->compressed_data,
					CurrentMemoryContext,
					TARGET_COMPRESSED_BATCH_SIZE);
	bit_array_init(&compressor->selectors,
				   /* expected_bits = */ (TARGET_COMPRESSED_BATCH_SIZE *
										  SIMPLE8B_BITS_PER_SELECTOR));
}

static inline void
simple8brle_compressor_partial_flush(Simple8bRleCompressor *compressor)
{
	Assert(compressor->num_buffered_elements > 0);

	uint8 bit_width = 0;
	uint64 mask = 0;
	int16 max_pack = 0;
	uint8 selector = 0;
	int32 num_buffered_elements = compressor->num_buffered_elements;

	int32 i;
	for (i = 0; i < num_buffered_elements; i++)
	{
		Simple8bRleBuffer *restrict buffer_base = &compressor->uncompressed_buffer[i];
		Assert(buffer_base[0].repcount > 0);

		uint8 first_bit_width = simple8brle_bits_for_value(buffer_base[0].data);
		selector = SIMPLE8B_SELECTOR_FOR_BIT_WIDTH[first_bit_width];
		bit_width = SIMPLE8B_BIT_LENGTH[selector];

		/* The current element times the bit width is large enough to flush. */
		if (bit_width * buffer_base[0].repcount >= SIMPLE8B_BITSIZE)
		{
			uint64 val = buffer_base[0].data;
			uint64 repcount = buffer_base[0].repcount;

			/* We can only RLE elements if they are small enough to fit in the
			 * data part of the RLE block.
			 */
			if (val <= SIMPLE8B_RLE_MAX_VALUE_MASK)
			{
				/* Flush the value as RLE */
				uint64 rle_block = (repcount << SIMPLE8B_RLE_MAX_VALUE_BITS) | val;

				bit_array_append(&compressor->selectors,
								 SIMPLE8B_BITS_PER_SELECTOR,
								 SIMPLE8B_RLE_SELECTOR);
				uint64_vec_append(&compressor->compressed_data, rle_block);
				continue;
			}
			/* Otherwise we need to flush each element as a full 64 bit block */
			else
			{
				/* Each repeated element needs to be flushed as separate blocks */
				for (uint32 k = 0; k < repcount; k++)
				{
					bit_array_append(&compressor->selectors,
									 SIMPLE8B_BITS_PER_SELECTOR,
									 SIMPLE8B_FULL_BLOCK_SELECTOR);
					uint64_vec_append(&compressor->compressed_data, val);
				}
				continue;
			}
		}

		int32 num_packed = buffer_base[0].repcount;
		max_pack = SIMPLE8B_NUM_ELEMENTS[selector];
		mask = simple8brle_selector_get_bitmask(selector);

		for (int32 j = 1; (i + j) < num_buffered_elements && num_packed < max_pack; ++j)
		{
			Assert(buffer_base[j].repcount > 0);
			uint64 val = buffer_base[j].data;

			while (val > mask)
			{
				/* We bumped into a value that doesn't fit, need to expand selector,
				 * but we need to make sure we don't leave gaps in the packed data. */
				++selector;
				mask = simple8brle_selector_get_bitmask(selector);
				if (num_packed >= SIMPLE8B_NUM_ELEMENTS[selector])
					break;
			}

			max_pack = SIMPLE8B_NUM_ELEMENTS[selector];
			num_packed += buffer_base[j].repcount;
		}

		/* Final calculations after selector is determined */
		bit_width = SIMPLE8B_BIT_LENGTH[selector];

		int32 num_buffer_taken = 0;
		uint64 packed_value = 0;
		num_packed = 0;

		/*
		 * We need to be smart with the repcounts. The case when the repeated values
		 * can fit an entire block is already handled above. Here we move the values
		 * from the current buffer entry to the packed value, thus we decrease the
		 * repcounts one by one.
		 */
		while (num_packed < max_pack && (i + num_buffer_taken) < num_buffered_elements)
		{
			Simple8bRleBuffer *restrict current_entry = &buffer_base[num_buffer_taken];

			current_entry->saved_repcount = current_entry->repcount;
			uint64 val = current_entry->data;
			uint32 entry_repcount = current_entry->repcount;

			while (entry_repcount > 0 && num_packed < max_pack)
			{
				packed_value |= (val & mask) << (num_packed * bit_width);
				++num_packed;
				--entry_repcount;
			}

			/* Write back the updated repcount. */
			current_entry->repcount = entry_repcount;

			if (entry_repcount > 0)
				break;
			++num_buffer_taken;
		}

		if (num_packed == max_pack)
		{
			/* Flush the packed value. */
			bit_array_append(&compressor->selectors, SIMPLE8B_BITS_PER_SELECTOR, selector);
			uint64_vec_append(&compressor->compressed_data, packed_value);
		}
		else
		{
			/*
			 * If we flushed some values from the uncompressed buffer, but there are
			 * some that didn't fit, we need to move them to the beginning of the buffer.
			 * We need to restore the repcounts to their original values first.
			 */
			for (int32 j = 0; j < num_buffer_taken; j++)
			{
				buffer_base[j].repcount = buffer_base[j].saved_repcount;
			}

			uint32 remaining = num_buffered_elements - i;

			if (remaining > 0)
			{
				memcpy(compressor->uncompressed_buffer,
					   &compressor->uncompressed_buffer[i],
					   remaining * sizeof(Simple8bRleBuffer));
			}
			else
			{
				/* If no elements are remaining, we can reset the buffer */
				compressor->num_buffered_elements = 0;
			}
			/* Update the number of buffered elements */
			compressor->num_buffered_elements = remaining;
			break;
		}

		i += (num_buffer_taken - 1);
	}

	/* If all elements were processed, reset buffer. */
	if (i == num_buffered_elements)
	{
		compressor->num_buffered_elements = 0;
	}
}

static inline void
simple8brle_compressor_full_flush(Simple8bRleCompressor *compressor)
{
	Assert(compressor->num_buffered_elements > 0);

	uint8 bit_width = 0;
	uint64 mask = 0;
	int16 max_pack = 0;
	uint8 selector = 0;
	int32 num_buffered_elements = compressor->num_buffered_elements;

	for (int32 i = 0; i < num_buffered_elements; i++)
	{
		Simple8bRleBuffer *restrict buffer_base = &compressor->uncompressed_buffer[i];
		Assert(buffer_base[0].repcount > 0);

		uint8 first_bit_width = simple8brle_bits_for_value(buffer_base[0].data);
		selector = SIMPLE8B_SELECTOR_FOR_BIT_WIDTH[first_bit_width];
		bit_width = SIMPLE8B_BIT_LENGTH[selector];

		/* The current element times the bit width is large enough to flush. */
		if (bit_width * buffer_base[0].repcount >= SIMPLE8B_BITSIZE)
		{
			uint64 val = buffer_base[0].data;
			uint64 repcount = buffer_base[0].repcount;

			/* We can only RLE elements if they are small enough to fit in the
			 * data part of the RLE block.
			 */
			if (val <= SIMPLE8B_RLE_MAX_VALUE_MASK)
			{
				/* Flush the value as RLE */
				uint64 rle_block = (repcount << SIMPLE8B_RLE_MAX_VALUE_BITS) | val;

				bit_array_append(&compressor->selectors,
								 SIMPLE8B_BITS_PER_SELECTOR,
								 SIMPLE8B_RLE_SELECTOR);
				uint64_vec_append(&compressor->compressed_data, rle_block);
				continue;
			}
			/* Otherwise we need to flush each element as a full 64 bit block */
			else
			{
				/* Each repeated element needs to be flushed as separate blocks */
				for (uint32 k = 0; k < repcount; k++)
				{
					bit_array_append(&compressor->selectors,
									 SIMPLE8B_BITS_PER_SELECTOR,
									 SIMPLE8B_FULL_BLOCK_SELECTOR);
					uint64_vec_append(&compressor->compressed_data, val);
				}
				continue;
			}
		}

		int32 num_packed = buffer_base[0].repcount;
		max_pack = SIMPLE8B_NUM_ELEMENTS[selector];
		mask = simple8brle_selector_get_bitmask(selector);

		for (int32 j = 1; (i + j) < num_buffered_elements && num_packed < max_pack; ++j)
		{
			Assert(buffer_base[j].repcount > 0);
			uint64 val = buffer_base[j].data;

			while (val > mask)
			{
				/* We bumped into a value that doesn't fit, need to expand selector,
				 * but we need to make sure we don't leave gaps in the packed data. */
				++selector;
				mask = simple8brle_selector_get_bitmask(selector);
				if (num_packed >= SIMPLE8B_NUM_ELEMENTS[selector])
					break;
			}

			max_pack = SIMPLE8B_NUM_ELEMENTS[selector];
			num_packed += buffer_base[j].repcount;
		}

		/* Final calculations after selector is determined */
		bit_width = SIMPLE8B_BIT_LENGTH[selector];

		int32 num_buffer_taken = 0;
		uint64 packed_value = 0;
		num_packed = 0;

		/*
		 * This is the main difference between the partial and full flush.
		 * Here we don't need to be smart with the repcounts, because we are
		 * flushing a full block no matter what. No elements can remain in the
		 * uncompressed buffer.
		 * */
		while (num_packed < max_pack && (i + num_buffer_taken) < num_buffered_elements)
		{
			uint64 val = buffer_base[num_buffer_taken].data;
			while (buffer_base[num_buffer_taken].repcount > 0 && num_packed < max_pack)
			{
				packed_value |= (val & mask) << (num_packed * bit_width);
				++num_packed;
				buffer_base[num_buffer_taken].repcount--;
			}
			if (buffer_base[num_buffer_taken].repcount > 0)
				break;
			++num_buffer_taken;
		}

		/* Flush the packed value */
		bit_array_append(&compressor->selectors, SIMPLE8B_BITS_PER_SELECTOR, selector);
		uint64_vec_append(&compressor->compressed_data, packed_value);

		i += (num_buffer_taken - 1);
	}

	compressor->num_buffered_elements = 0;
}

static void
simple8brle_compressor_append(Simple8bRleCompressor *compressor, uint64 val)
{
	Assert(compressor != NULL);

	if (unlikely(compressor->num_buffered_elements >= SIMPLE8B_MAX_BUFFERED))
	{
		simple8brle_compressor_partial_flush(compressor);
	}

	uint32 num_buffered = compressor->num_buffered_elements;

	/* Check for RLE against last element. This saves a few cycles instead of looking
	 * at the last buffered entry. */
	if (likely(num_buffered > 0))
	{
		if (likely(compressor->last_value == val))
		{
			Simple8bRleBuffer *restrict last_entry =
				&compressor->uncompressed_buffer[num_buffered - 1];

			if (likely(val == last_entry->data))
			{
				/* Increment count, no new buffer entry needed */
				last_entry->repcount++;
				compressor->num_elements++;
				return;
			}
		}
	}

	/* New unique value - buffer it. */
	Simple8bRleBuffer *restrict new_entry = &compressor->uncompressed_buffer[num_buffered];

	new_entry->data = val;
	new_entry->repcount = 1;

	compressor->num_buffered_elements = num_buffered + 1;
	compressor->num_elements++;
	compressor->last_value = val;
}

static bool
simple8brle_compressor_is_empty(Simple8bRleCompressor *compressor)
{
	return compressor->num_elements == 0;
}

static size_t
simple8brle_compressor_compressed_size(const Simple8bRleCompressor *compressor)
{
	/* we store 16 selectors per selector_slot, and one selector_slot per compressed_data_slot.
	 * use num_compressed_data_slots / 16 + 1 to ensure that rounding doesn't truncate our slots
	 * and that we always have a 0 slot at the end.
	 */
	return sizeof(Simple8bRleSerialized) +
		   compressor->compressed_data.num_elements * sizeof(*compressor->compressed_data.data) +
		   bit_array_data_bytes_used(&compressor->selectors);
}

static size_t
simple8brle_compressor_compressed_const_size(const Simple8bRleCompressor *compressor)
{
	/* Allocate temp space where the temp_compressor will put the data. Prefer static
	 * allocation to avoid palloc overhead, since this is only used for size calculation.
	 */
#define TEMP_DATA_SIZE TARGET_COMPRESSED_BATCH_SIZE
#define TEMP_SELECTORS_SIZE (TEMP_DATA_SIZE / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT)

	uint64 temp_data_static[TEMP_DATA_SIZE];
	uint64 temp_selectors_static[TEMP_SELECTORS_SIZE];
	uint64 temp_data_count = TEMP_DATA_SIZE;
	uint64 temp_selectors_count = TEMP_SELECTORS_SIZE;

	uint64 *temp_data = temp_data_static;
	uint64 *temp_selectors = temp_selectors_static;
	bool use_static = true;

	Simple8bRleCompressor temp_compressor = *compressor;

	/* Replace the data and selectors with the temp space.*/
	temp_compressor.compressed_data.data = temp_data;
	temp_compressor.compressed_data.num_elements = 0;
	temp_compressor.compressed_data.max_elements = TEMP_DATA_SIZE;
	temp_compressor.selectors.buckets.data = temp_selectors;
	temp_compressor.selectors.buckets.num_elements = 0;
	temp_compressor.selectors.buckets.max_elements = TEMP_SELECTORS_SIZE;
	temp_compressor.selectors.bits_used_in_last_bucket = 0;

	/* If the compressor is empty, we can return 0, similar to finish. */
	if (temp_compressor.num_elements == 0)
		return 0;

	/*
	 * If the compressor has no uncompressed data, we can use the original size calculation.
	 * Note that after every append, it is guaranteed that we have at least one uncompressed
	 * element. Not having uncompressed elements can only happen if the compressor is empty
	 * or finish was called, so we can use the original size calculation.
	 */
	if (compressor->num_buffered_elements == 0)
		return simple8brle_compressor_compressed_size(compressor);

	/* Because the buffering allows RLE entries to be stored, the worst case scenario for
	 * the buffer size is determined by the repcounts in the buffered elements.
	 */
	uint32 actual_buffered = 0;
	for (uint32 i = 0; i < compressor->num_buffered_elements; i++)
	{
		actual_buffered += compressor->uncompressed_buffer[i].repcount;
	}

	if (actual_buffered > temp_data_count)
	{
		/* We need to increase the temp buffer and allocate it dynamically.
		 * This can only happen if the TARGET_COMPRESSED_BATCH_SIZE is smaller
		 * than the actual buffered elements, which is unlikely.
		 */
		temp_data_count = actual_buffered;
		temp_data = palloc(temp_data_count * sizeof(uint64));

		/* Similarly the selectors */
		temp_selectors_count = (actual_buffered + 15) / 16;
		temp_selectors = palloc(temp_selectors_count * sizeof(uint64));

		use_static = false;
	}

	/* Flush the compressor to ensure all uncompressed data is compressed into the temp_compressor.
	 */
	simple8brle_compressor_full_flush(&temp_compressor);

	size_t num_data_blocks =
		compressor->compressed_data.num_elements + temp_compressor.compressed_data.num_elements;

	/* Add up the size of the compressed data blocks and the header. */
	size_t result = sizeof(Simple8bRleSerialized) +
					num_data_blocks * sizeof(*temp_compressor.compressed_data.data);

	/* Add up the selector bits. */
	size_t selector_bits = num_data_blocks * SIMPLE8B_BITS_PER_SELECTOR;
	result += ((selector_bits + 63) / 64) * sizeof(uint64);

#undef TEMP_DATA_SIZE
#undef TEMP_SELECTORS_SIZE

	if (!use_static)
	{
		/* If we allocated dynamically, we need to free the memory. */
		pfree(temp_data);
		pfree(temp_selectors);
	}

	return result;
}

static inline uint32
simple8brle_compressor_num_selectors(Simple8bRleCompressor *compressor)
{
	Assert(bit_array_num_bits(&compressor->selectors) % SIMPLE8B_BITS_PER_SELECTOR == 0);
	return bit_array_num_bits(&compressor->selectors) / SIMPLE8B_BITS_PER_SELECTOR;
}

static Simple8bRleSerialized *
simple8brle_compressor_finish(Simple8bRleCompressor *compressor)
{
	size_t size_left;
	size_t selector_size;
	size_t compressed_size;
	Simple8bRleSerialized *compressed;
	uint64 bits;

	if (compressor->num_elements == 0)
		return NULL;

	/* Flush any remaining state */
	if (compressor->num_buffered_elements > 0)
	{
		simple8brle_compressor_full_flush(compressor);
	}

	Assert(compressor->num_buffered_elements == 0);

	compressed_size = simple8brle_compressor_compressed_size(compressor);
	/* we use palloc0 despite initializing the entire structure,
	 * to ensure padding bits are zeroed, and that there's a 0 selector at the end.
	 * It would be more efficient to ensure there are no padding bits in the struct,
	 * and initialize everything ourselves
	 */
	compressed = palloc0(compressed_size);
	Assert(bit_array_num_buckets(&compressor->selectors) > 0);
	Assert(compressor->compressed_data.num_elements > 0);
	Assert(compressor->compressed_data.num_elements ==
		   simple8brle_compressor_num_selectors(compressor));

	*compressed = (Simple8bRleSerialized){
		.num_elements = compressor->num_elements,
		.num_blocks = compressor->compressed_data.num_elements,
	};

	size_left = compressed_size - sizeof(*compressed);
	Assert(size_left >= bit_array_data_bytes_used(&compressor->selectors));
	selector_size = bit_array_output(&compressor->selectors, compressed->slots, size_left, &bits);

	size_left -= selector_size;
	Assert(size_left ==
		   (compressor->compressed_data.num_elements * sizeof(*compressor->compressed_data.data)));
	Assert(compressor->selectors.buckets.num_elements ==
		   simple8brle_num_selector_slots_for_num_blocks(compressor->compressed_data.num_elements));

	memcpy(compressed->slots + compressor->selectors.buckets.num_elements,
		   compressor->compressed_data.data,
		   size_left);

	return compressed;
}

static char *
simple8brle_compressor_finish_into(Simple8bRleCompressor *compressor, char *dest,
								   size_t expected_size)
{
	size_t size_left;
	size_t selector_size;
	size_t compressed_size;
	char *end_ptr;
	Simple8bRleSerialized *compressed;
	uint64 bits;

	if (compressor->num_elements == 0)
		return NULL;

	Ensure(dest != NULL, "dest is NULL");

	/* Flush any remaining state */
	if (compressor->num_buffered_elements > 0)
	{
		simple8brle_compressor_full_flush(compressor);
	}

	Assert(compressor->num_buffered_elements == 0);

	compressed_size = simple8brle_compressor_compressed_size(compressor);
	Ensure(expected_size == compressed_size,
		   "expected_size: %zu, compressed_size: %zu",
		   expected_size,
		   compressed_size);

	compressed = (Simple8bRleSerialized *) dest;
	Assert(bit_array_num_buckets(&compressor->selectors) > 0);
	Assert(compressor->compressed_data.num_elements > 0);
	Assert(compressor->compressed_data.num_elements ==
		   simple8brle_compressor_num_selectors(compressor));

	*compressed = (Simple8bRleSerialized){
		.num_elements = compressor->num_elements,
		.num_blocks = compressor->compressed_data.num_elements,
	};

	size_left = compressed_size - sizeof(*compressed);
	Assert(size_left >= bit_array_data_bytes_used(&compressor->selectors));
	selector_size = bit_array_output(&compressor->selectors, compressed->slots, size_left, &bits);

	size_left -= selector_size;
	Assert(size_left ==
		   (compressor->compressed_data.num_elements * sizeof(*compressor->compressed_data.data)));
	Assert(compressor->selectors.buckets.num_elements ==
		   simple8brle_num_selector_slots_for_num_blocks(compressor->compressed_data.num_elements));

	memcpy(compressed->slots + compressor->selectors.buckets.num_elements,
		   compressor->compressed_data.data,
		   size_left);

	end_ptr = (char *) (compressed->slots + compressor->selectors.buckets.num_elements) + size_left;
	Assert(end_ptr == dest + expected_size);

	return end_ptr;
}

/******************************************
 ***  Simple8bRleDecompressionIterator  ***
 ******************************************/

static void
simple8brle_decompression_iterator_init_common(Simple8bRleDecompressionIterator *iter,
											   Simple8bRleSerialized *compressed)
{
	uint32 num_selector_slots =
		simple8brle_num_selector_slots_for_num_blocks(compressed->num_blocks);

	*iter = (Simple8bRleDecompressionIterator){
		.compressed_data = compressed->slots + num_selector_slots,
		.num_blocks = compressed->num_blocks,
		.current_compressed_pos = 0,
		.current_in_compressed_pos = 0,
		.num_elements = compressed->num_elements,
		.num_elements_returned = 0,
	};

	bit_array_wrap(&iter->selector_data,
				   compressed->slots,
				   compressed->num_blocks * SIMPLE8B_BITS_PER_SELECTOR);
}

static void
simple8brle_decompression_iterator_init_forward(Simple8bRleDecompressionIterator *iter,
												Simple8bRleSerialized *compressed)
{
	simple8brle_decompression_iterator_init_common(iter, compressed);
	bit_array_iterator_init(&iter->selectors, &iter->selector_data);
}

static uint32
simple8brle_decompression_iterator_max_elements(Simple8bRleDecompressionIterator *iter,
												const Simple8bRleSerialized *compressed)
{
	BitArrayIterator selectors;
	uint32 max_stored = 0;
	uint32 i;

	Assert(compressed->num_blocks > 0);

	bit_array_iterator_init(&selectors, iter->selectors.array);
	for (i = 0; i < compressed->num_blocks; i++)
	{
		uint8 selector = bit_array_iter_next(&selectors, SIMPLE8B_BITS_PER_SELECTOR);
		if (selector == 0)
			elog(ERROR, "invalid selector 0");

		if (simple8brle_selector_is_rle(selector) && iter->compressed_data)
		{
			Assert(simple8brle_rledata_repeatcount(iter->compressed_data[i]) > 0);
			max_stored += simple8brle_rledata_repeatcount(iter->compressed_data[i]);
		}
		else
		{
			Assert(selector < SIMPLE8B_MAXCODE);
			max_stored += SIMPLE8B_NUM_ELEMENTS[selector];
		}
	}
	return max_stored;
}

static void
simple8brle_decompression_iterator_init_reverse(Simple8bRleDecompressionIterator *iter,
												Simple8bRleSerialized *compressed)
{
	int32 skipped_in_last;
	simple8brle_decompression_iterator_init_common(iter, compressed);
	bit_array_iterator_init_rev(&iter->selectors, &iter->selector_data);
	skipped_in_last = simple8brle_decompression_iterator_max_elements(iter, compressed) -
					  compressed->num_elements;

	Assert(NULL != iter->compressed_data);

	iter->current_block =
		simple8brle_block_create(bit_array_iter_next_rev(&iter->selectors,
														 SIMPLE8B_BITS_PER_SELECTOR),
								 iter->compressed_data[compressed->num_blocks - 1]);
	iter->current_in_compressed_pos =
		iter->current_block.num_elements_compressed - 1 - skipped_in_last;
	iter->current_compressed_pos = compressed->num_blocks - 2;
	return;
}

/* returning a struct produces noticeably better assembly on x86_64 than returning
 * is_done and is_null via pointers; it uses two registers instead of any memory reads.
 * Since it is also easier to read, we prefer it here.
 */
static Simple8bRleDecompressResult
simple8brle_decompression_iterator_try_next_forward(Simple8bRleDecompressionIterator *iter)
{
	uint64 uncompressed;
	if (iter->num_elements_returned >= iter->num_elements)
		return (Simple8bRleDecompressResult){
			.is_done = true,
		};

	if ((uint32) iter->current_in_compressed_pos >= iter->current_block.num_elements_compressed)
	{
		CheckCompressedData(iter->current_compressed_pos < iter->num_blocks);

		iter->current_block =
			simple8brle_block_create(bit_array_iter_next(&iter->selectors,
														 SIMPLE8B_BITS_PER_SELECTOR),
									 iter->compressed_data[iter->current_compressed_pos]);
		CheckCompressedData(iter->current_block.selector != 0);
		CheckCompressedData(iter->current_block.num_elements_compressed <=
							GLOBAL_MAX_ROWS_PER_COMPRESSION);
		iter->current_compressed_pos += 1;
		iter->current_in_compressed_pos = 0;
	}

	uncompressed =
		simple8brle_block_get_element(iter->current_block, iter->current_in_compressed_pos);
	iter->num_elements_returned += 1;
	iter->current_in_compressed_pos += 1;

	return (Simple8bRleDecompressResult){
		.val = uncompressed,
	};
}

static Simple8bRleDecompressResult
simple8brle_decompression_iterator_try_next_reverse(Simple8bRleDecompressionIterator *iter)
{
	uint64 uncompressed;
	if (iter->num_elements_returned >= iter->num_elements)
		return (Simple8bRleDecompressResult){
			.is_done = true,
		};

	if (iter->current_in_compressed_pos < 0)
	{
		iter->current_block =
			simple8brle_block_create(bit_array_iter_next_rev(&iter->selectors,
															 SIMPLE8B_BITS_PER_SELECTOR),
									 iter->compressed_data[iter->current_compressed_pos]);
		iter->current_in_compressed_pos = iter->current_block.num_elements_compressed - 1;
		iter->current_compressed_pos -= 1;
	}

	uncompressed =
		simple8brle_block_get_element(iter->current_block, iter->current_in_compressed_pos);
	iter->num_elements_returned += 1;
	iter->current_in_compressed_pos -= 1;

	return (Simple8bRleDecompressResult){
		.val = uncompressed,
	};
}

/**************************
 ***  Simple8bRleBlock  ***
 **************************/

static pg_attribute_always_inline Simple8bRleBlock
simple8brle_block_create(uint8 selector, uint64 data)
{
	Simple8bRleBlock block = (Simple8bRleBlock){
		.selector = selector,
		.data = data,
	};

	if (simple8brle_selector_is_rle(block.selector))
	{
		block.num_elements_compressed = simple8brle_rledata_repeatcount(block.data);
	}
	else
	{
		block.num_elements_compressed = SIMPLE8B_NUM_ELEMENTS[block.selector];
	}

	return block;
}

static inline uint64
simple8brle_block_get_element(Simple8bRleBlock block, uint32 position_in_value)
{
	/* we're using 0 for end-of-stream, but haven't decided what to use it for */
	if (block.selector == 0)
	{
		elog(ERROR, "end of compressed integer stream");
	}
	else if (simple8brle_selector_is_rle(block.selector))
	{
		/* decode rle-encoded integers */
		uint64 repeated_value = simple8brle_rledata_value(block.data);
		CheckCompressedData(simple8brle_rledata_repeatcount(block.data) > 0);
		Assert(simple8brle_rledata_repeatcount(block.data) > position_in_value);
		return repeated_value;
	}
	else
	{
		uint64 compressed_value = block.data;
		uint32 bits_per_val = SIMPLE8B_BIT_LENGTH[block.selector];
		/* decode bit-packed integers*/
		Assert(position_in_value < SIMPLE8B_NUM_ELEMENTS[block.selector]);
		compressed_value >>= bits_per_val * position_in_value;
		compressed_value &= simple8brle_selector_get_bitmask(block.selector);
		return compressed_value;
	}

	pg_unreachable();
}

/***************************
 ***  Utility Functions  ***
 ***************************/

static pg_attribute_always_inline bool
simple8brle_selector_is_rle(uint8 selector)
{
	return selector == SIMPLE8B_RLE_SELECTOR;
}

static pg_attribute_always_inline uint32
simple8brle_rledata_repeatcount(uint64 rledata)
{
	return (uint32) ((rledata >> SIMPLE8B_RLE_MAX_VALUE_BITS) & SIMPLE8B_RLE_MAX_COUNT_MASK);
}

static pg_attribute_always_inline uint64
simple8brle_rledata_value(uint64 rledata)
{
	return rledata & SIMPLE8B_RLE_MAX_VALUE_MASK;
}

static pg_attribute_always_inline uint64
simple8brle_selector_get_bitmask(uint8 selector)
{
	uint8 bitLen = SIMPLE8B_BIT_LENGTH[selector];
	Assert(bitLen != 0);
	uint64 result = ((~0ULL) >> (64 - bitLen));
	return result;
}

static pg_attribute_always_inline uint32
simple8brle_num_selector_slots_for_num_blocks(uint32 num_blocks)
{
	return (num_blocks / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT) +
		   (num_blocks % SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT != 0 ? 1 : 0);
}

#ifdef HAVE_BUILTIN_CLZLL
static inline uint32
simple8brle_bits_for_value(uint64 v)
{
	if (v == 0)
		return 0;
	return 64 - __builtin_clzll(v);
}
#else
static inline uint32
simple8brle_bits_for_value(uint64 v)
{
	uint32 r = 0;
	if (v >= (1U << 31))
	{
		v >>= 32;
		r += 32;
	}
	if (v >= (1U << 15))
	{
		v >>= 16;
		r += 16;
	}
	if (v >= (1U << 7))
	{
		v >>= 8;
		r += 8;
	}
	if (v >= (1U << 3))
	{
		v >>= 4;
		r += 4;
	}
	if (v >= (1U << 1))
	{
		v >>= 2;
		r += 2;
	}
	if (v >= (1U << 0))
	{
		v >>= 1;
		r += 1;
	}
	return r;
}
#endif
