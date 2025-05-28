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
#include <port/pg_bitutils.h>

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
#define SIMPLE8B_UNLIMITED_BIT_LIMIT 0

#define SIMPLE8B_RLE_SELECTOR SIMPLE8B_MAXCODE
#define SIMPLE8B_RLE_MAX_VALUE_BITS 36
#define SIMPLE8B_RLE_MAX_COUNT_BITS (SIMPLE8B_BITSIZE - SIMPLE8B_RLE_MAX_VALUE_BITS)
#define SIMPLE8B_RLE_MAX_VALUE_MASK ((1ULL << SIMPLE8B_RLE_MAX_VALUE_BITS) - 1)
#define SIMPLE8B_RLE_MAX_COUNT_MASK ((1ULL << SIMPLE8B_RLE_MAX_COUNT_BITS) - 1)

#define SIMPLE8B_BITS_PER_SELECTOR 4
#define SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT 16

#define SIMPLE8B_MAX_VALUES_PER_SLOT 64

/* clang-format off */
/* selector value:                        0,  1,  2,  3,  4,  5,  6, 7, 8,  9, 10, 11, 12, 13, 14, RLE */
#define SIMPLE8B_NUM_ELEMENTS ((uint8[]){ 0, 64, 32, 21, 16, 12, 10, 9, 8,  6,  5,  4,  3,  2,  1, 0  })
#define SIMPLE8B_BIT_LENGTH   ((uint8[]){ 0,  1,  2,  3,  4,  5,  6, 7, 8, 10, 12, 16, 21, 32, 64, 36 })
/* convert number of bits to selector value */
#define SIMPLE8B_BITS_TO_SELECTOR ((uint8[]){ \
	/* 0 - 9 bits */ \
	1,  1,  2,  3,  4,  5,  6, 7,  8,  9, \
	/* 10 - 19 bits */ \
	9,  10,  10,  11,  11, 11,  11, 12,  12,  12, \
	/* 20 - 29 bits */ \
	12,  12,  13,  13,  13,  13,  13, 13,  13,  13, \
	/* 30 - 39 bits */ \
	13,  13,  13,  14,  14,  14,  14, 14,  14,  14, \
	/* 40 - 49 bits */ \
	14,  14,  14,  14,  14,  14,  14, 14,  14,  14, \
	/* 50 - 59 bits */ \
	14,  14,  14,  14,  14,  14,  14, 14,  14,  14, \
	/* 60 - 64 bits */ \
	14,  14,  14,  14,  14,  14,  14, 14,  14,  14, \
})
/* clang-format on */

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

typedef struct Simple8bRleCompressor
{
	BitArray selectors;
	bool last_block_set;

	Simple8bRleBlock last_block;

	uint64_vec compressed_data;

	uint32 num_elements;

	uint32 num_uncompressed_elements;
	uint64 uncompressed_elements[SIMPLE8B_MAX_VALUES_PER_SLOT];

	uint8 bit_limit;
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

static inline void simple8brle_compressor_init(Simple8bRleCompressor *compressor, uint8 bit_limit);
static inline void simple8brle_compressor_init_zero(Simple8bRleCompressor *compressor);
static inline void simple8brle_compressor_init_bits(Simple8bRleCompressor *compressor,
													uint16 num_bits, bool value);

static inline Simple8bRleSerialized *
simple8brle_compressor_finish(Simple8bRleCompressor *compressor);
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
static inline size_t simple8brle_compressor_compressed_size(Simple8bRleCompressor *compressor);

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
static void simple8brle_compressor_push_block(Simple8bRleCompressor *compressor,
											  Simple8bRleBlock block);
static void simple8brle_compressor_flush(Simple8bRleCompressor *compressor);
static void simple8brle_compressor_append_pcd(Simple8bRleCompressor *compressor,
											  const Simple8bRlePartiallyCompressedData *new_data);
static void
simple8brle_compressor_append_pcd_bits(Simple8bRleCompressor *compressor,
									   const Simple8bRlePartiallyCompressedData *new_data);

/* block */
static inline Simple8bRleBlock simple8brle_block_create_rle(uint32 rle_count, uint64 rle_val);
static inline Simple8bRleBlock simple8brle_block_create(uint8 selector, uint64 data);
static inline uint64 simple8brle_block_get_element(Simple8bRleBlock block,
												   uint32 position_in_value);
static inline void simple8brle_block_append_element(Simple8bRleBlock *block, uint64 val);
static inline uint32 simple8brle_block_append_rle(Simple8bRleBlock *compressed_block,
												  const uint64 *data, uint32 data_len);

/* utils */
static inline bool simple8brle_selector_is_rle(uint8 selector);
static inline uint64 simple8brle_selector_get_bitmask(uint8 selector);
static inline uint32 simple8brle_rledata_repeatcount(uint64 rledata);
static inline uint64 simple8brle_rledata_value(uint64 rledata);
static uint32 simple8brle_num_selector_slots_for_num_blocks(uint32 num_blocks);

/*******************************
 ***  Simple8bRleSerialized  ***
 *******************************/

static Simple8bRleSerialized *
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
simple8brle_compressor_init(Simple8bRleCompressor *compressor, uint8 bit_limit)
{
	*compressor = (Simple8bRleCompressor){
		.num_elements = 0,
		.num_uncompressed_elements = 0,
		.bit_limit = bit_limit,
	};
	/*
	 * It is good to have some estimate of the resulting size of compressed
	 * data, because it helps to allocate memory in advance to avoid frequent
	 * reallocations. Here we use a completely arbitrary but pretty realistic
	 * ratio of 10.
	 */
	const int expected_compression_ratio = 10;
	uint64_vec_init(&compressor->compressed_data,
					CurrentMemoryContext,
					bit_limit == 1 ?
						((127 + TARGET_COMPRESSED_BATCH_SIZE) / SIMPLE8B_MAX_VALUES_PER_SLOT) :
						GLOBAL_MAX_ROWS_PER_COMPRESSION / expected_compression_ratio);
	bit_array_init(&compressor->selectors,
				   /* expected_bits = */
				   bit_limit == 1 ? (127 + TARGET_COMPRESSED_BATCH_SIZE) *
										SIMPLE8B_BITS_PER_SELECTOR / SIMPLE8B_MAX_VALUES_PER_SLOT :
									(GLOBAL_MAX_ROWS_PER_COMPRESSION * SIMPLE8B_BITS_PER_SELECTOR) /
										expected_compression_ratio);

	compressor->selectors.buckets.data[compressor->selectors.buckets.num_elements] = 0;
	compressor->selectors.buckets.num_elements++;
}

static void
simple8brle_compressor_init_zero(Simple8bRleCompressor *compressor)
{
	memset(compressor, 0, sizeof(*compressor));
}

inline void
simple8brle_compressor_init_bits(Simple8bRleCompressor *compressor, uint16 num_bits, bool value)
{
	/*
	 * This function is used to allocate the compressor with a specific number of
	 * bits. This is used in a specific scenario where the compressor is used to
	 * store bitmaps. In this case all values are bits and it offers a few
	 * simplifications:
	 *
	 * - we can place an upper bound on memory we need to store the bitmap
	 * - we don't need to check the size of the values on insertion
	 */
	Assert(compressor->num_elements == 0);
	Assert(compressor->num_uncompressed_elements == 0);

	Assert(num_bits > 0 && num_bits <= GLOBAL_MAX_ROWS_PER_COMPRESSION);
	int n_expected_blocks = (127 + TARGET_COMPRESSED_BATCH_SIZE) / SIMPLE8B_MAX_VALUES_PER_SLOT;
	uint64_vec_init(&compressor->compressed_data, CurrentMemoryContext, n_expected_blocks);
	bit_array_init(&compressor->selectors,
				   /* expected_bits = */
				   (127 + TARGET_COMPRESSED_BATCH_SIZE) * SIMPLE8B_BITS_PER_SELECTOR /
					   SIMPLE8B_MAX_VALUES_PER_SLOT);

	/* Initialize the first selector bucket */
	compressor->selectors.buckets.data[compressor->selectors.buckets.num_elements] = 0;
	compressor->selectors.buckets.num_elements++;

	/* Help the compressor know how many bits we're storing per value */
	compressor->bit_limit = 1;
	if (num_bits < 64)
	{
		/* Add the bits to the uncompressed elements */
		for (int i = 0; i < num_bits; i++)
		{
			compressor->uncompressed_elements[i] = value;
			compressor->num_uncompressed_elements++;
		}
	}
	else
	{
		/* Generate an RLE block and add it */
		Assert(compressor->last_block_set == false);
		simple8brle_compressor_push_block(compressor,
										  simple8brle_block_create_rle(num_bits, (uint64) value));
		compressor->num_elements += num_bits;
	}
}

static void
simple8brle_compressor_append(Simple8bRleCompressor *compressor, uint64 val)
{
	Assert(compressor != NULL);

	if (compressor->num_uncompressed_elements >= SIMPLE8B_MAX_VALUES_PER_SLOT)
	{
		Assert(compressor->num_uncompressed_elements == SIMPLE8B_MAX_VALUES_PER_SLOT);
		simple8brle_compressor_flush(compressor);
		Assert(compressor->num_uncompressed_elements == 0);
	}

	compressor->uncompressed_elements[compressor->num_uncompressed_elements] = val;
	compressor->num_uncompressed_elements += 1;
}

static bool
simple8brle_compressor_is_empty(Simple8bRleCompressor *compressor)
{
	return compressor->num_elements == 0;
}

static size_t
simple8brle_compressor_compressed_size(Simple8bRleCompressor *compressor)
{
	/* we store 16 selectors per selector_slot, and one selector_slot per compressed_data_slot.
	 * use num_compressed_data_slots / 16 + 1 to ensure that rounding doesn't truncate our slots
	 * and that we always have a 0 slot at the end.
	 */
	return sizeof(Simple8bRleSerialized) +
		   compressor->compressed_data.num_elements * sizeof(*compressor->compressed_data.data) +
		   bit_array_data_bytes_used(&compressor->selectors);
}

static void
simple8brle_compressor_push_block(Simple8bRleCompressor *compressor, Simple8bRleBlock block)
{
	if (compressor->last_block_set)
	{
		bit_array_append(&compressor->selectors,
						 SIMPLE8B_BITS_PER_SELECTOR,
						 compressor->last_block.selector);
		uint64_vec_append(&compressor->compressed_data, compressor->last_block.data);
	}

	compressor->last_block = block;
	compressor->last_block_set = true;
}

static Simple8bRleBlock
simple8brle_compressor_pop_block(Simple8bRleCompressor *compressor)
{
	Assert(compressor->last_block_set);
	compressor->last_block_set = false;
	return compressor->last_block;
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

	simple8brle_compressor_flush(compressor);
	if (compressor->num_elements == 0)
		return NULL;

	Assert(compressor->last_block_set);
	simple8brle_compressor_push_block(compressor, compressor->last_block);

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

static void
simple8brle_compressor_flush(Simple8bRleCompressor *compressor)
{
	/* pop the latest compressed value and recompress it, this will take care of any gaps
	 * left from having too few values, and will re-attempt RLE if it's more efficient
	 */
	Simple8bRleBlock last_block = {
		.selector = 0,
	};
	Simple8bRlePartiallyCompressedData new_data;

	if (compressor->last_block_set)
		last_block = simple8brle_compressor_pop_block(compressor);

	if (last_block.selector == 0 && compressor->num_uncompressed_elements == 0)
		return;

	if (simple8brle_selector_is_rle(last_block.selector))
	{
		/* special case when the prev slot is RLE: we're always going to use RLE
		 * again, and recompressing could be expensive if the RLE contains a large
		 * amount of data
		 */
		uint32 appended_to_rle =
			simple8brle_block_append_rle(&last_block,
										 compressor->uncompressed_elements,
										 compressor->num_uncompressed_elements);

		simple8brle_compressor_push_block(compressor, last_block);

		new_data = (Simple8bRlePartiallyCompressedData){
			.data = compressor->uncompressed_elements + appended_to_rle,
			.data_size = compressor->num_uncompressed_elements - appended_to_rle,
			/* block is zeroed out, including it's selector */
		};
	}
	else
	{
		new_data = (Simple8bRlePartiallyCompressedData){
			.data = compressor->uncompressed_elements,
			.data_size = compressor->num_uncompressed_elements,
			.block = last_block,
		};
	}

	if (new_data.block.num_elements_compressed > 0 && new_data.block.selector == 0)
		elog(ERROR, "end of compressed integer stream");

	if (new_data.block.num_elements_compressed > 0 || new_data.data_size > 0)
		simple8brle_compressor_append_pcd(compressor, &new_data);

	compressor->num_elements += compressor->num_uncompressed_elements;
	compressor->num_uncompressed_elements = 0;
}

#define PUSH_BLOCK(block_to_push)                                                                  \
	do                                                                                             \
	{                                                                                              \
		uint64 masked_selector = (block_to_push).selector & 0xFULL;                                \
		uint32 increment_bucket = compressor->selectors.bits_used_in_last_bucket / 64;             \
		compressor->selectors.buckets.num_elements += increment_bucket;                            \
		uint64 *restrict bucket =                                                                  \
			&compressor->selectors.buckets.data[compressor->selectors.buckets.num_elements - 1];   \
		compressor->selectors.bits_used_in_last_bucket %= 64;                                      \
		*bucket = (*bucket & ~(-(uint64) increment_bucket)) |                                      \
				  (masked_selector << compressor->selectors.bits_used_in_last_bucket);             \
		compressor->selectors.bits_used_in_last_bucket += SIMPLE8B_BITS_PER_SELECTOR;              \
		/* We can now safely append without checking capacity since we pre-allocated */            \
		compressor->compressed_data.data[compressor->compressed_data.num_elements] =               \
			(block_to_push).data;                                                                  \
		compressor->compressed_data.num_elements++;                                                \
	} while (0)

static void
simple8brle_compressor_append_pcd(Simple8bRleCompressor *compressor,
								  const Simple8bRlePartiallyCompressedData *new_data)
{
	/* The RLE case is handled by the caller (in simple8brle_compressor_flush) */
	Assert(new_data->block.selector != SIMPLE8B_RLE_SELECTOR);

	/* We can't possibly have an empty data size here */
	Assert(new_data->data_size > 0);

	/* If we're storing 1 bit per value, we can use a faster path */
	if (compressor->bit_limit == 1)
	{
		simple8brle_compressor_append_pcd_bits(compressor, new_data);
		return;
	}

	uint32 idx = 0;
	uint32 n_compressed = new_data->block.num_elements_compressed;
	uint32 new_data_len = n_compressed + new_data->data_size;
	const uint64 *restrict new_data_ptr = new_data->data;

	/* TODO dbeck : unify allocation logic */

	if (compressor->compressed_data.num_elements + 64 > compressor->compressed_data.max_elements)
	{
		uint32 new_max = compressor->compressed_data.num_elements + 64;
		uint32 new_capacity = new_max > TARGET_COMPRESSED_BATCH_SIZE ?
								  new_max :
								  compressor->compressed_data.num_elements * 2;
		uint64 *new_data = palloc(new_capacity * sizeof(uint64));
		memcpy(new_data,
			   compressor->compressed_data.data,
			   compressor->compressed_data.num_elements * sizeof(uint64));
		pfree(compressor->compressed_data.data);
		compressor->compressed_data.data = new_data;
		compressor->compressed_data.max_elements = new_capacity;
	}

	if (compressor->selectors.buckets.num_elements >= compressor->selectors.buckets.max_elements)
	{
		uint64 num_new_elements = compressor->selectors.buckets.num_elements > 16 ?
									  16 :
									  compressor->selectors.buckets.num_elements;
		uint64 num_elements = compressor->selectors.buckets.num_elements + num_new_elements;
		if (num_elements >= PG_UINT32_MAX / sizeof(uint64))
			elog(ERROR, "vector allocation overflow");
		compressor->selectors.buckets.max_elements = num_elements;
		uint64 num_bytes = compressor->selectors.buckets.max_elements * sizeof(uint64);
		if (compressor->selectors.buckets.data == NULL)
			compressor->selectors.buckets.data =
				MemoryContextAlloc(compressor->selectors.buckets.ctx, num_bytes);
		else
			compressor->selectors.buckets.data =
				repalloc(compressor->selectors.buckets.data, num_bytes);
	}

	uint64 first_val;
	uint32 compressed_bits_per_val = SIMPLE8B_BIT_LENGTH[new_data->block.selector];
	{
		uint64 compressed_value = new_data->block.data;
		compressed_value >>= compressed_bits_per_val * idx;
		compressed_value &= ((~0ULL) >> (64 - compressed_bits_per_val));
		first_val = compressed_value;
	}
	uint64 compressed_val_mask = ((~0ULL) >> (64 - compressed_bits_per_val));

	/* Check if we can RLE the compressed and uncompressed blocks combined */
	if (first_val <= SIMPLE8B_RLE_MAX_VALUE_MASK)
	{
		/* runlength encode, if it would save space */
		uint64 bits_per_int;
		uint32 rle_count = 1;
		uint64 rle_val = first_val;
		bool stop_search = false;
		uint8 max_rle = n_compressed - idx;
		uint64 compressed_val_shift = compressed_bits_per_val * (idx + rle_count);
		uint64 compressed_val_mask = ((~0ULL) >> (64 - compressed_bits_per_val));

		/* check if we can RLE the compressed data */
		while (rle_count < max_rle)
		{
			uint64 next_val = (new_data->block.data >> compressed_val_shift) & compressed_val_mask;
			if (next_val != rle_val)
			{
				stop_search = true;
				break;
			}
			++rle_count;
			compressed_val_shift += compressed_bits_per_val;
		}

		if (!stop_search)
		{
			/* check the uncompressed data too */
			max_rle = new_data_len - idx;
			const uint64 *restrict data_ptr = new_data_ptr;
			while (rle_count < max_rle)
			{
				if (*data_ptr != rle_val)
					break;
				++rle_count;
				++data_ptr;
			}
		}

		bits_per_int = rle_val == 0 ? 1 : (pg_leftmost_one_pos64(rle_val) + 1);
		if (bits_per_int * rle_count >= SIMPLE8B_BITSIZE)
		{
			/* RLE would save space over slot-based encodings */
			uint64 data = ((uint64) rle_count << SIMPLE8B_RLE_MAX_VALUE_BITS) | rle_val;
			Simple8bRleBlock block = {
				.selector = SIMPLE8B_RLE_SELECTOR,
				.data = data,
				.num_elements_compressed = rle_count,
			};

			if (compressor->last_block_set)
			{
				PUSH_BLOCK(compressor->last_block);
			}
			compressor->last_block = block;
			compressor->last_block_set = true;
			idx += rle_count;
		}
	}

	/* Handle compressed data and uncompressed data */
	while (idx < n_compressed)
	{
		Simple8bRleBlock block = {
			.selector = SIMPLE8B_MINCODE,
		};
		uint8 num_packed = 0;
		uint8 i;
		uint8 bitLen = SIMPLE8B_BIT_LENGTH[block.selector];
		uint64 mask = ((~0ULL) >> (64 - bitLen));
		uint8 num_elements_to_process = SIMPLE8B_NUM_ELEMENTS[block.selector];

		{
			/* calculate the selector based on the compressed data */
           	uint8 max_packed = n_compressed - idx;
			uint8 shift_val = compressed_bits_per_val * idx;
	
			for (i = 0; i < max_packed && i < num_elements_to_process; ++i)
			{
				uint64 val = (new_data->block.data >> shift_val) & compressed_val_mask;
				shift_val += compressed_bits_per_val;

				while (val > mask)
				{
					++block.selector;
					num_elements_to_process = SIMPLE8B_NUM_ELEMENTS[block.selector];
					bitLen = SIMPLE8B_BIT_LENGTH[block.selector];
					mask = ((~0ULL) >> (64 - bitLen));
					/* subtle point: if we no longer have enough spaces left in the block for this
					* element, we should stop trying to fit it in. (even in that case, we still must
					* use the new selector to prevent gaps) */
					if (i >= num_elements_to_process)
					{
						break;
					}
				}
			}
		}

		{
			/* refine selector based on the uncomressed data */
			uint8 max_packed = new_data_len - idx;
			uint8 shift_val = compressed_bits_per_val * idx;
			for (; i < max_packed && i < num_elements_to_process; ++i)
			{
				uint64 val = new_data_ptr[idx + i - n_compressed];
				shift_val += compressed_bits_per_val;

				while (val > mask)
				{
					++block.selector;
					num_elements_to_process = SIMPLE8B_NUM_ELEMENTS[block.selector];
					bitLen = SIMPLE8B_BIT_LENGTH[block.selector];
					mask = ((~0ULL) >> (64 - bitLen));
					/* subtle point: if we no longer have enough spaces left in the block for this
					* element, we should stop trying to fit it in. (even in that case, we still must
					* use the new selector to prevent gaps) */
					if (i >= num_elements_to_process)
					{
						break;
					}
				}
			}
		}

		uint8 selector_shift_increment = SIMPLE8B_BIT_LENGTH[block.selector];
		uint8 selector_shift = selector_shift_increment * block.num_elements_compressed;

		{
			/* loop over the compressed data */
			uint32 max_packed = num_elements_to_process;
			if (max_packed > n_compressed - idx)
				max_packed = n_compressed - idx;
			uint8 shift_val = compressed_bits_per_val * idx;

			while (num_packed < max_packed)
			{
				uint64 new_val = (new_data->block.data >> shift_val) & compressed_val_mask;
				shift_val += compressed_bits_per_val;
				block.data = block.data | new_val << selector_shift;
				selector_shift += selector_shift_increment;
				++block.num_elements_compressed;
				++num_packed;
			}
		}

		{
			/* loop over the uncompressed data */
			uint32 max_packed = num_elements_to_process;
			if (max_packed > new_data_len - idx)
				max_packed = new_data_len - idx;

			while (num_packed < max_packed)
			{
				uint64 new_val = new_data_ptr[idx + num_packed - n_compressed];
				block.data = block.data | new_val << selector_shift;
				selector_shift += selector_shift_increment;
				++block.num_elements_compressed;
				++num_packed;
			}
		}

		if (compressor->last_block_set)
		{
			PUSH_BLOCK(compressor->last_block);
		}
		compressor->last_block = block;
		compressor->last_block_set = true;
		idx += num_packed;
	}

	if (new_data_ptr[idx - n_compressed] <= SIMPLE8B_RLE_MAX_VALUE_MASK)
	{
		/* runlength encode, if it would save space */
		uint64 bits_per_int;
		uint32 rle_count = 1;
		uint64 rle_val = new_data_ptr[idx - n_compressed];
		const uint64 *restrict data_ptr = new_data_ptr + (idx + rle_count - n_compressed);
		uint8 max_rle = new_data_len - idx;

		while (rle_count < max_rle)
		{
			if (*data_ptr != rle_val)
				break;
			++rle_count;
			++data_ptr;
		}

		bits_per_int = rle_val == 0 ? 1 : (pg_leftmost_one_pos64(rle_val) + 1);
		if (bits_per_int * rle_count >= SIMPLE8B_BITSIZE)
		{
			/* RLE would save space over slot-based encodings */
			uint64 data = ((uint64) rle_count << SIMPLE8B_RLE_MAX_VALUE_BITS) | rle_val;
			Simple8bRleBlock block = {
				.selector = SIMPLE8B_RLE_SELECTOR,
				.data = data,
				.num_elements_compressed = rle_count,
			};

			if (compressor->last_block_set)
			{
				PUSH_BLOCK(compressor->last_block);
			}
			compressor->last_block = block;
			compressor->last_block_set = true;
			idx += rle_count;
		}
	}

	/* Handle uncompressed data */
	while (idx < new_data_len)
	{
		Simple8bRleBlock block = {
			.selector = SIMPLE8B_MINCODE,
		};
		uint8 num_packed = 0;
		uint8 i;
		uint8 bitLen = SIMPLE8B_BIT_LENGTH[block.selector];
		uint64 mask = ((~0ULL) >> (64 - bitLen));
		uint8 num_elements_to_process = SIMPLE8B_NUM_ELEMENTS[block.selector];

		for (i = 0; idx + i < new_data_len && i < num_elements_to_process; ++i)
		{
			uint64 val = new_data_ptr[idx + i - n_compressed];

			while (val > mask)
			{
				++block.selector;
				num_elements_to_process = SIMPLE8B_NUM_ELEMENTS[block.selector];
				bitLen = SIMPLE8B_BIT_LENGTH[block.selector];
				mask = ((~0ULL) >> (64 - bitLen));
				/* subtle point: if we no longer have enough spaces left in the block for this
				 * element, we should stop trying to fit it in. (even in that case, we still must
				 * use the new selector to prevent gaps) */
				if (i >= num_elements_to_process)
					break;
			}
		}

		while (num_packed < num_elements_to_process && idx + num_packed < new_data_len)
		{
			uint64 new_val = new_data_ptr[idx + num_packed - n_compressed];
			block.data = block.data | new_val << (SIMPLE8B_BIT_LENGTH[block.selector] *
												  block.num_elements_compressed);
			++block.num_elements_compressed;
			++num_packed;
		}

		if (compressor->last_block_set)
		{
			PUSH_BLOCK(compressor->last_block);
		}
		compressor->last_block = block;
		compressor->last_block_set = true;
		idx += num_packed;
	}
}

static void
simple8brle_compressor_append_pcd_bits(Simple8bRleCompressor *compressor,
									   const Simple8bRlePartiallyCompressedData *new_data)
{
	/* The RLE case is handled by the caller (in simple8brle_compressor_flush) */
	Assert(new_data->block.selector != SIMPLE8B_RLE_SELECTOR);

	/* We can't possibly have an empty data size here */
	Assert(new_data->data_size > 0);

	uint32 idx = 0;
	uint32 n_compressed = new_data->block.num_elements_compressed;
	uint32 new_data_len = n_compressed + new_data->data_size;
	Assert(new_data->data_size <= 64);
	const uint64 *restrict new_data_ptr = new_data->data;

	/* TODO dbeck : unify allocation logic */

	if (compressor->compressed_data.num_elements >= compressor->compressed_data.max_elements)
	{
		uint32 new_max = compressor->compressed_data.num_elements + 2;
		uint32 new_capacity = new_max > (TARGET_COMPRESSED_BATCH_SIZE / 64 + 1) ?
								  new_max :
								  compressor->compressed_data.num_elements * 2;
		uint64 *new_data = palloc(new_capacity * sizeof(uint64));
		memcpy(new_data,
			   compressor->compressed_data.data,
			   compressor->compressed_data.num_elements * sizeof(uint64));
		pfree(compressor->compressed_data.data);
		compressor->compressed_data.data = new_data;
		compressor->compressed_data.max_elements = new_capacity;
	}

	if (compressor->selectors.buckets.num_elements >= compressor->selectors.buckets.max_elements)
	{
		uint64 num_new_elements = compressor->selectors.buckets.num_elements > 16 ?
									  16 :
									  compressor->selectors.buckets.num_elements;
		uint64 num_elements = compressor->selectors.buckets.num_elements + num_new_elements;
		if (num_elements >= PG_UINT32_MAX / sizeof(uint64))
			elog(ERROR, "vector allocation overflow");
		compressor->selectors.buckets.max_elements = num_elements;
		uint64 num_bytes = compressor->selectors.buckets.max_elements * sizeof(uint64);
		if (compressor->selectors.buckets.data == NULL)
			compressor->selectors.buckets.data =
				MemoryContextAlloc(compressor->selectors.buckets.ctx, num_bytes);
		else
			compressor->selectors.buckets.data =
				repalloc(compressor->selectors.buckets.data, num_bytes);
	}

	uint64 first_val;
	{
		uint64 compressed_value = new_data->block.data;
		compressed_value >>= idx;
		compressed_value &= 1ULL;
		first_val = compressed_value;
	}

	/* Check if we can RLE the compressed and uncompressed blocks combined */
	{
		/* runlength encode, if it would save space */
		uint32 rle_count = 1;
		uint64 rle_val = first_val;
		bool stop_search = false;
		uint8 max_rle = n_compressed - idx;
		uint64 compressed_val_shift = idx + rle_count;

		/* check if we can RLE the compressed data */
		while (rle_count < max_rle)
		{
			uint64 next_val = (new_data->block.data >> compressed_val_shift) & 1ULL;
			if (next_val != rle_val)
			{
				stop_search = true;
				break;
			}
			++rle_count;
			++compressed_val_shift;
		}
		if (!stop_search)
		{
			/* check the uncomressed data too */
			max_rle = new_data_len - idx;
			const uint64 *restrict data_ptr = new_data_ptr;
			while (rle_count < max_rle)
			{
				if (*data_ptr != rle_val)
					break;
				++rle_count;
				++data_ptr;
			}
		}

		if (rle_count >= SIMPLE8B_BITSIZE)
		{
			/* RLE would save space over slot-based encodings */
			uint64 data = ((uint64) rle_count << SIMPLE8B_RLE_MAX_VALUE_BITS) | rle_val;
			Simple8bRleBlock block = {
				.selector = SIMPLE8B_RLE_SELECTOR,
				.data = data,
				.num_elements_compressed = rle_count,
			};

			if (compressor->last_block_set)
			{
				PUSH_BLOCK(compressor->last_block);
			}
			compressor->last_block = block;
			compressor->last_block_set = true;
			idx += rle_count;
		}
	}

	/* Handle compressed and uncompressed data */
	while (idx < n_compressed)
	{
		Simple8bRleBlock block = {
			.selector = SIMPLE8B_MINCODE,
		};
		uint8 num_packed = 0;

		{
			/* loop over the compressed data */
			uint32 max_packed = 64;
			if (max_packed > n_compressed-idx)
				max_packed = n_compressed-idx;

			while (num_packed < max_packed)
			{
				uint64 new_val = (new_data->block.data >> (idx + num_packed)) & 1ULL;
				block.data = block.data | new_val << (block.num_elements_compressed);
				++block.num_elements_compressed;
				++num_packed;
			}
		}
		{
			/* loop over the uncompressed data */
			uint32 max_packed = 64;
			if (max_packed > new_data_len - idx)
				max_packed = new_data_len - idx;

			while (num_packed < max_packed)
			{
				uint64 new_val = new_data_ptr[num_packed + idx - n_compressed];
				block.data = block.data | new_val << (block.num_elements_compressed);
				++block.num_elements_compressed;
				++num_packed;
			}
		}

		if (compressor->last_block_set)
		{
			PUSH_BLOCK(compressor->last_block);
		}
		compressor->last_block = block;
		compressor->last_block_set = true;
		idx += num_packed;
	}

	/* Second round of RLE on uncompressed data only */
	{
		uint64 first_val = new_data_ptr[idx - n_compressed];
		uint32 rle_count = 1;
		uint64 rle_val = first_val;
		const uint64 *restrict data_ptr = new_data_ptr + (idx + rle_count - n_compressed);
		uint8 max_rle = new_data_len - idx;

		while (rle_count < max_rle)
		{
			if (*data_ptr != rle_val)
				break;
			++rle_count;
			++data_ptr;
		}

		if (rle_count >= SIMPLE8B_BITSIZE)
		{
			/* RLE would save space over slot-based encodings */
			uint64 data = ((uint64) rle_count << SIMPLE8B_RLE_MAX_VALUE_BITS) | rle_val;
			Simple8bRleBlock block = {
				.selector = SIMPLE8B_RLE_SELECTOR,
				.data = data,
				.num_elements_compressed = rle_count,
			};

			if (compressor->last_block_set)
			{
				PUSH_BLOCK(compressor->last_block);
			}
			compressor->last_block = block;
			compressor->last_block_set = true;
			idx += rle_count;
		}
	}

	/* Handle uncompressed data only */
	while (idx < new_data_len)
	{
		Simple8bRleBlock block = {
			.selector = SIMPLE8B_MINCODE,
		};
		uint8 num_packed = 0;
		
		const uint64 *restrict data_ptr = new_data_ptr + (idx - n_compressed);
		uint8 max_packed = new_data_len - idx;
		while (num_packed < max_packed)
		{
			block.data |= data_ptr[num_packed] << (block.num_elements_compressed);
			++block.num_elements_compressed;
			++num_packed;
		}

		if (compressor->last_block_set)
		{
			PUSH_BLOCK(compressor->last_block);
		}
		compressor->last_block = block;
		compressor->last_block_set = true;
		idx += num_packed;
	}
}
#undef PUSH_BLOCK

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

static inline Simple8bRleBlock
simple8brle_block_create_rle(uint32 rle_count, uint64 rle_val)
{
	uint64 data;
	Assert(rle_val <= SIMPLE8B_RLE_MAX_VALUE_MASK);
	Assert(rle_count <= SIMPLE8B_RLE_MAX_COUNT_MASK);
	data = ((uint64) rle_count << SIMPLE8B_RLE_MAX_VALUE_BITS) | rle_val;

	return (Simple8bRleBlock){
		.selector = SIMPLE8B_RLE_SELECTOR,
		.data = data,
		.num_elements_compressed = rle_count,
	};
}

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

static uint32
simple8brle_block_append_rle(Simple8bRleBlock *compressed_block, const uint64 *data,
							 uint32 data_len)
{
	uint64 repeated_value = simple8brle_rledata_value(compressed_block->data);
	uint64 repeat_count = simple8brle_rledata_repeatcount(compressed_block->data);
	uint32 i = 0;

	Assert(simple8brle_selector_is_rle(compressed_block->selector));

	for (; i < data_len && data[i] == repeated_value && repeat_count < SIMPLE8B_RLE_MAX_COUNT_MASK;
		 i++)
		repeat_count += 1;

	compressed_block->data = (repeat_count << SIMPLE8B_RLE_MAX_VALUE_BITS) | repeated_value;

	return i;
}

static inline void
simple8brle_block_append_element(Simple8bRleBlock *block, uint64 val)
{
	Assert(val <= simple8brle_selector_get_bitmask(block->selector));
	Assert(block->num_elements_compressed < SIMPLE8B_NUM_ELEMENTS[block->selector]);
	block->data = block->data |
				  val << (SIMPLE8B_BIT_LENGTH[block->selector] * block->num_elements_compressed);
	block->num_elements_compressed += 1;
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
