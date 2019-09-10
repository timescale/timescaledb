/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_SIMPLE8B_RLE_OOB_H
#define TIMESCALEDB_TSL_SIMPLE8B_RLE_OOB_H

#include <postgres.h>
#include <c.h>
#include <fmgr.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>

#include <export.h>
#include <adts/bit_array.h>

#include <adts/uint64_vec.h>
#include "compat.h"

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

#define SIMPLE8B_MAX_VALUES_PER_SLOT 64

#define SIMPLE8B_NUM_ELEMENTS ((uint8[]){ 0, 64, 32, 21, 16, 12, 10, 9, 8, 6, 5, 4, 3, 2, 1 })
#define SIMPLE8B_BIT_LENGTH ((uint8[]){ 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 16, 21, 32, 64, 36 })

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
} Simple8bRleCompressor;

typedef struct Simple8bRleDecompressionIterator
{
	BitArray selector_data;
	BitArrayIterator selectors;
	Simple8bRleBlock current_block;

	const uint64 *compressed_data;
	uint32 current_compressed_pos;
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
static inline void simple8brle_compressor_append(Simple8bRleCompressor *compressor, uint64 val);
static inline bool simple8brle_compressor_is_empty(Simple8bRleCompressor *compressor);

static inline void
simple8brle_decompression_iterator_init_forward(Simple8bRleDecompressionIterator *iter,
												Simple8bRleSerialized *compressed);
static inline void
simple8brle_decompression_iterator_init_reverse(Simple8bRleDecompressionIterator *iter,
												Simple8bRleSerialized *compressed);
static inline Simple8bRleDecompressResult
simple8brle_decompression_iterator_try_next_forward(Simple8bRleDecompressionIterator *iter);
static inline Simple8bRleDecompressResult
simple8brle_decompression_iterator_try_next_reverse(Simple8bRleDecompressionIterator *iter);

static inline void simple8brle_serialized_send(StringInfo buffer,
											   const Simple8bRleSerialized *data);
static inline char *bytes_serialize_simple8b_and_advance(char *dest, size_t expected_size,
														 const Simple8bRleSerialized *data);
static inline Simple8bRleSerialized *bytes_deserialize_simple8b_and_advance(const char **data);
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

/* pcd */
static inline uint32 simple8brle_pcd_num_elements(const Simple8bRlePartiallyCompressedData *pcd);
static inline uint64 simple8brle_pcd_get_element(const Simple8bRlePartiallyCompressedData *pcd,
												 uint32 element_pos);

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
static inline uint32 simple8brle_bits_for_value(uint64 v);
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
	uint32 num_blocks = pq_getmsgint32(buffer);
	uint32 num_selector_slots = simple8brle_num_selector_slots_for_num_blocks(num_blocks);
	Simple8bRleSerialized *data;
	Size compressed_size =
		sizeof(Simple8bRleSerialized) + (num_blocks + num_selector_slots) * sizeof(uint64);
	if (!AllocSizeIsValid(compressed_size))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("compressed size exceeds the maximum allowed (%d)", (int) MaxAllocSize)));

	data = palloc0(compressed_size);
	data->num_elements = num_elements;
	data->num_blocks = num_blocks;

	for (i = 0; i < data->num_blocks + num_selector_slots; i++)
		data->slots[i] = pq_getmsgint64(buffer);

	return data;
}

static void
simple8brle_serialized_send(StringInfo buffer, const Simple8bRleSerialized *data)
{
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
bytes_deserialize_simple8b_and_advance(const char **data)
{
	Simple8bRleSerialized *ser = (Simple8bRleSerialized *) *data;
	*data += simple8brle_serialized_total_size(ser);
	return ser;
}

static size_t
simple8brle_serialized_slot_size(const Simple8bRleSerialized *data)
{
	if (data == NULL)
		return 0;

	return sizeof(uint64) *
		   (data->num_blocks + simple8brle_num_selector_slots_for_num_blocks(data->num_blocks));
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
		.num_uncompressed_elements = 0,
	};
	uint64_vec_init(&compressor->compressed_data, CurrentMemoryContext, 0);
	bit_array_init(&compressor->selectors);
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

static inline int32
simple8brle_compressor_num_selectors(Simple8bRleCompressor *compressor)
{
	Assert(bit_array_num_bits(&compressor->selectors) % SIMPLE8B_BITS_PER_SELECTOR == 0);
	return bit_array_num_bits(&compressor->selectors) / SIMPLE8B_BITS_PER_SELECTOR;
}

// TODO replace with simple8b_rle_compressor_finish_into(Simple8bRleCompressor *compressor,
// simple8b_rle compressed)
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
	 * to ensure padding bits are zeroed, and that there's a 0 seletor at the end.
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

	simple8brle_compressor_append_pcd(compressor, &new_data);

	compressor->num_elements += compressor->num_uncompressed_elements;
	compressor->num_uncompressed_elements = 0;
}

static void
simple8brle_compressor_append_pcd(Simple8bRleCompressor *compressor,
								  const Simple8bRlePartiallyCompressedData *new_data)
{
	uint32 idx = 0;
	uint32 new_data_len = simple8brle_pcd_num_elements(new_data);
	while (idx < new_data_len)
	{
		Simple8bRleBlock block = {
			.selector = SIMPLE8B_MINCODE,
		};
		uint8 num_packed = 0;
		uint8 i;
		uint64 mask = simple8brle_selector_get_bitmask(block.selector);

		if (simple8brle_pcd_get_element(new_data, idx) <= SIMPLE8B_RLE_MAX_VALUE_MASK)
		{
			/* runlength encode, if it would save space */
			uint64 bits_per_int;
			uint32 rle_count = 1;
			uint64 rle_val = simple8brle_pcd_get_element(new_data, idx);
			while (idx + rle_count < new_data_len &&
				   simple8brle_pcd_get_element(new_data, idx + rle_count) == rle_val)
			{
				rle_count += 1;
				if (rle_count == SIMPLE8B_RLE_MAX_COUNT_MASK)
					break;
			}
			bits_per_int = rle_val == 0 ? 1 : simple8brle_bits_for_value(rle_val);
			if (bits_per_int * rle_count >= SIMPLE8B_BITSIZE)
			{
				/* RLE would save space over slot-based encodings */
				Simple8bRleBlock block = simple8brle_block_create_rle(rle_count, rle_val);
				Assert(bits_per_int <= SIMPLE8B_RLE_MAX_VALUE_BITS);
				Assert(simple8brle_rledata_repeatcount(block.data) == rle_count);
				Assert(simple8brle_rledata_value(block.data) == rle_val);
				simple8brle_compressor_push_block(compressor, block);
				idx += rle_count;
				continue;
			}
		}

		for (i = 0; idx + i < new_data_len && i < SIMPLE8B_NUM_ELEMENTS[block.selector]; ++i)
		{
			uint64 val = simple8brle_pcd_get_element(new_data, idx + i);
			while (val > mask)
			{
				block.selector += 1;
				mask = simple8brle_selector_get_bitmask(block.selector);
				/* subtle point: if we no longer have enough spaces left in the block for this
				 * element, we should stop trying to fit it in. (even in that case, we still must
				 * use the new selector to prevent gaps) */
				if (i >= SIMPLE8B_NUM_ELEMENTS[block.selector])
					break;
			}
		}

		Assert(block.selector < SIMPLE8B_MAXCODE);
		Assert(mask == simple8brle_selector_get_bitmask(block.selector));

		while (num_packed < SIMPLE8B_NUM_ELEMENTS[block.selector] &&
			   idx + num_packed < new_data_len)
		{
			uint64 new_val = simple8brle_pcd_get_element(new_data, idx + num_packed);

			Assert(new_val <= mask);
			simple8brle_block_append_element(&block, new_val);
			num_packed += 1;
		}
		simple8brle_compressor_push_block(compressor, block);
		idx += num_packed;
	}
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

		if (simple8brle_selector_is_rle(selector))
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
 * Since it is also easier to read, we perfer it here.
 */
static Simple8bRleDecompressResult
simple8brle_decompression_iterator_try_next_forward(Simple8bRleDecompressionIterator *iter)
{
	uint64 uncompressed;
	if (iter->num_elements_returned >= iter->num_elements)
		return (Simple8bRleDecompressResult){
			.is_done = true,
		};

	if (iter->current_in_compressed_pos >= iter->current_block.num_elements_compressed)
	{
		iter->current_block =
			simple8brle_block_create(bit_array_iter_next(&iter->selectors,
														 SIMPLE8B_BITS_PER_SELECTOR),
									 iter->compressed_data[iter->current_compressed_pos]);
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
		Assert(iter->current_compressed_pos >= 0);

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

/********************************************
 ***  Simple8bRlePartiallyCompressedData  ***
 ********************************************/

static inline uint32
simple8brle_pcd_num_elements(const Simple8bRlePartiallyCompressedData *pcd)
{
	Assert(pcd->block.num_elements_compressed <= SIMPLE8B_NUM_ELEMENTS[pcd->block.selector]);
	return pcd->block.num_elements_compressed + pcd->data_size;
}

static inline uint64
simple8brle_pcd_get_element(const Simple8bRlePartiallyCompressedData *pcd, uint32 element_pos)
{
	Assert(element_pos < simple8brle_pcd_num_elements(pcd));
	Assert(pcd->block.num_elements_compressed <= SIMPLE8B_NUM_ELEMENTS[pcd->block.selector]);
	return element_pos < pcd->block.num_elements_compressed ?
			   simple8brle_block_get_element(pcd->block, element_pos) :
			   pcd->data[element_pos - pcd->block.num_elements_compressed];
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

static inline Simple8bRleBlock
simple8brle_block_create(uint8 selector, uint64 data)
{
	Simple8bRleBlock block = (Simple8bRleBlock){
		.selector = selector,
		.data = data,
	};

	Assert(block.selector != 0);
	if (simple8brle_selector_is_rle(block.selector))
		block.num_elements_compressed = simple8brle_rledata_repeatcount(block.data);
	else
		block.num_elements_compressed = SIMPLE8B_NUM_ELEMENTS[block.selector];
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
}

/***************************
 ***  Utility Functions  ***
 ***************************/

static inline bool
simple8brle_selector_is_rle(uint8 selector)
{
	return selector == SIMPLE8B_RLE_SELECTOR;
}

static inline uint32
simple8brle_rledata_repeatcount(uint64 rledata)
{
	return (uint32)((rledata >> SIMPLE8B_RLE_MAX_VALUE_BITS) & SIMPLE8B_RLE_MAX_COUNT_MASK);
}

static inline uint64
simple8brle_rledata_value(uint64 rledata)
{
	return rledata & SIMPLE8B_RLE_MAX_VALUE_MASK;
}

static inline uint64
simple8brle_selector_get_bitmask(uint8 selector)
{
	uint8 bitLen = SIMPLE8B_BIT_LENGTH[selector];
	/* note: left shift by 64 bits is UB */
	return bitLen < 64 ? (1ULL << bitLen) - 1 : PG_UINT64_MAX;
}

static uint32
simple8brle_num_selector_slots_for_num_blocks(uint32 num_blocks)
{
	return (num_blocks / SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT) +
		   (num_blocks % SIMPLE8B_SELECTORS_PER_SELECTOR_SLOT != 0 ? 1 : 0);
}

// FIXME replace with count leading ones as in float.c
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
