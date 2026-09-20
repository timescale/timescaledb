/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "rr_iter_decompress.h"
#include "compression/algorithms/fastlanes/fastlanes_types.h"
#include "rr_blocks.h"
#include "rr_internal.h"
#include "rr_nulls.h"
#include <guc.h>

/*
 * iterator based decompression for rapid_raccoon batches
 *
 * both the forward and the reverse iterators decompress and buffer
 * one block at a time. the forward iterator starts with the first
 * block, and the reverse starts with the last one.
 *
 * the RLE blocks are not expanded/decompressed. the elements are
 * calculated during decompression.
 *
 * the iterators buffer maximum RR_ITER_BUF_ELEMS (320) elements
 * because this allows the DFOR decompression to save one copy.
 * the DFOR decompression would need maximum 64 elements for the
 * S base values, and because we encode the strided delta residuals
 * separately, the FastLanes output buffer requirements apply again
 * and mandates 256 elements for the residuals.
 */

#define RR_ITER_BUF_ELEMS 320

typedef struct RRDecompressionIterator
{
	DecompressionIterator base;

	StringInfoData si;
	fl_elem_width_t T;

	uint64 *validity_bitmap;
	size_t total_count;
	size_t row;
	uint32 blocks_left;
	size_t valid_remaining;

	RRBlock blk;
	uint32 block_fill;
	uint32 block_pos;

	/* reverse only */
	uint32 *block_offsets;
	uint32 block_idx;

	uint64 buf[RR_ITER_BUF_ELEMS];
	uint64 scratch_raw[RR_SCRATCH_WORDS];

	/* aligned ptr into scratch_raw: */
	uint8 *scratch;
} RRDecompressionIterator;

static inline bool
rr_iter_row_is_valid(const RRDecompressionIterator *it, size_t row)
{
	if (it->validity_bitmap == NULL)
	{
		return true;
	}
	return (it->validity_bitmap[row / 64] >> (row % 64)) & 1;
}

static uint32
rr_iter_load_block(RRDecompressionIterator *it, size_t max_elements)
{
	size_t count = rr_parse_one_block(&it->blk, &it->si, it->T);

	CheckCompressedData(count <= max_elements);
	if (it->blk.type != RR_BT_RLE)
	{
		Assert(count <= RR_ITER_BUF_ELEMS);
		rr_decompress_one_block(&it->blk, (uint8 *) it->buf, it->scratch, it->T);
	}
	return (uint32) count;
}

static inline uint64
rr_iter_raw_value(const RRDecompressionIterator *it, uint32 pos)
{
	if (it->blk.type == RR_BT_RLE)
	{
		return rr_block_rle_value_at(&it->blk.data.rle, pos);
	}

	switch (it->T)
	{
		case FL_ELEM_W16:
			return ((const uint16 *) it->buf)[pos];
		case FL_ELEM_W32:
			return ((const uint32 *) it->buf)[pos];
		case FL_ELEM_W64:
			return ((const uint64 *) it->buf)[pos];
		default:
			Assert(false);
			return 0;
	}
}

static inline Datum
rr_iter_value_datum(const RRDecompressionIterator *it, uint32 pos)
{
	uint64 v = rr_iter_raw_value(it, pos);

	switch (it->T)
	{
		case FL_ELEM_W16:
			return Int16GetDatum((int16) v);
		case FL_ELEM_W32:
			return Int32GetDatum((int32) v);
		case FL_ELEM_W64:
			return Int64GetDatum((int64) v);
		default:
			Assert(false);
			return (Datum) 0;
	}
}

static DecompressResult
rr_iter_try_next_forward(DecompressionIterator *base)
{
	RRDecompressionIterator *it = (RRDecompressionIterator *) base;

	if (it->row == it->total_count)
	{
		CheckCompressedData(it->blocks_left == 0 && it->block_pos == it->block_fill);
		return (DecompressResult){ .is_done = true };
	}

	size_t row = it->row++;
	if (!rr_iter_row_is_valid(it, row))
	{
		return (DecompressResult){ .is_null = true };
	}

	if (it->block_pos == it->block_fill)
	{
		CheckCompressedData(it->blocks_left > 0);
		it->blocks_left--;
		it->block_fill = rr_iter_load_block(it, it->valid_remaining);
		it->valid_remaining -= it->block_fill;
		it->block_pos = 0;
	}

	return (DecompressResult){ .val = rr_iter_value_datum(it, it->block_pos++) };
}

static DecompressResult
rr_iter_try_next_reverse(DecompressionIterator *base)
{
	RRDecompressionIterator *it = (RRDecompressionIterator *) base;

	if (it->row == 0)
	{
		return (DecompressResult){ .is_done = true };
	}

	size_t row = --it->row;
	if (!rr_iter_row_is_valid(it, row))
	{
		return (DecompressResult){ .is_null = true };
	}

	if (it->block_pos == 0)
	{
		CheckCompressedData(it->block_idx > 0);
		it->block_idx--;
		it->si.cursor = it->block_offsets[it->block_idx];
		it->block_fill = rr_iter_load_block(it, it->total_count);
		it->block_pos = it->block_fill;
	}

	return (DecompressResult){ .val = rr_iter_value_datum(it, --it->block_pos) };
}

static RRDecompressionIterator *
rr_iter_create(Datum compressed, Oid element_type, bool forward, const RRCompressed **header_out)
{
	StringInfoData si = { .data = DatumGetPointer(compressed),
						  .len = VARSIZE(DatumGetPointer(compressed)) };
	const RRCompressed *header =
		(const RRCompressed *) consumeCompressedData(&si, sizeof(RRCompressed));

	/* all-null and empty batches are the NULL compressor's job */
	CheckCompressedData(header->valid_count > 0);
	CheckCompressedData(header->num_blocks > 0);

	fl_elem_width_t T = rr_global_flag_to_elem_width(header->flags);
	fl_elem_width_t expected_width = rr_oid_to_elem_width_t(element_type);
	CheckCompressedData(expected_width == T);

	RRDecompressionIterator *it = palloc0(sizeof(RRDecompressionIterator));

	it->base.compression_algorithm = COMPRESSION_ALGORITHM_RAPID_RACCOON;
	it->base.forward = forward;
	it->base.element_type = element_type;
	it->base.try_next = forward ? rr_iter_try_next_forward : rr_iter_try_next_reverse;
	it->T = T;
	it->total_count = (size_t) header->valid_count + header->null_count;
	it->scratch = (uint8 *) (((uintptr_t) it->scratch_raw + (FL_MAX_ALIGNMENT - 1)) &
							 ~(uintptr_t) (FL_MAX_ALIGNMENT - 1));

	if (header->null_count > 0)
	{
		size_t validity_size = ((it->total_count + 63) / 64) * 8;
		size_t n_valid_runs;
		size_t decoded;

		it->validity_bitmap = palloc(validity_size);
		decoded = rr_null_bitmap_decode(&si,
										it->total_count,
										header->null_count,
										it->validity_bitmap,
										header->flags,
										&n_valid_runs);
		CheckCompressedData(
			(decoded == validity_size && (header->flags & RR_FLAG_NULL_SUMMARY) == 0) ||
			(decoded < validity_size && (header->flags & RR_FLAG_NULL_SUMMARY) != 0));
	}

	CheckCompressedData(header->num_blocks <=
						((GLOBAL_MAX_ROWS_PER_COMPRESSION / RR_DELTA_RLE_CHECKPOINT) + 1));

	it->si = si; /* cursor now sits at the first block */
	*header_out = header;
	return it;
}

DecompressionIterator *
rapid_raccoon_decompression_iterator_from_datum_forward(Datum compressed, Oid element_type)
{
	const RRCompressed *header;
	RRDecompressionIterator *it = rr_iter_create(compressed, element_type, true, &header);

	it->blocks_left = header->num_blocks;
	it->valid_remaining = header->valid_count;
	return &it->base;
}

DecompressionIterator *
rapid_raccoon_decompression_iterator_from_datum_reverse(Datum compressed, Oid element_type)
{
	const RRCompressed *header;
	RRDecompressionIterator *it = rr_iter_create(compressed, element_type, false, &header);
	size_t total_packed = 0;

	/* one pass over the blocks to parse them without decompression */
	it->block_offsets = palloc(sizeof(uint32) * header->num_blocks);
	for (uint32 b = 0; b < header->num_blocks; b++)
	{
		RRBlock blk;

		it->block_offsets[b] = (uint32) it->si.cursor;
		total_packed += rr_parse_one_block(&blk, &it->si, it->T);
		CheckCompressedData(total_packed <= header->valid_count);
	}
	CheckCompressedData(total_packed == header->valid_count);

	it->block_idx = header->num_blocks;
	it->row = it->total_count;
	return &it->base;
}
