/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "aic_iter_decompress.h"
#include "aic_blocks.h"
#include "aic_internal.h"
#include "aic_nulls.h"
#include "compression/algorithms/fastlanes/fastlanes_types.h"
#include <guc.h>
#include <utils/palloc.h>

/*
 * iterator based decompression for aic batches
 *
 * both the forward and the reverse iterators decompress and buffer
 * one block at a time. the forward iterator starts with the first
 * block, and the reverse starts with the last one.
 *
 * the RLE blocks are not expanded/decompressed. the elements are
 * calculated during decompression.
 *
 * the iterators buffer maximum AIC_ITER_BUF_ELEMS (320) elements
 * because this allows the DFOR decompression to save one copy.
 * the DFOR decompression would need maximum 64 elements for the
 * S base values, and because we encode the strided delta residuals
 * separately, the FastLanes output buffer requirements apply again
 * and mandates 256 elements for the residuals.
 */

#define AIC_ITER_BUF_ELEMS 320

typedef struct AICDecompressionIterator
{
	DecompressionIterator base;

	StringInfoData si;
	fl_elem_width_t T;

	uint64 *validity_bitmap;
	size_t total_count;
	size_t row;
	uint32 blocks_left;
	size_t valid_remaining;

	AICBlock blk;
	uint32 block_fill;
	uint32 block_pos;

	/* reverse only */
	uint32 *block_offsets;
	uint32 block_idx;

	/* lazily allocated on the first non-RLE block */
	MemoryContext lazy_context;
	uint8 *buf;
	uint8 *scratch;
} AICDecompressionIterator;

static inline bool
aic_iter_row_is_valid(const AICDecompressionIterator *it, size_t row)
{
	if (it->validity_bitmap == NULL)
	{
		return true;
	}
	return (it->validity_bitmap[row / 64] >> (row % 64)) & 1;
}

static uint32
aic_iter_load_block(AICDecompressionIterator *it, size_t max_elements)
{
	size_t count = aic_parse_one_block(&it->blk, &it->si, it->T);

	CheckCompressedData(count <= max_elements);
	if (it->blk.type != AIC_BT_RLE)
	{
		Assert(count <= AIC_ITER_BUF_ELEMS);
		if (it->buf == NULL)
		{
			size_t scratch_bytes = AIC_SCRATCH_BYTES - FL_MAX_ALIGNMENT;
			size_t buf_bytes = (size_t) AIC_ITER_BUF_ELEMS * ((uint8) it->T / 8);
			/* we need to allocate from the same context as the iterator */
			MemoryContext old_context = MemoryContextSwitchTo(it->lazy_context);
			uint8 *raw = palloc_aligned(scratch_bytes + buf_bytes, FL_MAX_ALIGNMENT, 0);
			MemoryContextSwitchTo(old_context);

			it->scratch = raw;
			it->buf = raw + scratch_bytes;
		}
		aic_decompress_one_block(&it->blk, it->buf, it->scratch, it->T);
	}
	return (uint32) count;
}

static inline uint64
aic_iter_raw_value(const AICDecompressionIterator *it, uint32 pos)
{
	if (it->blk.type == AIC_BT_RLE)
	{
		return aic_block_rle_value_at(&it->blk.data.rle, pos);
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
aic_iter_value_datum(const AICDecompressionIterator *it, uint32 pos)
{
	uint64 v = aic_iter_raw_value(it, pos);

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
aic_iter_try_next_forward(DecompressionIterator *base)
{
	AICDecompressionIterator *it = (AICDecompressionIterator *) base;

	if (it->row == it->total_count)
	{
		CheckCompressedData(it->blocks_left == 0 && it->block_pos == it->block_fill);
		return (DecompressResult){ .is_done = true };
	}

	size_t row = it->row++;
	if (!aic_iter_row_is_valid(it, row))
	{
		return (DecompressResult){ .is_null = true };
	}

	if (it->block_pos == it->block_fill)
	{
		CheckCompressedData(it->blocks_left > 0);
		it->blocks_left--;
		it->block_fill = aic_iter_load_block(it, it->valid_remaining);
		it->valid_remaining -= it->block_fill;
		it->block_pos = 0;
	}

	return (DecompressResult){ .val = aic_iter_value_datum(it, it->block_pos++) };
}

static DecompressResult
aic_iter_try_next_reverse(DecompressionIterator *base)
{
	AICDecompressionIterator *it = (AICDecompressionIterator *) base;

	if (it->row == 0)
	{
		return (DecompressResult){ .is_done = true };
	}

	size_t row = --it->row;
	if (!aic_iter_row_is_valid(it, row))
	{
		return (DecompressResult){ .is_null = true };
	}

	if (it->block_pos == 0)
	{
		CheckCompressedData(it->block_idx > 0);
		it->block_idx--;
		it->si.cursor = it->block_offsets[it->block_idx];
		it->block_fill = aic_iter_load_block(it, it->total_count);
		it->block_pos = it->block_fill;
	}

	return (DecompressResult){ .val = aic_iter_value_datum(it, --it->block_pos) };
}

static AICDecompressionIterator *
aic_iter_create(Datum compressed, Oid element_type, bool forward, const AICCompressed **header_out)
{
	StringInfoData si = { .data = DatumGetPointer(compressed),
						  .len = VARSIZE(DatumGetPointer(compressed)) };
	const AICCompressed *header =
		(const AICCompressed *) consumeCompressedData(&si, sizeof(AICCompressed));

	/* all-null and empty batches are the NULL compressor's job */
	CheckCompressedData(header->valid_count > 0);
	CheckCompressedData(header->num_blocks > 0);

	fl_elem_width_t T = aic_global_flag_to_elem_width(header->flags);
	fl_elem_width_t expected_width = aic_oid_to_elem_width_t(element_type);
	CheckCompressedData(expected_width == T);

	AICDecompressionIterator *it = palloc0(sizeof(AICDecompressionIterator));

	it->base.compression_algorithm = COMPRESSION_ALGORITHM_AIC;
	it->base.forward = forward;
	it->base.element_type = element_type;
	it->base.try_next = forward ? aic_iter_try_next_forward : aic_iter_try_next_reverse;
	it->T = T;
	it->total_count = (size_t) header->valid_count + header->null_count;
	it->lazy_context = CurrentMemoryContext;

	if (header->null_count > 0)
	{
		size_t validity_size = ((it->total_count + 63) / 64) * 8;
		size_t n_valid_runs;
		size_t decoded;

		it->validity_bitmap = palloc(validity_size);
		decoded = aic_null_bitmap_decode(&si,
										 it->total_count,
										 header->null_count,
										 it->validity_bitmap,
										 header->flags,
										 &n_valid_runs);
		CheckCompressedData(
			(decoded == validity_size && (header->flags & AIC_FLAG_NULL_SUMMARY) == 0) ||
			(decoded < validity_size && (header->flags & AIC_FLAG_NULL_SUMMARY) != 0));
	}

	CheckCompressedData(header->num_blocks <=
						((GLOBAL_MAX_ROWS_PER_COMPRESSION / AIC_DELTA_RLE_CHECKPOINT) + 1));

	it->si = si; /* cursor now sits at the first block */
	*header_out = header;
	return it;
}

DecompressionIterator *
aic_decompression_iterator_from_datum_forward(Datum compressed, Oid element_type)
{
	const AICCompressed *header;
	AICDecompressionIterator *it = aic_iter_create(compressed, element_type, true, &header);

	it->blocks_left = header->num_blocks;
	it->valid_remaining = header->valid_count;
	return &it->base;
}

DecompressionIterator *
aic_decompression_iterator_from_datum_reverse(Datum compressed, Oid element_type)
{
	const AICCompressed *header;
	AICDecompressionIterator *it = aic_iter_create(compressed, element_type, false, &header);
	size_t total_packed = 0;

	/* one pass over the blocks to parse them without decompression */
	it->block_offsets = palloc(sizeof(uint32) * header->num_blocks);
	for (uint32 b = 0; b < header->num_blocks; b++)
	{
		AICBlock blk;

		it->block_offsets[b] = (uint32) it->si.cursor;
		total_packed += aic_parse_one_block(&blk, &it->si, it->T);
		CheckCompressedData(total_packed <= header->valid_count);
	}
	CheckCompressedData(total_packed == header->valid_count);

	it->block_idx = header->num_blocks;
	it->row = it->total_count;
	return &it->base;
}
