/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "rr_bulk_decompress.h"
#include "compression/algorithms/fastlanes/fastlanes_types.h"
#include "compression/compression.h"
#include "rr_blocks.h"
#include "rr_internal.h"
#include "rr_nulls.h"
#include <guc.h>
#include <port/pg_bitutils.h>
#include <stdint.h>

/* please read the README.md file for more details. */

/* clang-tidy doesn't like the type argument and insists on using
 * parens, but that doesn't compile */
/* NOLINTBEGIN(bugprone-macro-parentheses) */

/*
 * the compressed data has the validity bits (non-null flags) and the values
 * separately. the scatter loop takes the validity bits and puts the values
 * back into the position, where they interleave with the NULL values, as in
 * the original compressed stream.
 *
 * when we have enough values that we can memmove together we use the
 * 'memmove scatter' method instead of going through the bits one by one.
 *
 * the MEMMOVE_SCATTER parameter activates one branch of the function and
 * the other branch will be optimized away.
 */
#define RR_SCATTER_LOOP(NAME, STY, MEMMOVE_SCATTER)                                                \
	static inline void NAME(uint8 *dest,                                                           \
							const uint64 *validity,                                                \
							size_t total_count,                                                    \
							size_t valid_count)                                                    \
	{                                                                                              \
		STY *data = (STY *) dest;                                                                  \
		int32 last_word = (int32) ((total_count - 1) / 64);                                        \
		uint32 src_end = valid_count;                                                              \
		for (int32 w = last_word; w >= 0; --w)                                                     \
		{                                                                                          \
			uint64 word = validity[w];                                                             \
			uint32 chunk = (w == last_word) ? (total_count - (uint32) w * 64) : 64;                \
			uint32 nbits = (uint32) rr_popcount64(word);                                           \
			uint32 src_start = src_end - nbits;                                                    \
			uint32 dst_start = (uint32) w * 64;                                                    \
			if (nbits == chunk)                                                                    \
			{                                                                                      \
				if (src_start != dst_start)                                                        \
				{                                                                                  \
					memmove(data + dst_start, data + src_start, nbits * sizeof(STY));              \
				}                                                                                  \
			}                                                                                      \
			else if (nbits > 0)                                                                    \
			{                                                                                      \
				if (MEMMOVE_SCATTER)                                                               \
				{                                                                                  \
					uint32 src_idx = src_start + nbits;                                            \
					uint64 bits = word;                                                            \
					while (bits != 0)                                                              \
					{                                                                              \
						int hi = pg_leftmost_one_pos64(bits);                                      \
						uint64 zeros = ~bits & (((uint64) 1 << hi) - 1);                           \
						int lo = (zeros == 0) ? 0 : pg_leftmost_one_pos64(zeros) + 1;              \
						uint32 len = (uint32) (hi - lo + 1);                                       \
						src_idx -= len;                                                            \
						memmove(data + dst_start + (uint32) lo,                                    \
								data + src_idx,                                                    \
								len * sizeof(STY));                                                \
						bits &= (lo == 0) ? 0 : (((uint64) 1 << lo) - 1);                          \
					}                                                                              \
				}                                                                                  \
				else                                                                               \
				{                                                                                  \
					uint32 src_idx = src_start + nbits - 1;                                        \
					uint64 bits = word;                                                            \
					while (bits != 0)                                                              \
					{                                                                              \
						int b = pg_leftmost_one_pos64(bits);                                       \
						data[dst_start + (uint32) b] = data[src_idx--];                            \
						bits ^= ((uint64) 1 << b);                                                 \
					}                                                                              \
				}                                                                                  \
			}                                                                                      \
			src_end = src_start;                                                                   \
		}                                                                                          \
	}

#define RR_BACKWARD_SCATTER(TY, STY)                                                               \
	RR_SCATTER_LOOP(rr_backward_scatter_##TY##_bits, STY, 0)                                       \
	RR_SCATTER_LOOP(rr_backward_scatter_##TY##_runs, STY, 1)                                       \
	static inline void rr_backward_scatter_##TY(uint8 *dest,                                       \
												const uint64 *validity,                            \
												size_t total_count,                                \
												size_t valid_count,                                \
												bool memmove_scatter)                              \
	{                                                                                              \
		if (memmove_scatter)                                                                       \
			rr_backward_scatter_##TY##_runs(dest, validity, total_count, valid_count);             \
		else                                                                                       \
			rr_backward_scatter_##TY##_bits(dest, validity, total_count, valid_count);             \
	}

RR_BACKWARD_SCATTER(uint16, int16)
RR_BACKWARD_SCATTER(uint32, int32)
RR_BACKWARD_SCATTER(uint64, int64)
#undef RR_BACKWARD_SCATTER
#undef RR_SCATTER_LOOP

/* NOLINTEND(bugprone-macro-parentheses) */

extern ArrowArray *
rapid_raccoon_decompress_all(Datum compressed, Oid element_type, MemoryContext dest_mctx)
{
	StringInfoData si = { .data = DatumGetPointer(compressed),
						  .len = VARSIZE(DatumGetPointer(compressed)) };
	RRCompressed *header = (RRCompressed *) consumeCompressedData(&si, sizeof(RRCompressed));

	/* the valid_count has to be a positive value, otherwise the block is either
	 * empty or all NULLs. In that case we use the null compression block, not RR. */
	CheckCompressedData(header->valid_count > 0);
	CheckCompressedData(header->num_blocks > 0);

	/* calculate the bitwidth of the stored elements */
	fl_elem_width_t T = rr_global_flag_to_elem_width(header->flags);

	/* cross check against supported oids */
	fl_elem_width_t expected_width = rr_oid_to_elem_width_t(element_type);
	CheckCompressedData(expected_width == T);

	/* fill validity bits */
	size_t total_count = header->valid_count + header->null_count;
	size_t validity_size =
		(header->null_count > 0) ? (((header->valid_count + header->null_count + 63) / 64) * 8) : 0;
	uint64 *validity_bitmap =
		(uint64 *) ((header->null_count > 0) ? MemoryContextAlloc(dest_mctx, validity_size) : NULL);
	bool memmove_scatter = false;

	if (header->null_count > 0)
	{
		size_t n_valid_runs = 0;
		size_t decoded = rr_null_bitmap_decode(&si,
											   total_count,
											   header->null_count,
											   validity_bitmap,
											   header->flags,
											   &n_valid_runs);

		memmove_scatter = (n_valid_runs * RR_SCATTER_RUN_THRESHOLD <= header->valid_count);

		/* we either use the RAW format which is a simple array of 'validity' bits,
		 * one per element, _or_ we use the SUMMARY format, which has a high level
		 * summary bitmap that allows us to skip blocks of all 'valid'/'invalid' values.
		 */
		CheckCompressedData(
			(decoded == validity_size && (header->flags & RR_FLAG_NULL_SUMMARY) == 0) ||
			(decoded < validity_size && (header->flags & RR_FLAG_NULL_SUMMARY) != 0));
	}

	/* the worst case for the number of blocks is to have alternating DELTA_RLE blocks
	 * of 32 elements each, all other blocks absorb more elements, except the last one */
	CheckCompressedData(header->num_blocks <=
						((GLOBAL_MAX_ROWS_PER_COMPRESSION / RR_DELTA_RLE_CHECKPOINT) + 1));

	size_t values_to_alloc = (size_t) header->valid_count + header->null_count + 256;
	size_t values_size = (((values_to_alloc * (uint8) T) + 63) / 64) * 8;

	/* Allocate the output buffer for the values and the arrow array in one allocation. */
	size_t arrow_data_size = sizeof(ArrowArray) + sizeof(void *) * 2;
	ArrowArray *array = (ArrowArray *) MemoryContextAlloc(dest_mctx, arrow_data_size + values_size);
	memset(array, 0, arrow_data_size);
	const void **buffers = (const void **) &array[1];
	uint8 *decompressed_values = ((uint8 *) array) + arrow_data_size;
	array->length = total_count;
	array->null_count = header->null_count;
	array->n_buffers = 2;
	array->buffers = buffers;
	array->buffers[0] = validity_bitmap;
	array->buffers[1] = decompressed_values;

	uint64 unaligned_scratch_buffer[RR_SCRATCH_WORDS];
	uint8 *scratch_buffer =
		(uint8 *) (((uintptr_t) unaligned_scratch_buffer + (FL_MAX_ALIGNMENT - 1)) &
				   ~(uintptr_t) (FL_MAX_ALIGNMENT - 1));

	size_t num_decompressed = rr_decompress_blocks(header->num_blocks,
												   &si,
												   decompressed_values,
												   scratch_buffer,
												   header->valid_count,
												   T);
	CheckCompressedData(num_decompressed == header->valid_count);

	if (header->null_count > 0)
	{
		switch (T)
		{
			case FL_ELEM_W16:
				rr_backward_scatter_uint16(decompressed_values,
										   validity_bitmap,
										   total_count,
										   header->valid_count,
										   memmove_scatter);
				break;
			case FL_ELEM_W32:
				rr_backward_scatter_uint32(decompressed_values,
										   validity_bitmap,
										   total_count,
										   header->valid_count,
										   memmove_scatter);
				break;
			case FL_ELEM_W64:
				rr_backward_scatter_uint64(decompressed_values,
										   validity_bitmap,
										   total_count,
										   header->valid_count,
										   memmove_scatter);
				break;
			default:
				CheckCompressedData(false);
				break;
		}
	}

	return array;
}
