/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "rr_nulls.h"
#include "compression/compression.h"
#include "rr_utils.h"
#include <port/pg_bitutils.h>

/* please read the README.md file for more details. */

/* NULL bitmap encoder
 *
 * The nulls are either stored as raw, padded to 64 bits, or as
 * a two level summary bitmap (TLSB). The summary bitmap is helpful if the
 * majority of the bits are the same and there are large consecutive areas
 * of the same bits. In these cases we encode 64 flags as a single bit.
 *
 * One summary bit per uint64 output word:
 *   bit=1 -> word matches pre-fill (skip)
 *   bit=0 -> word differs (read from detail stream)
 *
 * Stored as:
 *   [summary: uint8 * S] [detail: uint64 * D]
 *   S = ceil(W/8), W = ceil(N/64), D = number of 0-bits in summary
 *
 * The prefill is either all zeros or all ones, depending on the
 * number of nulls vs the total values:
 *
 *    null_count < total_count/2 -> fill with 1s.
 *
 * The encoder returns the flag telling the caller if we store
 * the NULL bitmap as raw or TLSB, and also the size of the output
 * in 'out_size'.
 */
RRGlobalFlags
rr_null_bitmap_encode(const uint64 *bitmap, size_t total_count, size_t null_count, uint8 *out,
					  size_t *out_size)
{
	Assert(bitmap != NULL);
	Assert(null_count > 0);
	Assert(total_count > 0);
	Assert(out != NULL);
	Assert(out_size != NULL);

	size_t W = (total_count + 63) / 64;
	size_t S = (W + 7) / 8;
	size_t raw_bitmap_bytes = W * (int) sizeof(uint64);
	uint64 fill = (null_count < total_count / 2) ? ~(uint64) 0 : (uint64) 0;

	/* clear the flag bits */
	memset(out, 0, (size_t) S);
	uint8 *detail_ptr = out + S;
	int detail_count = 0;

	for (size_t i = 0; i < W; i++)
	{
		uint64 word = bitmap[i];
		uint64 cmp = fill;

		/* mask the last word to only compare valid bits */
		if (i == W - 1 && (total_count & 63))
		{
			uint64 valid_mask = ((uint64) 1 << (total_count & 63)) - 1;
			word &= valid_mask;
			cmp = fill & valid_mask;
		}

		if (word == cmp)
		{
			out[i >> 3] |= (uint8) (1 << (i & 7));
		}
		else
		{
			rr_store_le64(detail_ptr, bitmap[i], 8);
			detail_ptr += 8;
			detail_count++;
		}
	}

	size_t compressed_size = (size_t) (S + detail_count * 8);

	if (compressed_size >= raw_bitmap_bytes)
	{
		rr_store_le64_words(out, bitmap, (size_t) raw_bitmap_bytes);
		*out_size = raw_bitmap_bytes;
		return RR_FLAG_NULL_RAW;
	}
	else
	{
		*out_size = compressed_size;
		return RR_FLAG_NULL_SUMMARY;
	}
}

/* NULL bitmap decoder
 *
 * decodes the stored bitmap from 'si' to a raw bitmap. it clears the
 * padding bits over 'total_count'. it validates the bits set against
 * the stored 'null_count'.
 *
 * it returns the 'out_n_valid_runs' info to the caller, so the caller
 * can decide if there are enough runs to justify memmove in back
 * scatter.
 *
 * returns the number of bytes read from the input.
 */
size_t
rr_null_bitmap_decode(StringInfo si, size_t total_count, size_t null_count, uint64 *bitmap,
					  RRGlobalFlags flags, size_t *out_n_valid_runs)
{
	size_t W = (total_count + 63) / 64;
	size_t S = (W + 7) / 8;
	size_t valid_count = total_count - null_count;
	uint64 last_mask =
		((total_count & 63) != 0) ? (((uint64) 1 << (total_count & 63)) - 1) : ~(uint64) 0;
	size_t counted_valid = 0;
	size_t n_valid_runs = 0;
	size_t consumed;

	if ((flags & RR_FLAG_NULL_SUMMARY) == 0)
	{
		/* RAW format: a straight copy of the bitmap words */
		size_t raw_bitmap_bytes = W * sizeof(uint64);
		uint8 *raw = (uint8 *) consumeCompressedData(si, raw_bitmap_bytes);

		for (size_t i = 0; i < W; i++)
		{
			uint64 word = rr_load_le64(raw + i * sizeof(uint64));
			if (i == W - 1)
			{
				word &= last_mask;
			}
			bitmap[i] = word;
			counted_valid += (size_t) rr_popcount64(word);
			n_valid_runs += (size_t) rr_popcount64(word & ~(word << 1));
		}
		consumed = raw_bitmap_bytes;
	}
	else
	{
		uint8 *summary = (uint8 *) consumeCompressedData(si, S);
		uint64 fill = (null_count < total_count / 2) ? ~(uint64) 0 : (uint64) 0;
		int detail_count = 0;

		/* prefill with the fill value, so we only need to fill the details */
		memset(bitmap, (fill != 0) ? 0xFF : 0x00, W * sizeof(uint64));

		/* patch in the detail words */
		for (size_t sbyte = 0; sbyte < S; sbyte++)
		{
			uint8 details = (uint8) ~summary[sbyte];

			/* bits at/above W in the last summary byte are padding */
			if (sbyte == S - 1 && (W & 7) != 0)
			{
				details &= (uint8) ((1u << (W & 7)) - 1);
			}

			while (details != 0)
			{
				int b = pg_rightmost_one_pos64((uint64) details);
				uint8 *detail = (uint8 *) consumeCompressedData(si, sizeof(uint64));
				uint64 word = rr_load_le64(detail);

				details &= (uint8) (details - 1);
				bitmap[sbyte * 8 + (size_t) b] = word;
				counted_valid += (size_t) rr_popcount64(word);
				n_valid_runs += (size_t) rr_popcount64(word & ~(word << 1));
				detail_count++;
			}
		}

		/* when fill represents valid bits, we need to adjust the number of
		 * valid runs too
		 */
		if (fill != 0)
		{
			counted_valid += 64 * (W - (size_t) detail_count);
			n_valid_runs += W - (size_t) detail_count;
		}

		/* mask the padding bits of the last word and re-count it */
		{
			uint64 old_word = bitmap[W - 1];
			uint64 new_word = old_word & last_mask;

			bitmap[W - 1] = new_word;
			counted_valid -= (size_t) rr_popcount64(old_word);
			counted_valid += (size_t) rr_popcount64(new_word);
			n_valid_runs -= (size_t) rr_popcount64(old_word & ~(old_word << 1));
			n_valid_runs += (size_t) rr_popcount64(new_word & ~(new_word << 1));
		}
		consumed = (size_t) (S + detail_count * 8);
	}

	/* validate the decoded bitmap against the stored counts */
	CheckCompressedData(counted_valid == valid_count);

	*out_n_valid_runs = n_valid_runs;
	return consumed;
}

/* maximum bytes needed to assemble the NULL bitmap, that is
 * the first level summary bytes (S) plus the second level
 * bitmap words padded to 64 bits. in practice we never exceed
 * W words, because that is the size of the RAW bitmap, but we
 * need this many bytes during the assembly.
 */
extern size_t
rr_null_size_cap(int total_count)
{
	if (total_count == 0)
	{
		return 0;
	}
	size_t W = (total_count + 63) / 64;
	size_t S = (W + 7) / 8;
	return (S + W * sizeof(uint64));
}
