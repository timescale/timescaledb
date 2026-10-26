/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <stdint.h>

/*
 * common utility functions. many of the below functions are
 * here to support a specific variable length integer storage
 * format that is used in the block headers. this takes the
 * integer and only stores the bytes that has data in a little
 * endian form. the negative values are handled by sign extension
 * which is the extension of the two-complement form's bits
 * when shifting the signed integer right. (the headers have a
 * separate field that tells how many bytes the integer has)
 */

/* please read the README.md file for more details. */

/* returns number of bits needed to represent `val` */
static inline uint8
aic_bit_width_u64(uint64 val)
{
	if (val == 0)
	{
		return 0;
	}
	return (uint8) (pg_leftmost_one_pos64(val) + 1);
}

/* returns the minimum bytes (1..8) needed to store `v`
 * such that it can be 'sign extended' back to `v` in
 * the two's complement form.
 */
static inline uint8
aic_signed_bytes_needed(int64 v)
{
	uint64 mag = (v < 0) ? (uint64) (~v) : (uint64) v;
	uint8 bits = (uint8) (aic_bit_width_u64(mag) + 1);
	return (uint8) ((bits + 7) / 8);
}

/* the number of bytes needed to store the value in the block
 * headers, where we don't store any value for v=0.
 */
static inline uint8
aic_block_header_scalar_len(int64 v)
{
	return (v == 0) ? 0 : aic_signed_bytes_needed(v);
}

/*  store the little endian low `n` bytes of `v` */
static inline void
aic_store_le64(uint8 *dst, uint64 v, uint8 n)
{
	for (uint8 i = 0; i < n; i++)
	{
		dst[i] = (uint8) (v >> (8 * i));
	}
}

static inline uint64
aic_load_le64(const uint8 *src)
{
	uint64 v = 0;
	for (uint8 i = 0; i < 8; i++)
	{
		v |= (uint64) src[i] << (8 * i);
	}
	return v;
}

/* store the whole uint32 data `v` as little endian */
static inline void
aic_store_le32(uint8 *dst, uint32 v)
{
	for (uint8 i = 0; i < 4; i++)
	{
		dst[i] = (uint8) (v >> (8 * i));
	}
}

/* load the whole 32 bit value as little endian */
static inline uint32
aic_load_le32(const uint8 *src)
{
	uint32 v = 0;
	for (uint8 i = 0; i < 4; i++)
	{
		v |= (uint32) src[i] << (8 * i);
	}
	return v;
}

/* interpret the low `width_bits` of v as two's-complement
 * and sign-extend to int64. width_bits in 1..64. */
static inline int64
aic_sign_extend_u64(uint64 v, uint8 width_bits)
{
	Assert(width_bits > 0 && width_bits <= 64);
	uint8 shift = (uint8) (64 - width_bits);
	return ((int64) (v << shift)) >> shift; /* arithmetic >> replicates the sign bit */
}

/* read a little endian, `n` bytes, signed scalar from `src` */
static inline int64
aic_read_signed(const uint8 *src, uint8 n) /* n in 0..8 */
{
	if (n == 0)
	{
		return 0;
	}

	uint64 v = 0;
	for (uint8 i = 0; i < n; i++)
	{
		v |= (uint64) src[i] << (8 * i);
	}

	return aic_sign_extend_u64(v, (uint8) (8 * n));
}

/* copy uint64 words into little-endian stream order */
static inline void
aic_store_le64_words(uint8 *dst, const uint64 *src, size_t nbytes)
{
#ifndef WORDS_BIGENDIAN
	memcpy(dst, src, nbytes);
#else
	for (size_t off = 0; off < nbytes; off += 8)
	{
		aic_store_le64(dst + off, src[off / 8], 8);
	}
#endif
}

#define AIC_DEFINE_ZIGZAG(TY)                                                                      \
	static inline TY aic_zigzag_encode_##TY(TY v)                                                  \
	{                                                                                              \
		TY sign = (TY) (v >> (sizeof(TY) * 8 - 1));                                                \
		return (TY) ((TY) (v << 1) ^ (TY) ((TY) 0 - sign));                                        \
	}                                                                                              \
	static inline TY aic_zigzag_decode_##TY(TY zz)                                                 \
	{                                                                                              \
		return (TY) ((TY) (zz >> 1) ^ (TY) ((TY) 0 - (TY) (zz & 1)));                              \
	}

AIC_DEFINE_ZIGZAG(uint16)
AIC_DEFINE_ZIGZAG(uint32)
AIC_DEFINE_ZIGZAG(uint64)
#undef AIC_DEFINE_ZIGZAG
