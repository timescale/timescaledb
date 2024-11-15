#include "umash.h"

/*
 * UMASH is distributed under the MIT license.
 *
 * SPDX-License-Identifier: MIT
 *
 * Copyright 2020-2022 Backtrace I/O, Inc.
 * Copyright 2022 Paul Khuong
 * Copyright 2022 Dougall Johnson
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#if !defined(UMASH_TEST_ONLY) && !defined(NDEBUG)
#define NDEBUG
#endif

/**
 * -DUMASH_LONG_INPUTS=0 to disable the routine specialised for long
 * inputs, and -DUMASH_LONG_INPUTS=1 to enable it.  If the variable
 * isn't defined, we try to probe for `umash_long.inc`: that's where
 * the long input routines are defined.
 */
#ifndef UMASH_LONG_INPUTS
#ifdef __has_include
#if __has_include("umash_long.inc")
#define UMASH_LONG_INPUTS 1
#endif /* __has_include() */
#endif /* __has_include */

#ifndef UMASH_LONG_INPUTS
#define UMASH_LONG_INPUTS 0
#endif /* !UMASH_LONG_INPUTS */
#endif /* !UMASH_LONG_INPUTS */

/*
 * Default to dynamically dispatching implementations on x86-64
 * (there's nothing to dispatch on aarch64).
 */
#ifndef UMASH_DYNAMIC_DISPATCH
#ifdef __x86_64__
#define UMASH_DYNAMIC_DISPATCH 1
#else
#define UMASH_DYNAMIC_DISPATCH 0
#endif
#endif

/*
 * Enable inline assembly by default when building with recent GCC or
 * compatible compilers.  It should always be safe to disable this
 * option, although there may be a performance cost.
 */
#ifndef UMASH_INLINE_ASM

#if defined(__clang__)
/*
 * We need clang 8+ for output flags, and 10+ for relaxed vector
 * constraints.
 */
#if __clang_major__ >= 10
#define UMASH_INLINE_ASM 1
#else
#define UMASH_INLINE_ASM 0
#endif /* __clang_major__ */

#elif defined(__GNUC__)
#if __GNUC__ >= 6
#define UMASH_INLINE_ASM 1
#else
#define UMASH_INLINE_ASM 0
#endif /* __GNUC__ */

#else
#define UMASH_INLINE_ASM 0
#endif

#endif

#include <assert.h>
#include <string.h>

#ifdef __PCLMUL__
/* If we have access to x86 PCLMUL (and some basic SSE). */
#include <immintrin.h>

/* We only use 128-bit vector, as pairs of 64-bit integers. */
typedef __m128i v128;

#define V128_ZERO { 0 };

static inline v128
v128_create(uint64_t lo, uint64_t hi)
{
	return _mm_set_epi64x(hi, lo);
}

/* Shift each 64-bit lane left by one bit. */
static inline v128
v128_shift(v128 x)
{
	return _mm_add_epi64(x, x);
}

/* Computes the 128-bit carryless product of x and y. */
static inline v128
v128_clmul(uint64_t x, uint64_t y)
{
	return _mm_clmulepi64_si128(_mm_cvtsi64_si128(x), _mm_cvtsi64_si128(y), 0);
}

/* Computes the 128-bit carryless product of the high and low halves of x. */
static inline v128
v128_clmul_cross(v128 x)
{
	return _mm_clmulepi64_si128(x, x, 1);
}

#elif defined(__ARM_FEATURE_CRYPTO)

#include <arm_neon.h>

typedef uint64x2_t v128;

#define V128_ZERO { 0 };

static inline v128
v128_create(uint64_t lo, uint64_t hi)
{
	return vcombine_u64(vcreate_u64(lo), vcreate_u64(hi));
}

static inline v128
v128_shift(v128 x)
{
	return vshlq_n_u64(x, 1);
}

static inline v128
v128_clmul(uint64_t x, uint64_t y)
{
	return vreinterpretq_u64_p128(vmull_p64(x, y));
}

static inline v128
v128_clmul_cross(v128 x)
{
	v128 swapped = vextq_u64(x, x, 1);
#if UMASH_INLINE_ASM
	/* Keep the result out of GPRs. */
	__asm__("" : "+w"(swapped));
#endif

	return v128_clmul(vgetq_lane_u64(x, 0), vgetq_lane_u64(swapped, 0));
}

#else

#error \
    "Unsupported platform: umash requires CLMUL (-mpclmul) on x86-64, or crypto (-march=...+crypto) extensions on aarch64."
#endif

/*
 * #define UMASH_STAP_PROBE=1 to insert probe points in public UMASH
 * functions.
 *
 * This functionality depends on Systemtap's SDT header file.
 */
#if defined(UMASH_STAP_PROBE) && UMASH_STAP_PROBE
#include <sys/sdt.h>
#else
#define DTRACE_PROBE1(lib, name, a0)
#define DTRACE_PROBE2(lib, name, a0, a1)
#define DTRACE_PROBE3(lib, name, a0, a1, a2)
#define DTRACE_PROBE4(lib, name, a0, a1, a2, a3)
#endif

/*
 * #define UMASH_SECTION="special_section" to emit all UMASH symbols
 * in the `special_section` ELF section.
 */
#if defined(UMASH_SECTION) && defined(__GNUC__)
#define FN __attribute__((__section__(UMASH_SECTION)))
#else
#define FN
#endif

/*
 * Defining UMASH_TEST_ONLY switches to a debug build with internal
 * symbols exposed.
 */
#ifdef UMASH_TEST_ONLY
#define TEST_DEF FN
#include "t/umash_test_only.h"
#else
#define TEST_DEF static FN
#endif

#ifdef __GNUC__
#define LIKELY(X) __builtin_expect(!!(X), 1)
#define UNLIKELY(X) __builtin_expect(!!(X), 0)
#define HOT __attribute__((__hot__))
#define COLD __attribute__((__cold__))
#else
#define LIKELY(X) X
#define UNLIKELY(X) X
#define HOT
#define COLD
#endif

#define ARRAY_SIZE(ARR) (sizeof(ARR) / sizeof(ARR[0]))

#define BLOCK_SIZE (sizeof(uint64_t) * UMASH_OH_PARAM_COUNT)

/*
 * We derive independent short hashes by offsetting the constant array
 * by four u64s.  In theory, any positive even number works, but this
 * is the constant we used in an earlier incarnation, and it works.
 */
#define OH_SHORT_HASH_SHIFT 4

/* Incremental UMASH consumes 16 bytes at a time. */
#define INCREMENTAL_GRANULARITY 16

/**
 * Modular arithmetic utilities.
 *
 * The code below uses GCC extensions.  It should be possible to add
 * support for other compilers.
 */

#if !defined(__x86_64__) || !UMASH_INLINE_ASM
static inline void
mul128(uint64_t x, uint64_t y, uint64_t *hi, uint64_t *lo)
{
	__uint128_t product = x;

	product *= y;
	*hi = product >> 64;
	*lo = product;
	return;
}
#else
static inline void
mul128(uint64_t x, uint64_t y, uint64_t *hi, uint64_t *lo)
{
	uint64_t mulhi, mullo;

	__asm__("mul %3" : "=a"(mullo), "=d"(mulhi) : "%a"(x), "r"(y) : "cc");
	*hi = mulhi;
	*lo = mullo;
	return;
}
#endif

TEST_DEF inline uint64_t
add_mod_fast(uint64_t x, uint64_t y)
{
	unsigned long long sum;

	/* If `sum` overflows, `sum + 8` does not. */
	return (__builtin_uaddll_overflow(x, y, &sum) ? sum + 8 : sum);
}

static FN COLD uint64_t
add_mod_slow_slow_path(uint64_t sum, uint64_t fixup)
{
	/* Reduce sum, mod 2**64 - 8. */
	sum = (sum >= (uint64_t)-8) ? sum + 8 : sum;
	/* sum < 2**64 - 8, so this doesn't overflow. */
	sum += fixup;
	/* Reduce again. */
	sum = (sum >= (uint64_t)-8) ? sum + 8 : sum;
	return sum;
}

TEST_DEF inline uint64_t
add_mod_slow(uint64_t x, uint64_t y)
{
	unsigned long long sum;
	uint64_t fixup = 0;

	/* x + y \equiv sum + fixup */
	if (__builtin_uaddll_overflow(x, y, &sum))
		fixup = 8;

	/*
	 * We must ensure `sum + fixup < 2**64 - 8`.
	 *
	 * We want a conditional branch here, but not in the
	 * overflowing add: overflows happen roughly half the time on
	 * pseudorandom inputs, but `sum < 2**64 - 16` is almost
	 * always true, for pseudorandom `sum`.
	 */
	if (LIKELY(sum < (uint64_t)-16))
		return sum + fixup;

#ifdef UMASH_INLINE_ASM
	/*
	 * Some compilers like to compile the likely branch above with
	 * conditional moves or predication.  Insert a compiler barrier
	 * in the slow path here to force a branch.
	 */
	__asm__("" : "+r"(sum));
#endif
	return add_mod_slow_slow_path(sum, fixup);
}

TEST_DEF inline uint64_t
mul_mod_fast(uint64_t m, uint64_t x)
{
	uint64_t hi, lo;

	mul128(m, x, &hi, &lo);
	return add_mod_fast(lo, 8 * hi);
}

TEST_DEF inline uint64_t
horner_double_update(uint64_t acc, uint64_t m0, uint64_t m1, uint64_t x, uint64_t y)
{

	acc = add_mod_fast(acc, x);
	return add_mod_slow(mul_mod_fast(m0, acc), mul_mod_fast(m1, y));
}

/**
 * Salsa20 stream generator, used to derive struct umash_param.
 *
 * Slightly prettified version of D. J. Bernstein's public domain NaCL
 * (version 20110121), without paying any attention to constant time
 * execution or any other side-channel.
 */
static inline uint32_t
rotate(uint32_t u, int c)
{

	return (u << c) | (u >> (32 - c));
}

static inline uint32_t
load_littleendian(const void *buf)
{
	uint32_t ret = 0;
	uint8_t x[4];

	memcpy(x, buf, sizeof(x));
	for (size_t i = 0; i < 4; i++)
		ret |= (uint32_t)x[i] << (8 * i);

	return ret;
}

static inline void
store_littleendian(void *dst, uint32_t u)
{

	for (size_t i = 0; i < 4; i++) {
		uint8_t lo = u;

		memcpy(dst, &lo, 1);
		u >>= 8;
		dst = (char *)dst + 1;
	}

	return;
}

static FN void
core_salsa20(char *out, const uint8_t in[static 16], const uint8_t key[static 32],
    const uint8_t constant[16])
{
	enum { ROUNDS = 20 };
	uint32_t x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15;
	uint32_t j0, j1, j2, j3, j4, j5, j6, j7, j8, j9, j10, j11, j12, j13, j14, j15;

	j0 = x0 = load_littleendian(constant + 0);
	j1 = x1 = load_littleendian(key + 0);
	j2 = x2 = load_littleendian(key + 4);
	j3 = x3 = load_littleendian(key + 8);
	j4 = x4 = load_littleendian(key + 12);
	j5 = x5 = load_littleendian(constant + 4);
	j6 = x6 = load_littleendian(in + 0);
	j7 = x7 = load_littleendian(in + 4);
	j8 = x8 = load_littleendian(in + 8);
	j9 = x9 = load_littleendian(in + 12);
	j10 = x10 = load_littleendian(constant + 8);
	j11 = x11 = load_littleendian(key + 16);
	j12 = x12 = load_littleendian(key + 20);
	j13 = x13 = load_littleendian(key + 24);
	j14 = x14 = load_littleendian(key + 28);
	j15 = x15 = load_littleendian(constant + 12);

	for (size_t i = 0; i < ROUNDS; i += 2) {
		x4 ^= rotate(x0 + x12, 7);
		x8 ^= rotate(x4 + x0, 9);
		x12 ^= rotate(x8 + x4, 13);
		x0 ^= rotate(x12 + x8, 18);
		x9 ^= rotate(x5 + x1, 7);
		x13 ^= rotate(x9 + x5, 9);
		x1 ^= rotate(x13 + x9, 13);
		x5 ^= rotate(x1 + x13, 18);
		x14 ^= rotate(x10 + x6, 7);
		x2 ^= rotate(x14 + x10, 9);
		x6 ^= rotate(x2 + x14, 13);
		x10 ^= rotate(x6 + x2, 18);
		x3 ^= rotate(x15 + x11, 7);
		x7 ^= rotate(x3 + x15, 9);
		x11 ^= rotate(x7 + x3, 13);
		x15 ^= rotate(x11 + x7, 18);
		x1 ^= rotate(x0 + x3, 7);
		x2 ^= rotate(x1 + x0, 9);
		x3 ^= rotate(x2 + x1, 13);
		x0 ^= rotate(x3 + x2, 18);
		x6 ^= rotate(x5 + x4, 7);
		x7 ^= rotate(x6 + x5, 9);
		x4 ^= rotate(x7 + x6, 13);
		x5 ^= rotate(x4 + x7, 18);
		x11 ^= rotate(x10 + x9, 7);
		x8 ^= rotate(x11 + x10, 9);
		x9 ^= rotate(x8 + x11, 13);
		x10 ^= rotate(x9 + x8, 18);
		x12 ^= rotate(x15 + x14, 7);
		x13 ^= rotate(x12 + x15, 9);
		x14 ^= rotate(x13 + x12, 13);
		x15 ^= rotate(x14 + x13, 18);
	}

	x0 += j0;
	x1 += j1;
	x2 += j2;
	x3 += j3;
	x4 += j4;
	x5 += j5;
	x6 += j6;
	x7 += j7;
	x8 += j8;
	x9 += j9;
	x10 += j10;
	x11 += j11;
	x12 += j12;
	x13 += j13;
	x14 += j14;
	x15 += j15;

	store_littleendian(out + 0, x0);
	store_littleendian(out + 4, x1);
	store_littleendian(out + 8, x2);
	store_littleendian(out + 12, x3);
	store_littleendian(out + 16, x4);
	store_littleendian(out + 20, x5);
	store_littleendian(out + 24, x6);
	store_littleendian(out + 28, x7);
	store_littleendian(out + 32, x8);
	store_littleendian(out + 36, x9);
	store_littleendian(out + 40, x10);
	store_littleendian(out + 44, x11);
	store_littleendian(out + 48, x12);
	store_littleendian(out + 52, x13);
	store_littleendian(out + 56, x14);
	store_littleendian(out + 60, x15);
	return;
}

TEST_DEF void
salsa20_stream(
    void *dst, size_t len, const uint8_t nonce[static 8], const uint8_t key[static 32])
{
	static const uint8_t sigma[16] = "expand 32-byte k";
	uint8_t in[16];

	if (len == 0)
		return;

	memcpy(in, nonce, 8);
	memset(in + 8, 0, 8);

	while (len >= 64) {
		unsigned int u;

		core_salsa20(dst, in, key, sigma);
		u = 1;
		for (size_t i = 8; i < 16; i++) {
			u += in[i];
			in[i] = u;
			u >>= 8;
		}

		dst = (char *)dst + 64;
		len -= 64;
	}

	if (len > 0) {
		char block[64];

		core_salsa20(block, in, key, sigma);
		memcpy(dst, block, len);
	}

	return;
}

#if defined(UMASH_TEST_ONLY) || UMASH_LONG_INPUTS
#include "umash_long.inc"
#endif

/**
 * OH block compression.
 */
TEST_DEF struct umash_oh
oh_varblock(const uint64_t *params, uint64_t tag, const void *block, size_t n_bytes)
{
	struct umash_oh ret;
	v128 acc = V128_ZERO;

	/* The final block processes `remaining > 0` bytes. */
	size_t remaining = 1 + ((n_bytes - 1) % sizeof(v128));
	size_t end_full_pairs = (n_bytes - remaining) / sizeof(uint64_t);
	const void *last_ptr = (const char *)block + n_bytes - sizeof(v128);
	size_t i;

	for (i = 0; i < end_full_pairs; i += 2) {
		v128 x, k;

		memcpy(&x, block, sizeof(x));
		block = (const char *)block + sizeof(x);

		memcpy(&k, &params[i], sizeof(k));
		x ^= k;
		acc ^= v128_clmul_cross(x);
	}

	memcpy(&ret, &acc, sizeof(ret));

	/* Compress the final (potentially partial) pair. */
	{
		uint64_t x, y, enh_hi, enh_lo;

		memcpy(&x, last_ptr, sizeof(x));
		last_ptr = (const char *)last_ptr + sizeof(x);
		memcpy(&y, last_ptr, sizeof(y));

		x += params[i];
		y += params[i + 1];
		mul128(x, y, &enh_hi, &enh_lo);
		enh_hi += tag;

		ret.bits[0] ^= enh_lo;
		ret.bits[1] ^= enh_hi ^ enh_lo;
	}

	return ret;
}

TEST_DEF void
oh_varblock_fprint(struct umash_oh dst[static restrict 2],
    const uint64_t *restrict params, uint64_t tag, const void *restrict block,
    size_t n_bytes)
{
	v128 acc = V128_ZERO; /* Base umash */
	v128 acc_shifted = V128_ZERO; /* Accumulates shifted values */
	v128 lrc;
	/* The final block processes `remaining > 0` bytes. */
	size_t remaining = 1 + ((n_bytes - 1) % sizeof(v128));
	size_t end_full_pairs = (n_bytes - remaining) / sizeof(uint64_t);
	const void *last_ptr = (const char *)block + n_bytes - sizeof(v128);
	size_t i;

	lrc = v128_create(params[UMASH_OH_PARAM_COUNT], params[UMASH_OH_PARAM_COUNT + 1]);
	for (i = 0; i < end_full_pairs; i += 2) {
		v128 x, k;

		memcpy(&x, block, sizeof(x));
		block = (const char *)block + sizeof(x);

		memcpy(&k, &params[i], sizeof(k));

		x ^= k;
		lrc ^= x;

		x = v128_clmul_cross(x);

		acc ^= x;
		if (i + 2 >= end_full_pairs)
			break;

		acc_shifted ^= x;
		acc_shifted = v128_shift(acc_shifted);
	}

	/*
	 * Update the LRC for the last chunk before treating it
	 * specially.
	 */
	{
		v128 x, k;

		memcpy(&x, last_ptr, sizeof(x));
		memcpy(&k, &params[end_full_pairs], sizeof(k));

		lrc ^= x ^ k;
	}

	acc_shifted ^= acc;
	acc_shifted = v128_shift(acc_shifted);

	acc_shifted ^= v128_clmul_cross(lrc);

	memcpy(&dst[0], &acc, sizeof(dst[0]));
	memcpy(&dst[1], &acc_shifted, sizeof(dst[1]));

	{
		uint64_t x, y, kx, ky, enh_hi, enh_lo;

		memcpy(&x, last_ptr, sizeof(x));
		last_ptr = (const char *)last_ptr + sizeof(x);
		memcpy(&y, last_ptr, sizeof(y));

		kx = x + params[end_full_pairs];
		ky = y + params[end_full_pairs + 1];

		mul128(kx, ky, &enh_hi, &enh_lo);
		enh_hi += tag;

		enh_hi ^= enh_lo;
		dst[0].bits[0] ^= enh_lo;
		dst[0].bits[1] ^= enh_hi;

		dst[1].bits[0] ^= enh_lo;
		dst[1].bits[1] ^= enh_hi;
	}

	return;
}

/**
 * Returns `then` if `cond` is true, `otherwise` if false.
 *
 * This noise helps compiler emit conditional moves.
 */
static inline const void *
select_ptr(bool cond, const void *then, const void *otherwise)
{
	const char *ret;

#if UMASH_INLINE_ASM
	/* Force strict evaluation of both arguments. */
	__asm__("" ::"r"(then), "r"(otherwise));
#endif

	ret = (cond) ? then : otherwise;

#if UMASH_INLINE_ASM
	/* And also force the result to be materialised with a blackhole. */
	__asm__("" : "+r"(ret));
#endif
	return ret;
}

/**
 * Short UMASH (<= 8 bytes).
 */
TEST_DEF inline uint64_t
vec_to_u64(const void *data, size_t n_bytes)
{
	const char zeros[2] = { 0 };
	uint32_t hi, lo;

	/*
	 * If there are at least 4 bytes to read, read the first 4 in
	 * `lo`, and the last 4 in `hi`.  This covers the whole range,
	 * since `n_bytes` is at most 8.
	 */
	if (LIKELY(n_bytes >= sizeof(lo))) {
		memcpy(&lo, data, sizeof(lo));
		memcpy(&hi, (const char *)data + n_bytes - sizeof(hi), sizeof(hi));
	} else {
		/* 0 <= n_bytes < 4.  Decode the size in binary. */
		uint16_t word;
		uint8_t byte;

		/*
		 * If the size is odd, load the first byte in `byte`;
		 * otherwise, load in a zero.
		 */
		memcpy(&byte, select_ptr(n_bytes & 1, data, zeros), 1);
		lo = byte;

		/*
		 * If the size is 2 or 3, load the last two bytes in `word`;
		 * otherwise, load in a zero.
		 */
		memcpy(&word,
		    select_ptr(n_bytes & 2, (const char *)data + n_bytes - 2, zeros), 2);
		/*
		 * We have now read `bytes[0 ... n_bytes - 1]`
		 * exactly once without overwriting any data.
		 */
		hi = word;
	}

	/*
	 * Mix `hi` with the `lo` bits: SplitMix64 seems to have
	 * trouble with the top 4 bits.
	 */
	return ((uint64_t)hi << 32) | (lo + hi);
}

TEST_DEF uint64_t
umash_short(const uint64_t *params, uint64_t seed, const void *data, size_t n_bytes)
{
	uint64_t h;

	seed += params[n_bytes];
	h = vec_to_u64(data, n_bytes);
	h ^= h >> 30;
	h *= 0xbf58476d1ce4e5b9ULL;
	h = (h ^ seed) ^ (h >> 27);
	h *= 0x94d049bb133111ebULL;
	h ^= h >> 31;
	return h;
}

static FN struct umash_fp
umash_fp_short(const uint64_t *params, uint64_t seed, const void *data, size_t n_bytes)
{
	struct umash_fp ret;
	uint64_t h;

	ret.hash[0] = seed + params[n_bytes];
	ret.hash[1] = seed + params[n_bytes + OH_SHORT_HASH_SHIFT];

	h = vec_to_u64(data, n_bytes);
	h ^= h >> 30;
	h *= 0xbf58476d1ce4e5b9ULL;
	h ^= h >> 27;

#define TAIL(i)                                       \
	do {                                          \
		ret.hash[i] ^= h;                     \
		ret.hash[i] *= 0x94d049bb133111ebULL; \
		ret.hash[i] ^= ret.hash[i] >> 31;     \
	} while (0)

	TAIL(0);
	TAIL(1);
#undef TAIL

	return ret;
}

/**
 * Rotates `x` left by `n` bits.
 */
static inline uint64_t
rotl64(uint64_t x, int n)
{

	return (x << n) | (x >> (64 - n));
}

TEST_DEF inline uint64_t
finalize(uint64_t x)
{

	return (x ^ rotl64(x, 8)) ^ rotl64(x, 33);
}

TEST_DEF uint64_t
umash_medium(const uint64_t multipliers[static 2], const uint64_t *oh, uint64_t seed,
    const void *data, size_t n_bytes)
{
	uint64_t enh_hi, enh_lo;

	{
		uint64_t x, y;

		memcpy(&x, data, sizeof(x));
		memcpy(&y, (const char *)data + n_bytes - sizeof(y), sizeof(y));
		x += oh[0];
		y += oh[1];

		mul128(x, y, &enh_hi, &enh_lo);
		enh_hi += seed ^ n_bytes;
	}

	enh_hi ^= enh_lo;
	return finalize(horner_double_update(
	    /*acc=*/0, multipliers[0], multipliers[1], enh_lo, enh_hi));
}

static FN struct umash_fp
umash_fp_medium(const uint64_t multipliers[static 2][2], const uint64_t *oh,
    uint64_t seed, const void *data, size_t n_bytes)
{
	struct umash_fp ret;
	const uint64_t offset = seed ^ n_bytes;
	uint64_t enh_hi, enh_lo;
	union {
		v128 v;
		uint64_t u64[2];
	} mixed_lrc;
	uint64_t lrc[2] = { oh[UMASH_OH_PARAM_COUNT], oh[UMASH_OH_PARAM_COUNT + 1] };
	uint64_t x, y;
	uint64_t a, b;

	/* Expand the 9-16 bytes to 16. */
	memcpy(&x, data, sizeof(x));
	memcpy(&y, (const char *)data + n_bytes - sizeof(y), sizeof(y));

	a = oh[0];
	b = oh[1];

	lrc[0] ^= x ^ a;
	lrc[1] ^= y ^ b;
	mixed_lrc.v = v128_clmul(lrc[0], lrc[1]);

	a += x;
	b += y;

	mul128(a, b, &enh_hi, &enh_lo);
	enh_hi += offset;
	enh_hi ^= enh_lo;

	ret.hash[0] = finalize(horner_double_update(
	    /*acc=*/0, multipliers[0][0], multipliers[0][1], enh_lo, enh_hi));

	ret.hash[1] = finalize(horner_double_update(/*acc=*/0, multipliers[1][0],
	    multipliers[1][1], enh_lo ^ mixed_lrc.u64[0], enh_hi ^ mixed_lrc.u64[1]));

	return ret;
}

TEST_DEF uint64_t
umash_long(const uint64_t multipliers[static 2], const uint64_t *oh, uint64_t seed,
    const void *data, size_t n_bytes)
{
	uint64_t acc = 0;

	/*
	 * umash_long.inc defines this variable when the long input
	 * routine is enabled.
	 */
#ifdef UMASH_MULTIPLE_BLOCKS_THRESHOLD
	if (UNLIKELY(n_bytes >= UMASH_MULTIPLE_BLOCKS_THRESHOLD)) {
		size_t n_block = n_bytes / BLOCK_SIZE;
		const void *remaining;

		n_bytes %= BLOCK_SIZE;
		remaining = (const char *)data + (n_block * BLOCK_SIZE);
		acc = umash_multiple_blocks(acc, multipliers, oh, seed, data, n_block);

		data = remaining;
		if (n_bytes == 0)
			goto finalize;

		goto last_block;
	}
#else
	/* Avoid warnings about the unused labels. */
	if (0) {
		goto last_block;
		goto finalize;
	}
#endif

	while (n_bytes > BLOCK_SIZE) {
		struct umash_oh compressed;

		compressed = oh_varblock(oh, seed, data, BLOCK_SIZE);
		data = (const char *)data + BLOCK_SIZE;
		n_bytes -= BLOCK_SIZE;

		acc = horner_double_update(acc, multipliers[0], multipliers[1],
		    compressed.bits[0], compressed.bits[1]);
	}

last_block:
	/* Do the final block. */
	{
		struct umash_oh compressed;

		seed ^= (uint8_t)n_bytes;
		compressed = oh_varblock(oh, seed, data, n_bytes);
		acc = horner_double_update(acc, multipliers[0], multipliers[1],
		    compressed.bits[0], compressed.bits[1]);
	}

finalize:
	return finalize(acc);
}

TEST_DEF struct umash_fp
umash_fp_long(const uint64_t multipliers[static 2][2], const uint64_t *oh, uint64_t seed,
    const void *data, size_t n_bytes)
{
	struct umash_oh compressed[2];
	struct umash_fp ret;
	uint64_t acc[2] = { 0, 0 };

#ifdef UMASH_MULTIPLE_BLOCKS_THRESHOLD
	if (UNLIKELY(n_bytes >= UMASH_MULTIPLE_BLOCKS_THRESHOLD)) {
		struct umash_fp poly = { .hash = { 0, 0 } };
		size_t n_block = n_bytes / BLOCK_SIZE;
		const void *remaining;

		n_bytes %= BLOCK_SIZE;
		remaining = (const char *)data + (n_block * BLOCK_SIZE);
		poly = umash_fprint_multiple_blocks(
		    poly, multipliers, oh, seed, data, n_block);

		acc[0] = poly.hash[0];
		acc[1] = poly.hash[1];

		data = remaining;
		if (n_bytes == 0)
			goto finalize;

		goto last_block;
	}
#else
	/* Avoid warnings about the unused labels. */
	if (0) {
		goto last_block;
		goto finalize;
	}
#endif

	while (n_bytes > BLOCK_SIZE) {
		oh_varblock_fprint(compressed, oh, seed, data, BLOCK_SIZE);

#define UPDATE(i)                                                                   \
	acc[i] = horner_double_update(acc[i], multipliers[i][0], multipliers[i][1], \
	    compressed[i].bits[0], compressed[i].bits[1])

		UPDATE(0);
		UPDATE(1);
#undef UPDATE

		data = (const char *)data + BLOCK_SIZE;
		n_bytes -= BLOCK_SIZE;
	}

last_block:
	oh_varblock_fprint(compressed, oh, seed ^ (uint8_t)n_bytes, data, n_bytes);

#define FINAL(i)                                                                      \
	do {                                                                          \
		acc[i] = horner_double_update(acc[i], multipliers[i][0],              \
		    multipliers[i][1], compressed[i].bits[0], compressed[i].bits[1]); \
	} while (0)

	FINAL(0);
	FINAL(1);
#undef FINAL

finalize:
	ret.hash[0] = finalize(acc[0]);
	ret.hash[1] = finalize(acc[1]);
	return ret;
}

static FN bool
value_is_repeated(const uint64_t *values, size_t n, uint64_t needle)
{

	for (size_t i = 0; i < n; i++) {
		if (values[i] == needle)
			return true;
	}

	return false;
}

FN bool
umash_params_prepare(struct umash_params *params)
{
	static const uint64_t modulo = (1UL << 61) - 1;
	/*
	 * The polynomial parameters have two redundant fields (for
	 * the pre-squared multipliers).  Use them as our source of
	 * extra entropy if needed.
	 */
	uint64_t buf[] = { params->poly[0][0], params->poly[1][0] };
	size_t buf_idx = 0;

#define GET_RANDOM(DST)                         \
	do {                                    \
		if (buf_idx >= ARRAY_SIZE(buf)) \
			return false;           \
                                                \
		(DST) = buf[buf_idx++];         \
	} while (0)

	/* Check the polynomial multipliers: we don't want 0s. */
	for (size_t i = 0; i < ARRAY_SIZE(params->poly); i++) {
		uint64_t f = params->poly[i][1];

		while (true) {
			/*
			 * Zero out bits and use rejection sampling to
			 * guarantee uniformity.
			 */
			f &= (1UL << 61) - 1;
			if (f != 0 && f < modulo)
				break;

			GET_RANDOM(f);
		}

		/* We can work in 2**64 - 8 and reduce after the fact. */
		params->poly[i][0] = mul_mod_fast(f, f) % modulo;
		params->poly[i][1] = f;
	}

	/* Avoid repeated OH noise values. */
	for (size_t i = 0; i < ARRAY_SIZE(params->oh); i++) {
		while (value_is_repeated(params->oh, i, params->oh[i]))
			GET_RANDOM(params->oh[i]);
	}

	return true;
}

FN void
umash_params_derive(struct umash_params *params, uint64_t bits, const void *key)
{
	uint8_t umash_key[32] = "Do not use UMASH VS adversaries.";

	if (key != NULL)
		memcpy(umash_key, key, sizeof(umash_key));

	while (true) {
		uint8_t nonce[8];

		for (size_t i = 0; i < 8; i++)
			nonce[i] = bits >> (8 * i);

		salsa20_stream(params, sizeof(*params), nonce, umash_key);
		if (umash_params_prepare(params))
			return;

		/*
		 * This should practically never fail, so really
		 * shouldn't happen multiple times.  If it does, an
		 * infinite loop is as good as anything else.
		 */
		bits++;
	}
}

/*
 * Updates the polynomial state at the end of a block.
 */
static FN void
sink_update_poly(struct umash_sink *sink)
{
	uint64_t oh0, oh1;

	oh0 = sink->oh_acc.bits[0];
	oh1 = sink->oh_acc.bits[1];
	sink->poly_state[0].acc = horner_double_update(sink->poly_state[0].acc,
	    sink->poly_state[0].mul[0], sink->poly_state[0].mul[1], oh0, oh1);

	sink->oh_acc = (struct umash_oh) { .bits = { 0 } };
	if (sink->hash_wanted == 0)
		return;

	oh0 = sink->oh_twisted.acc.bits[0];
	oh1 = sink->oh_twisted.acc.bits[1];
	sink->poly_state[1].acc = horner_double_update(sink->poly_state[1].acc,
	    sink->poly_state[1].mul[0], sink->poly_state[1].mul[1], oh0, oh1);

	sink->oh_twisted =
	    (struct umash_twisted_oh) { .lrc = { sink->oh[UMASH_OH_PARAM_COUNT],
					    sink->oh[UMASH_OH_PARAM_COUNT + 1] } };
	return;
}

/*
 * Updates the OH state with 16 bytes of data.  If `final` is true, we
 * are definitely consuming the last chunk in the input.
 */
static FN void
sink_consume_buf(
    struct umash_sink *sink, const char buf[static INCREMENTAL_GRANULARITY], bool final)
{
	const size_t buf_begin = sizeof(sink->buf) - INCREMENTAL_GRANULARITY;
	const size_t param = sink->oh_iter;
	const uint64_t k0 = sink->oh[param];
	const uint64_t k1 = sink->oh[param + 1];
	uint64_t x, y;

	/* Use GPR loads to avoid forwarding stalls.  */
	memcpy(&x, buf, sizeof(x));
	memcpy(&y, buf + sizeof(x), sizeof(y));

	/* All but the last 16-byte chunk of each block goes through PH. */
	if (sink->oh_iter < UMASH_OH_PARAM_COUNT - 2 && !final) {
		v128 acc, h, twisted_acc, prev;
		uint64_t m0, m1;

		m0 = x ^ k0;
		m1 = y ^ k1;

		memcpy(&acc, &sink->oh_acc, sizeof(acc));
		h = v128_clmul(m0, m1);
		acc ^= h;
		memcpy(&sink->oh_acc, &acc, sizeof(acc));

		if (sink->hash_wanted == 0)
			goto next;

		sink->oh_twisted.lrc[0] ^= m0;
		sink->oh_twisted.lrc[1] ^= m1;

		memcpy(&twisted_acc, &sink->oh_twisted.acc, sizeof(twisted_acc));
		memcpy(&prev, sink->oh_twisted.prev, sizeof(prev));

		twisted_acc ^= prev;
		twisted_acc = v128_shift(twisted_acc);
		memcpy(&sink->oh_twisted.acc, &twisted_acc, sizeof(twisted_acc));
		memcpy(&sink->oh_twisted.prev, &h, sizeof(h));
	} else {
		/* The last chunk is combined with the size tag with ENH. */
		uint64_t tag = sink->seed ^ (uint8_t)(sink->block_size + sink->bufsz);
		uint64_t enh_hi, enh_lo;

		mul128(x + k0, y + k1, &enh_hi, &enh_lo);
		enh_hi += tag;
		enh_hi ^= enh_lo;

		if (sink->hash_wanted != 0) {
			union {
				v128 vec;
				uint64_t h[2];
			} lrc_hash;
			uint64_t lrc0, lrc1;
			uint64_t oh0, oh1;
			uint64_t oh_twisted0, oh_twisted1;

			lrc0 = sink->oh_twisted.lrc[0] ^ x ^ k0;
			lrc1 = sink->oh_twisted.lrc[1] ^ y ^ k1;
			lrc_hash.vec = v128_clmul(lrc0, lrc1);

			oh_twisted0 = sink->oh_twisted.acc.bits[0];
			oh_twisted1 = sink->oh_twisted.acc.bits[1];

			oh0 = sink->oh_acc.bits[0];
			oh1 = sink->oh_acc.bits[1];
			oh0 ^= oh_twisted0;
			oh0 <<= 1;
			oh1 ^= oh_twisted1;
			oh1 <<= 1;

			oh0 ^= lrc_hash.h[0];
			oh1 ^= lrc_hash.h[1];
			sink->oh_twisted.acc.bits[0] = oh0 ^ enh_lo;
			sink->oh_twisted.acc.bits[1] = oh1 ^ enh_hi;
		}

		sink->oh_acc.bits[0] ^= enh_lo;
		sink->oh_acc.bits[1] ^= enh_hi;
	}

next:
	memmove(&sink->buf, buf, buf_begin);
	sink->block_size += sink->bufsz;
	sink->bufsz = 0;
	sink->oh_iter += 2;

	if (sink->oh_iter == UMASH_OH_PARAM_COUNT || final) {
		sink_update_poly(sink);
		sink->block_size = 0;
		sink->oh_iter = 0;
	}

	return;
}

/**
 * Hashes full 256-byte blocks into a sink that just dumped its OH
 * state in the toplevel polynomial hash and reset the block state.
 */
static FN size_t
block_sink_update(struct umash_sink *sink, const void *data, size_t n_bytes)
{
	size_t consumed = 0;

	assert(n_bytes >= BLOCK_SIZE);
	assert(sink->bufsz == 0);
	assert(sink->block_size == 0);
	assert(sink->oh_iter == 0);

#ifdef UMASH_MULTIPLE_BLOCKS_THRESHOLD
	if (UNLIKELY(n_bytes > UMASH_MULTIPLE_BLOCKS_THRESHOLD)) {
		/*
		 * We leave the last block (partial or not) for the
		 * caller: incremental hashing must save some state
		 * at the end of a block.
		 */
		size_t n_blocks = (n_bytes - 1) / BLOCK_SIZE;

		if (sink->hash_wanted != 0) {
			const uint64_t multipliers[2][2] = {
				[0][0] = sink->poly_state[0].mul[0],
				[0][1] = sink->poly_state[0].mul[1],
				[1][0] = sink->poly_state[1].mul[0],
				[1][1] = sink->poly_state[1].mul[1],
			};
			struct umash_fp poly = {
				.hash[0] = sink->poly_state[0].acc,
				.hash[1] = sink->poly_state[1].acc,
			};

			poly = umash_fprint_multiple_blocks(
			    poly, multipliers, sink->oh, sink->seed, data, n_blocks);

			sink->poly_state[0].acc = poly.hash[0];
			sink->poly_state[1].acc = poly.hash[1];
		} else {
			sink->poly_state[0].acc = umash_multiple_blocks(
			    sink->poly_state[0].acc, sink->poly_state[0].mul, sink->oh,
			    sink->seed, data, n_blocks);
		}

		return n_blocks * BLOCK_SIZE;
	}
#endif

	while (n_bytes > BLOCK_SIZE) {
		/*
		 * Is this worth unswitching?  Not obviously, given
		 * the amount of work in one OH block.
		 */
		if (sink->hash_wanted != 0) {
			struct umash_oh hashes[2];

			oh_varblock_fprint(
			    hashes, sink->oh, sink->seed, data, BLOCK_SIZE);
			sink->oh_acc = hashes[0];
			sink->oh_twisted.acc = hashes[1];
		} else {
			sink->oh_acc =
			    oh_varblock(sink->oh, sink->seed, data, BLOCK_SIZE);
		}

		sink_update_poly(sink);
		consumed += BLOCK_SIZE;
		data = (const char *)data + BLOCK_SIZE;
		n_bytes -= BLOCK_SIZE;
	}

	return consumed;
}

FN void
umash_sink_update(struct umash_sink *sink, const void *data, size_t n_bytes)
{
	const size_t buf_begin = sizeof(sink->buf) - INCREMENTAL_GRANULARITY;
	size_t remaining = INCREMENTAL_GRANULARITY - sink->bufsz;

	DTRACE_PROBE4(libumash, umash_sink_update, sink, remaining, data, n_bytes);

	if (n_bytes < remaining) {
		memcpy(&sink->buf[buf_begin + sink->bufsz], data, n_bytes);
		sink->bufsz += n_bytes;
		return;
	}

	memcpy(&sink->buf[buf_begin + sink->bufsz], data, remaining);
	data = (const char *)data + remaining;
	n_bytes -= remaining;
	/* We know we're hashing at least 16 bytes. */
	sink->large_umash = true;
	sink->bufsz = INCREMENTAL_GRANULARITY;

	/*
	 * We can't compress a 16-byte buffer until we know whether
	 * data is coming: the last 16-byte chunk goes to `NH` instead
	 * of `PH`.  We could try to detect when the buffer is the
	 * last chunk in a block and immediately go to `NH`, but it
	 * seems more robust to always let the stores settle before we
	 * read them, just in case the combination is bad for forwarding.
	 */
	if (n_bytes == 0)
		return;

	sink_consume_buf(sink, sink->buf + buf_begin, /*final=*/false);

	while (n_bytes > INCREMENTAL_GRANULARITY) {
		size_t consumed;

		if (sink->oh_iter == 0 && n_bytes > BLOCK_SIZE) {
			consumed = block_sink_update(sink, data, n_bytes);
			assert(consumed >= BLOCK_SIZE);

			/*
			 * Save the tail of the data we just consumed
			 * in `sink->buf[0 ... buf_begin - 1]`: the
			 * final digest may need those bytes for its
			 * redundant read.
			 */
			memcpy(sink->buf,
			    (const char *)data + (consumed - INCREMENTAL_GRANULARITY),
			    buf_begin);
		} else {
			consumed = INCREMENTAL_GRANULARITY;
			sink->bufsz = INCREMENTAL_GRANULARITY;
			sink_consume_buf(sink, data, /*final=*/false);
		}

		n_bytes -= consumed;
		data = (const char *)data + consumed;
	}

	memcpy(&sink->buf[buf_begin], data, n_bytes);
	sink->bufsz = n_bytes;
	return;
}

FN uint64_t
umash_full(const struct umash_params *params, uint64_t seed, int which, const void *data,
    size_t n_bytes)
{

	DTRACE_PROBE4(libumash, umash_full, params, which, data, n_bytes);

	/*
	 * We don't (yet) implement code that only evaluates the
	 * second hash.  We don't currently use that logic, and it's
	 * about to become a bit more complex, so let's just go for a
	 * full fingerprint and take what we need.
	 *
	 * umash_full is also rarely used that way: usually we want
	 * either the main hash, or the full fingerprint.
	 */
	if (UNLIKELY(which != 0)) {
		struct umash_fp fp;

		fp = umash_fprint(params, seed, data, n_bytes);
		return fp.hash[1];
	}

	/*
	 * It's not that short inputs are necessarily more likely, but
	 * we want to make sure they fall through correctly to
	 * minimise latency.
	 */
	if (LIKELY(n_bytes <= sizeof(v128))) {
		if (LIKELY(n_bytes <= sizeof(uint64_t)))
			return umash_short(params->oh, seed, data, n_bytes);

		return umash_medium(params->poly[0], params->oh, seed, data, n_bytes);
	}

	return umash_long(params->poly[0], params->oh, seed, data, n_bytes);
}

FN struct umash_fp
umash_fprint(
    const struct umash_params *params, uint64_t seed, const void *data, size_t n_bytes)
{

	DTRACE_PROBE3(libumash, umash_fprint, params, data, n_bytes);
	if (LIKELY(n_bytes <= sizeof(v128))) {
		if (LIKELY(n_bytes <= sizeof(uint64_t)))
			return umash_fp_short(params->oh, seed, data, n_bytes);

		return umash_fp_medium(params->poly, params->oh, seed, data, n_bytes);
	}

	return umash_fp_long(params->poly, params->oh, seed, data, n_bytes);
}

FN void
umash_init(struct umash_state *state, const struct umash_params *params, uint64_t seed,
    int which)
{

	which = (which == 0) ? 0 : 1;
	DTRACE_PROBE3(libumash, umash_init, state, params, which);

	state->sink = (struct umash_sink) {
		.poly_state[0] = {
			.mul = {
				params->poly[0][0],
				params->poly[0][1],
			},
		},
		.poly_state[1]= {
			.mul = {
				params->poly[1][0],
				params->poly[1][1],
			},
		},
		.oh = params->oh,
		.hash_wanted = which,
		.oh_twisted.lrc = { params->oh[UMASH_OH_PARAM_COUNT],
			params->oh[UMASH_OH_PARAM_COUNT + 1] },
		.seed = seed,
	};

	return;
}

FN void
umash_fp_init(
    struct umash_fp_state *state, const struct umash_params *params, uint64_t seed)
{

	DTRACE_PROBE2(libumash, umash_fp_init, state, params);

	state->sink = (struct umash_sink) {
		.poly_state[0] = {
			.mul = {
				params->poly[0][0],
				params->poly[0][1],
			},
		},
		.poly_state[1]= {
			.mul = {
				params->poly[1][0],
				params->poly[1][1],
			},
		},
		.oh = params->oh,
		.hash_wanted = 2,
		.oh_twisted.lrc = { params->oh[UMASH_OH_PARAM_COUNT],
			params->oh[UMASH_OH_PARAM_COUNT + 1] },
		.seed = seed,
	};

	return;
}

/**
 * Pumps any last block out of the incremental state.
 */
static FN void
digest_flush(struct umash_sink *sink)
{

	if (sink->bufsz > 0)
		sink_consume_buf(sink, &sink->buf[sink->bufsz], /*final=*/true);
	return;
}

/**
 * Finalizes a digest out of `sink`'s current state.
 *
 * The `sink` must be `digest_flush`ed if it is a `large_umash`.
 *
 * @param index 0 to return the first (only, if hashing) value, 1 for the
 *   second independent value for fingerprinting.
 */
static FN uint64_t
digest(const struct umash_sink *sink, int index)
{
	const size_t buf_begin = sizeof(sink->buf) - INCREMENTAL_GRANULARITY;
	const size_t shift = (index == 0) ? 0 : OH_SHORT_HASH_SHIFT;

	if (sink->large_umash)
		return finalize(sink->poly_state[index].acc);

	if (sink->bufsz <= sizeof(uint64_t))
		return umash_short(
		    &sink->oh[shift], sink->seed, &sink->buf[buf_begin], sink->bufsz);

	return umash_medium(sink->poly_state[index].mul, sink->oh, sink->seed,
	    &sink->buf[buf_begin], sink->bufsz);
}

static FN struct umash_fp
fp_digest_sink(const struct umash_sink *sink)
{
	struct umash_sink copy;
	struct umash_fp ret;
	const size_t buf_begin = sizeof(sink->buf) - INCREMENTAL_GRANULARITY;

	if (sink->large_umash) {
		copy = *sink;
		digest_flush(&copy);
		sink = &copy;
	} else if (sink->bufsz <= sizeof(uint64_t)) {
		return umash_fp_short(
		    sink->oh, sink->seed, &sink->buf[buf_begin], sink->bufsz);
	} else {
		const struct umash_params *params;

		/*
		 * Back out the params struct from our pointer to its
		 * `oh` member.
		 */
		params = (const void *)((const char *)sink->oh -
		    __builtin_offsetof(struct umash_params, oh));
		return umash_fp_medium(params->poly, sink->oh, sink->seed,
		    &sink->buf[buf_begin], sink->bufsz);
	}

	for (size_t i = 0; i < ARRAY_SIZE(ret.hash); i++)
		ret.hash[i] = digest(sink, i);

	return ret;
}

FN uint64_t
umash_digest(const struct umash_state *state)
{
	struct umash_sink copy;
	const struct umash_sink *sink = &state->sink;

	DTRACE_PROBE1(libumash, umash_digest, state);

	if (sink->hash_wanted == 1) {
		struct umash_fp fp;

		fp = fp_digest_sink(sink);
		return fp.hash[1];
	}

	if (sink->large_umash) {
		copy = *sink;
		digest_flush(&copy);
		sink = &copy;
	}

	return digest(sink, 0);
}

FN struct umash_fp
umash_fp_digest(const struct umash_fp_state *state)
{

	DTRACE_PROBE1(libumash, umash_fp_digest, state);
	return fp_digest_sink(&state->sink);
}
