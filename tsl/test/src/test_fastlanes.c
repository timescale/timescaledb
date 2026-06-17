/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "compression/algorithms/fastlanes/fastlanes.h"

#include "port.h"
#include "test_utils.h"
#include <c.h>
#include <port/pg_bitutils.h>
#include <time.h>
#include <utils/palloc.h>

TS_FUNCTION_INFO_V1(ts_test_fastlanes);

#define pfree_aligned(p) pfree(p)

/*
 * The below tests have a few goals to achieve, to act as a full
 * correctness suite is _not_ one of them. The main goals are:
 *
 *  - have a decent coverage of the fl/ library such that the code
 *    coverage tools in the CI will pass. this is because the fl/
 *    library will be rolled out before the actual compression code is
 *
 *  - serve as a tutorial for how to use the fl/ library, and to
 *    demonstrate the caller contract and the expected behavior of the
 *    library. there is further information about this in the fl/README.md
 *
 *  - to highlight certain properties of the library with examples
 */

/*
 * This test demonstrates the packing and unpacking of 7 elements with
 * 3 bits per element. This will use the FL8 tier and it shows that we
 * can pack these numbers in 3 bytes.
 */
static void
test_fl8_pack_7x3b()
{
	const uint32 n = 7; /* seven elements */
	uint8 values_in[8] = {
		1, 2, 3, 4, 5, 6, 7, 8,
	};
	size_t input_size = fl_input_bytes(n, FL_ELEM_W8);
	TestAssertInt64Eq(input_size, 8LL);
	TestAssertInt64Eq((int64) sizeof(values_in), input_size);

	size_t required_size = fl_required_bytes(n, 3, FL_ELEM_W8);
	TestAssertInt64Eq(required_size, 3LL);
	size_t result_size = fl_result_bytes(n, 3, FL_ELEM_W8);
	TestAssertInt64Eq(result_size, 3LL);

	uint8 values_out[8] = { 255, 255, 255, 255, 255, 255, 255, 255 };
	size_t packed_bytes = fl_pack(values_in, values_out, n, 3, FL_ELEM_W8);
	TestAssertInt64Eq(packed_bytes, result_size);

	/* the tail padding must not change */
	for (int i = result_size; i < 8; ++i)
	{
		TestAssertInt64Eq(values_out[i], 255LL);
	}

	/*
	 * copy the compressed payload only, so we make sure we don't fool
	 * ourselves by allowing the decompressor to see parts that it shouldn't
	 * see.
	 */
	uint8 compressed_payload[8] = { 0 };
	memcpy(compressed_payload, values_out, packed_bytes);

	/* try the decoding too, with the right decoded output size */
	uint8 decoded[8] = { 255, 255, 255, 255, 255, 255, 255, 255 };
	fl_unpack(compressed_payload, decoded, n, 3, FL_ELEM_W8);
	for (uint32 i = 0; i < n; ++i)
	{
		TestAssertInt64Eq(decoded[i], values_in[i]);
	}
}

/*
 * This test demonstrates the FFOR packing, which subtracts a base value
 * from the input values before packing. This allows us to pack 7 elements
 * that originally took 8 bits each into 3 bytes by using a base value of 210.
 */
static void
test_fl8_pack_7x3b_ffor()
{
	const uint32 n = 7; /* seven elements */
	uint8 values_in[8] = {
		211, 212, 213, 214, 215, 216, 217, 255,
	};
	uint8 values_out[8] = { 255, 255, 255, 255, 255, 255, 255, 255 };
	uint8 base = 210;
	size_t packed_bytes = fl_pack_ffor(values_in, values_out, n, 3, FL_ELEM_W8, base);
	size_t result_size = fl_result_bytes(n, 3, FL_ELEM_W8);
	TestAssertInt64Eq(packed_bytes, result_size);
	TestAssertInt64Eq(packed_bytes, 3ULL);

	/* the tail padding must not change */
	for (int i = result_size; i < 8; ++i)
	{
		TestAssertInt64Eq(values_out[i], 255LL);
	}

	/*
	 * copy the compressed payload only, so we make sure we don't fool
	 * ourselves by allowing the decompressor to see parts that it shouldn't
	 * see.
	 */
	uint8 compressed_payload[8] = { 0 };
	memcpy(compressed_payload, values_out, packed_bytes);

	/* try the decoding too, with the right decoded output size */
	uint8 decoded[8] = { 255, 255, 255, 255, 255, 255, 255, 255 };
	fl_unpack_ffor(compressed_payload, decoded, n, 3, FL_ELEM_W8, base);
	for (uint32 i = 0; i < n; ++i)
	{
		TestAssertInt64Eq(decoded[i], values_in[i]);
	}
}

/*
 * This test exercises the FL16 tier (N > 8 elements forces FL16) and
 * demonstrates two things:
 *
 *  (1) Alignment contract via the proper allocator pattern: ask the
 *      library for the required alignment and size, then use
 *      palloc_aligned() to obtain a buffer that satisfies both. For
 *      FL16 the alignment is 2 bytes (= the tier's WORD_BYTES).
 *
 *  (2) An eye-catching packing ratio: 16 boolean-like values stored
 *      naively as uint16 take 32 bytes; packed at W=1 they fit in
 *      2 bytes -- a 16x reduction.
 */
static void
test_fl16_pack_16x1b()
{
	const uint32 n = 16;
	const uint8 W = 1;
	const fl_elem_width_t T = FL_ELEM_W16;

	/* Step 1: ask the library what we need. */
	size_t alignment = fl_alignment(n, T);
	size_t input_size = fl_input_bytes(n, T);
	size_t required_size = fl_required_bytes(n, W, T);
	size_t result_size = fl_result_bytes(n, W, T);

	/* These verifications are to demonstrating the contract */
	TestAssertInt64Eq(alignment, 2LL);
	TestAssertInt64Eq(input_size, 32LL);
	TestAssertInt64Eq(required_size, 2LL);
	TestAssertInt64Eq(result_size, 2LL);

	/* The headline: 32 bytes in -> 2 bytes out : 16x */
	TestAssertInt64Eq((int64) (input_size / result_size), 16LL);

	/*
	 * Step 2: allocate buffers satisfying both alignment and size.
	 * palloc_aligned(size, alignment, 0) requires size to be a multiple
	 * of alignment -- both fl_input_bytes and fl_required_bytes are
	 * multiples of fl_alignment by construction.
	 */
	uint16 *values_in = palloc_aligned(input_size, alignment, 0);
	uint8 *packed = palloc_aligned(required_size, alignment, 0);
	uint16 *decoded = palloc_aligned(input_size, alignment, 0);
	TestAssertTrue(values_in != NULL);
	TestAssertTrue(packed != NULL);
	TestAssertTrue(decoded != NULL);

	/* Step 3: verify the addresses actually meet the contract. */
	TestAssertInt64Eq((int64) ((uintptr_t) values_in % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) packed % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) decoded % alignment), 0LL);

	/* Step 4: fill the input. */
	const uint16 pattern[16] = {
		1, 0, 1, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 0, 1, 1,
	};
	memcpy(values_in, pattern, input_size);

	/* Step 5: pack and verify the returned byte count. */
	size_t packed_bytes = fl_pack(values_in, packed, n, W, T);
	TestAssertInt64Eq(packed_bytes, result_size);

	/* Step 6: unpack and verify roundtrip. */
	fl_unpack(packed, decoded, n, W, T);
	for (int i = 0; i < (int) n; ++i)
	{
		TestAssertInt64Eq(decoded[i], pattern[i]);
	}

	pfree_aligned(values_in);
	pfree_aligned(packed);
	pfree_aligned(decoded);
}

/*
 * This test exercises the FL32 tier (N=23 routes to FL32) and
 * demonstrates three things:
 *
 *  (1) Truncation in action: at N=23 (partial N within FL32's 32-
 *      element vector) the meaningful packed result is 16 bytes while
 *      the kernel writes the full 20-byte alloc. The caller stores
 *      only result_size bytes; before decode the caller must
 *      pre-zero the [truncated_bytes..alloc_bytes) tail of a fresh
 *      scratch buffer.
 *
 *  (2) An inconvenient bit width: W=5 doesn't align to any byte
 *      boundary, but the bit-packing handles it. 23 uint32 values
 *      (92 bytes of meaningful data) compress to 16 bytes -- 5.75x.
 *
 *  (3) The full allocator-based alignment contract for FL32, which
 *      requires 4-byte alignment.
 */
static void
test_fl32_pack_23x5b()
{
	const uint32 n = 23;
	const uint8 W = 5;
	const fl_elem_width_t T = FL_ELEM_W32;

	/* Step 1: ask the library what we need. */
	size_t alignment = fl_alignment(n, T);
	size_t input_size = fl_input_bytes(n, T);
	size_t required_size = fl_required_bytes(n, W, T);
	size_t result_size = fl_result_bytes(n, W, T);

	TestAssertInt64Eq(alignment, 4LL);
	TestAssertInt64Eq(input_size, 128LL);	/* 32 elements x 4 bytes */
	TestAssertInt64Eq(required_size, 20LL); /* 5 words x 4 bytes     */
	TestAssertInt64Eq(result_size, 16LL);	/* 4 words x 4 bytes -- saves 4 vs alloc */

	/* Headline: 23 uint32 (92 bytes meaningful) -> 16 bytes packed = 5.75x. */
	TestAssertInt64Eq((int64) (n * sizeof(uint32)), 92LL);

	/* Step 2: allocate buffers satisfying alignment and size. */
	uint32 *values_in = palloc_aligned(input_size, alignment, 0);
	uint8 *packed = palloc_aligned(required_size, alignment, 0);
	uint32 *decoded = palloc_aligned(input_size, alignment, 0);
	TestAssertTrue(values_in != NULL);
	TestAssertTrue(packed != NULL);
	TestAssertTrue(decoded != NULL);

	/* Step 3: verify the addresses meet the contract. */
	TestAssertInt64Eq((int64) ((uintptr_t) values_in % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) packed % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) decoded % alignment), 0LL);

	/*
	 * Step 4: fill 23 values that fit in 5 bits (0..31). Positions
	 * 23..31 are left with whatever values aligned_alloc returned;
	 * the kernel reads them per the contract but the decoder, given
	 * the correct N, ignores their encoded bits.
	 */
	for (int i = 0; i < (int) n; ++i)
	{
		values_in[i] = (uint32) (i % 32); /* 0, 1, 2, ..., 22 */
	}

	/* Step 5: pack. fl_pack returns result_size, not required_size. */
	size_t packed_bytes = fl_pack(values_in, packed, n, W, T);
	TestAssertInt64Eq(packed_bytes, result_size);

	/*
	 * Step 6: simulate "store the meaningful prefix; reconstruct a
	 * decode-ready buffer". A fresh scratch buffer receives the
	 * truncated bytes followed by an explicit zero-pad of
	 * [result_size..required_size). The kernel reads required_size
	 * bytes on decode; the zero-pad satisfies the contract for the
	 * region the caller never stored.
	 */
	uint8 *decode_in = palloc_aligned(required_size, alignment, 0);
	TestAssertTrue(decode_in != NULL);
	memcpy(decode_in, packed, result_size);
	memset(decode_in + result_size, 0, required_size - result_size);

	/* Step 7: unpack and verify roundtrip for the first N elements. */
	fl_unpack(decode_in, decoded, n, W, T);
	for (int i = 0; i < (int) n; ++i)
	{
		TestAssertInt64Eq(decoded[i], values_in[i]);
	}

	pfree_aligned(values_in);
	pfree_aligned(packed);
	pfree_aligned(decoded);
	pfree_aligned(decode_in);
}

/*
 * This test demonstrates the W=0 constant-block special case on the
 * FL64 tier. When all values are equal, the encoded form is empty
 * (0 packed bytes). The decoder reconstructs them from (N, base)
 * alone -- the maximum-compression endpoint of FastLanes.
 *
 *  - Plain pack returns 0 bytes; unpack fills the first N positions
 *    with 0.
 *  - FFOR pack returns 0 bytes; unpack fills the first N positions
 *    with `base`.
 *
 * Because no packed bytes are produced, the `packed` argument can
 * be NULL on both pack and unpack -- the kernel never dereferences
 * it on the W=0 fast path.
 */
static void
test_fl64_constant_block()
{
	const uint32 n = 50; /* routes to FL64 (33..64) */
	const uint8 W = 0;	 /* the constant-block special case */
	const fl_elem_width_t T = FL_ELEM_W32;
	const uint32 base_value = 0xCAFEBABE;

	/* Step 1: ask the library what we need. For W=0, required_size is 0. */
	size_t alignment = fl_alignment(n, T);
	size_t input_size = fl_input_bytes(n, T);
	size_t required_size = fl_required_bytes(n, W, T);

	TestAssertInt64Eq(alignment, 8LL);
	TestAssertInt64Eq(input_size, 256LL);  /* 64 elements x 4 bytes */
	TestAssertInt64Eq(required_size, 0LL); /* the headline: zero packed bytes */

	/* No packed buffer needed; we still allocate values_in and decoded
	 * because a real caller would have data prepared. */
	uint32 *values_in = palloc_aligned(input_size, alignment, 0);
	uint32 *decoded = palloc_aligned(input_size, alignment, 0);
	TestAssertTrue(values_in != NULL);
	TestAssertTrue(decoded != NULL);

	for (int i = 0; i < (int) n; ++i)
	{
		values_in[i] = base_value;
	}

	/* Plain pack returns 0; packed buffer can be NULL. */
	size_t packed_bytes = fl_pack(values_in, NULL, n, W, T);
	TestAssertInt64Eq(packed_bytes, 0LL);

	/* Plain unpack fills the first N positions with 0. */
	for (int i = 0; i < (int) n; ++i)
	{
		decoded[i] = 0xDEADBEEF; /* pre-fill to verify overwrite */
	}
	fl_unpack(NULL, decoded, n, W, T);
	for (int i = 0; i < (int) n; ++i)
	{
		TestAssertInt64Eq(decoded[i], 0LL);
	}

	/* FFOR pack: same -- returns 0. */
	size_t ffor_bytes = fl_pack_ffor(values_in, NULL, n, W, T, (uint64) base_value);
	TestAssertInt64Eq(ffor_bytes, 0LL);

	/* FFOR unpack fills the first N positions with `base`. */
	for (int i = 0; i < (int) n; ++i)
	{
		decoded[i] = 0xDEADBEEF;
	}
	fl_unpack_ffor(NULL, decoded, n, W, T, (uint64) base_value);
	for (int i = 0; i < (int) n; ++i)
	{
		TestAssertInt64Eq(decoded[i], base_value);
	}

	pfree_aligned(values_in);
	pfree_aligned(decoded);
}

/*
 * This test demonstrates FL64 with the interleaved multi-lane layout
 * (T=8 gives S = 64/8 = 8 lanes) and FFOR base offset. 64 byte-sized
 * values that all fall in [100, 107] compress to 24 bytes -- 2.67x.
 * The base captures the shared 100 prefix; the 3-bit residuals 0..7
 * pack tightly across the 8 parallel lanes.
 *
 * On the layout side: with T=8 in FL64, adjacent input elements land
 * in different lanes (round-robin: element i is in lane i % 8). The
 * kernel processes all 8 lanes per FL64 word, with the FFOR fusion
 * subtracting the base on pack and re-adding on unpack -- a single
 * pass across the lanes. This is the FastLanes property that makes
 * SIMD auto-vectorization free at higher tiers.
 */
static void
test_fl64_ffor_8lanes()
{
	const uint32 n = 64; /* full FL64 vector */
	const uint8 W = 3;
	const fl_elem_width_t T = FL_ELEM_W8;
	const uint8 base = 100;

	size_t alignment = fl_alignment(n, T);
	size_t input_size = fl_input_bytes(n, T);
	size_t required_size = fl_required_bytes(n, W, T);
	size_t result_size = fl_result_bytes(n, W, T);

	TestAssertInt64Eq(alignment, 8LL);
	TestAssertInt64Eq(input_size, 64LL);	/* 64 elements x 1 byte */
	TestAssertInt64Eq(required_size, 24LL); /* 3 words x 8 bytes    */
	TestAssertInt64Eq(result_size, 24LL);	/* N == VEC_SIZE: no truncation savings */

	/* Headline: 64 bytes in -> 24 bytes packed = 2.67x compression. */

	uint8 *values_in = palloc_aligned(input_size, alignment, 0);
	uint8 *packed = palloc_aligned(required_size, alignment, 0);
	uint8 *decoded = palloc_aligned(input_size, alignment, 0);
	TestAssertTrue(values_in != NULL);
	TestAssertTrue(packed != NULL);
	TestAssertTrue(decoded != NULL);

	TestAssertInt64Eq((int64) ((uintptr_t) values_in % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) packed % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) decoded % alignment), 0LL);

	/* 64 values cycling 100, 101, ..., 107, 100, 101, ... */
	for (int i = 0; i < (int) n; ++i)
	{
		values_in[i] = (uint8) (base + (i % 8));
	}

	size_t packed_bytes = fl_pack_ffor(values_in, packed, n, W, T, base);
	TestAssertInt64Eq(packed_bytes, result_size);

	fl_unpack_ffor(packed, decoded, n, W, T, base);
	for (int i = 0; i < (int) n; ++i)
	{
		TestAssertInt64Eq(decoded[i], values_in[i]);
	}

	pfree_aligned(values_in);
	pfree_aligned(packed);
	pfree_aligned(decoded);
}

/*
 * This test exercises FL128 with the widest element width and an
 * inconvenient bit budget:
 *
 *  - T=64 (uint64 elements -- the library's widest)
 *  - W=43 (not a power of 2, not byte-aligned, not native; W=43
 *          forces T=64 since W can't exceed T)
 *  - N=100 (partial within FL128's 128-element vector)
 *
 * In FL128 T=64 gives S = 128/64 = 2 lanes -- a real but minimal
 * multi-lane layout. 16-byte alignment is required.
 *
 * 100 uint64 values (800 bytes of meaningful input) compress to
 * 544 bytes (1.47x). The full alloc is 688 bytes; partial-N
 * truncation saves 144 bytes -- visible at FL128's 16-byte word
 * granularity.
 */
static void
test_fl128_pack_100x43b()
{
	const uint32 n = 100;
	const uint8 W = 43;
	const fl_elem_width_t T = FL_ELEM_W64;

	/* Step 1: ask the library. */
	size_t alignment = fl_alignment(n, T);
	size_t input_size = fl_input_bytes(n, T);
	size_t required_size = fl_required_bytes(n, W, T);
	size_t result_size = fl_result_bytes(n, W, T);

	TestAssertInt64Eq(alignment, 16LL);
	TestAssertInt64Eq(input_size, 1024LL);	 /* 128 elements x 8 bytes */
	TestAssertInt64Eq(required_size, 688LL); /* 43 words x 16 bytes */
	TestAssertInt64Eq(result_size, 544LL);	 /* 34 words x 16 bytes -- saves 144 vs alloc */

	/* Headline: 100 uint64 (800 bytes meaningful) -> 544 bytes packed = 1.47x. */
	TestAssertInt64Eq((int64) (n * sizeof(uint64)), 800LL);

	/* Step 2: allocate buffers satisfying alignment and size. */
	uint64 *values_in = palloc_aligned(input_size, alignment, 0);
	uint8 *packed = palloc_aligned(required_size, alignment, 0);
	uint64 *decoded = palloc_aligned(input_size, alignment, 0);
	TestAssertTrue(values_in != NULL);
	TestAssertTrue(packed != NULL);
	TestAssertTrue(decoded != NULL);

	/* Step 3: verify the addresses meet the contract. */
	TestAssertInt64Eq((int64) ((uintptr_t) values_in % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) packed % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) decoded % alignment), 0LL);

	/*
	 * Step 4: fill 100 varied 64-bit values that fit in 43 bits.
	 * The multiplier 0x1000000000 = 2^36 puts the highest set bit
	 * around position 42 for the largest index, exercising most of
	 * the W=43 budget without overflowing.
	 */
	for (int i = 0; i < (int) n; ++i)
	{
		values_in[i] = (uint64) i * 0x1000000000ULL;
	}

	/* Step 5: pack. fl_pack returns result_size; the kernel
	 * still writes required_size bytes total (the 144-byte tail past
	 * result_size is scratch). */
	size_t packed_bytes = fl_pack(values_in, packed, n, W, T);
	TestAssertInt64Eq(packed_bytes, result_size);

	/*
	 * Step 6: zero the alloc-vs-truncated tail in place before
	 * decoding. Simpler variant of the FL32 test's "store the
	 * prefix elsewhere" pattern -- here we reuse `packed`.
	 */
	memset(packed + result_size, 0, required_size - result_size);

	/* Step 7: unpack and verify roundtrip. */
	fl_unpack(packed, decoded, n, W, T);
	for (int i = 0; i < (int) n; ++i)
	{
		TestAssertInt64Eq(decoded[i], values_in[i]);
	}

	pfree_aligned(values_in);
	pfree_aligned(packed);
	pfree_aligned(decoded);
}

/*
 * This test exercises FL256, the largest tier, with FFOR and
 * truncation:
 *
 *  - N=144 (partial within FL256's 256-element vector; N>128 forces
 *           routing into FL256 rather than FL128)
 *  - T=32 (uint32 elements)
 *  - W=13 (inconvenient: not a power of 2, not byte-aligned)
 *  - base=100000 (a large uint32 base that captures the shared
 *                 prefix; residuals fit in 13 bits)
 *
 * FL256 with T=32 gives S = 256/32 = 8 parallel lanes -- same lane
 * count as the FL64 multi-lane test, but in a 256-bit virtual word
 * (32-byte WORD_BYTES). 32-byte alignment is required.
 *
 * 144 uint32 values clustered in [100000, 100000+8191] (576 bytes
 * of meaningful input) compress to 256 bytes via FFOR's base
 * subtraction + 13-bit packing -- 2.25x. The full alloc is 416
 * bytes; partial-N truncation saves 160 bytes.
 */
static void
test_fl256_ffor_144x13b()
{
	const uint32 n = 144;
	const uint8 W = 13;
	const fl_elem_width_t T = FL_ELEM_W32;
	const uint32 base = 100000;

	/* Step 1: ask the library. */
	size_t alignment = fl_alignment(n, T);
	size_t input_size = fl_input_bytes(n, T);
	size_t required_size = fl_required_bytes(n, W, T);
	size_t result_size = fl_result_bytes(n, W, T);

	TestAssertInt64Eq(alignment, 32LL);
	TestAssertInt64Eq(input_size, 1024LL);	 /* 256 elements x 4 bytes */
	TestAssertInt64Eq(required_size, 416LL); /* 13 words x 32 bytes */
	TestAssertInt64Eq(result_size, 256LL);	 /* 8 words x 32 bytes -- saves 160 vs alloc */

	/* Headline: 144 uint32 (576 bytes meaningful) -> 256 bytes packed = 2.25x. */
	TestAssertInt64Eq((int64) (n * sizeof(uint32)), 576LL);

	/* Step 2: allocate. */
	uint32 *values_in = palloc_aligned(input_size, alignment, 0);
	uint8 *packed = palloc_aligned(required_size, alignment, 0);
	uint32 *decoded = palloc_aligned(input_size, alignment, 0);
	TestAssertTrue(values_in != NULL);
	TestAssertTrue(packed != NULL);
	TestAssertTrue(decoded != NULL);

	/* Step 3: verify alignment. */
	TestAssertInt64Eq((int64) ((uintptr_t) values_in % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) packed % alignment), 0LL);
	TestAssertInt64Eq((int64) ((uintptr_t) decoded % alignment), 0LL);

	/*
	 * Step 4: fill 144 values in [base, base + 8191]. Residuals
	 * must fit in W=13 bits (< 2^13 = 8192). The multiplier 53 is
	 * prime and coprime with 8192, so (i * 53) % 8192 cycles
	 * through distinct residuals over the full 13-bit range.
	 */
	for (int i = 0; i < (int) n; ++i)
	{
		values_in[i] = base + (uint32) ((i * 53) % 8192);
	}

	/* Step 5: FFOR pack. fl_pack_ffor returns result_size; the
	 * kernel writes required_size bytes (the 160-byte tail past
	 * result_size is scratch). */
	size_t packed_bytes = fl_pack_ffor(values_in, packed, n, W, T, base);
	TestAssertInt64Eq(packed_bytes, result_size);

	/* Step 6: zero the alloc-vs-truncated tail in place before
	 * decoding. */
	memset(packed + result_size, 0, required_size - result_size);

	/* Step 7: FFOR unpack and verify roundtrip. */
	fl_unpack_ffor(packed, decoded, n, W, T, base);
	for (int i = 0; i < (int) n; ++i)
	{
		TestAssertInt64Eq(decoded[i], values_in[i]);
	}

	pfree_aligned(values_in);
	pfree_aligned(packed);
	pfree_aligned(decoded);
}

#define CALC_MINMAX(min, max, values, n)                                                           \
	do                                                                                             \
	{                                                                                              \
		(min) = *(values);                                                                         \
		(max) = *(values);                                                                         \
		for (size_t i = 0; i < (n); ++i)                                                           \
		{                                                                                          \
			if ((min) > (values)[i])                                                               \
			{                                                                                      \
				(min) = (values)[i];                                                               \
			}                                                                                      \
			if ((max) < (values)[i])                                                               \
			{                                                                                      \
				(max) = (values)[i];                                                               \
			}                                                                                      \
		}                                                                                          \
	} while (0)

static uint8
calc_width(uint64 val)
{
	if (val == 0)
	{
		return 1;
	}
	return 1 + pg_leftmost_one_pos64(val);
}

#define TEST_PASTE3_(a, b, c) a##b##c
#define TEST_PASTE3(a, b, c) TEST_PASTE3_(a, b, c)
#define TEST_FN(TT) TEST_PASTE3(test_, TT, _pack)

#define TEST_FN_IMPL(IN_T, TT)                                                                     \
	static void TEST_FN(IN_T)(const IN_T *values, size_t n)                                        \
	{                                                                                              \
		size_t test_input_size = n * sizeof(*values);                                              \
		IN_T min, max;                                                                             \
		CALC_MINMAX(min, max, values, n);                                                          \
		uint8 W = calc_width(max);                                                                 \
                                                                                                   \
		const fl_elem_width_t T = TT;                                                              \
		size_t alignment = fl_alignment(n, T);                                                     \
		size_t input_size = fl_input_bytes(n, T);                                                  \
		size_t required_size = fl_required_bytes(n, W, T);                                         \
		/* add input_count() too to make codecov happy */                                          \
		size_t input_count = fl_input_count(n, T);                                                 \
		TestAssertInt64Eq(input_count * sizeof(IN_T), input_size);                                 \
                                                                                                   \
		uint8 *values_in = palloc_aligned(input_size, alignment, 0);                               \
		memcpy(values_in, values, test_input_size);                                                \
		uint8 *packed = palloc_aligned(required_size, alignment, 0);                               \
		size_t packed_size = fl_pack(values_in, packed, n, W, T);                                  \
                                                                                                   \
		/* copy the result into a different buffer up to packed_size */                            \
		uint8 *to_unpack = palloc_aligned(required_size, alignment, 0);                            \
		memset(to_unpack, 0, required_size);                                                       \
		memcpy(to_unpack, packed, packed_size);                                                    \
                                                                                                   \
		/* allocate a separate result buffer */                                                    \
		IN_T *unpacked = palloc_aligned(input_size, alignment, 0);                                 \
		fl_unpack(to_unpack, unpacked, n, W, T);                                                   \
                                                                                                   \
		int eq = memcmp(unpacked, values, test_input_size);                                        \
		if (eq != 0)                                                                               \
		{                                                                                          \
			/* print the values for debugging and then fail the test */                            \
			elog(WARNING,                                                                          \
				 "Unpack failed for the following [%zu] values (W=%u, min=" UINT64_FORMAT          \
				 ", max=" UINT64_FORMAT ")",                                                       \
				 n,                                                                                \
				 (unsigned) W,                                                                     \
				 (uint64) min,                                                                     \
				 (uint64) max);                                                                    \
			for (size_t i = 0; i < n; ++i)                                                         \
			{                                                                                      \
				elog(WARNING,                                                                      \
					 "%zu : in=" UINT64_FORMAT " %s out=" UINT64_FORMAT,                           \
					 i,                                                                            \
					 (uint64) values[i],                                                           \
					 (values[i] == unpacked[i] ? "==" : "!="),                                     \
					 (uint64) unpacked[i]);                                                        \
			}                                                                                      \
			TestAssertTrue(eq == 0);                                                               \
		}                                                                                          \
                                                                                                   \
		memset(packed, 0, required_size);                                                          \
		memset(unpacked, 0, required_size);                                                        \
		memset(to_unpack, 0, required_size);                                                       \
		memset(unpacked, 0, input_size);                                                           \
                                                                                                   \
		/* try the same with FFOR */                                                               \
		uint64 base = min;                                                                         \
		uint8 bW = calc_width(max - min);                                                          \
		packed_size = fl_pack_ffor(values_in, packed, n, bW, T, base);                             \
		memcpy(to_unpack, packed, packed_size);                                                    \
		fl_unpack_ffor(to_unpack, unpacked, n, bW, T, base);                                       \
                                                                                                   \
		eq = memcmp(unpacked, values, test_input_size);                                            \
		if (eq != 0)                                                                               \
		{                                                                                          \
			/* print the values for debugging and then fail the test */                            \
			elog(WARNING,                                                                          \
				 "FFOR Unpack failed for the following [%zu] values (bW=%u, min=" UINT64_FORMAT    \
				 ", max=" UINT64_FORMAT ", base=" UINT64_FORMAT ")",                               \
				 n,                                                                                \
				 (unsigned) bW,                                                                    \
				 (uint64) min,                                                                     \
				 (uint64) max,                                                                     \
				 base);                                                                            \
			for (size_t i = 0; i < n; ++i)                                                         \
			{                                                                                      \
				elog(WARNING,                                                                      \
					 "%zu : in=" UINT64_FORMAT " %s out=" UINT64_FORMAT,                           \
					 i,                                                                            \
					 (uint64) values[i],                                                           \
					 (values[i] == unpacked[i] ? "==" : "!="),                                     \
					 (uint64) unpacked[i]);                                                        \
			}                                                                                      \
			TestAssertTrue(eq == 0);                                                               \
		}                                                                                          \
	}

TEST_FN_IMPL(uint8, FL_ELEM_W8)
TEST_FN_IMPL(uint16, FL_ELEM_W16)
TEST_FN_IMPL(uint32, FL_ELEM_W32)
TEST_FN_IMPL(uint64, FL_ELEM_W64)

/*
 * Randomized tests for better code coverage.
 */
static void
test_randomized()
{
	uint64 values[256];
	if (!pg_strong_random(values, sizeof(values)))
	{
		elog(ERROR, "pg_strong_random failed");
	}
	/* take the first 200 random values and generate a test length */
	for (int i = 0; i < 200; ++i)
	{
		size_t len = 1 + (values[i] % 255);
		test_uint8_pack((const uint8 *) values, len);
		test_uint16_pack((const uint16 *) values, len);
		test_uint32_pack((const uint32 *) values, len);
		test_uint64_pack((const uint64 *) values, len);
	}
}

#define CHECK_VALUES(V, SZ, E)                                                                     \
	do                                                                                             \
	{                                                                                              \
		for (int i = 0; i < (SZ); ++i)                                                             \
		{                                                                                          \
			TestAssertInt64Eq((V)[i], (E));                                                        \
		}                                                                                          \
	} while (0)

/* this test doesn't make a lot of sense, I only add this to make codecove happy */
static void
test_w0_coverage()
{
	uint8 v8[2] = { 0 };
	uint16 v16[2] = { 0 };
	uint32 v32[2] = { 0 };
	uint64 v64[2] = { 0 };

	/* start with FFOR to see the values are filled with base as expected */
	fl_unpack_ffor(NULL, v8, 2, 0, FL_ELEM_W8, 9);
	CHECK_VALUES(v8, 2, 9);
	fl_unpack_ffor(NULL, v16, 2, 0, FL_ELEM_W16, 9999);
	CHECK_VALUES(v16, 2, 9999);
	fl_unpack_ffor(NULL, v32, 2, 0, FL_ELEM_W32, 99998888UL);
	CHECK_VALUES(v32, 2, 99998888UL);
	fl_unpack_ffor(NULL, v64, 2, 0, FL_ELEM_W64, 9999888877776666ULL);
	CHECK_VALUES(v64, 2, 9999888877776666ULL);

	/* reused the same buffer so I can test that the values are zeroed */
	fl_unpack(NULL, v8, 2, 0, FL_ELEM_W8);
	CHECK_VALUES(v8, 2, 0);
	fl_unpack(NULL, v16, 2, 0, FL_ELEM_W16);
	CHECK_VALUES(v16, 2, 0);
	fl_unpack(NULL, v32, 2, 0, FL_ELEM_W32);
	CHECK_VALUES(v32, 2, 0);
	fl_unpack(NULL, v64, 2, 0, FL_ELEM_W64);
	CHECK_VALUES(v64, 2, 0);
}

Datum
ts_test_fastlanes(PG_FUNCTION_ARGS)
{
	test_fl8_pack_7x3b();
	test_fl8_pack_7x3b_ffor();
	test_fl16_pack_16x1b();
	test_fl32_pack_23x5b();
	test_fl64_constant_block();
	test_fl64_ffor_8lanes();
	test_fl128_pack_100x43b();
	test_fl256_ffor_144x13b();
	test_randomized();
	test_w0_coverage();
	PG_RETURN_VOID();
}
