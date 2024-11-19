/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * We can use crc32 as a hash function, it has bad properties but takes only one
 * cycle, which is why it is sometimes used in the existing hash table
 * implementations.
 * When we don't have the crc32 instruction, use the SplitMix64 finalizer.
 */
static pg_attribute_always_inline uint64
hash64_splitmix(uint64 x)
{
	x ^= x >> 30;
	x *= 0xbf58476d1ce4e5b9U;
	x ^= x >> 27;
	x *= 0x94d049bb133111ebU;
	x ^= x >> 31;
	return x;
}

#ifdef USE_SSE42_CRC32C
#include <nmmintrin.h>
static pg_attribute_always_inline uint64
hash64_crc(uint64 x)
{
	return _mm_crc32_u64(~0ULL, x);
}

#define HASH64 hash64_crc
#else
#define HASH64 hash64_splitmix
#endif

static pg_attribute_always_inline uint32
hash32(uint32 x)
{
	x ^= x >> 16;
	x *= 0x7feb352d;
	x ^= x >> 15;
	x *= 0x846ca68b;
	x ^= x >> 16;
	return x;
}

static pg_attribute_always_inline uint16
hash16(uint16 x)
{
	x ^= x >> 8;
	x *= 0x88b5U;
	x ^= x >> 7;
	x *= 0xdb2dU;
	x ^= x >> 9;
	return x;
}
