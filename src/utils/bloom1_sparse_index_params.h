/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <common/hashfn.h>

#define BLOOM1_HASHES 4
#define BLOOM1_SEED_1 0x71d924af
#define BLOOM1_SEED_2 0xba48b314

static inline uint32_t
lowbias32(uint32_t x)
{
	x ^= x >> 15;
	x *= 0xd168aaad;
	x ^= x >> 15;
	x *= 0xaf723597;
	x ^= x >> 15;
	return x;
}

static inline uint64
bloom1_hash64(uint64 x)
{
	x ^= x >> 32;
	x *= 0xd6e8feb86659fd93U;
	x ^= x >> 32;
	x *= 0xd6e8feb86659fd93U;
	x ^= x >> 32;
	return x;
}

static inline uint32
bloom1_get_one_hash(uint64 value_hash, uint32 index)
{
	//	const uint32 h1 = hash_combine(value_hash, BLOOM1_SEED_1);
	//	const uint32 h2 = hash_combine(value_hash, BLOOM1_SEED_2);
	//	const uint32 h1 = hash_bytes_uint32_extended(value_hash, BLOOM1_SEED_1);
	//	const uint32 h2 = hash_bytes_uint32_extended(value_hash, BLOOM1_SEED_2);
	//	return value_hash + h1 * index + h2 * index * index;
	const uint32 low = value_hash & ~(uint32) 0;
	const uint32 high = (value_hash >> 32) & ~(uint32) 0;
	return low + index * high;
}

static inline int
bloom1_bytea_alloc_size(int num_bits)
{
	const int words = (num_bits + 63) / 64;
	const int header = TYPEALIGN(8, VARHDRSZ);
	return header + words * 8;
}

static inline uint64 *
bloom1_words(bytea *bloom)
{
	uint64 *ptr = (uint64 *) TYPEALIGN(sizeof(ptr), VARDATA(bloom));
	return ptr;
}

static inline int
bloom1_num_bits(const bytea *bloom)
{
	const uint64 *words = bloom1_words((bytea *) bloom);
	return 8 * (VARSIZE_ANY(bloom) + (char *) bloom - (char *) words);
}
