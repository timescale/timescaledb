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

static inline uint32
bloom1_get_one_hash(uint32 value_hash, uint32 index)
{
	const uint32 h1 = hash_combine(value_hash, BLOOM1_SEED_1);
	const uint32 h2 = hash_combine(value_hash, BLOOM1_SEED_2);
	return h1 + index * h2 + index * index;
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
