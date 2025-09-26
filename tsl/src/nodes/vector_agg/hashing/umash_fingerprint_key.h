/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * Helpers to use the umash fingerprint as a hash table key in our hashing
 * strategies for vectorized grouping.
 */

#include "import/umash.h"

/*
 * The struct is packed so that the hash table entry fits into 16
 * bytes with the uint32 key index that goes after.
 */
struct umash_fingerprint_key
{
	uint64 hash;
	uint32 rest;
} pg_attribute_packed();

#define HASH_TABLE_KEY_TYPE struct umash_fingerprint_key
#define KEY_HASH(X) (X.hash)
#define KEY_EQUAL(a, b) (a.hash == b.hash && a.rest == b.rest)

static inline struct umash_fingerprint_key
umash_fingerprint_get_key(struct umash_fp fp)
{
	const struct umash_fingerprint_key key = {
		.hash = fp.hash[0] & (~(uint32) 0),
		.rest = fp.hash[1],
	};
	return key;
}

static inline struct umash_params *
umash_key_hashing_init()
{
	struct umash_params *params = palloc0(sizeof(struct umash_params));
	umash_params_derive(params, 0xabcdef1234567890ull, NULL);
	return params;
}
