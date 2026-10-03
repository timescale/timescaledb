/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * The Dictionary compressions scheme can store any type of data but is optimized for
 * low-cardinality data sets. The dictionary of distinct items is stored as an `array` compressed
 * object. The row->dictionary item mapping is stored as a series of integer-based indexes into the
 * dictionary array ordered by row number (called dictionary_indexes; compressed using
 * `simple8b_rle`).
 */
#include <postgres.h>
#include <utils/datum.h>
#include <utils/typcache.h>

#include "compat/compat.h"

/*
 * The dictionary deduplicates values by their binary image, not by the
 * equality operator of the type. Values that compare equal with the type's
 * equality operator can still be distinguishable, e.g. the intervals '1 mon'
 * and '30 days', the numerics 1.0 and 1.00, or the floats 0 and -0 inside an
 * array. Replacing one of them with the other would change the data, so they
 * must get separate dictionary entries (see #10709).
 */
typedef struct HashMeta
{
	int16 typlen;
	bool typbyval;
} HashMeta;

typedef struct DictionaryHashItem
{
	Datum key;
	/* hash entry status */
	uint32 hash;
	uint16 status;
	uint16 index;
} DictionaryHashItem;

typedef struct dictionary_hash dictionary_hash;
static uint32 datum_hash(dictionary_hash *tb, Datum key);
static bool datum_eq(dictionary_hash *tb, Datum a, Datum b);

#define SH_PREFIX dictionary
#define SH_ELEMENT_TYPE DictionaryHashItem
#define SH_KEY_TYPE Datum
#define SH_KEY key
#define SH_HASH_KEY(tb, key) datum_hash(tb, key)
#define SH_EQUAL(tb, a, b) datum_eq(tb, a, b)
#define SH_STORE_HASH
#define SH_GET_HASH(tb, entry) entry->hash
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

static uint32
datum_hash(dictionary_hash *tb, Datum key)
{
	HashMeta *meta = (HashMeta *) tb->private_data;

	return datum_image_hash(key, meta->typbyval, meta->typlen);
}

static bool
datum_eq(dictionary_hash *tb, Datum a, Datum b)
{
	HashMeta *meta = (HashMeta *) tb->private_data;

	return datum_image_eq(a, b, meta->typbyval, meta->typlen);
}

static dictionary_hash *
dictionary_hash_alloc(TypeCacheEntry *tentry)
{
	HashMeta *meta = palloc(sizeof(*meta));

	if (tentry->hash_proc_finfo.fn_addr == NULL || tentry->eq_opr_finfo.fn_addr == NULL)
	{
		elog(ERROR,
			 "invalid type for dictionary compression, type must have both a hash function and "
			 "equality function");
	}

	meta->typlen = tentry->typlen;
	meta->typbyval = tentry->typbyval;

	return dictionary_create(CurrentMemoryContext, 10, meta);
}
