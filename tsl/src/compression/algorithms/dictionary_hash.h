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
#include <funcapi.h>
#include <utils/typcache.h>

#include "compat/compat.h"

typedef struct HashMeta
{
	FunctionCallInfo hash_info;
	FunctionCallInfo eq_info;
} HashMeta;

typedef struct DictionaryHashItem
{
	Datum key;
	/* hash entry status */
	char status;
	uint32 index;
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
#define SH_SCOPE static inline
#define SH_DEFINE
#define SH_DECLARE
#include "lib/simplehash.h"

static uint32
datum_hash(dictionary_hash *tb, Datum key)
{
	HashMeta *meta = (HashMeta *) tb->private_data;
	FunctionCallInfo fcinfo = meta->hash_info;
	Datum value;

	FC_SET_ARG(fcinfo, 0, key);
	fcinfo->isnull = false;

	value = FunctionCallInvoke(fcinfo);
	Assert(!fcinfo->isnull);

	return DatumGetUInt32(value);
}

static bool
datum_eq(dictionary_hash *tb, Datum a, Datum b)
{
	HashMeta *meta = (HashMeta *) tb->private_data;
	FunctionCallInfo fcinfo = meta->eq_info;
	Datum value;

	FC_SET_ARG(fcinfo, 0, a);
	FC_SET_ARG(fcinfo, 1, b);
	fcinfo->isnull = false;

	value = FunctionCallInvoke(fcinfo);
	Assert(!fcinfo->isnull);

	return DatumGetBool(value);
}

static dictionary_hash *
dictionary_hash_alloc(TypeCacheEntry *tentry)
{
	HashMeta *meta = palloc(sizeof(*meta));
	Oid collation = InvalidOid;
	collation = tentry->typcollation;

	if (tentry->hash_proc_finfo.fn_addr == NULL || tentry->eq_opr_finfo.fn_addr == NULL)
		elog(ERROR,
			 "invalid type for dictionary compression, type must have both a hash function and "
			 "equality function");

	/* May be more correct to get collation defined on the column, which may be different than the
	 * collation defined on the type (what we're currently using). We need to think about
	 * backwards compatibility, and different collations. Should only affect compression ratios
	 * anyway.
	 */
	meta->eq_info = HEAP_FCINFO(2);
	InitFunctionCallInfoData(*meta->eq_info, &tentry->eq_opr_finfo, 2, collation, NULL, NULL);

	meta->hash_info = HEAP_FCINFO(2);
	InitFunctionCallInfoData(*meta->hash_info, &tentry->hash_proc_finfo, 1, collation, NULL, NULL);

	return dictionary_create(CurrentMemoryContext, 10, meta);
}
