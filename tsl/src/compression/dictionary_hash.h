/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/*
 * The Dictionary compressions scheme can store any type of data but is optimized for
 * low-cardinality data sets. The dictionary of distinct items is stored as an `array` compressed
 * object. The row->dictionary item mapping is stored as a series of integer-based indexes into the
 * dictionary array ordered by row number (called dictionary_indexes; compressed using
 * `simple8b_rle`).
 */
#ifndef TIMESCALEDB_TSL_COMPRESSION_DICTIONARY_HASH_H
#define TIMESCALEDB_TSL_COMPRESSION_DICTIONARY_HASH_H

#include <postgres.h>
#include <funcapi.h>
#include <utils/typcache.h>

typedef struct HashMeta
{
	FunctionCallInfoData hash_info;
	FunctionCallInfoData eq_info;
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
#include "adts/simplehash.h"

static uint32
datum_hash(dictionary_hash *tb, Datum key)
{
	HashMeta *meta = (HashMeta *) tb->private_data;
	FunctionCallInfoData *fcinfo = &meta->hash_info;
	Datum value;

	fcinfo->arg[0] = key;
	fcinfo->argnull[0] = false;

	fcinfo->isnull = false;
	value = FunctionCallInvoke(fcinfo);
	Assert(!fcinfo->isnull);

	return DatumGetUInt32(value);
}

static bool
datum_eq(dictionary_hash *tb, Datum a, Datum b)
{
	HashMeta *meta = (HashMeta *) tb->private_data;
	FunctionCallInfoData *fcinfo = &meta->eq_info;
	Datum value;

	fcinfo->arg[0] = a;
	fcinfo->argnull[0] = false;

	fcinfo->arg[1] = b;
	fcinfo->argnull[1] = false;

	fcinfo->isnull = false;
	value = FunctionCallInvoke(fcinfo);
	Assert(!fcinfo->isnull);

	return DatumGetBool(value);
}

static dictionary_hash *
dictionary_hash_alloc(TypeCacheEntry *tentry)
{
	HashMeta *meta = palloc(sizeof(*meta));

	if (tentry->hash_proc_finfo.fn_addr == NULL || tentry->eq_opr_finfo.fn_addr == NULL)
		elog(ERROR,
			 "invalid type for dictionary compression, type must have both a hash function and "
			 "equality function");

	InitFunctionCallInfoData(meta->eq_info, &tentry->eq_opr_finfo, 2, 0, NULL, NULL);
	InitFunctionCallInfoData(meta->hash_info, &tentry->hash_proc_finfo, 2, 0, NULL, NULL);

	return dictionary_create(CurrentMemoryContext, 10, meta);
}

#endif
