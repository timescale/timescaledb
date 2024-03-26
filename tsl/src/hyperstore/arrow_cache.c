/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/attnum.h>
#include <access/tupdesc.h>
#include <catalog/pg_attribute.h>
#include <nodes/bitmapset.h>
#include <stdint.h>
#include <storage/itemptr.h>
#include <utils/guc.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>

#include "arrow_cache.h"
#include "arrow_cache_explain.h"
#include "arrow_tts.h"
#include "compression/compression.h"
#include "debug_assert.h"
#include <utils/palloc.h>

#define ARROW_DECOMPRESSION_CACHE_LRU_ENTRIES 100

typedef struct ArrowColumnKey
{
	ItemPointerData ctid; /* Compressed TID for the compressed tuple. */
} ArrowColumnKey;

/*
 * Cache entry for an arrow tuple.
 *
 * We just cache the column data right now. We could potentially cache more
 * data such as the segmentby column and similar, but this does not pose a big
 * problem right now.
 */
typedef struct ArrowColumnCacheEntry
{
	ArrowColumnKey key;
	dlist_node node; /* List link in LRU list. */
	ArrowArray **arrow_arrays;
	int16 num_arrays; /* Number of entries in arrow_arrays */
} ArrowColumnCacheEntry;

static ArrowArray *arrow_column_cache_decompress(const ArrowColumnCache *acache, Oid typid,
												 Datum datum);

/*
 * The function dlist_move_tail only exists for PG14 and above, so provide it
 * for PG13 here.
 */
#if PG14_LT
/*
 * Move element from its current position in the list to the tail position in
 * the same list.
 *
 * Undefined behaviour if 'node' is not already part of the list.
 */
static inline void
dlist_move_tail(dlist_head *head, dlist_node *node)
{
	/* fast path if it's already at the tail */
	if (head->head.prev == node)
		return;

	dlist_delete(node);
	dlist_push_tail(head, node);

	dlist_check(head);
}
#endif

static uint16
get_cache_maxsize(void)
{
#ifdef TS_DEBUG
	const char *maxsize = GetConfigOption("timescaledb.arrow_cache_maxsize", true, false);
	char *endptr = NULL;

	if (maxsize == NULL)
		return ARROW_DECOMPRESSION_CACHE_LRU_ENTRIES;

	if (maxsize[0] == '\0')
		elog(ERROR, "invalid arrow_cache_maxsize value");

	long maxsizel = strtol(maxsize, &endptr, 10);

	errno = 0;

	if (errno == ERANGE || endptr == NULL || endptr[0] != '\0' || maxsizel <= 0 ||
		maxsizel > UINT16_MAX)
		elog(ERROR, "invalid arrow_cache_maxsize value");

	elog(DEBUG2, "arrow cache maxsize is %ld", maxsizel);

	return maxsizel & 0xFFFF;
#else
	return ARROW_DECOMPRESSION_CACHE_LRU_ENTRIES;
#endif
}

void
arrow_column_cache_init(ArrowColumnCache *acache, MemoryContext mcxt)
{
	HASHCTL ctl;

	/*
	 * Not sure if we need a dedicated memory context for this data since it
	 * will have the same lifetime as tts_mcxt.
	 *
	 * Consider adding an identifier using MemoryContextSetIdentifier for
	 * debug purposes.
	 */
	acache->mcxt = AllocSetContextCreate(mcxt, "Arrow data", ALLOCSET_START_SMALL_SIZES);
	acache->decompression_mcxt = AllocSetContextCreate(acache->mcxt,
													   "bulk decompression",
													   /* minContextSize = */ 0,
													   /* initBlockSize = */ 64 * 1024,
													   /* maxBlockSize = */ 64 * 1024);
	acache->maxsize = get_cache_maxsize();

	elog(DEBUG2, "arrow decompression cache is under memory context '%s'", mcxt->name);

	ctl.keysize = sizeof(ArrowColumnKey);
	ctl.entrysize = sizeof(ArrowColumnCacheEntry);
	ctl.hcxt = acache->mcxt;
	acache->htab =
		hash_create("Arrow column data cache", 32, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	acache->arrow_column_cache_lru_count = 0;
	dlist_init(&acache->arrow_column_cache_lru);
}

void
arrow_column_cache_release(ArrowColumnCache *acache)
{
	hash_destroy(acache->htab);
	MemoryContextDelete(acache->mcxt);
}

static void
decompress_one_attr(const ArrowTupleTableSlot *aslot, ArrowColumnCacheEntry *entry,
					AttrNumber attno, AttrNumber cattno)
{
	const ArrowColumnCache *acache = &aslot->arrow_cache;
	const TupleDesc tupdesc = aslot->base.base.tts_tupleDescriptor;
	const TupleDesc compressed_tupdesc = aslot->compressed_slot->tts_tupleDescriptor;
	const int16 attoff = AttrNumberGetAttrOffset(attno);

	if (TupleDescAttr(tupdesc, attoff)->attisdropped)
		return;

	Assert(namestrcmp(&TupleDescAttr(tupdesc, attoff)->attname,
					  NameStr(TupleDescAttr(compressed_tupdesc, AttrNumberGetAttrOffset(cattno))
								  ->attname)) == 0);

	/*
	 * Only decompress columns that are actually needed, but only if the
	 * node is marked for decompression.
	 *
	 * There are other cases (in particular in ATRewriteTable) where the
	 * decompression columns are not set up at all and in these cases we
	 * should read all columns up to the attribute number.
	 */
	if (entry->arrow_arrays[attoff] == NULL && is_compressed_col(compressed_tupdesc, cattno) &&
		(!aslot->referenced_attrs || bms_is_member(attno, aslot->referenced_attrs)))
	{
		bool isnull;
		Datum value = slot_getattr(aslot->child_slot, cattno, &isnull);

		if (!isnull)
		{
			const Form_pg_attribute attr = TupleDescAttr(tupdesc, attoff);
			entry->arrow_arrays[attoff] =
				arrow_column_cache_decompress(acache, attr->atttypid, value);
		}
	}
}

/*
 * Lookup the Arrow cache entry for the tuple.
 *
 * If the entry does not exist, a new entry is created via LRU eviction.
 */
static ArrowColumnCacheEntry *
arrow_cache_get_entry(ArrowTupleTableSlot *aslot)
{
	ArrowColumnCache *acache = &aslot->arrow_cache;
	const TupleDesc tupdesc = aslot->base.base.tts_tupleDescriptor;
	const ItemPointer compressed_tid = &aslot->compressed_slot->tts_tid;
	const ArrowColumnKey key = { .ctid = *compressed_tid };
	bool found;

	ArrowColumnCacheEntry *restrict entry = hash_search(acache->htab, &key, HASH_FIND, &found);

	/* If entry was not found, we might have to prune the LRU list before
	 * allocating a new entry */
	if (!found)
	{
		if (acache->arrow_column_cache_lru_count >= acache->maxsize)
		{
			/* If we don't have room in the cache for the new entry, pick the
			 * least recently used and remove it. */
			entry = dlist_container(ArrowColumnCacheEntry,
									node,
									dlist_pop_head_node(&acache->arrow_column_cache_lru));
			if (!hash_search(acache->htab, &entry->key, HASH_REMOVE, NULL))
				elog(ERROR, "LRU cache for compressed rows corrupt");
			--acache->arrow_column_cache_lru_count;

			/* Free allocated memory in the entry. The entry itself is managed
			 * by the hash table and might be recycled so should not be freed
			 * here. */
			for (int i = 0; i < entry->num_arrays; ++i)
			{
				if (entry->arrow_arrays[i])
				{
					/* TODO: instead of all these pfrees the entry should
					 * either use its own MemoryContext or implement the
					 * release function in the ArrowArray. That's the only way
					 * to know for sure which buffers are actually separately
					 * palloc'd. */
					ArrowArray *arr = entry->arrow_arrays[i];

					for (int64 j = 0; j < arr->n_buffers; j++)
					{
						/* Validity bitmap might be NULL even if it is counted
						 * in n_buffers, so need to check for NULL values. */
						if (arr->buffers[j])
							pfree((void *) arr->buffers[j]);
					}
					pfree(entry->arrow_arrays[i]);
					entry->arrow_arrays[i] = NULL;
				}
			}
			pfree(entry->arrow_arrays);
			entry->arrow_arrays = NULL;
		}

		/* Allocate a new entry in the hash table. */
		entry = hash_search(acache->htab, &key, HASH_ENTER, &found);
		dlist_push_tail(&acache->arrow_column_cache_lru, &entry->node);
		++acache->arrow_column_cache_lru_count;
		Assert(!found);
	}
	else
	{
		dlist_move_tail(&acache->arrow_column_cache_lru, &entry->node);
	}

	Assert(entry);

	/*
	 * Entry might be new so fill in default values.
	 *
	 * We allocate space for (pointers to) *all* columns in the tuple
	 * descriptor but we might not use all.
	 */
	if (!found)
	{
		entry->num_arrays = tupdesc->natts;
		entry->arrow_arrays =
			MemoryContextAllocZero(acache->mcxt, sizeof(ArrowArray *) * entry->num_arrays);
		if (decompress_cache_print)
			decompress_cache_misses++;
	}
	else
	{
		/* Move the entry found to the front of the LRU list */
		dlist_move_tail(&acache->arrow_column_cache_lru, &entry->node);
		if (decompress_cache_print)
			decompress_cache_hits++;
	}

	return entry;
}

/*
 * Fetch and decompress data into arrow arrays for the first N attributes.
 *
 * Note that only "referenced attributes" are decompressed (i.e., those that
 * are actually referenced in a query), so some of the "natts" returned arrays
 * may be NULL for this reason.
 */
ArrowArray **
arrow_column_cache_read_many(ArrowTupleTableSlot *aslot, unsigned int natts)
{
	const int16 *attrs_offset_map = arrow_slot_get_attribute_offset_map(&aslot->base.base);
	ArrowColumnCacheEntry *restrict entry = arrow_cache_get_entry(aslot);

	Assert(natts > 0);

	/*
	 * Decompress any missing columns in the cache entry.
	 *
	 * Note that this will skip columns that are already dealt with before
	 * and stored in the cache.
	 */
	for (unsigned int i = 0; i < natts; i++)
	{
		const int16 cattoff = attrs_offset_map[i];
		const AttrNumber attno = AttrOffsetGetAttrNumber(i);
		const AttrNumber cattno = AttrOffsetGetAttrNumber(cattoff);
		decompress_one_attr(aslot, entry, attno, cattno);
	}

	return entry->arrow_arrays;
}

/*
 * Fetch and decompress data into an arrow array for the given
 * attribute. Arrays for other attributes are returned too, and these may be
 * valid if decompressed via a previous call.
 *
 * Note that only "referenced attributes" are decompressed (i.e., those that
 * are actually referenced in a query), so some returned arrays may be NULL
 * for this reason.
 */
ArrowArray **
arrow_column_cache_read_one(ArrowTupleTableSlot *aslot, AttrNumber attno)
{
	const int16 *attrs_offset_map = arrow_slot_get_attribute_offset_map(&aslot->base.base);
	ArrowColumnCacheEntry *restrict entry = arrow_cache_get_entry(aslot);
	const AttrNumber cattno =
		AttrOffsetGetAttrNumber(attrs_offset_map[AttrNumberGetAttrOffset(attno)]);

	decompress_one_attr(aslot, entry, attno, cattno);
	return entry->arrow_arrays;
}

/*
 * Decompress a column and add it to the arrow tuple cache.
 *
 * Returns a pointer to the cached entry.
 */
static ArrowArray *
arrow_column_cache_decompress(const ArrowColumnCache *acache, Oid typid, Datum datum)
{
	const CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(datum);
	DecompressAllFunction decompress_all =
		tsl_get_decompress_all_function(header->compression_algorithm, typid);
	Ensure(decompress_all != NULL,
		   "missing decompression function %d",
		   header->compression_algorithm);
	MemoryContext oldcxt = MemoryContextSwitchTo(acache->decompression_mcxt);
	ArrowArray *arrow_column = decompress_all(PointerGetDatum(header), typid, acache->mcxt);
	if (decompress_cache_print)
		decompress_cache_decompress_count++;

	/*
	 * Not sure how necessary this reset is, but keeping it for now.
	 *
	 * The amount of data is bounded by the number of columns in the tuple
	 * table slot, so it might be possible to skip this reset.
	 */
	MemoryContextReset(acache->decompression_mcxt);
	MemoryContextSwitchTo(oldcxt);
	return arrow_column;
}
