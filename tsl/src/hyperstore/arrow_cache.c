/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/tupdesc.h>
#include <c.h>
#include <catalog/pg_attribute.h>
#include <nodes/bitmapset.h>
#include <stdint.h>
#include <storage/itemptr.h>
#include <utils/guc.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/syscache.h>

#include "arrow_array.h"
#include "arrow_cache.h"
#include "arrow_cache_explain.h"
#include "arrow_tts.h"
#include "compression/compression.h"

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
	const TupleDesc PG_USED_FOR_ASSERTS_ONLY compressed_tupdesc =
		aslot->compressed_slot->tts_tupleDescriptor;
	const int16 attoff = AttrNumberGetAttrOffset(attno);

	Assert(!TupleDescAttr(tupdesc, attoff)->attisdropped &&
		   (aslot->referenced_attrs == NULL || aslot->referenced_attrs[attoff]));

	/* Should never try to decompress a dropped attribute */
	Ensure(!TupleDescAttr(tupdesc, attoff)->attisdropped,
		   "cannot decompress dropped column %s",
		   NameStr(TupleDescAttr(compressed_tupdesc, AttrNumberGetAttrOffset(cattno))->attname));

	Assert(namestrcmp(&TupleDescAttr(tupdesc, attoff)->attname,
					  NameStr(TupleDescAttr(compressed_tupdesc, AttrNumberGetAttrOffset(cattno))
								  ->attname)) == 0);

	/* Should not try to decompress something that is not compressed */
	Assert(is_compressed_col(compressed_tupdesc, cattno));

	TS_DEBUG_LOG("name: %s, attno: %d, cattno: %d, referenced: %s",
				 NameStr(TupleDescAttr(tupdesc, attoff)->attname),
				 attno,
				 cattno,
				 yes_no(!aslot->referenced_attrs || aslot->referenced_attrs[attoff]));

	/*
	 * Only decompress columns that are actually needed, but only if the
	 * node is marked for decompression.
	 *
	 * There are other cases (in particular in ATRewriteTable) where the
	 * decompression columns are not set up at all and in these cases we
	 * should read all columns up to the attribute number.
	 */
	if (entry->arrow_arrays[attoff] == NULL)
	{
		bool isnull;
		Datum value = slot_getattr(aslot->child_slot, cattno, &isnull);

		/* Can this ever be NULL? */
		if (!isnull)
		{
			const Form_pg_attribute attr = TupleDescAttr(tupdesc, attoff);
			entry->arrow_arrays[attoff] = arrow_from_compressed(value,
																attr->atttypid,
																acache->mcxt,
																acache->decompression_mcxt);

			if (decompress_cache_print)
				decompress_cache_misses++;
		}
	}
	else if (decompress_cache_print)
		decompress_cache_hits++;
}

static void
arrow_cache_clear_entry(ArrowColumnCacheEntry *restrict entry)
{
	for (int i = 0; i < entry->num_arrays; ++i)
	{
		if (entry->arrow_arrays[i])
		{
			ArrowArray *array = entry->arrow_arrays[i];

			if (array->release)
			{
				array->release(array);
				array->release = NULL;
			}
			pfree(array);
			entry->arrow_arrays[i] = NULL;
		}
	}
	pfree((void *) entry->arrow_arrays);
	entry->arrow_arrays = NULL;
}

/*
 * Lookup the Arrow cache entry for the tuple.
 *
 * If the entry does not exist, a new entry is created via LRU eviction.
 */
static ArrowColumnCacheEntry *
arrow_cache_get_entry_resolve(ArrowColumnCache *acache, const TupleDesc tupdesc,
							  const ItemPointer compressed_tid)
{
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

			/*
			 * Free allocated memory in the entry.
			 *
			 * The entry itself is managed by the hash table and might be
			 * recycled so should not be freed here.
			 */
			arrow_cache_clear_entry(entry);
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
			(ArrowArray **) MemoryContextAllocZero(acache->mcxt,
												   sizeof(ArrowArray *) * entry->num_arrays);
	}
	else
	{
		/* Move the entry found to the front of the LRU list */
		dlist_move_tail(&acache->arrow_column_cache_lru, &entry->node);
	}

	return entry;
}

static pg_attribute_always_inline ArrowColumnCacheEntry *
arrow_cache_get_entry(ArrowTupleTableSlot *aslot)
{
	if (aslot->arrow_cache_entry == NULL)
		aslot->arrow_cache_entry =
			arrow_cache_get_entry_resolve(&aslot->arrow_cache,
										  aslot->base.base.tts_tupleDescriptor,
										  &aslot->compressed_slot->tts_tid);

	return aslot->arrow_cache_entry;
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
pg_attribute_always_inline ArrowArray **
arrow_column_cache_read_one(ArrowTupleTableSlot *aslot, AttrNumber attno)
{
	const int16 *attrs_offset_map = arrow_slot_get_attribute_offset_map(&aslot->base.base);
	const AttrNumber cattno =
		AttrOffsetGetAttrNumber(attrs_offset_map[AttrNumberGetAttrOffset(attno)]);
	const TupleDesc compressed_tupdesc = aslot->compressed_slot->tts_tupleDescriptor;
	ArrowColumnCacheEntry *restrict entry = arrow_cache_get_entry(aslot);

	if (is_compressed_col(compressed_tupdesc, cattno))
		decompress_one_attr(aslot, entry, attno, cattno);

	return entry->arrow_arrays;
}
