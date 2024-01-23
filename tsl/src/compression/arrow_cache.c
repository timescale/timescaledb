/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/tupdesc.h>
#include <nodes/bitmapset.h>
#include <storage/itemptr.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>

#include "arrow_cache.h"
#include "arrow_tts.h"
#include "compression.h"
#include "debug_assert.h"

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

void
arrow_column_cache_init(ArrowTupleTableSlot *aslot)
{
	HASHCTL ctl;

	/*
	 * Not sure if we need a dedicated memory context for this data since it
	 * will have the same lifetime as tts_mcxt.
	 *
	 * Consider adding an identifier using MemoryContextSetIdentifier for
	 * debug purposes.
	 */
	aslot->arrowdata_mcxt =
		AllocSetContextCreate(aslot->base.base.tts_mcxt, "Arrow data", ALLOCSET_START_SMALL_SIZES);
	aslot->cache_total = 0;
	aslot->cache_misses = 0;

	elog(DEBUG2,
		 "arrow decompression cache is under memory context '%s'",
		 aslot->base.base.tts_mcxt->name);

	ctl.keysize = sizeof(ArrowColumnKey);
	ctl.entrysize = sizeof(ArrowColumnCacheEntry);
	ctl.hcxt = aslot->arrowdata_mcxt;
	aslot->arrow_column_cache =
		hash_create("Arrow column data cache", 32, &ctl, HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	aslot->arrow_column_cache_lru_count = 0;
	dlist_init(&aslot->arrow_column_cache_lru);
}

void
arrow_column_cache_release(ArrowTupleTableSlot *aslot)
{
	elog(DEBUG2,
		 "cache hits=%zu misses=%zu",
		 aslot->cache_total - aslot->cache_misses,
		 aslot->cache_misses);
	hash_destroy(aslot->arrow_column_cache);
}

ArrowColumnCacheEntry *
arrow_column_cache_read(ArrowTupleTableSlot *aslot, int attnum)
{
	const ItemPointer compressed_tid = &aslot->compressed_slot->tts_tid;
	const TupleDesc compressed_tupdesc = aslot->compressed_slot->tts_tupleDescriptor;
	const int uncompressed_natts = aslot->noncompressed_slot->tts_tupleDescriptor->natts;
	const ArrowColumnKey key = { .ctid = *compressed_tid };
	bool found;

	Assert(aslot->arrow_column_cache != NULL);

	ArrowColumnCacheEntry *restrict entry =
		hash_search(aslot->arrow_column_cache, &key, HASH_FIND, &found);

	aslot->cache_total++;

	/* If entry was not found, we might have to prune the LRU list before
	 * allocating a new entry */
	if (!found)
	{
		if (aslot->arrow_column_cache_lru_count >= ARROW_DECOMPRESSION_CACHE_LRU_ENTRIES)
		{
			/* If we don't have room in the cache for the new entry, pick the
			 * least recently used and remove it. */
			entry = dlist_container(ArrowColumnCacheEntry,
									node,
									dlist_pop_head_node(&aslot->arrow_column_cache_lru));
			if (!hash_search(aslot->arrow_column_cache, &entry->key, HASH_REMOVE, NULL))
				elog(ERROR, "LRU cache for compressed rows corrupt");
			--aslot->arrow_column_cache_lru_count;

			/* Free allocated memory in the entry. The entry itself is managed
			 * by the hash table and might be recycled so should not be freed
			 * here. */
			for (int i = 0; i < entry->nvalid; ++i)
			{
				if (entry->arrow_columns[i])
				{
					/* TODO: instead of all these pfrees the entry should
					 * either use its own MemoryContext or implement the
					 * release function in the ArrowArray. That's the only way
					 * to know for sure which buffers are actually separately
					 * palloc'd. */
					ArrowArray *arr = entry->arrow_columns[i];

					for (int64 j = 0; j < arr->n_buffers; j++)
					{
						/* Validity bitmap might be NULL even if it is counted
						 * in n_buffers, so need to check for NULL values. */
						if (arr->buffers[j])
							pfree((void *) arr->buffers[j]);
					}
					pfree(entry->arrow_columns[i]);
				}
			}
			pfree(entry->segmentby_columns);
			pfree(entry->arrow_columns);
		}

		/* Allocate a new entry in the hash table. */
		entry = hash_search(aslot->arrow_column_cache, &key, HASH_ENTER, &found);
		dlist_push_tail(&aslot->arrow_column_cache_lru, &entry->node);
		++aslot->arrow_column_cache_lru_count;
		Assert(!found);
	}
	else
	{
		dlist_move_tail(&aslot->arrow_column_cache_lru, &entry->node);
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
		MemoryContext oldmctx = MemoryContextSwitchTo(aslot->arrowdata_mcxt);
		const int16 *attrs_map = arrow_slot_get_attribute_offset_map(&aslot->base.base);

		entry->nvalid = 0;
		entry->segmentby_columns = NULL;
		entry->arrow_columns =
			(ArrowArray **) palloc0(sizeof(ArrowArray *) * compressed_tupdesc->natts);

		/*
		 * Populate the segmentby_column bitmap with information about *all*
		 * columns in the non-compressed version of the tuple. That way we do
		 * not have to update this field if we start fetching more attributes.
		 */
		for (int i = 0; i < uncompressed_natts; i++)
		{
			const int16 cattoff = attrs_map[i];
			const AttrNumber attno = AttrOffsetGetAttrNumber(i);
			const AttrNumber cattno = AttrOffsetGetAttrNumber(cattoff);

			if (!is_compressed_col(compressed_tupdesc, cattno))
				entry->segmentby_columns = bms_add_member(entry->segmentby_columns, attno);
		}

		MemoryContextSwitchTo(oldmctx);
		aslot->cache_misses++;
	}
	else
	{
		/* Move the entry found to the front of the LRU list */
		dlist_move_tail(&aslot->arrow_column_cache_lru, &entry->node);
	}

	/*
	 * Fill in missing columns in the cache entry.
	 *
	 * Note that this will skip columns that are already dealt with before
	 * and stored in the cache.
	 */
	for (; entry->nvalid < attnum; entry->nvalid++)
	{
		const int16 cattoff = aslot->attrs_offset_map[entry->nvalid];
		const AttrNumber attno = AttrOffsetGetAttrNumber(entry->nvalid);
		const AttrNumber cattno = AttrOffsetGetAttrNumber(cattoff);

		if (is_compressed_col(compressed_tupdesc, cattno))
		{
			bool isnull;
			Datum value = slot_getattr(aslot->child_slot, cattno, &isnull);

			if (!isnull)
				entry->arrow_columns[entry->nvalid] =
					arrow_column_cache_decompress(aslot, value, attno);
		}
	}

	return entry;
}

/*
 * Decompress a column and add it to the arrow tuple cache.
 *
 * Returns a pointer to the cached entry.
 */
ArrowArray *
arrow_column_cache_decompress(ArrowTupleTableSlot *aslot, Datum datum, AttrNumber attnum)
{
	const CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(datum);
	const TupleDesc noncompressed_tupdesc = aslot->noncompressed_slot->tts_tupleDescriptor;
	Form_pg_attribute attr = TupleDescAttr(noncompressed_tupdesc, AttrNumberGetAttrOffset(attnum));
	DecompressAllFunction decompress_all =
		tsl_get_decompress_all_function(header->compression_algorithm, attr->atttypid);
	Ensure(decompress_all != NULL,
		   "missing decompression function %d",
		   header->compression_algorithm);
	MemoryContext oldcxt = MemoryContextSwitchTo(aslot->decompression_mcxt);
	ArrowArray *arrow_column =
		decompress_all(PointerGetDatum(header), attr->atttypid, aslot->arrowdata_mcxt);

	/*
	 * Not sure how necessary this reset is, but keeping it for now.
	 *
	 * The amount of data is bounded by the number of columns in the tuple
	 * table slot, so it might be possible to skip this reset.
	 */
	MemoryContextReset(aslot->decompression_mcxt);
	MemoryContextSwitchTo(oldcxt);
	return arrow_column;
}
