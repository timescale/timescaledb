/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
/**
 * @module
 *
 * The arrow tuple is a decompressed compressed tuple (!).
 *
 * It is stored in the structure below and all the data is allocated in the
 * ArrowTupleCacheMemoryContext memory context.
 *
 * Since not all columns might be fetched, only memory for the fetched columns
 * are available. The existing tuple can safely be extended as necessary since
 * it does not change existing Arrow arrays. Also note that we only store
 * arrays for compressed columns and uncompressed columns have to be fetched
 * from the compressed tuple table slot.
 *
 *                     nvalid      nattr
 *                       |           |
 *                       v           v
 *     +---+---+---+---+---+---+---+
 *     | * | * | - | * | - | - | - |   Arrow tuple
 *     +-|-+-|-+---+-|-+---+---+---+
 *       v   v       v
 *      +-+ +-+     +-+
 *      | | | |     | |
 *      +-+ +-+     +-+
 *      | | | |     | |    Arrow arrays
 *      +-+ +-+     +-+
 *      | | | |     | |
 *      +-+ +-+     +-+
 */

#ifndef COMPRESSION_ARROW_CACHE_H_
#define COMPRESSION_ARROW_CACHE_H_

#include <postgres.h>

#include <access/tupdesc.h>
#include <catalog/pg_attribute.h>
#include <lib/ilist.h>
#include <nodes/bitmapset.h>
#include <storage/itemptr.h>
#include <utils/hsearch.h>

#include "arrow_c_data_interface.h"

/* Number of arrow decompression cache LRU entries  */
#define ARROW_DECOMPRESSION_CACHE_LRU_ENTRIES 100

typedef struct ArrowColumnKey
{
	ItemPointerData ctid; /* Compressed TID for the compressed tuple. */
} ArrowColumnKey;

typedef struct ArrowColumnCache
{
	MemoryContext mcxt;
	MemoryContext decompression_mcxt;	 /* Temporary data during decompression */
	size_t arrow_column_cache_lru_count; /* Arrow column cache LRU list count */
	dlist_head arrow_column_cache_lru;	 /* Arrow column cache LRU list */
	HTAB *htab;							 /* Arrow column cache */
	uint16 maxsize;
} ArrowColumnCache;

/*
 * Cache entry for an arrow tuple.
 *
 * We just cache the column data right now. We could potentially cache more
 * data such as the segmentby column and similar, but this does not pose a big
 * problem right now.
 *
 * ArrowArray
 */
typedef struct ArrowColumnCacheEntry
{
	ArrowColumnKey key;
	int nvalid;		 /* Valid columns from the compressed tuple. */
	dlist_node node; /* List link in LRU list. */
	ArrowArray **arrow_columns;
} ArrowColumnCacheEntry;

typedef struct ArrowTupleTableSlot ArrowTupleTableSlot;

extern void arrow_column_cache_init(ArrowColumnCache *acache, MemoryContext mcxt);
extern void arrow_column_cache_release(ArrowColumnCache *acache);
extern ArrowColumnCacheEntry *arrow_column_cache_read(ArrowTupleTableSlot *aslot, int attnum);

#endif /* COMPRESSION_ARROW_CACHE_H_ */
