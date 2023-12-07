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
#include <nodes/bitmapset.h>
#include <storage/itemptr.h>

#include "arrow_tts.h"

/* Number of arrow decompression cache LRU entries  */
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
 *
 * ArrowArray
 */
typedef struct ArrowColumnCacheEntry
{
	ArrowColumnKey key;
	int nvalid;		 /* Valid columns from the compressed tuple. */
	dlist_node node; /* List link in LRU list. */
	Bitmapset *segmentby_columns;
	ArrowArray **arrow_columns;
} ArrowColumnCacheEntry;

extern void arrow_column_cache_init(ArrowTupleTableSlot *aslot);
extern void arrow_column_cache_release(ArrowTupleTableSlot *aslot);
extern ArrowColumnCacheEntry *arrow_column_cache_read(ArrowTupleTableSlot *aslot, int attnum);
extern ArrowArray *arrow_column_cache_decompress(ArrowTupleTableSlot *aslot, Datum datum,
												 AttrNumber attnum);
#endif /* COMPRESSION_ARROW_CACHE_H_ */
