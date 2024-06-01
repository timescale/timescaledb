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

#pragma once

#include <postgres.h>

#include <access/tupdesc.h>
#include <catalog/pg_attribute.h>
#include <lib/ilist.h>
#include <nodes/bitmapset.h>
#include <storage/itemptr.h>
#include <utils/hsearch.h>

#include "compression/arrow_c_data_interface.h"

typedef struct ArrowColumnCache
{
	MemoryContext mcxt;
	MemoryContext decompression_mcxt;	 /* Temporary data during decompression */
	size_t arrow_column_cache_lru_count; /* Arrow column cache LRU list count */
	dlist_head arrow_column_cache_lru;	 /* Arrow column cache LRU list */
	HTAB *htab;							 /* Arrow column cache */
	uint16 maxsize;
} ArrowColumnCache;

typedef struct ArrowTupleTableSlot ArrowTupleTableSlot;

extern void arrow_column_cache_init(ArrowColumnCache *acache, MemoryContext mcxt);
extern void arrow_column_cache_release(ArrowColumnCache *acache);
extern ArrowArray **arrow_column_cache_read_many(ArrowTupleTableSlot *aslot, unsigned int natts);
extern ArrowArray **arrow_column_cache_read_one(ArrowTupleTableSlot *aslot, AttrNumber attno);
