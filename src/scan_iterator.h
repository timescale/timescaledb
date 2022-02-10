/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_SCAN_ITERATOR_H
#define TIMESCALEDB_SCAN_ITERATOR_H

#include <postgres.h>
#include <utils/palloc.h>

#include "scanner.h"
#include "ts_catalog/catalog.h"

#define EMBEDDED_SCAN_KEY_SIZE 5

typedef struct ScanIterator
{
	ScannerCtx ctx;
	TupleInfo *tinfo;
	MemoryContext scankey_mcxt;
	ScanKeyData scankey[EMBEDDED_SCAN_KEY_SIZE];
} ScanIterator;

#define ts_scan_iterator_create(catalog_table_id, lock_mode, mctx)                                   \
	(ScanIterator)                                                                                   \
	{                                                                                                \
		.ctx = {                                                                                   \
			.internal = {													\
				.ended = true,											\
			},															\
			.table = catalog_get_table_id(ts_catalog_get(), catalog_table_id),                     \
			.nkeys = 0,                                                                            \
			.scandirection = ForwardScanDirection,                                                 \
			.lockmode = lock_mode,                                                                 \
			.result_mctx = mctx,                                                                   \
			.flags = SCANNER_F_NOFLAGS,									\
		},																\
		.scankey_mcxt = CurrentMemoryContext, \
	}

static inline TupleInfo *
ts_scan_iterator_tuple_info(const ScanIterator *iterator)
{
	return iterator->tinfo;
}

static inline TupleTableSlot *
ts_scan_iterator_slot(const ScanIterator *iterator)
{
	return iterator->tinfo->slot;
}

static inline HeapTuple
ts_scan_iterator_fetch_heap_tuple(const ScanIterator *iterator, bool materialize, bool *should_free)
{
	return ts_scanner_fetch_heap_tuple(iterator->tinfo, materialize, should_free);
}

static inline TupleDesc
ts_scan_iterator_tupledesc(const ScanIterator *iterator)
{
	return ts_scanner_get_tupledesc(iterator->tinfo);
}

static inline MemoryContext
ts_scan_iterator_get_result_memory_context(const ScanIterator *iterator)
{
	return iterator->ctx.result_mctx;
}

static inline void *
ts_scan_iterator_alloc_result(const ScanIterator *iterator, Size size)
{
	return MemoryContextAllocZero(iterator->ctx.result_mctx, size);
}

static inline void
ts_scan_iterator_start_scan(ScanIterator *iterator)
{
	MemoryContext oldmcxt = MemoryContextSwitchTo(iterator->scankey_mcxt);
	ts_scanner_start_scan(&(iterator)->ctx);
	MemoryContextSwitchTo(oldmcxt);
}

static inline TupleInfo *
ts_scan_iterator_next(ScanIterator *iterator)
{
	iterator->tinfo = ts_scanner_next(&(iterator)->ctx);
	return iterator->tinfo;
}

static inline void
ts_scan_iterator_scan_key_reset(ScanIterator *iterator)
{
	iterator->ctx.nkeys = 0;
}

static inline bool
ts_scan_iterator_is_started(ScanIterator *iterator)
{
	return iterator->ctx.internal.started;
}

void TSDLLEXPORT ts_scan_iterator_set_index(ScanIterator *iterator, CatalogTable table,
											int indexid);
void TSDLLEXPORT ts_scan_iterator_end(ScanIterator *iterator);
void TSDLLEXPORT ts_scan_iterator_close(ScanIterator *iterator);
void TSDLLEXPORT ts_scan_iterator_scan_key_init(ScanIterator *iterator, AttrNumber attributeNumber,
												StrategyNumber strategy, RegProcedure procedure,
												Datum argument);

/*
 * Reset the scan to use a new scan key.
 *
 * Note that the scan key should typically be reinitialized before a rescan.
 */
void TSDLLEXPORT ts_scan_iterator_rescan(ScanIterator *iterator);

static inline void
ts_scan_iterator_start_or_restart_scan(ScanIterator *iterator)
{
	if (ts_scan_iterator_is_started(iterator))
		ts_scan_iterator_rescan(iterator);
	else
		ts_scan_iterator_start_scan(iterator);
}

/* You must use `ts_scan_iterator_close` if terminating this loop early */
#define ts_scanner_foreach(scan_iterator)                                                          \
	for (ts_scan_iterator_start_scan((scan_iterator));                                             \
		 ts_scan_iterator_next(scan_iterator) != NULL;)

#endif /* TIMESCALEDB_SCAN_ITERATOR_H */
