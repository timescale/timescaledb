/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPAT_TABLEAM_H
#define TIMESCALEDB_COMPAT_TABLEAM_H

#include <postgres.h>
#include <executor/tuptable.h>
#include <nodes/pg_list.h>
#include <utils/relcache.h>
#include <access/heapam.h>
#include <access/genam.h>

#define TableScanDesc HeapScanDesc

#define table_open(r, l) heap_open(r, l)
#define table_openrv(r, l) heap_openrv(r, l)
#define table_close(r, l) heap_close(r, l)

#define table_beginscan(rel, snapshot, nkeys, keys) heap_beginscan(rel, snapshot, nkeys, keys)
#define table_beginscan_catalog(rel, nkeys, keys) heap_beginscan_catalog(rel, nkeys, keys)
#define table_endscan(scan) heap_endscan(scan)

#define table_slot_create(rel, reglist) ts_table_slot_create(rel, reglist)
#define table_tuple_insert(rel, slot, cid, options, bistate)                                       \
	ts_table_tuple_insert(rel, slot, cid, options, bistate)
#define table_scan_getnextslot(scan, direction, slot)                                              \
	ts_table_scan_getnextslot(scan, direction, slot)
#define index_getnext_slot(scan, direction, slot) ts_index_getnext_slot(scan, direction, slot)

extern TupleTableSlot *ts_table_slot_create(Relation rel, List **reglist);
extern void ts_table_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, int options,
								  struct BulkInsertStateData *bistate);
extern bool ts_table_scan_getnextslot(TableScanDesc scan, const ScanDirection direction,
									  TupleTableSlot *slot);
extern bool ts_index_getnext_slot(IndexScanDesc scan, const ScanDirection direction,
								  TupleTableSlot *slot);

#endif /* TIMESCALEDB_COMPAT_TABLEAM_H */
