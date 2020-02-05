/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPAT_TUPTABLE_H
#define TIMESCALEDB_COMPAT_TUPTABLE_H

#include <postgres.h>
#include <executor/tuptable.h>

#define ExecFetchSlotHeapTuple(slot, materialize, should_free)                                     \
	ts_exec_fetch_slot_heap_tuple(slot, materialize, should_free)

extern HeapTuple ts_exec_fetch_slot_heap_tuple(TupleTableSlot *slot, bool materialize,
											   bool *should_free);
extern void ts_tuptableslot_set_table_oid(TupleTableSlot *slot, Oid table_oid);

#endif /* TIMESCALEDB_COMPAT_TUPTABLE_H */
