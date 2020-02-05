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

#define table_slot_create(rel, reglist) ts_table_slot_create(rel, reglist)
#define table_tuple_insert(rel, slot, cid, options, bistate)                                       \
	ts_table_tuple_insert(rel, slot, cid, options, bistate)

extern TupleTableSlot *ts_table_slot_create(Relation rel, List **reglist);
extern void ts_table_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, int options,
								  struct BulkInsertStateData *bistate);

#endif /* TIMESCALEDB_COMPAT_TABLEAM_H */
