/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPAT_TUPCONVERT_H
#define TIMESCALEDB_COMPAT_TUPCONVERT_H

#include <postgres.h>
#include <access/attnum.h>
#include <executor/tuptable.h>

#define execute_attr_map_slot(attrmap, in_slot, out_slot)                                          \
	ts_execute_attr_map_slot(attrmap, in_slot, out_slot)

extern TupleTableSlot *ts_execute_attr_map_slot(AttrNumber *attrmap, TupleTableSlot *in_slot,
												TupleTableSlot *out_slot);

#endif /* TIMESCALEDB_COMPAT_TUPCONVERT_H */
