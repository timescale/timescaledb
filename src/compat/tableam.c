/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#include <postgres.h>
#include <utils/rel.h>

#include "tableam.h"
#include "tuptable.h"

TupleTableSlot *
ts_table_slot_create(Relation rel, List **reglist)
{
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(rel));

	if (reglist)
		*reglist = lappend(*reglist, slot);

	return slot;
}

void
ts_table_tuple_insert(Relation rel, TupleTableSlot *slot, CommandId cid, int options,
					  struct BulkInsertStateData *bistate)
{
	bool should_free = true;
	HeapTuple tuple = ExecFetchSlotHeapTuple(slot, true, &should_free);

	/* Update the tuple with table oid */
	tuple->t_tableOid = RelationGetRelid(rel);

	/* Perform the insertion, and copy the resulting ItemPointer */
	heap_insert(rel, tuple, cid, options, bistate);

	if (should_free)
		pfree(tuple);
}

bool
ts_table_scan_getnextslot(TableScanDesc scan, const ScanDirection direction, TupleTableSlot *slot)
{
	HeapTuple tuple = heap_getnext(scan, direction);

	if (HeapTupleIsValid(tuple))
	{
		/* Tuple managed by heap so shouldn't free when replacing the tuple in
		 * the slot */
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
		return true;
	}

	return false;
}

bool
ts_index_getnext_slot(IndexScanDesc scan, const ScanDirection direction, TupleTableSlot *slot)
{
	HeapTuple tuple = index_getnext(scan, direction);

	if (HeapTupleIsValid(tuple))
	{
		/* Tuple managed by index so shouldn't free when replacing the tuple in
		 * the slot */
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);
		return true;
	}

	return false;
}
