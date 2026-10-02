/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <access/table.h>
#include <access/tableam.h>
#include <utils/memutils.h>
#include <utils/snapmgr.h>

#include "chunk.h"
#include "copy.h"
#include "hypertable.h"
#include "test_utils.h"

TS_TEST_FN(ts_test_copy_invalidation)
{
	Relation rel = table_open(PG_GETARG_OID(0), AccessShareLock);
	Chunk *chunk = ts_chunk_get_by_relid(RelationGetRelid(rel), true);
	Hypertable *ht = ts_hypertable_get_by_id(chunk->fd.hypertable_id);
	TupleTableSlot *source = table_slot_create(rel, NULL);
	TupleTableSlot *slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsVirtual);
	TableScanDesc scan = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	bool isnull;

	TestAssertTrue(table_scan_getnextslot(scan, ForwardScanDirection, source));
	ExecCopySlot(slot, source);

	/* Warm caches before measuring live memory, excluding reusable free space. */
	MemoryContext context = AllocSetContextCreate(CurrentMemoryContext,
												  "COPY invalidation test",
												  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldcontext = MemoryContextSwitchTo(context);
	ts_copy_invalidate_cagg_test(ht, rel, slot);
	MemoryContextCounters before = { 0 }, after = { 0 };
	context->methods->stats(context, NULL, NULL, &before, false);
	ts_copy_invalidate_cagg_test(ht, rel, slot);
	context->methods->stats(context, NULL, NULL, &after, false);
	MemoryContextSwitchTo(oldcontext);
	TestAssertInt64Eq(after.totalspace - after.freespace, before.totalspace - before.freespace);
	MemoryContextDelete(context);

	/* Invalidation must leave the slot's tuple intact. The fixture uses bigint time. */
	TestAssertInt64Eq(DatumGetInt64(slot_getattr(slot, 1, &isnull)),
					  DatumGetInt64(slot_getattr(source, 1, &isnull)));
	TestAssertTrue(!isnull);

	ExecDropSingleTupleTableSlot(slot);
	ExecDropSingleTupleTableSlot(source);
	table_endscan(scan);
	table_close(rel, AccessShareLock);
	PG_RETURN_VOID();
}
