/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/heapam.h>
#include <access/htup_details.h>
#include <utils/snapmgr.h>

#include "export.h"
#include "hypercore/arrow_tts.h"
#include "hypercore/hypercore_handler.h"
#include "test_utils.h"

/*
 * Test that Hypercore rescan API works correctly.
 *
 * In particular, test that scanning only non-compressed data across rescans
 * work.
 */
static void
test_rescan_hypercore(Oid relid)
{
	Relation rel = table_open(relid, AccessShareLock);
	TupleTableSlot *slot = table_slot_create(rel, NULL);
	TableScanDesc scan;
	Snapshot snapshot = GetTransactionSnapshot();
	ScanKeyData scankey = {
		/* Let compression TAM know it should only return tuples from the
		 * non-compressed relation. No actual scankey necessary */
		.sk_flags = SK_NO_COMPRESSED,
	};
	unsigned int compressed_tuple_count = 0;
	unsigned int noncompressed_tuple_count = 0;
	unsigned int prev_noncompressed_tuple_count = 0;
	unsigned int prev_compressed_tuple_count = 0;

	TestAssertTrue(TTS_IS_ARROWTUPLE(slot));

	/* Scan only non-compressed data */
	scan = table_beginscan(rel, snapshot, 0, &scankey);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		if (is_compressed_tid(&slot->tts_tid))
			compressed_tuple_count++;
		else
			noncompressed_tuple_count++;
	}

	TestAssertTrue(compressed_tuple_count == 0);
	TestAssertTrue(noncompressed_tuple_count > 0);
	prev_noncompressed_tuple_count = noncompressed_tuple_count;
	prev_compressed_tuple_count = compressed_tuple_count;
	compressed_tuple_count = 0;
	noncompressed_tuple_count = 0;

	/* Rescan only non-compressed data */
	table_rescan(scan, &scankey);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		if (is_compressed_tid(&slot->tts_tid))
			compressed_tuple_count++;
		else
			noncompressed_tuple_count++;
	}

	TestAssertTrue(compressed_tuple_count == 0);
	TestAssertTrue(noncompressed_tuple_count == prev_noncompressed_tuple_count);
	TestAssertTrue(compressed_tuple_count == prev_compressed_tuple_count);
	prev_noncompressed_tuple_count = noncompressed_tuple_count;
	prev_compressed_tuple_count = compressed_tuple_count;
	compressed_tuple_count = 0;
	noncompressed_tuple_count = 0;

	/* Rescan only non-compressed data even though giving no new scan key */
	table_rescan(scan, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		if (is_compressed_tid(&slot->tts_tid))
			compressed_tuple_count++;
		else
			noncompressed_tuple_count++;
	}

	TestAssertTrue(compressed_tuple_count == 0);
	TestAssertTrue(noncompressed_tuple_count == prev_noncompressed_tuple_count);
	TestAssertTrue(compressed_tuple_count == prev_compressed_tuple_count);
	prev_noncompressed_tuple_count = noncompressed_tuple_count;
	prev_compressed_tuple_count = compressed_tuple_count;
	compressed_tuple_count = 0;
	noncompressed_tuple_count = 0;

	/* Rescan both compressed and non-compressed data by specifying new flag */
	scankey.sk_flags = 0;
	table_rescan(scan, &scankey);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		if (is_compressed_tid(&slot->tts_tid))
			compressed_tuple_count++;
		else
			noncompressed_tuple_count++;
	}

	TestAssertTrue(noncompressed_tuple_count == prev_noncompressed_tuple_count);
	TestAssertTrue(compressed_tuple_count > 0);

	table_endscan(scan);
	table_close(rel, NoLock);
	ExecDropSingleTupleTableSlot(slot);
}

TS_FUNCTION_INFO_V1(ts_test_hypercore);

Datum
ts_test_hypercore(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	test_rescan_hypercore(relid);
	PG_RETURN_VOID();
}
