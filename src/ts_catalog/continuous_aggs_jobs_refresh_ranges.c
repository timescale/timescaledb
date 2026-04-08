/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <miscadmin.h>
#include <storage/lmgr.h>
#include <storage/procarray.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "debug_point.h"
#include "scan_iterator.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_aggs_jobs_refresh_ranges.h"

static void
init_scan_by_materialization_id(ScanIterator *iterator, const int32 materialization_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_JOBS_REFRESH_RANGES,
											CONTINUOUS_AGGS_JOBS_REFRESH_RANGES_IDX);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_aggs_jobs_refresh_ranges_idx_materialization_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(materialization_id));
}

static void
ts_cagg_jobs_refresh_ranges_insert(int32 materialization_id, int64 start_range, int64 end_range,
								   int32 pid)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel = table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_JOBS_REFRESH_RANGES),
							  RowExclusiveLock);
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_continuous_aggs_jobs_refresh_ranges];
	bool nulls[Natts_continuous_aggs_jobs_refresh_ranges] = { false };
	CatalogSecurityContext sec_ctx;

	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_jobs_refresh_ranges_materialization_id)] =
		Int32GetDatum(materialization_id);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_jobs_refresh_ranges_start_range)] =
		Int64GetDatum(start_range);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_jobs_refresh_ranges_end_range)] =
		Int64GetDatum(end_range);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_jobs_refresh_ranges_pid)] =
		Int32GetDatum(pid);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_jobs_refresh_ranges_job_id)] =
		Int32GetDatum(0);
	values[AttrNumberGetAttrOffset(Anum_continuous_aggs_jobs_refresh_ranges_created_at)] =
		TimestampTzGetDatum(GetCurrentTimestamp());

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	table_close(rel, NoLock);
}

TSDLLEXPORT void
ts_cagg_jobs_refresh_ranges_delete_by_pid(int32 materialization_id, int32 pid)
{
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_JOBS_REFRESH_RANGES,
													RowExclusiveLock,
													CurrentMemoryContext);

	init_scan_by_materialization_id(&iterator, materialization_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool isnull;
		Datum pid_datum = slot_getattr(ts_scan_iterator_slot(&iterator),
									   Anum_continuous_aggs_jobs_refresh_ranges_pid,
									   &isnull);

		Assert(!isnull);

		if (DatumGetInt32(pid_datum) == pid)
			ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
	ts_scan_iterator_close(&iterator);
}

/*
 * Acquire ShareUpdateExclusiveLock on the jobs_refresh_ranges table (which
 * conflicts with itself) to serialize concurrent registrations, then:
 *   1. Delete entries whose PID is no longer alive.
 *   2. Check whether any live entry for the same mat_id overlaps
 *      [start_range, end_range).
 *   3. If no overlap found, insert our entry and return true.
 *      If an overlapping live entry exists, return false without inserting.
 *
 * The lock is held until end of transaction, protecting the window for the
 * full duration of the refresh.
 */
TSDLLEXPORT bool
ts_cagg_jobs_refresh_ranges_lock_and_register(int32 materialization_id, int64 start_range,
											  int64 end_range, int32 pid)
{
	Catalog *catalog = ts_catalog_get();
	Oid relid = catalog_get_table_id(catalog, CONTINUOUS_AGGS_JOBS_REFRESH_RANGES);

	/*
	 * ShareUpdateExclusiveLock conflicts with itself, serializing the
	 * check-and-insert so two concurrent sessions cannot both pass the overlap
	 * check before either has inserted its row.  Held until end of transaction.
	 */
	LockRelationOid(relid, ShareUpdateExclusiveLock);

	/*
	 * Use RowExclusiveLock for the scanner so that the index is also locked.
	 * The table is already covered by the ShareUpdateExclusiveLock above, but
	 * PostgreSQL requires at least AccessShareLock on each relation opened,
	 * including indexes.
	 */
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGGS_JOBS_REFRESH_RANGES,
								RowExclusiveLock,
								CurrentMemoryContext);
	init_scan_by_materialization_id(&iterator, materialization_id);

	/*
	 * Use the latest snapshot so that we can see rows committed by the
	 * transaction that previously held the ShareUpdateExclusiveLock.
	 * Without this, our scan would use the statement snapshot taken before
	 * we blocked on the lock, missing their inserts entirely.
	 */
	iterator.ctx.snapshot = RegisterSnapshot(GetLatestSnapshot());

	bool overlap_found = false;

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		TupleTableSlot *slot = ts_scan_iterator_slot(&iterator);
		bool isnull;

		Datum pid_datum = slot_getattr(slot, Anum_continuous_aggs_jobs_refresh_ranges_pid, &isnull);
		Assert(!isnull);
		int32 row_pid = DatumGetInt32(pid_datum);

		/*
		 * Remove entries whose backend is no longer alive, or entries of the same PID
		 * (due to PID reuse).
		 */
		if (BackendPidGetProc(row_pid) == NULL || row_pid == MyProcPid)
		{
			ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
			continue;
		}

		/*
		 * Check for overlap: [row_start, row_end) overlaps [start_range, end_range)
		 * iff row_start < end_range AND row_end > start_range.
		 */
		Datum start_datum =
			slot_getattr(slot, Anum_continuous_aggs_jobs_refresh_ranges_start_range, &isnull);
		Assert(!isnull);
		int64 row_start = DatumGetInt64(start_datum);

		Datum end_datum =
			slot_getattr(slot, Anum_continuous_aggs_jobs_refresh_ranges_end_range, &isnull);
		Assert(!isnull);
		int64 row_end = DatumGetInt64(end_datum);

		if (row_start < end_range && row_end > start_range)
		{
			overlap_found = true;
			break;
		}
	}
	ts_scan_iterator_close(&iterator);
	UnregisterSnapshot(iterator.ctx.snapshot);

	if (overlap_found)
		return false;

	ts_cagg_jobs_refresh_ranges_insert(materialization_id, start_range, end_range, pid);
        DEBUG_ERROR_INJECTION("cagg_refresh_fail_in_registration");
	return true;
}
