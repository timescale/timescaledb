/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <miscadmin.h>

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

TSDLLEXPORT void
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

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	table_close(rel, NoLock);
}

TSDLLEXPORT void
ts_cagg_jobs_refresh_ranges_delete_by_materialization_id(int32 materialization_id)
{
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_JOBS_REFRESH_RANGES,
													RowExclusiveLock,
													CurrentMemoryContext);

	init_scan_by_materialization_id(&iterator, materialization_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
	ts_scan_iterator_close(&iterator);
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
