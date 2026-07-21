/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>

#include "scan_iterator.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/hypertable_cagg_settings.h"

static void
init_scan_by_hypertable_id(ScanIterator *iterator, const int32 hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											HYPERTABLE_CAGG_SETTINGS,
											HYPERTABLE_CAGG_SETTINGS_PKEY);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_hypertable_cagg_settings_pkey_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(hypertable_id));
}

/* Delete the settings row for a hypertable, if any. */
TSDLLEXPORT void
ts_hypertable_cagg_settings_delete(int32 hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(HYPERTABLE_CAGG_SETTINGS, RowExclusiveLock, CurrentMemoryContext);

	init_scan_by_hypertable_id(&iterator, hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);

		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
	ts_scan_iterator_close(&iterator);
}
