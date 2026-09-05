/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "cache.h"
#include "dimension.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "scan_iterator.h"
#include "time_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/hypertable_cagg_settings.h"
#include "utils.h"

/*
 * Whether typid is allowed as a granular-refresh tracking column.
 *
 * TIMESTAMP/TIMESTAMPTZ are deliberately excluded: their output
 * is DateStyle- (and for TIMESTAMPTZ, timezone-) dependent
 * If we want to add this, need canonical-encoding handling in
 * record_tenant_invalidation()
 */
TSDLLEXPORT bool
ts_tenant_type_is_supported(Oid typid)
{
	switch (typid)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case UUIDOID:
		case DATEOID:
			return true;
		default:
			return false;
	}
}

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

static void
hypertable_cagg_settings_formdata_fill(FormData_hypertable_cagg_settings *fd, const TupleInfo *ti)
{
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	Datum values[Natts_hypertable_cagg_settings];
	bool nulls[Natts_hypertable_cagg_settings] = { false };

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	fd->hypertable_id =
		DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_hypertable_cagg_settings_hypertable_id)]);
	namestrcpy(&fd->granular_refresh_column,
			   NameStr(*DatumGetName(values[AttrNumberGetAttrOffset(
				   Anum_hypertable_cagg_settings_granular_refresh_column)])));
	fd->granular_refresh_start_offset = DatumGetTextPCopy(values[AttrNumberGetAttrOffset(
		Anum_hypertable_cagg_settings_granular_refresh_start_offset)]);
	fd->granular_refresh_end_offset = DatumGetTextPCopy(
		values[AttrNumberGetAttrOffset(Anum_hypertable_cagg_settings_granular_refresh_end_offset)]);

	if (should_free)
	{
		heap_freetuple(tuple);
	}
}

static HeapTuple
hypertable_cagg_settings_formdata_make_tuple(const FormData_hypertable_cagg_settings *fd,
											 TupleDesc desc)
{
	Datum values[Natts_hypertable_cagg_settings] = { 0 };
	bool nulls[Natts_hypertable_cagg_settings] = { false };

	values[AttrNumberGetAttrOffset(Anum_hypertable_cagg_settings_hypertable_id)] =
		Int32GetDatum(fd->hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_hypertable_cagg_settings_granular_refresh_column)] =
		NameGetDatum(&fd->granular_refresh_column);
	values[AttrNumberGetAttrOffset(Anum_hypertable_cagg_settings_granular_refresh_start_offset)] =
		PointerGetDatum(fd->granular_refresh_start_offset);
	values[AttrNumberGetAttrOffset(Anum_hypertable_cagg_settings_granular_refresh_end_offset)] =
		PointerGetDatum(fd->granular_refresh_end_offset);

	return heap_form_tuple(desc, values, nulls);
}

TSDLLEXPORT bool
ts_hypertable_cagg_settings_get(int32 hypertable_id, FormData_hypertable_cagg_settings *form)
{
	bool found = false;
	ScanIterator iterator =
		ts_scan_iterator_create(HYPERTABLE_CAGG_SETTINGS, AccessShareLock, CurrentMemoryContext);

	init_scan_by_hypertable_id(&iterator, hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);

		hypertable_cagg_settings_formdata_fill(form, ti);
		found = true;
	}
	ts_scan_iterator_close(&iterator);

	return found;
}

TSDLLEXPORT void
ts_hypertable_cagg_settings_insert(const FormData_hypertable_cagg_settings *form)
{
	CatalogSecurityContext sec_ctx;
	Catalog *catalog = ts_catalog_get();
	Relation rel =
		table_open(catalog_get_table_id(catalog, HYPERTABLE_CAGG_SETTINGS), RowExclusiveLock);
	HeapTuple tuple = hypertable_cagg_settings_formdata_make_tuple(form, RelationGetDescr(rel));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert(rel, tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(tuple);
	table_close(rel, NoLock);
}

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

static Datum
cast_offset_from_text(const text *offset, Oid dimtype)
{
	Datum cstr = CStringGetDatum(text_to_cstring(offset));

	switch (dimtype)
	{
		case INT2OID:
			return DirectFunctionCall1(int2in, cstr);
		case INT4OID:
			return DirectFunctionCall1(int4in, cstr);
		case INT8OID:
			return DirectFunctionCall1(int8in, cstr);
		default:
			return DirectFunctionCall3(interval_in,
									   cstr,
									   ObjectIdGetDatum(InvalidOid),
									   Int32GetDatum(-1));
	}
}

/*
 * Late-arrival window [*window_start, *window_end) to seed/flush the tracker.
 * window = [now - start_offset, now - end_offset). Integer dimensions with no
 * integer_now func are skipped (nothing tracked). Mirrors the offset->bound
 * conversion used by the refresh policies (get_time_from_interval).
 */
TSDLLEXPORT bool
ts_hypertable_cagg_settings_get_tenant_tracking_window(int32 hypertable_id, int64 *window_start,
													   int64 *window_end)
{
	FormData_hypertable_cagg_settings settings;
	Cache *ht_cache;
	Hypertable *ht;
	bool applicable = false;

	/* Empty window (start > end) => nothing tracked. */
	*window_start = PG_INT64_MAX;
	*window_end = PG_INT64_MIN;

	if (!ts_hypertable_cagg_settings_get(hypertable_id, &settings))
	{
		return false;
	}

	ht_cache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry_by_id(ht_cache, hypertable_id);
	if (ht != NULL)
	{
		const Dimension *open_dim = hyperspace_get_open_dimension(ht->space, 0);
		Oid dimtype = ts_dimension_get_partition_type(open_dim);
		Datum start_datum = cast_offset_from_text(settings.granular_refresh_start_offset, dimtype);
		Datum end_datum = cast_offset_from_text(settings.granular_refresh_end_offset, dimtype);

		if (IS_INTEGER_TYPE(dimtype))
		{
			Oid now_func = ts_get_integer_now_func(open_dim, false);

			if (OidIsValid(now_func))
			{
				int64 start_offset = ts_interval_value_to_internal(start_datum, dimtype);
				int64 end_offset = ts_interval_value_to_internal(end_datum, dimtype);

				/* integer_now is a SQL/PLpgSQL function; executing it needs an
				 * active snapshot, which the commit drain has not pushed. */
				bool pushed = !ActiveSnapshotSet();

				if (pushed)
				{
					PushActiveSnapshot(GetTransactionSnapshot());
				}
				*window_start =
					ts_subtract_integer_from_now_saturating(now_func, start_offset, dimtype);
				*window_end =
					ts_subtract_integer_from_now_saturating(now_func, end_offset, dimtype);
				if (pushed)
				{
					PopActiveSnapshot();
				}
				applicable = true;
			}
		}
		else if (IS_TIMESTAMP_TYPE(dimtype))
		{
			*window_start =
				ts_time_value_to_internal(ts_subtract_interval_from_now(DatumGetIntervalP(
																			start_datum),
																		dimtype),
										  dimtype);
			*window_end = ts_time_value_to_internal(ts_subtract_interval_from_now(DatumGetIntervalP(
																					  end_datum),
																				  dimtype),
													dimtype);
			applicable = true;
		}
	}
	ts_cache_release(&ht_cache);

	/*
	 * The offsets are compared against each other as microseconds when the
	 * settings are validated and stored (a month counts as 30 days, a day as 24 hours),
	 * while the bounds above come from calendar-aware subtraction. Therefore,
	 * at runtime depending on now(), end offset can endup smaller than start
	 * offset. For example, "1 month" end offset is smaller than
	 * "29 days" start offset in February, while is valid with a general 30-day
	 * month assumption. Similar for a 23-day when clock moves forward in Spring
	 * offset in days, or a 23 hour day across a DST spring forward. In such a
	 * case, treat the late window as empty: no tenant is recorded
	 * into the generation and every invalidation keeps seqnum 0, which is the
	 * full-refresh path.
	 */
	if (applicable && *window_start >= *window_end)
	{
		ereport(LOG,
				(errmsg("per-tenant tracking window for hypertable %d is empty", hypertable_id),
				 errdetail("The configured offsets resolve to a window start at or after its "
						   "end."),
				 errhint("No tenant is tracked while the window is empty; continuous aggregate "
						 "refreshes fall back to the full invalidation log.")));
		*window_start = PG_INT64_MAX;
		*window_end = PG_INT64_MIN;
	}

	return applicable;
}

TSDLLEXPORT bool
ts_hypertable_cagg_settings_get_tenant_tracking_column(int32 hypertable_id,
													   const char **column_name)
{
	FormData_hypertable_cagg_settings settings;
	bool applicable = false;
	*column_name = NULL;

	if (!ts_hypertable_cagg_settings_get(hypertable_id, &settings))
	{
		return false;
	}

	*column_name = pstrdup(NameStr(settings.granular_refresh_column));
	applicable = true;

	return applicable;
}
