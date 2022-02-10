/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/builtins.h>

#include "hypertable.h"
#include "hypertable_cache.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/hypertable_compression.h"
#include "scanner.h"
#include "scan_iterator.h"

static void
hypertable_compression_fill_from_tuple(FormData_hypertable_compression *fd, TupleInfo *ti)
{
	Datum values[Natts_hypertable_compression];
	bool isnulls[Natts_hypertable_compression];
	bool should_free;
	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, isnulls);

	Assert(!isnulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_hypertable_id)]);
	Assert(!isnulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_attname)]);
	Assert(!isnulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_algo_id)]);

	fd->hypertable_id =
		DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_hypertable_compression_hypertable_id)]);
	memcpy(&fd->attname,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_hypertable_compression_attname)]),
		   NAMEDATALEN);
	fd->algo_id =
		DatumGetInt16(values[AttrNumberGetAttrOffset(Anum_hypertable_compression_algo_id)]);

	if (isnulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_segmentby_column_index)])
		fd->segmentby_column_index = 0;
	else
		fd->segmentby_column_index = DatumGetInt16(
			values[AttrNumberGetAttrOffset(Anum_hypertable_compression_segmentby_column_index)]);

	if (isnulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_column_index)])
		fd->orderby_column_index = 0;
	else
	{
		Assert(!isnulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_asc)]);
		Assert(!isnulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_nullsfirst)]);

		fd->orderby_column_index = DatumGetInt16(
			values[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_column_index)]);
		fd->orderby_asc =
			BoolGetDatum(values[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_asc)]);
		fd->orderby_nullsfirst = BoolGetDatum(
			values[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_nullsfirst)]);
	}

	if (should_free)
		heap_freetuple(tuple);
}

TSDLLEXPORT void
ts_hypertable_compression_fill_tuple_values(FormData_hypertable_compression *fd, Datum *values,
											bool *nulls)
{
	memset(nulls, 0, sizeof(bool) * Natts_hypertable_compression);
	values[AttrNumberGetAttrOffset(Anum_hypertable_compression_hypertable_id)] =
		Int32GetDatum(fd->hypertable_id);

	values[AttrNumberGetAttrOffset(Anum_hypertable_compression_attname)] =
		NameGetDatum(&fd->attname);
	values[AttrNumberGetAttrOffset(Anum_hypertable_compression_algo_id)] =
		Int16GetDatum(fd->algo_id);
	if (fd->segmentby_column_index > 0)
	{
		values[AttrNumberGetAttrOffset(Anum_hypertable_compression_segmentby_column_index)] =
			Int16GetDatum(fd->segmentby_column_index);
	}
	else
	{
		nulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_segmentby_column_index)] = true;
	}
	if (fd->orderby_column_index > 0)
	{
		values[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_column_index)] =
			Int16GetDatum(fd->orderby_column_index);
		values[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_asc)] =
			BoolGetDatum(fd->orderby_asc);
		values[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_nullsfirst)] =
			BoolGetDatum(fd->orderby_nullsfirst);
	}
	else
	{
		nulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_column_index)] = true;
		nulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_asc)] = true;
		nulls[AttrNumberGetAttrOffset(Anum_hypertable_compression_orderby_nullsfirst)] = true;
	}
}

/* returns length of list and fills passed in list with pointers
 * to FormData_hypertable_compression
 */
TSDLLEXPORT List *
ts_hypertable_compression_get(int32 htid)
{
	List *fdlist = NIL;
	FormData_hypertable_compression *colfd = NULL;
	ScanIterator iterator =
		ts_scan_iterator_create(HYPERTABLE_COMPRESSION, AccessShareLock, CurrentMemoryContext);
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), HYPERTABLE_COMPRESSION, HYPERTABLE_COMPRESSION_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_hypertable_compression_pkey_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(htid));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);
		FormData_hypertable_compression *data =
			(FormData_hypertable_compression *) GETSTRUCT(tuple);
		MemoryContext oldmctx;

		if (data->hypertable_id != htid)
			continue;

		oldmctx = MemoryContextSwitchTo(ts_scan_iterator_get_result_memory_context(&iterator));
		colfd = palloc0(sizeof(FormData_hypertable_compression));
		hypertable_compression_fill_from_tuple(colfd, ti);
		fdlist = lappend(fdlist, colfd);
		MemoryContextSwitchTo(oldmctx);
	}
	return fdlist;
}

TSDLLEXPORT FormData_hypertable_compression *
ts_hypertable_compression_get_by_pkey(int32 htid, const char *attname)
{
	FormData_hypertable_compression *colfd = NULL;
	ScanIterator iterator =
		ts_scan_iterator_create(HYPERTABLE_COMPRESSION, AccessShareLock, CurrentMemoryContext);
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), HYPERTABLE_COMPRESSION, HYPERTABLE_COMPRESSION_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_hypertable_compression_pkey_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(htid));
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_hypertable_compression_pkey_attname,
								   BTEqualStrategyNumber,
								   F_NAMEEQ,
								   CStringGetDatum(attname));

	ts_scanner_start_scan(&iterator.ctx);
	TupleInfo *ti = ts_scanner_next(&iterator.ctx);
	if (!ti)
		return NULL;

	colfd = palloc0(sizeof(FormData_hypertable_compression));
	hypertable_compression_fill_from_tuple(colfd, ti);
	ts_scan_iterator_close(&iterator);

	return colfd;
}

TSDLLEXPORT bool
ts_hypertable_compression_delete_by_hypertable_id(int32 htid)
{
	int count = 0;
	ScanIterator iterator =
		ts_scan_iterator_create(HYPERTABLE_COMPRESSION, RowExclusiveLock, CurrentMemoryContext);
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), HYPERTABLE_COMPRESSION, HYPERTABLE_COMPRESSION_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_hypertable_compression_pkey_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(htid));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
		count++;
	}
	return count > 0;
}

TSDLLEXPORT bool
ts_hypertable_compression_delete_by_pkey(int32 htid, const char *attname)
{
	ScanIterator iterator =
		ts_scan_iterator_create(HYPERTABLE_COMPRESSION, RowExclusiveLock, CurrentMemoryContext);
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), HYPERTABLE_COMPRESSION, HYPERTABLE_COMPRESSION_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_hypertable_compression_pkey_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(htid));
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_hypertable_compression_pkey_attname,
								   BTEqualStrategyNumber,
								   F_NAMEEQ,
								   CStringGetDatum(attname));

	ts_scanner_start_scan(&iterator.ctx);
	TupleInfo *ti = ts_scanner_next(&iterator.ctx);
	if (!ti)
		return false;

	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	ts_scan_iterator_close(&iterator);

	return true;
}

TSDLLEXPORT void
ts_hypertable_compression_rename_column(int32 htid, char *old_column_name, char *new_column_name)
{
	bool found = false;
	ScanIterator iterator =
		ts_scan_iterator_create(HYPERTABLE_COMPRESSION, AccessShareLock, CurrentMemoryContext);
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), HYPERTABLE_COMPRESSION, HYPERTABLE_COMPRESSION_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_hypertable_compression_pkey_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(htid));

	ts_scanner_foreach(&iterator)
	{
		NameData name_new_column_name;
		bool isnull;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		Datum datum = slot_getattr(ti->slot, Anum_hypertable_compression_attname, &isnull);
		char *attname = NameStr(*DatumGetName(datum));
		if (strncmp(attname, old_column_name, NAMEDATALEN) == 0)
		{
			Datum values[Natts_hypertable_compression];
			bool isnulls[Natts_hypertable_compression];
			bool repl[Natts_hypertable_compression] = { false };
			bool should_free;
			HeapTuple tuple, new_tuple;
			TupleDesc tupdesc = ts_scanner_get_tupledesc(ti);
			found = true;
			tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
			heap_deform_tuple(tuple, tupdesc, values, isnulls);

			namestrcpy(&name_new_column_name, new_column_name);
			values[AttrNumberGetAttrOffset(Anum_hypertable_compression_attname)] =
				NameGetDatum(&name_new_column_name);
			repl[AttrNumberGetAttrOffset(Anum_hypertable_compression_attname)] = true;
			new_tuple = heap_modify_tuple(tuple, tupdesc, values, isnulls, repl);
			ts_catalog_update(ti->scanrel, new_tuple);

			if (should_free)
				heap_freetuple(new_tuple);
		}
	}
	if (found == false)
		elog(ERROR, "column %s not found in hypertable_compression catalog table", old_column_name);
}
