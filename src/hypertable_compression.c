/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include "hypertable.h"
#include "hypertable_cache.h"
#include "catalog.h"
#include "hypertable_compression.h"
#include "scanner.h"
#include "scan_iterator.h"

static void
hypertable_compression_fill_from_tuple(FormData_hypertable_compression *fd, TupleInfo *ti)
{
	Datum values[Natts_hypertable_compression];
	bool isnulls[Natts_hypertable_compression];

	heap_deform_tuple(ti->tuple, ti->desc, values, isnulls);

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
		FormData_hypertable_compression *data =
			(FormData_hypertable_compression *) GETSTRUCT(ti->tuple);
		if (data->hypertable_id != htid)
			continue;
		colfd = palloc0(sizeof(FormData_hypertable_compression));
		hypertable_compression_fill_from_tuple(colfd, ti);
		fdlist = lappend(fdlist, colfd);
	}
	return fdlist;
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
		ts_catalog_delete(ti->scanrel, ti->tuple);
		count++;
	}
	return count > 0;
}
