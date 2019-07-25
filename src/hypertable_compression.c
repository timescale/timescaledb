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
	HeapTuple tuple = ti->tuple;
	TupleDesc desc = ti->desc;
	Datum val;
	bool isnull;
	memcpy((void *) fd, GETSTRUCT(tuple), sizeof(FormData_hypertable_compression));
	/* copy the part that could have null values explictly */
	val = heap_getattr(tuple, Anum_hypertable_compression_segmentby_column_index, desc, &isnull);
	if (isnull)
		fd->segmentby_column_index = 0;
	else
		fd->segmentby_column_index = DatumGetInt16(val);
	val = heap_getattr(tuple, Anum_hypertable_compression_orderby_column_index, desc, &isnull);
	if (isnull)
		fd->orderby_column_index = 0;
	else
	{
		fd->orderby_column_index = DatumGetInt16(val);
		val = heap_getattr(tuple, Anum_hypertable_compression_orderby_asc, desc, &isnull);
		fd->orderby_asc = BoolGetDatum(val);
		val = heap_getattr(tuple, Anum_hypertable_compression_orderby_nullsfirst, desc, &isnull);
		fd->orderby_nullsfirst = BoolGetDatum(val);
	}
}

void
hypertable_compression_fill_tuple_values(FormData_hypertable_compression *fd, Datum *values,
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
List *
get_hypertablecompression_info(int32 htid)
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
