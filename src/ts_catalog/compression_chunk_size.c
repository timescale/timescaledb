/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/catalog.h"
#include "scanner.h"
#include "scan_iterator.h"

static void
init_scan_by_uncompressed_chunk_id(ScanIterator *iterator, int32 uncompressed_chunk_id)
{
	iterator->ctx.index =
		catalog_get_index(ts_catalog_get(), COMPRESSION_CHUNK_SIZE, COMPRESSION_CHUNK_SIZE_PKEY);
	ts_scan_iterator_scan_key_init(iterator,
								   Anum_compression_chunk_size_pkey_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(uncompressed_chunk_id));
}

TSDLLEXPORT int
ts_compression_chunk_size_delete(int32 uncompressed_chunk_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, RowExclusiveLock, CurrentMemoryContext);
	int count = 0;

	init_scan_by_uncompressed_chunk_id(&iterator, uncompressed_chunk_id);
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
	return count;
}

TotalSizes
ts_compression_chunk_size_totals()
{
	TotalSizes sizes = { 0 };
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, AccessExclusiveLock, CurrentMemoryContext);

	ts_scanner_foreach(&iterator)
	{
		bool nulls[Natts_compression_chunk_size];
		Datum values[Natts_compression_chunk_size];
		FormData_compression_chunk_size fd;
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);

		heap_deform_tuple(tuple, ts_scan_iterator_tupledesc(&iterator), values, nulls);
		memset(&fd, 0, sizeof(FormData_compression_chunk_size));

		Assert(!nulls[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)]);
		Assert(
			!nulls[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)]);
		Assert(
			!nulls[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)]);
		Assert(!nulls[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)]);
		Assert(!nulls[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)]);
		Assert(!nulls[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)]);
		fd.uncompressed_heap_size = DatumGetInt64(
			values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_heap_size)]);
		fd.uncompressed_index_size = DatumGetInt64(
			values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_index_size)]);
		fd.uncompressed_toast_size = DatumGetInt64(
			values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_uncompressed_toast_size)]);
		fd.compressed_heap_size = DatumGetInt64(
			values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)]);
		fd.compressed_index_size = DatumGetInt64(
			values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)]);
		fd.compressed_toast_size = DatumGetInt64(
			values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)]);

		sizes.uncompressed_heap_size += fd.uncompressed_heap_size;
		sizes.uncompressed_index_size += fd.uncompressed_index_size;
		sizes.uncompressed_toast_size += fd.uncompressed_toast_size;
		sizes.compressed_heap_size += fd.compressed_heap_size;
		sizes.compressed_index_size += fd.compressed_index_size;
		sizes.compressed_toast_size += fd.compressed_toast_size;

		if (should_free)
			heap_freetuple(tuple);
	}

	return sizes;
}

/* Return the pre-compression row count for the chunk */
int64
ts_compression_chunk_size_row_count(int32 uncompressed_chunk_id)
{
	int found_cnt = 0;
	int64 rowcnt = 0;
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, AccessShareLock, CurrentMemoryContext);
	init_scan_by_uncompressed_chunk_id(&iterator, uncompressed_chunk_id);
	ts_scanner_foreach(&iterator)
	{
		bool nulls[Natts_compression_chunk_size];
		Datum values[Natts_compression_chunk_size];
		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);

		heap_deform_tuple(tuple, ts_scan_iterator_tupledesc(&iterator), values, nulls);
		if (!nulls[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)])
			rowcnt = DatumGetInt64(values[AttrNumberGetAttrOffset(
				Anum_compression_chunk_size_numrows_pre_compression)]);
		if (should_free)
			heap_freetuple(tuple);
		found_cnt++;
	}
	if (found_cnt != 1)
		elog(ERROR,
			 "missing record for chunk with id %d in %s",
			 uncompressed_chunk_id,
			 COMPRESSION_CHUNK_SIZE_TABLE_NAME);
	return rowcnt;
}
