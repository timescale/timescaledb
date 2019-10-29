/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include "compression_chunk_size.h"
#include "catalog.h"
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
		ts_catalog_delete(ti->scanrel, ti->tuple);
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
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		FormData_compression_chunk_size *fd = STRUCT_FROM_TUPLE(ti->tuple,
																ti->mctx,
																FormData_compression_chunk_size,
																FormData_compression_chunk_size);
		sizes.uncompressed_heap_size += fd->uncompressed_heap_size;
		sizes.uncompressed_index_size += fd->uncompressed_index_size;
		sizes.uncompressed_toast_size += fd->uncompressed_toast_size;
		sizes.compressed_heap_size += fd->compressed_heap_size;
		sizes.compressed_index_size += fd->compressed_index_size;
		sizes.compressed_toast_size += fd->compressed_toast_size;
	}

	return sizes;
}
