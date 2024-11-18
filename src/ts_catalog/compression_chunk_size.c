/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include "scan_iterator.h"
#include "scanner.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_chunk_size.h"

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
		ts_catalog_delete_tid_only(ti->scanrel, ts_scanner_get_tuple_tid(ti));
		count++;
	}

	/* Make catalog changes visible */
	if (count > 0)
		CommandCounterIncrement();

	return count;
}
