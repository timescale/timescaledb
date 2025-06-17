/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <executor/tuptable.h>

#include "export.h"
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

TSDLLEXPORT bool
ts_compression_chunk_size_get(int32 chunk_id, Form_compression_chunk_size form)
{
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, AccessExclusiveLock, CurrentMemoryContext);
	bool found = false;

	Assert(form != NULL);

	init_scan_by_uncompressed_chunk_id(&iterator, chunk_id);
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool should_free;
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		memcpy(form, GETSTRUCT(tuple), sizeof(*form));
		found = true;
		Assert(form->chunk_id == chunk_id);

		if (should_free)
			heap_freetuple(tuple);

		break;
	}

	ts_scan_iterator_close(&iterator);

	return found;
}

TSDLLEXPORT bool
ts_compression_chunk_size_update(int32 chunk_id, Form_compression_chunk_size form)
{
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, RowExclusiveLock, CurrentMemoryContext);
	bool found = false;
	CatalogSecurityContext sec_ctx;

	Assert(form != NULL);

	init_scan_by_uncompressed_chunk_id(&iterator, chunk_id);
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		bool should_free;
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		HeapTuple copy = heap_copytuple(tuple);
		Form_compression_chunk_size tupform = (Form_compression_chunk_size) GETSTRUCT(copy);

		/* Don't update chunk IDs so copy from existing tuple */
		form->chunk_id = tupform->chunk_id;
		form->compressed_chunk_id = tupform->compressed_chunk_id;

		memcpy(tupform, form, sizeof(FormData_compression_chunk_size));
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		ts_catalog_update_tid_only(ti->scanrel, ts_scanner_get_tuple_tid(ti), copy);
		ts_catalog_restore_user(&sec_ctx);
		found = true;

		heap_freetuple(copy);

		if (should_free)
			heap_freetuple(tuple);

		break;
	}

	ts_scan_iterator_close(&iterator);

	return found;
}
