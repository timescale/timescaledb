/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/builtins.h>

#include "hypertable.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"
#include <chunk.h>

static ScanTupleResult compression_settings_tuple_update(TupleInfo *ti, void *data);
static HeapTuple compression_settings_formdata_make_tuple(const FormData_compression_settings *fd,
														  TupleDesc desc);

bool
ts_compression_settings_equal(const CompressionSettings *left, const CompressionSettings *right)
{
	return ts_array_equal(left->fd.segmentby, right->fd.segmentby) &&
		   ts_array_equal(left->fd.orderby, right->fd.orderby) &&
		   ts_array_equal(left->fd.orderby_desc, right->fd.orderby_desc) &&
		   ts_array_equal(left->fd.orderby_nullsfirst, right->fd.orderby_nullsfirst);
}

CompressionSettings *
ts_compression_settings_materialize(Oid ht_relid, Oid dst_relid)
{
	CompressionSettings *src = ts_compression_settings_get(ht_relid);
	Assert(src);
	CompressionSettings *dst = ts_compression_settings_create(dst_relid,
															  src->fd.segmentby,
															  src->fd.orderby,
															  src->fd.orderby_desc,
															  src->fd.orderby_nullsfirst);

	return dst;
}

CompressionSettings *
ts_compression_settings_create(Oid relid, ArrayType *segmentby, ArrayType *orderby,
							   ArrayType *orderby_desc, ArrayType *orderby_nullsfirst)
{
	Catalog *catalog = ts_catalog_get();
	CatalogSecurityContext sec_ctx;
	Relation rel;
	FormData_compression_settings fd;

	Assert(OidIsValid(relid));

	/*
	 * The default compression settings will always have orderby settings but the user may have
	 * chosen to overwrite it. For both cases all 3 orderby arrays must either have the same number
	 * of entries or be all NULL.
	 */
	Assert((orderby && orderby_desc && orderby_nullsfirst) ||
		   (!orderby && !orderby_desc && !orderby_nullsfirst));

	fd.relid = relid;
	fd.segmentby = segmentby;
	fd.orderby = orderby;
	fd.orderby_desc = orderby_desc;
	fd.orderby_nullsfirst = orderby_nullsfirst;

	rel = table_open(catalog_get_table_id(catalog, COMPRESSION_SETTINGS), RowExclusiveLock);

	HeapTuple new_tuple = compression_settings_formdata_make_tuple(&fd, RelationGetDescr(rel));
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert(rel, new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);

	table_close(rel, RowExclusiveLock);

	return ts_compression_settings_get(relid);
}

static void
compression_settings_fill_from_tuple(CompressionSettings *settings, TupleInfo *ti)
{
	FormData_compression_settings *fd = &settings->fd;
	Datum values[Natts_compression_settings];
	bool nulls[Natts_compression_settings];
	bool should_free;

	HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);

	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	MemoryContext old = MemoryContextSwitchTo(ti->mctx);

	fd->relid = DatumGetObjectId(values[AttrNumberGetAttrOffset(Anum_compression_settings_relid)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_segmentby)])
		fd->segmentby = NULL;
	else
		fd->segmentby = DatumGetArrayTypeP(
			values[AttrNumberGetAttrOffset(Anum_compression_settings_segmentby)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_orderby)])
		fd->orderby = NULL;
	else
		fd->orderby =
			DatumGetArrayTypeP(values[AttrNumberGetAttrOffset(Anum_compression_settings_orderby)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_desc)])
		fd->orderby_desc = NULL;
	else
		fd->orderby_desc = DatumGetArrayTypeP(
			values[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_desc)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_nullsfirst)])
		fd->orderby_nullsfirst = NULL;
	else
		fd->orderby_nullsfirst = DatumGetArrayTypeP(
			values[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_nullsfirst)]);

	MemoryContextSwitchTo(old);

	if (should_free)
		heap_freetuple(tuple);
}

TSDLLEXPORT CompressionSettings *
ts_compression_settings_get(Oid relid)
{
	CompressionSettings *settings = NULL;
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_SETTINGS, AccessShareLock, CurrentMemoryContext);
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), COMPRESSION_SETTINGS, COMPRESSION_SETTINGS_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_compression_settings_pkey_relid,
								   BTEqualStrategyNumber,
								   F_OIDEQ,
								   ObjectIdGetDatum(relid));

	ts_scanner_start_scan(&iterator.ctx);
	TupleInfo *ti = ts_scanner_next(&iterator.ctx);
	if (!ti)
		return NULL;

	settings = palloc0(sizeof(CompressionSettings));
	compression_settings_fill_from_tuple(settings, ti);
	ts_scan_iterator_close(&iterator);

	return settings;
}

TSDLLEXPORT bool
ts_compression_settings_delete(Oid relid)
{
	if (!OidIsValid(relid))
		return false;

	int count = 0;
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_SETTINGS, RowExclusiveLock, CurrentMemoryContext);
	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), COMPRESSION_SETTINGS, COMPRESSION_SETTINGS_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_compression_settings_pkey_relid,
								   BTEqualStrategyNumber,
								   F_OIDEQ,
								   ObjectIdGetDatum(relid));

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
		count++;
	}
	return count > 0;
}

TSDLLEXPORT void
ts_compression_settings_rename_column_hypertable(Hypertable *ht, char *old, char *new)
{
	ts_compression_settings_rename_column(ht->main_table_relid, old, new);
	if (ht->fd.compressed_hypertable_id)
	{
		ListCell *lc;
		List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.compressed_hypertable_id);
		foreach (lc, chunks)
		{
			Chunk *chunk = lfirst(lc);
			ts_compression_settings_rename_column(chunk->table_id, old, new);
		}
	}
}

TSDLLEXPORT void
ts_compression_settings_rename_column(Oid relid, char *old, char *new)
{
	CompressionSettings *settings = ts_compression_settings_get(relid);

	if (!settings)
		return;

	settings->fd.segmentby = ts_array_replace_text(settings->fd.segmentby, old, new);

	settings->fd.orderby = ts_array_replace_text(settings->fd.orderby, old, new);

	ts_compression_settings_update(settings);
}

TSDLLEXPORT int
ts_compression_settings_update(CompressionSettings *settings)
{
	Catalog *catalog = ts_catalog_get();
	FormData_compression_settings *fd = &settings->fd;
	ScanKeyData scankey[1];

	if (settings->fd.orderby && settings->fd.segmentby)
	{
		Datum datum;
		bool isnull;

		ArrayIterator it = array_create_iterator(settings->fd.orderby, 0, NULL);
		while (array_iterate(it, &datum, &isnull))
		{
			if (ts_array_is_member(settings->fd.segmentby, TextDatumGetCString(datum)))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use column \"%s\" for both ordering and segmenting",
								TextDatumGetCString(datum)),
						 errhint("Use separate columns for the timescaledb.compress_orderby and"
								 " timescaledb.compress_segmentby options.")));
		}
	}

	/*
	 * The default compression settings will always have orderby settings but the user may have
	 * chosen to overwrite it. For both cases all 3 orderby arrays must either have the same number
	 * of entries or be all NULL.
	 */
	Assert(
		(settings->fd.orderby && settings->fd.orderby_desc && settings->fd.orderby_nullsfirst) ||
		(!settings->fd.orderby && !settings->fd.orderby_desc && !settings->fd.orderby_nullsfirst));

	ScanKeyInit(&scankey[0],
				Anum_compression_settings_pkey_relid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(fd->relid));

	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, COMPRESSION_SETTINGS),
		.index = catalog_get_index(catalog, COMPRESSION_SETTINGS, COMPRESSION_SETTINGS_PKEY),
		.nkeys = 1,
		.scankey = scankey,
		.data = settings,
		.tuple_found = compression_settings_tuple_update,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};
	return ts_scanner_scan(&scanctx);
}

static HeapTuple
compression_settings_formdata_make_tuple(const FormData_compression_settings *fd, TupleDesc desc)
{
	Datum values[Natts_compression_settings] = { 0 };
	bool nulls[Natts_compression_settings] = { false };

	values[AttrNumberGetAttrOffset(Anum_compression_settings_relid)] = ObjectIdGetDatum(fd->relid);

	if (fd->segmentby)
		values[AttrNumberGetAttrOffset(Anum_compression_settings_segmentby)] =
			PointerGetDatum(fd->segmentby);
	else
		nulls[AttrNumberGetAttrOffset(Anum_compression_settings_segmentby)] = true;

	if (fd->orderby)
		values[AttrNumberGetAttrOffset(Anum_compression_settings_orderby)] =
			PointerGetDatum(fd->orderby);
	else
		nulls[AttrNumberGetAttrOffset(Anum_compression_settings_orderby)] = true;

	if (fd->orderby_desc)
		values[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_desc)] =
			PointerGetDatum(fd->orderby_desc);
	else
		nulls[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_desc)] = true;

	if (fd->orderby_nullsfirst)
		values[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_nullsfirst)] =
			PointerGetDatum(fd->orderby_nullsfirst);
	else
		nulls[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_nullsfirst)] = true;

	return heap_form_tuple(desc, values, nulls);
}

static ScanTupleResult
compression_settings_tuple_update(TupleInfo *ti, void *data)
{
	CompressionSettings *settings = data;
	HeapTuple new_tuple;
	CatalogSecurityContext sec_ctx;

	new_tuple =
		compression_settings_formdata_make_tuple(&settings->fd, ts_scanner_get_tupledesc(ti));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
	ts_catalog_restore_user(&sec_ctx);
	heap_freetuple(new_tuple);
	return SCAN_DONE;
}
