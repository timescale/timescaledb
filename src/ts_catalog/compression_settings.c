/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_inherits.h>
#include <parser/parse_func.h>
#include <utils/builtins.h>

#include "jsonb_utils.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"
#include <utils/palloc.h>

TSDLLEXPORT const char *ts_sparse_index_type_names[] = { "bloom", "minmax" };
TSDLLEXPORT const char *ts_sparse_index_common_keys[] = { "type", "col", NULL };
static ScanTupleResult compression_settings_tuple_update(TupleInfo *ti, void *data);
static HeapTuple compression_settings_formdata_make_tuple(const FormData_compression_settings *fd,
														  TupleDesc desc);
bool
ts_compression_settings_equal(const CompressionSettings *left, const CompressionSettings *right)
{
	return ts_array_equal(left->fd.segmentby, right->fd.segmentby) &&
		   ts_array_equal(left->fd.orderby, right->fd.orderby) &&
		   ts_array_equal(left->fd.orderby_desc, right->fd.orderby_desc) &&
		   ts_array_equal(left->fd.orderby_nullsfirst, right->fd.orderby_nullsfirst) &&
		   ts_jsonb_equal(left->fd.sparse_index, right->fd.sparse_index);
}

CompressionSettings *
ts_compression_settings_materialize(const CompressionSettings *src, Oid relid, Oid compress_relid)
{
	CompressionSettings *dst = ts_compression_settings_create(relid,
															  compress_relid,
															  src->fd.segmentby,
															  src->fd.orderby,
															  src->fd.orderby_desc,
															  src->fd.orderby_nullsfirst,
															  src->fd.sparse_index);

	return dst;
}

CompressionSettings *
ts_compression_settings_create(Oid relid, Oid compress_relid, ArrayType *segmentby,
							   ArrayType *orderby, ArrayType *orderby_desc,
							   ArrayType *orderby_nullsfirst, Jsonb *sparse_index)
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
	fd.compress_relid = compress_relid;
	fd.segmentby = segmentby;
	fd.orderby = orderby;
	fd.orderby_desc = orderby_desc;
	fd.orderby_nullsfirst = orderby_nullsfirst;
	fd.sparse_index = sparse_index;

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

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_compress_relid)])
		fd->compress_relid = InvalidOid;
	else
		fd->compress_relid = DatumGetObjectId(
			values[AttrNumberGetAttrOffset(Anum_compression_settings_compress_relid)]);

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

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_index)])
		fd->sparse_index = NULL;
	else
		fd->sparse_index =
			DatumGetJsonbP(values[AttrNumberGetAttrOffset(Anum_compression_settings_index)]);
	MemoryContextSwitchTo(old);

	if (should_free)
		heap_freetuple(tuple);
}

static void
compression_settings_iterator_init(ScanIterator *iterator, Oid relid, bool by_compress_relid)
{
	int indexid =
		by_compress_relid ? COMPRESSION_SETTINGS_COMPRESS_RELID_IDX : COMPRESSION_SETTINGS_PKEY;
	iterator->ctx.index = catalog_get_index(ts_catalog_get(), COMPRESSION_SETTINGS, indexid);
	ts_scan_iterator_scan_key_init(iterator,
								   by_compress_relid ?
									   Anum_compression_settings_compress_relid_idx_relid :
									   Anum_compression_settings_pkey_relid,
								   BTEqualStrategyNumber,
								   F_OIDEQ,
								   ObjectIdGetDatum(relid));
}

/*
 * Get compression settings for a relation.
 *
 * When 'by_compress_relid' is false, the 'relid' refers to the "main"
 * relation being compressed. When it is true the 'relid' refers to the
 * relation containing the associated compressed data.
 */
static CompressionSettings *
compression_settings_get(Oid relid, bool by_compress_relid)
{
	CompressionSettings *settings = NULL;
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_SETTINGS, AccessShareLock, CurrentMemoryContext);
	compression_settings_iterator_init(&iterator, relid, by_compress_relid);

	ts_scanner_start_scan(&iterator.ctx);
	TupleInfo *ti = ts_scanner_next(&iterator.ctx);
	if (!ti)
		return NULL;

	settings = palloc0(sizeof(CompressionSettings));
	compression_settings_fill_from_tuple(settings, ti);
	ts_scan_iterator_close(&iterator);

	return settings;
}

/*
 * Get the compression settings for the relation referred to by 'relid'.
 */
TSDLLEXPORT CompressionSettings *
ts_compression_settings_get(Oid relid)
{
	return compression_settings_get(relid, false);
}

/*
 * Get the compression settings for a relation given its associated compressed
 * relation.
 *
 * Ideally, settings should only be looked up by "primary key", i.e., the
 * non-compressed chunk's 'relid', and in that case this function wouldn't be
 * needed. It might be possible to remove this function in the future.
 */
TSDLLEXPORT CompressionSettings *
ts_compression_settings_get_by_compress_relid(Oid relid)
{
	CompressionSettings *settings = compression_settings_get(relid, true);
	Ensure(settings, "compression settings not found for %s", get_rel_name(relid));
	return settings;
}

/*
 * Delete compression settings for a relation.
 *
 * When 'by_compress_relid' is false, the 'relid' refers to the "main"
 * relation being compressed. When it is true the 'relid' refers to the
 * relation containing the associated compressed data.
 */
static bool
compression_settings_delete(Oid relid, bool by_compress_relid)
{
	if (!OidIsValid(relid))
		return false;

	int count = 0;
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_SETTINGS, RowExclusiveLock, CurrentMemoryContext);
	compression_settings_iterator_init(&iterator, relid, by_compress_relid);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
		count++;
	}
	return count > 0;
}

/*
 * Delete entries matching the non-compressed relation.
 */
TSDLLEXPORT bool
ts_compression_settings_delete(Oid relid)
{
	return compression_settings_delete(relid, false);
}

/*
 * Delete entries matching a compressed relation.
 */
TSDLLEXPORT bool
ts_compression_settings_delete_by_compress_relid(Oid relid)
{
	return compression_settings_delete(relid, true);
}

/*
 * Delete entries matching either the primary key (non-compressed relation) or
 * the secondary key (compressed relation).
 */
TSDLLEXPORT bool
ts_compression_settings_delete_any(Oid relid)
{
	if (!ts_compression_settings_delete(relid))
		return ts_compression_settings_delete_by_compress_relid(relid);
	return true;
}

static void
compression_settings_rename_column(CompressionSettings *settings, const char *old, const char *new)
{
	Jsonb *replacejsonb = NULL;
	bool replaced = false;
	settings->fd.segmentby = ts_array_replace_text(settings->fd.segmentby, old, new);
	settings->fd.orderby = ts_array_replace_text(settings->fd.orderby, old, new);

	/* jsonb filed can not be mutated inplace, a new copy will be created */
	if (settings->fd.sparse_index)
	{
		replacejsonb =
			ts_jsonb_replace_key_value_str_field(settings->fd.sparse_index,
												 ts_sparse_index_common_keys[SparseIndexKeyCol],
												 old,
												 new,
												 &replaced);
	}

	settings->fd.sparse_index = replaced ? replacejsonb : settings->fd.sparse_index;
	ts_compression_settings_update(settings);
}

TSDLLEXPORT void
ts_compression_settings_rename_column_cascade(Oid parent_relid, const char *old, const char *new)
{
	CompressionSettings *settings = ts_compression_settings_get(parent_relid);

	if (settings)
		compression_settings_rename_column(settings, old, new);

	List *children = find_inheritance_children(parent_relid, NoLock);
	ListCell *lc;

	foreach (lc, children)
	{
		Oid relid = lfirst_oid(lc);

		settings = ts_compression_settings_get(relid);

		if (settings)
			compression_settings_rename_column(settings, old, new);
	}
}

TSDLLEXPORT int
ts_compression_settings_update(CompressionSettings *settings)
{
	Catalog *catalog = ts_catalog_get();
	FormData_compression_settings *fd = &settings->fd;
	ScanKeyData scankey[1];

	if (settings->fd.orderby && (settings->fd.segmentby || settings->fd.sparse_index))
	{
		Datum datum;
		bool isnull;

		ArrayIterator it = array_create_iterator(settings->fd.orderby, 0, NULL);
		while (array_iterate(it, &datum, &isnull))
		{
			if (settings->fd.segmentby &&
				ts_array_is_member(settings->fd.segmentby, TextDatumGetCString(datum)))
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use column \"%s\" for both ordering and segmenting",
								TextDatumGetCString(datum)),
						 errhint("Use separate columns for the timescaledb.compress_orderby and"
								 " timescaledb.compress_segmentby options.")));
			else if (settings->fd.sparse_index)
			{
				for (int i = 0; i < SparseIndexTypeMax; i++)
				{
					if (ts_contains_sparse_index_config(settings,
														TextDatumGetCString(datum),
														ts_sparse_index_type_names[i]))
						ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("cannot use column \"%s\" for both ordering and "
										"sparse_index",
										TextDatumGetCString(datum))));
				}
			}
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

	if (OidIsValid(fd->compress_relid))
		values[AttrNumberGetAttrOffset(Anum_compression_settings_compress_relid)] =
			ObjectIdGetDatum(fd->compress_relid);
	else
		nulls[AttrNumberGetAttrOffset(Anum_compression_settings_compress_relid)] = true;

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

	if (fd->sparse_index)
		values[AttrNumberGetAttrOffset(Anum_compression_settings_index)] =
			JsonbPGetDatum(fd->sparse_index);
	else
		nulls[AttrNumberGetAttrOffset(Anum_compression_settings_index)] = true;

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

void
ts_convert_sparse_index_config_to_json(JsonbParseState *parse_state, SparseIndexConfig *config)
{
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_str(parse_state,
					 ts_sparse_index_common_keys[SparseIndexKeyType],
					 ts_sparse_index_type_names[config->base.type]); /* type */

	ts_jsonb_add_str(parse_state,
					 ts_sparse_index_common_keys[SparseIndexKeyCol],
					 config->base.col); /* col */
	pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
}

static Jsonb *
find_sparse_index_config(const Jsonb *jsonb, const char *attname, const char *sparse_index_type)
{
	Oid fnargtypes[] = { JSONBOID, TEXTOID, TEXTOID };
	FmgrInfo flinfo;
	LOCAL_FCINFO(fcinfo, 3);
	Datum result_datum;
	if (jsonb == NULL)
		elog(ERROR, "sparse index json config is null");
	else if (attname == NULL)
		elog(ERROR, "sparse index attname is null");

	bool valid_type = false;
	for (int i = 0; i < SparseIndexTypeMax; i++)
	{
		if (strcmp(ts_sparse_index_type_names[i], sparse_index_type) == 0)
		{
			valid_type = true;
			break;
		}
	}
	if (!valid_type)
		elog(ERROR, "\"%s\" is not a valid sparse index configuration type", sparse_index_type);

	List *funcname =
		list_make2(makeString(FUNCTIONS_SCHEMA_NAME), makeString("jsonb_get_matching_index_entry"));

	Oid fnoid =
		LookupFuncName(funcname, sizeof(fnargtypes) / sizeof(fnargtypes[0]), fnargtypes, false);

	if (!OidIsValid(fnoid))
		elog(ERROR,
			 "function %s.%s does not exist",
			 FUNCTIONS_SCHEMA_NAME,
			 "jsonb_get_matching_index_entry");

	fmgr_info(fnoid, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, 3, InvalidOid, NULL, NULL);

	FC_SET_ARG(fcinfo, 0, JsonbPGetDatum(jsonb));
	FC_SET_ARG(fcinfo, 1, CStringGetTextDatum(attname));
	FC_SET_ARG(fcinfo, 2, CStringGetTextDatum(sparse_index_type));

	result_datum = FunctionCallInvoke(fcinfo);

	if (fcinfo->isnull)
		return NULL;

	return DatumGetJsonbP(result_datum);
}

static bool
contains_sparse_index_config(const Jsonb *jsonb, const char *attname, const char *sparse_index_type)
{
	return (find_sparse_index_config(jsonb, attname, sparse_index_type) != NULL);
}

bool
ts_contains_sparse_index_config(CompressionSettings *settings, const char *attname,
								const char *sparse_index_type)
{
	return contains_sparse_index_config(settings->fd.sparse_index, attname, sparse_index_type);
}
