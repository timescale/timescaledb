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
TSDLLEXPORT const char *ts_sparse_index_source_names[] = { "config", "default", "orderby" };
TSDLLEXPORT const char *ts_sparse_index_common_keys[] = { "type", "column", "source", NULL };
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
		   ts_jsonb_equal(left->fd.index, right->fd.index);
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
															  src->fd.index);

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
	fd.index = sparse_index;

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
		fd->segmentby = DatumGetArrayTypePCopy(
			values[AttrNumberGetAttrOffset(Anum_compression_settings_segmentby)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_orderby)])
		fd->orderby = NULL;
	else
		fd->orderby = DatumGetArrayTypePCopy(
			values[AttrNumberGetAttrOffset(Anum_compression_settings_orderby)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_desc)])
		fd->orderby_desc = NULL;
	else
		fd->orderby_desc = DatumGetArrayTypePCopy(
			values[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_desc)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_nullsfirst)])
		fd->orderby_nullsfirst = NULL;
	else
		fd->orderby_nullsfirst = DatumGetArrayTypePCopy(
			values[AttrNumberGetAttrOffset(Anum_compression_settings_orderby_nullsfirst)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_compression_settings_index)])
		fd->index = NULL;
	else
		fd->index =
			DatumGetJsonbPCopy(values[AttrNumberGetAttrOffset(Anum_compression_settings_index)]);

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
	if (settings->fd.index &&
		ts_jsonb_has_key_value_str_field(settings->fd.index,
										 ts_sparse_index_common_keys[SparseIndexKeyCol],
										 old))
	{
		replacejsonb =
			ts_jsonb_replace_key_value_str_field(settings->fd.index,
												 ts_sparse_index_common_keys[SparseIndexKeyCol],
												 old,
												 new,
												 &replaced);
	}

	settings->fd.index = replaced ? replacejsonb : settings->fd.index;
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

	if (settings->fd.orderby && (settings->fd.segmentby || settings->fd.index))
	{
		Datum datum;
		bool isnull;

		ArrayIterator it = array_create_iterator(settings->fd.orderby, 0, NULL);
		while (array_iterate(it, &datum, &isnull))
		{
			if (settings->fd.segmentby &&
				ts_array_is_member(settings->fd.segmentby, TextDatumGetCString(datum)))
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("cannot use column \"%s\" for both ordering and segmenting",
								TextDatumGetCString(datum)),
						 errhint("Use separate columns for the timescaledb.compress_orderby and"
								 " timescaledb.compress_segmentby options.")));
			}

			if (settings->fd.index &&
				ts_contains_sparse_index_config(settings,
												TextDatumGetCString(datum),
												ts_sparse_index_type_names
													[_SparseIndexTypeEnumBloom]))
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("the orderby column \"%s\" cannot have a bloom sparse index ",
								TextDatumGetCString(datum)),
						 errdetail("For orderby columns, a minmax sparse index is added "
								   "automatically and cannot have bloom sparse index.")));
			}
		}
	}

	if (settings->fd.index && settings->fd.segmentby)
	{
		Datum datum;
		bool isnull;

		ArrayIterator it = array_create_iterator(settings->fd.segmentby, 0, NULL);
		while (array_iterate(it, &datum, &isnull))
		{
			for (int i = 0; i < _SparseIndexTypeEnumMax; i++)
			{
				if (ts_contains_sparse_index_config(settings,
													TextDatumGetCString(datum),
													ts_sparse_index_type_names[i]))
				{
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("the segmentby column \"%s\" can not have sparse "
									"indexes",
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

	if (fd->index)
		values[AttrNumberGetAttrOffset(Anum_compression_settings_index)] =
			JsonbPGetDatum(fd->index);
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
ts_convert_sparse_index_config_to_jsonb(JsonbParseState *parse_state, SparseIndexConfig *config)
{
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_str(parse_state,
					 ts_sparse_index_common_keys[SparseIndexKeyType],
					 ts_sparse_index_type_names[config->base.type]); /* type */

	ts_jsonb_add_str(parse_state,
					 ts_sparse_index_common_keys[SparseIndexKeyCol],
					 config->base.col); /* column */

	ts_jsonb_add_str(parse_state,
					 ts_sparse_index_common_keys[SparseIndexKeySource],
					 ts_sparse_index_source_names[config->base.source]); /* source */
	pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
}

static Jsonb *
find_sparse_index_config(Jsonb *jsonb, const char *attname, const char *sparse_index_type)
{
	Oid fnargtypes[] = { JSONBOID, TEXTOID, TEXTOID };
	FmgrInfo flinfo;
	LOCAL_FCINFO(fcinfo, 3);
	Datum result_datum;
	if (jsonb == NULL || JB_ROOT_COUNT(jsonb) == 0)
		return NULL;
	else if (attname == NULL)
		elog(ERROR, "sparse index attname is null");

	bool valid_type = false;
	for (int i = 0; i < _SparseIndexTypeEnumMax; i++)
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
contains_sparse_index_config(Jsonb *jsonb, const char *attname, const char *sparse_index_type)
{
	return (find_sparse_index_config(jsonb, attname, sparse_index_type) != NULL);
}

/* checks if given attribute has a sparse index of given type */
bool
ts_contains_sparse_index_config(CompressionSettings *settings, const char *attname,
								const char *sparse_index_type)
{
	return contains_sparse_index_config(settings->fd.index, attname, sparse_index_type);
}

/* adds orderby sparse index settings into fd.index */
Jsonb *
ts_add_orderby_sparse_index(CompressionSettings *settings)
{
	Datum datum;
	bool isnull;
	JsonbParseState *parse_state = NULL;
	JsonbIterator *it_json;
	JsonbValue v;
	JsonbIteratorToken r;
	Jsonb *sparse_index = settings->fd.index;

	/* nothing to do if no orderby columns */
	if (!settings->fd.orderby)
	{
		return sparse_index;
	}

	pushJsonbValue(&parse_state, WJB_BEGIN_ARRAY, NULL);

	/* add existing sparse index */
	if (settings->fd.index)
	{
		it_json = JsonbIteratorInit(&sparse_index->root);
		JsonbIteratorNext(&it_json, &v, false); /* WJB_BEGIN_ARRAY */
		while ((r = JsonbIteratorNext(&it_json, &v, true)) != WJB_END_ARRAY)
		{
			Assert(r == WJB_ELEM);
			pushJsonbValue(&parse_state, r, &v);
		}
	}

	/* add orderby sparse settings */
	ArrayIterator it = array_create_iterator(settings->fd.orderby, 0, NULL);
	while (array_iterate(it, &datum, &isnull))
	{
		/*
		 * check if sparse index for column already exists
		 * Validation is done by ts_compression_settings_update
		 */
		if (settings->fd.index &&
			ts_jsonb_has_key_value_str_field(settings->fd.index,
											 ts_sparse_index_common_keys[SparseIndexKeyCol],
											 TextDatumGetCString(datum)))
		{
			continue;
		}

		SparseIndexConfig config;
		config.base.type = _SparseIndexTypeEnumMinmax;
		config.base.col = TextDatumGetCString(datum);
		config.base.source = _SparseIndexSourceEnumOrderby;
		ts_convert_sparse_index_config_to_jsonb(parse_state, &config);
	}

	return JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_ARRAY, NULL));
}

/* removed orderby sparse index settings from fd.index */
Jsonb *
ts_remove_orderby_sparse_index(CompressionSettings *settings)
{
	JsonbParseState *parse_state = NULL;
	JsonbContainer *container;
	JsonbIterator *it_json;
	JsonbIteratorToken r;
	JsonbValue v;

	Jsonb *sparse_index = settings->fd.index;
	bool removed = false;
	bool has_object = false;
	const char *key_name_source = ts_sparse_index_common_keys[SparseIndexKeySource];
	const char *value_name_orderby = ts_sparse_index_source_names[_SparseIndexSourceEnumOrderby];

	/* nothing to do if no orderby columns */
	if (!settings->fd.orderby || !sparse_index)
	{
		return sparse_index;
	}

	pushJsonbValue(&parse_state, WJB_BEGIN_ARRAY, NULL);

	it_json = JsonbIteratorInit(&sparse_index->root);
	JsonbIteratorNext(&it_json, &v, false); /* WJB_BEGIN_ARRAY */
	while ((r = JsonbIteratorNext(&it_json, &v, true)) != WJB_END_ARRAY)
	{
		Ensure(r == WJB_ELEM && v.type == jbvBinary && JsonContainerIsObject(v.val.binary.data),
			   "sparse index format must be an array of objects");

		container = v.val.binary.data;
		JsonbValue value;
		getKeyJsonValueFromContainer(container, key_name_source, strlen(key_name_source), &value);
		if (value.type == jbvString && ((int) strlen(value_name_orderby) == value.val.string.len) &&
			(int) strncmp(value_name_orderby, value.val.string.val, value.val.string.len) == 0)
		{
			removed = true;
			continue;
		}

		has_object = true;
		pushJsonbValue(&parse_state, r, &v);
	}

	/* this is a possible edge case, log it just in case */
	if (!removed)
		elog(LOG, "orderby settings existed, but no orderby sparse index was removed");

	return has_object ? JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_ARRAY, NULL)) : NULL;
}
