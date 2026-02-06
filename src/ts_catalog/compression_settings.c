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
#include <common/md5.h>
#include <utils/palloc.h>

TSDLLEXPORT const char *ts_sparse_index_type_names[] = { "bloom", "minmax" };
TSDLLEXPORT const char *ts_sparse_index_source_names[] = { "config", "default", "orderby" };
TSDLLEXPORT const char *ts_sparse_index_common_keys[] = { "type", "column", "source", NULL };
static ScanTupleResult compression_settings_tuple_update(TupleInfo *ti, void *data);
static HeapTuple compression_settings_formdata_make_tuple(const FormData_compression_settings *fd,
														  TupleDesc desc);
static Bitmapset *resolve_columns_to_attnos(List *column_names, Oid relid);

/*
 * Compare two compression settings for equality
 */
bool
ts_compression_settings_equal(const CompressionSettings *left, const CompressionSettings *right)
{
	return ts_array_equal(left->fd.segmentby, right->fd.segmentby) &&
		   ts_array_equal(left->fd.orderby, right->fd.orderby) &&
		   ts_array_equal(left->fd.orderby_desc, right->fd.orderby_desc) &&
		   ts_array_equal(left->fd.orderby_nullsfirst, right->fd.orderby_nullsfirst) &&
		   ts_jsonb_equal(left->fd.index, right->fd.index);
}

/*
 * Compare two compression settings for equality while ignoring default values.
 *
 * This essentially means that any NULL
 * values should be considered a match because they represent default
 * values which are determined at chunk level.
 *
 * This also means first argument needs to be the hypertable because chunks
 * cannot have implicit defaults.
 */
bool
ts_compression_settings_equal_with_defaults(const CompressionSettings *ht,
											const CompressionSettings *chunk)
{
	Assert(!OidIsValid(ht->fd.compress_relid));
	return (ht->fd.segmentby == NULL || ts_array_equal(ht->fd.segmentby, chunk->fd.segmentby)) &&
		   (ht->fd.orderby == NULL || ts_array_equal(ht->fd.orderby, chunk->fd.orderby)) &&
		   (ht->fd.orderby_desc == NULL ||
			ts_array_equal(ht->fd.orderby_desc, chunk->fd.orderby_desc)) &&
		   (ht->fd.orderby_nullsfirst == NULL ||
			ts_array_equal(ht->fd.orderby_nullsfirst, chunk->fd.orderby_nullsfirst)) &&
		   (ht->fd.index == NULL || ts_jsonb_equal(ht->fd.index, chunk->fd.index));
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

	replacejsonb = settings->fd.index;
	if (replacejsonb)
	{
		ParsedCompressionSettings *parsed_settings =
			ts_convert_to_parsed_compression_settings(replacejsonb);
		if (parsed_settings)
		{
			ListCell *obj_cell = NULL;
			foreach (obj_cell, parsed_settings->objects)
			{
				ParsedCompressionSettingsObject *obj =
					(ParsedCompressionSettingsObject *) lfirst(obj_cell);
				if (obj)
				{
					ListCell *pair_cell = NULL;
					foreach (pair_cell, obj->pairs)
					{
						ParsedCompressionSettingsPair *pair =
							(ParsedCompressionSettingsPair *) lfirst(pair_cell);
						if (pair)
						{
							ListCell *value_cell = NULL;
							foreach (value_cell, pair->values)
							{
								const char *value = (const char *) lfirst(value_cell);
								if (value)
								{
									if (strcmp(value, old) == 0)
									{
										value_cell->ptr_value =
											ts_parsed_compression_settings_pstrdup(parsed_settings,
																				   new);
										replaced = true;
									}
								}
							}
						}
					}
				}
			}
			if (replaced)
			{
				replacejsonb = ts_convert_from_parsed_compression_settings(parsed_settings);
			}
			ts_free_parsed_compression_settings(parsed_settings);
		}
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
													[_SparseIndexTypeEnumBloom],
												/* skip_column_arrays = */ true))
			{
				/* disallow single column bloom index on orderby columns, composite bloom is
				 * allowed, that is why we set 'skip_column_arrays' to true */
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
													ts_sparse_index_type_names[i],
													/* skip_column_arrays = */ false))
				{
					/* segmentby columns cannot have sparse indexes of any type, including composite
					 * bloom that is why we set 'skip_column_arrays' to false, which will look
					 * inside column name arrays */
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
ts_convert_sparse_index_config_to_jsonb(JsonbParseState *parse_state, SparseIndexConfigBase *config)
{
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	ts_jsonb_add_str(parse_state,
					 ts_sparse_index_common_keys[SparseIndexKeyType],
					 ts_sparse_index_type_names[config->type]); /* type */
	switch (config->type)
	{
		case _SparseIndexTypeEnumMinmax:
			MinmaxIndexColumnConfig *minmax_config = (MinmaxIndexColumnConfig *) config;
			ts_jsonb_add_str(parse_state,
							 ts_sparse_index_common_keys[SparseIndexKeyCol],
							 minmax_config->col); /* column */
			break;
		case _SparseIndexTypeEnumBloom:
			BloomFilterConfig *bloom_config = (BloomFilterConfig *) config;

			if (bloom_config->num_columns > 1)
			{
				/* add the column names as an array */
				const char *column_names[MAX_BLOOM_FILTER_COLUMNS] = { 0 };
				for (int i = 0; i < bloom_config->num_columns; i++)
				{
					column_names[i] = bloom_config->columns[i].name;
				}

				ts_jsonb_add_str_array(parse_state,
									   ts_sparse_index_common_keys[SparseIndexKeyCol],
									   column_names,
									   bloom_config->num_columns);
			}
			else
			{
				ts_jsonb_add_str(parse_state,
								 ts_sparse_index_common_keys[SparseIndexKeyCol],
								 bloom_config->columns[0].name); /* column */
			}
			break;

		default:
			elog(ERROR, "invalid sparse index type: %d", config->type);
	};
	ts_jsonb_add_str(parse_state,
					 ts_sparse_index_common_keys[SparseIndexKeySource],
					 ts_sparse_index_source_names[config->source]); /* source */
	pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
}

bool
ts_contains_sparse_index_config(CompressionSettings *settings, const char *attname,
								const char *sparse_index_type, bool skip_column_arrays)
{
	bool result = false;
	if (settings == NULL || settings->fd.index == NULL || attname == NULL)
		return false;

	ParsedCompressionSettings *parsed =
		ts_convert_to_parsed_compression_settings(settings->fd.index);

	if (parsed == NULL)
	{
		return false;
	}

	ListCell *cell = NULL;
	foreach (cell, parsed->objects)
	{
		ParsedCompressionSettingsObject *obj = (ParsedCompressionSettingsObject *) lfirst(cell);
		ListCell *pair_cell = NULL;
		const char *index_type = NULL;
		bool attname_found = false;
		foreach (pair_cell, obj->pairs)
		{
			ParsedCompressionSettingsPair *pair =
				(ParsedCompressionSettingsPair *) lfirst(pair_cell);
			if (strcmp(pair->key, ts_sparse_index_common_keys[SparseIndexKeyCol]) == 0)
			{
				ListCell *value_cell = NULL;
				if (skip_column_arrays && list_length(pair->values) > 1)
				{
					continue;
				}
				foreach (value_cell, pair->values)
				{
					const char *value = (const char *) lfirst(value_cell);
					if (strcmp(value, attname) == 0)
					{
						attname_found = true;
						break;
					}
				}
			}
			if (strcmp(pair->key, ts_sparse_index_common_keys[SparseIndexKeyType]) == 0)
			{
				index_type = (const char *) lfirst(list_head(pair->values));
				if (strcmp(index_type, sparse_index_type) != 0)
				{
					break;
				}
				else if (attname_found)
				{
					result = true;
					break;
				}
			}
			if (attname_found && index_type != NULL)
			{
				result = strcmp(index_type, sparse_index_type) == 0;
				break;
			}
		}
		if (result)
		{
			break;
		}
	}

	ts_free_parsed_compression_settings(parsed);
	return result;
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

		MinmaxIndexColumnConfig config;
		config.base.type = _SparseIndexTypeEnumMinmax;
		config.col = TextDatumGetCString(datum);
		config.base.source = _SparseIndexSourceEnumOrderby;
		ts_convert_sparse_index_config_to_jsonb(parse_state, (SparseIndexConfigBase *) &config);
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
			strncmp(value_name_orderby, value.val.string.val, value.val.string.len) == 0)
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

int
ts_qsort_attrnumber_cmp(const void *a, const void *b)
{
	AttrNumber *attrnum_a = (AttrNumber *) a;
	AttrNumber *attrnum_b = (AttrNumber *) b;

	return ((int) (*attrnum_a)) - ((int) (*attrnum_b));
}

ParsedCompressionSettings *
ts_convert_to_parsed_compression_settings(Jsonb *jsonb)
{
	enum ParseState
	{
		PARSE_STATE_INIT,
		PARSE_STATE_KEY,
		PARSE_STATE_VALUE,
		PARSE_STATE_ARRAY_ENTRIES
	};

	MemoryContext tmp_context, new_context;
	enum ParseState state = PARSE_STATE_INIT;
	JsonbValue jsonb_value;
	JsonbIterator *it;
	JsonbIteratorToken r;
	int num_arrays = 0;

	Assert(jsonb != NULL);

	if (jsonb == NULL)
		return NULL;

	if (JB_ROOT_IS_SCALAR(jsonb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot convert scalar to ParsedCompressionSettings")));

	if (JB_ROOT_COUNT(jsonb) == 0)
		return NULL;

	it = JsonbIteratorInit(&jsonb->root);

	new_context = AllocSetContextCreate(CurrentMemoryContext,
										"ParsedCompressionSettings",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);

	ParsedCompressionSettings *parsed_settings =
		MemoryContextAllocZero(new_context, sizeof(ParsedCompressionSettings));

	parsed_settings->objects = NIL;
	parsed_settings->context = new_context;

	while ((r = JsonbIteratorNext(&it, &jsonb_value, false)) != WJB_DONE)
	{
		switch (state)
		{
			case PARSE_STATE_INIT:
				if (r == WJB_END_OBJECT || r == WJB_END_ARRAY)
				{
					/* Ignore*/
				}
				else if (r == WJB_BEGIN_OBJECT)
				{
					ParsedCompressionSettingsObject *current_object = NULL;

					/* If the previous object has no pairs, reuse its space for the new object */
					if (list_length(parsed_settings->objects) > 0)
					{
						current_object = llast(parsed_settings->objects);
						if (list_length(current_object->pairs) > 0)
						{
							/* We can't reuse the previous object, so we need to create a new one */
							current_object = NULL;
						}
					}

					if (current_object == NULL)
					{
						/* Create a new object */
						tmp_context = MemoryContextSwitchTo(parsed_settings->context);
						current_object = palloc0(sizeof(ParsedCompressionSettingsObject));
						parsed_settings->objects =
							lappend(parsed_settings->objects, current_object);
						MemoryContextSwitchTo(tmp_context);
					}
					state = PARSE_STATE_KEY;
				}
				else if (r == WJB_KEY)
				{
					Ensure(jsonb_value.type == jbvString,
						   "Jsonb value is of type \"%s\", but expected of type string, in state "
						   "INIT",
						   JsonbTypeName(&jsonb_value));

					ParsedCompressionSettingsObject *current_object =
						llast(parsed_settings->objects);
					Assert(current_object != NULL);
					tmp_context = MemoryContextSwitchTo(parsed_settings->context);
					char *tmp_str =
						pnstrdup(jsonb_value.val.string.val, jsonb_value.val.string.len);
					ParsedCompressionSettingsPair *current_pair =
						palloc0(sizeof(ParsedCompressionSettingsPair));
					current_object->pairs = lappend(current_object->pairs, current_pair);
					current_pair->key = tmp_str;
					current_pair->values = NIL;
					MemoryContextSwitchTo(tmp_context);
					state = PARSE_STATE_VALUE;
				}
				else if (r == WJB_BEGIN_ARRAY)
				{
					num_arrays++;
					/* We can ignore one begin array, but more than one is not allowed as we don't
					 * support nested arrays */
					if (num_arrays > 1)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								 errmsg("Jsonb value is of type \"%s\", but expected of type begin "
										"object or end object, in state INIT",
										JsonbTypeName(&jsonb_value))));
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Jsonb value is of type \"%s\", but expected of type begin "
									"object or end object, in state INIT",
									JsonbTypeName(&jsonb_value))));
				}
				break;

			case PARSE_STATE_KEY:
				if (r == WJB_KEY)
				{
					Ensure(jsonb_value.type == jbvString,
						   "Jsonb value is of type \"%s\", but expected of type string, in state "
						   "KEY",
						   JsonbTypeName(&jsonb_value));

					ParsedCompressionSettingsObject *current_object =
						llast(parsed_settings->objects);
					Assert(current_object != NULL);
					tmp_context = MemoryContextSwitchTo(parsed_settings->context);
					char *tmp_str =
						pnstrdup(jsonb_value.val.string.val, jsonb_value.val.string.len);
					ParsedCompressionSettingsPair *current_pair =
						palloc0(sizeof(ParsedCompressionSettingsPair));
					current_object->pairs = lappend(current_object->pairs, current_pair);
					current_pair->key = tmp_str;
					current_pair->values = NIL;
					MemoryContextSwitchTo(tmp_context);
					state = PARSE_STATE_VALUE;
				}
				else if (r == WJB_END_OBJECT)
				{
					state = PARSE_STATE_INIT;
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Jsonb value is of type \"%s\", but expected of type key or "
									"end object, in state KEY",
									JsonbTypeName(&jsonb_value))));
				}
				break;

			case PARSE_STATE_VALUE:
				if (r == WJB_VALUE)
				{
					ParsedCompressionSettingsObject *current_object =
						llast(parsed_settings->objects);
					Assert(current_object != NULL);

					Ensure(jsonb_value.type == jbvString,
						   "Jsonb value is of type \"%s\", but expected of type string, in state "
						   "VALUE",
						   JsonbTypeName(&jsonb_value));

					tmp_context = MemoryContextSwitchTo(parsed_settings->context);
					char *tmp_str =
						pnstrdup(jsonb_value.val.string.val, jsonb_value.val.string.len);
					ParsedCompressionSettingsPair *current_pair = llast(current_object->pairs);

					/* The pair should have been created in the KEY state, but no values should have
					 * been added yet */
					Assert(current_pair != NULL);
					Assert(current_pair->values == NIL);

					current_pair->values = lappend(current_pair->values, tmp_str);
					MemoryContextSwitchTo(tmp_context);
					state = PARSE_STATE_KEY;
				}
				else if (r == WJB_BEGIN_ARRAY)
				{
					ParsedCompressionSettingsObject *current_object =
						llast(parsed_settings->objects);
					Assert(current_object != NULL);

					/* The pair list should have been created in the key state */
					Assert(list_length(current_object->pairs) > 0);

#ifdef USE_ASSERT_CHECKING
					ParsedCompressionSettingsPair *current_pair = llast(current_object->pairs);
					Assert(current_pair != NULL);
					/* The values list should be empty when we arrive to the array start */
					Assert(current_pair->values == NIL);
#endif
					state = PARSE_STATE_ARRAY_ENTRIES;
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Jsonb value is of type \"%s\", but expected of type value or "
									"array, in state VALUE",
									JsonbTypeName(&jsonb_value))));
				}
				break;

			case PARSE_STATE_ARRAY_ENTRIES:
				if (r == WJB_ELEM)
				{
					ParsedCompressionSettingsObject *current_object =
						llast(parsed_settings->objects);
					Assert(current_object != NULL);

					ParsedCompressionSettingsPair *current_pair = llast(current_object->pairs);
					Assert(current_pair != NULL);

					Ensure(jsonb_value.type == jbvString,
						   "Jsonb value is of type \"%s\", but expected of type string, in state "
						   "ARRAY_ENTRIES",
						   JsonbTypeName(&jsonb_value));

					tmp_context = MemoryContextSwitchTo(parsed_settings->context);
					char *tmp_str =
						pnstrdup(jsonb_value.val.string.val, jsonb_value.val.string.len);
					current_pair->values = lappend(current_pair->values, tmp_str);
					MemoryContextSwitchTo(tmp_context);
					/* state remains ARRAY_ENTRIES */
				}
				else if (r == WJB_END_ARRAY)
				{
					state = PARSE_STATE_INIT;
				}
				else
				{
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("Jsonb value is of type \"%s\", but expected of type elem or "
									"end array, in state ARRAY_ENTRIES",
									JsonbTypeName(&jsonb_value))));
				}
				break;
		}
	}

	/* If the last object has no pairs, remove it */
	if (list_length(parsed_settings->objects) > 0)
	{
		ParsedCompressionSettingsObject *current_object = llast(parsed_settings->objects);
		Assert(current_object != NULL);
		if (list_length(current_object->pairs) == 0)
		{
			parsed_settings->objects = list_delete_last(parsed_settings->objects);
		}
	}

	/* If there are no objects, free the parsed settings */
	if (list_length(parsed_settings->objects) == 0)
	{
		ts_free_parsed_compression_settings(parsed_settings);
		parsed_settings = NULL;
	}
	return parsed_settings;
}

Jsonb *
ts_convert_from_parsed_compression_settings(ParsedCompressionSettings *settings)
{
	JsonbParseState *parse_state = NULL;
	ListCell *obj_cell = NULL;
	ListCell *pair_cell = NULL;
	ListCell *value_cell = NULL;

	Assert(settings != NULL);
	if (settings == NULL)
		return NULL;

	if (list_length(settings->objects) == 0)
		return NULL;

	pushJsonbValue(&parse_state, WJB_BEGIN_ARRAY, NULL);
	foreach (obj_cell, settings->objects)
	{
		pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
		ParsedCompressionSettingsObject *obj = (ParsedCompressionSettingsObject *) lfirst(obj_cell);
		Assert(list_length(obj->pairs) > 0);
		foreach (pair_cell, obj->pairs)
		{
			ParsedCompressionSettingsPair *pair =
				(ParsedCompressionSettingsPair *) lfirst(pair_cell);
			Assert(pair->key != NULL);
			Assert(list_length(pair->values) > 0);
			JsonbValue key = { .type = jbvString,
							   .val = {
								   .string = { .val = pair->key, .len = strlen(pair->key) } } };
			pushJsonbValue(&parse_state, WJB_KEY, &key);

			if (list_length(pair->values) == 1)
			{
				const char *value = (const char *) lfirst(list_head(pair->values));
				Assert(value != NULL);
				int len = strlen(value);
				Assert(len > 0);
				JsonbValue value_jsonb = {
					.type = jbvString, .val = { .string = { .val = pstrdup(value), .len = len } }
				};
				pushJsonbValue(&parse_state, WJB_VALUE, &value_jsonb);
			}
			else
			{
				pushJsonbValue(&parse_state, WJB_BEGIN_ARRAY, NULL);
				foreach (value_cell, pair->values)
				{
					const char *value = (const char *) lfirst(value_cell);
					Assert(value != NULL);
					int len = strlen(value);
					Assert(len > 0);
					JsonbValue value_jsonb = { .type = jbvString,
											   .val = { .string = { .val = pstrdup(value),
																	.len = len } } };
					pushJsonbValue(&parse_state, WJB_ELEM, &value_jsonb);
				}
				pushJsonbValue(&parse_state, WJB_END_ARRAY, NULL);
			}
		}
		pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	}
	return JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_ARRAY, NULL));
}

void
ts_free_parsed_compression_settings(ParsedCompressionSettings *settings)
{
	if (settings == NULL)
		return;

	if (settings->context != NULL)
		MemoryContextDelete(settings->context);
}

const char *
ts_parsed_compression_settings_to_cstring(const ParsedCompressionSettings *settings)
{
	StringInfoData buf;
	ListCell *obj_cell = NULL;
	ListCell *pair_cell = NULL;
	ListCell *value_cell = NULL;
	int i = 0, j = 0, k = 0;

	initStringInfo(&buf);
	appendStringInfo(&buf, "[");

	foreach (obj_cell, settings->objects)
	{
		ParsedCompressionSettingsObject *obj = (ParsedCompressionSettingsObject *) lfirst(obj_cell);
		if (i > 0)
			appendStringInfo(&buf, ", ");
		appendStringInfo(&buf, "{");

		j = 0;
		foreach (pair_cell, obj->pairs)
		{
			ParsedCompressionSettingsPair *pair =
				(ParsedCompressionSettingsPair *) lfirst(pair_cell);
			if (j > 0)
				appendStringInfo(&buf, ", ");
			escape_json(&buf, pair->key);
			appendStringInfo(&buf, ": ");

			if (list_length(pair->values) == 1)
			{
				escape_json(&buf, (const char *) lfirst(list_head(pair->values)));
			}
			else
			{
				k = 0;
				appendStringInfo(&buf, "[");
				foreach (value_cell, pair->values)
				{
					const char *value = (const char *) lfirst(value_cell);
					if (k > 0)
						appendStringInfo(&buf, ", ");
					escape_json(&buf, value);
					k++;
				}
				appendStringInfo(&buf, "]");
			}
			j++;
		}
		appendStringInfo(&buf, "}");
		i++;
	}

	appendStringInfo(&buf, "]");
	return buf.data;
}

char *
ts_parsed_compression_settings_pstrdup(ParsedCompressionSettings *settings, const char *str)
{
	Assert(settings != NULL);
	Assert(str != NULL);
	Assert(settings->context != NULL);

	MemoryContext old_context = MemoryContextSwitchTo(settings->context);
	char *new_str = pstrdup(str);
	MemoryContextSwitchTo(old_context);
	return new_str;
}

/* returns a list of PerColumnCompressionSettings objects */
List *
ts_get_per_column_compression_settings(const ParsedCompressionSettings *settings)
{
	if (settings == NULL)
		return NIL;

	List *result_settings = NIL;
	ListCell *obj_cell = NULL;
	int obj_id = 0;
	foreach (obj_cell, settings->objects)
	{
		const char *index_type = NULL;
		ParsedCompressionSettingsPair *column_names_pair = NULL;

		ParsedCompressionSettingsObject *obj = (ParsedCompressionSettingsObject *) lfirst(obj_cell);
		Assert(obj != NULL);

		ListCell *pair_cell = NULL;
		foreach (pair_cell, obj->pairs)
		{
			ParsedCompressionSettingsPair *pair =
				(ParsedCompressionSettingsPair *) lfirst(pair_cell);
			if (strcmp(pair->key, ts_sparse_index_common_keys[SparseIndexKeyType]) == 0)
			{
				Assert(list_length(pair->values) > 0);
				index_type = (const char *) lfirst(list_head(pair->values));
			}
			else if (strcmp(pair->key, ts_sparse_index_common_keys[SparseIndexKeyCol]) == 0)
			{
				column_names_pair = pair;
			}
		}

		/* we may have an empty object, that is the default */
		if (index_type != NULL && column_names_pair != NULL)
		{
			/* find the column names and iterate over them */
			int num_columns = list_length(column_names_pair->values);
			ListCell *value_cell = NULL;
			foreach (value_cell, column_names_pair->values)
			{
				const char *column_name = (const char *) lfirst(value_cell);
				Assert(column_name != NULL);

				PerColumnCompressionSettings *per_column_setting = NULL;
				/* check if the column name is already in the list */
				ListCell *per_column_setting_cell = NULL;
				foreach (per_column_setting_cell, result_settings)
				{
					PerColumnCompressionSettings *tmp =
						(PerColumnCompressionSettings *) lfirst(per_column_setting_cell);
					if (strcmp(tmp->column_name, column_name) == 0)
					{
						per_column_setting = tmp;
						break;
					}
				}

				/* if no object is found, create a new one */
				if (per_column_setting == NULL)
				{
					per_column_setting = palloc0(sizeof(PerColumnCompressionSettings));
					per_column_setting->column_name = column_name;
					per_column_setting->minmax_obj_id = -1;
					per_column_setting->single_bloom_obj_id = -1;
					per_column_setting->composite_bloom_index_obj_ids = NULL;
					result_settings = lappend(result_settings, per_column_setting);
				}

				if (strcmp(index_type, ts_sparse_index_type_names[_SparseIndexTypeEnumMinmax]) == 0)
				{
					Assert(num_columns == 1);
					per_column_setting->minmax_obj_id = obj_id;
				}
				else if (strcmp(index_type,
								ts_sparse_index_type_names[_SparseIndexTypeEnumBloom]) == 0)
				{
					if (num_columns == 1)
					{
						per_column_setting->single_bloom_obj_id = obj_id;
					}
					else if (num_columns > 1)
					{
						if (per_column_setting->composite_bloom_index_obj_ids == NULL)
						{
							per_column_setting->composite_bloom_index_obj_ids =
								bms_make_singleton(obj_id);
						}
						else
						{
							per_column_setting->composite_bloom_index_obj_ids =
								bms_add_member(per_column_setting->composite_bloom_index_obj_ids,
											   obj_id);
						}
					}
				}
			}
		}
		obj_id++;
	}
	return result_settings;
}

PerColumnCompressionSettings *
ts_get_per_column_compression_settings_by_column_name(List *per_column_settings,
													  const char *column_name)
{
	Assert(column_name != NULL);

	ListCell *per_column_setting_cell = NULL;
	foreach (per_column_setting_cell, per_column_settings)
	{
		PerColumnCompressionSettings *tmp =
			(PerColumnCompressionSettings *) lfirst(per_column_setting_cell);
		if (strcmp(tmp->column_name, column_name) == 0)
		{
			return tmp;
		}
	}
	return NULL;
}

List *
ts_get_column_names_from_parsed_object(ParsedCompressionSettingsObject *obj)
{
	Assert(obj != NULL);
	ListCell *pair_cell = NULL;
	foreach (pair_cell, obj->pairs)
	{
		ParsedCompressionSettingsPair *pair = (ParsedCompressionSettingsPair *) lfirst(pair_cell);
		if (strcmp(pair->key, ts_sparse_index_common_keys[SparseIndexKeyCol] /* "column" */) == 0)
		{
			return pair->values;
		}
	}
	return NULL;
}

static Bitmapset *
resolve_columns_to_attnos(List *column_names, Oid relid)
{
	Assert(column_names != NULL);
	Assert(relid != InvalidOid);

	Bitmapset *result = NULL;
	ListCell *name_cell = NULL;

	foreach (name_cell, column_names)
	{
		const char *name = (const char *) lfirst(name_cell);
		AttrNumber attno = get_attnum(relid, name);
		if (AttributeNumberIsValid(attno))
		{
			result = bms_add_member(result, attno);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%ld\" does not exist",
							name,
							(long) relid)));
		}
	}
	return result;
}

/*
 * Resolve the column names in the parsed settings to attribute numbers for the given relation
 * and return a list of bitmapsets corresponging to each object in the parsed settings.
 */
TsBmsList
ts_resolve_columns_to_attnos_from_parsed_settings(ParsedCompressionSettings *settings, Oid relid)
{
	Assert(settings != NULL);
	Assert(relid != InvalidOid);

	TsBmsList result = NIL;
	ListCell *obj_cell = NULL;
	foreach (obj_cell, settings->objects)
	{
		ParsedCompressionSettingsObject *obj = (ParsedCompressionSettingsObject *) lfirst(obj_cell);
		List *column_names = ts_get_column_names_from_parsed_object(obj);
		Bitmapset *attnos = resolve_columns_to_attnos(column_names, relid);
		result = lappend(result, attnos);
	}

	return result;
}

List *
ts_get_values_by_key_from_parsed_object(ParsedCompressionSettingsObject *obj, const char *key)
{
	Assert(obj != NULL);
	Assert(key != NULL);

	List *result = NIL;
	ListCell *pair_cell = NULL;
	foreach (pair_cell, obj->pairs)
	{
		ParsedCompressionSettingsPair *pair = (ParsedCompressionSettingsPair *) lfirst(pair_cell);
		if (strcmp(pair->key, key) == 0)
		{
			result = pair->values;
			return result;
		}
	}
	return NIL;
}
