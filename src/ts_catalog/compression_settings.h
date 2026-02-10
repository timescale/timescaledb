/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <catalog/pg_type.h>

#include "bmslist_utils.h"
#include "ts_catalog/catalog.h"

typedef struct CompressionSettings
{
	FormData_compression_settings fd;
} CompressionSettings;

typedef enum SparseIndexTypeEnum
{
	_SparseIndexTypeEnumBloom = 0,
	_SparseIndexTypeEnumMinmax,
	_SparseIndexTypeEnumMax
} SparseIndexTypeEnum;

typedef enum SparseIndexSourceEnum
{
	_SparseIndexSourceEnumConfig = 0,
	_SparseIndexSourceEnumDefault,
	_SparseIndexSourceEnumOrderby,
	_SparseIndexSourceEnumMax
} SparseIndexSourceEnum;

typedef enum SparseIndexConfigKeys
{
	SparseIndexKeyType = 0,
	SparseIndexKeyCol,
	SparseIndexKeySource,
	SparseIndexKeyCustom
} SparseIndexConfigKeys;

extern TSDLLEXPORT const char *ts_sparse_index_type_names[];
extern TSDLLEXPORT const char *ts_sparse_index_source_names[];
extern TSDLLEXPORT const char *ts_sparse_index_common_keys[];

typedef struct SparseIndexConfigBase
{
	SparseIndexTypeEnum type;
	SparseIndexSourceEnum source;
} SparseIndexConfigBase;

typedef struct MinmaxIndexColumnConfig
{
	SparseIndexConfigBase base;
	const char *col;
} MinmaxIndexColumnConfig;

typedef struct SparseIndexColumn
{
	/* composite bloom indexes will have multiple SparseIndexColumn entries and
	 * they will be sorted by the attribute number */
	AttrNumber attnum;
	const char *name;
	Oid type;
} SparseIndexColumn;

#define MAX_BLOOM_FILTER_COLUMNS 8

typedef struct BloomFilterConfig
{
	SparseIndexConfigBase base;
	int num_columns;
	SparseIndexColumn *columns;
} BloomFilterConfig;

/*
 * The ParsedCompressionSettings structure is used to parse the compression
 * settings from the JSONB structure.
 * With this we can turn the stored JSONB into this structure, modify it and
 * turn it back into JSONB and we can avoid the messy and error prone JSONB
 * manipulation.
 *
 * The structure is a list of objects, each object is a list of pairs, each
 * pair is a key and a list of values. This allows us to store and manipulate
 * JSONB structures like this:
 *
 * [
 *   {"type": "bloom", "column": "big1", "source": "config"},
 *   {"type": "bloom", "column": ["value", "big1", "big2"], "source": "config"},
 *   {"type": "bloom", "column": ["o", "big2"], "source": "config"},
 *   {"type": "minmax", "column": "ts", "source": "orderby"}
 * ]
 *
 * Notice that the "column" key can have a string or an array of strings as value.
 */
typedef struct ParsedCompressionSettingsPair
{
	char *key;
	List *values; /* List of strings */
} ParsedCompressionSettingsPair;
typedef struct ParsedCompressionSettingsObject
{
	List *pairs; /* List of ParsedCompressionSettingsPair */
} ParsedCompressionSettingsObject;

typedef struct ParsedCompressionSettings
{
	MemoryContext context;
	List *objects; /* List of ParsedCompressionSettingsObject */
} ParsedCompressionSettings;

typedef struct PerColumnCompressionSettings
{
	const char *column_name;

	/* the index of the minmax index object that the column participates in, -1 if not present */
	int minmax_obj_id;

	/* the index of the single bloom index object that the column participates in, -1 if not present
	 */
	int single_bloom_obj_id;

	/* the object ids of the composite bloom index objects that the column participates in */
	Bitmapset *composite_bloom_index_obj_ids;
} PerColumnCompressionSettings;

TSDLLEXPORT int ts_qsort_attrnumber_cmp(const void *a, const void *b);

TSDLLEXPORT CompressionSettings *
ts_compression_settings_create(Oid relid, Oid compress_relid, ArrayType *segmentby,
							   ArrayType *orderby, ArrayType *orderby_desc,
							   ArrayType *orderby_nullsfirst, Jsonb *sparse_index);
TSDLLEXPORT CompressionSettings *ts_compression_settings_get(Oid relid);
TSDLLEXPORT CompressionSettings *ts_compression_settings_get_by_compress_relid(Oid relid);
TSDLLEXPORT CompressionSettings *ts_compression_settings_materialize(const CompressionSettings *src,
																	 Oid relid, Oid compress_relid);
TSDLLEXPORT bool ts_compression_settings_delete(Oid relid);
TSDLLEXPORT bool ts_compression_settings_delete_by_compress_relid(Oid relid);
TSDLLEXPORT bool ts_compression_settings_delete_any(Oid relid);
TSDLLEXPORT bool ts_compression_settings_equal(const CompressionSettings *left,
											   const CompressionSettings *right);
TSDLLEXPORT bool ts_compression_settings_equal_with_defaults(const CompressionSettings *ht,
															 const CompressionSettings *chunk);

TSDLLEXPORT int ts_compression_settings_update(CompressionSettings *settings);
TSDLLEXPORT void ts_compression_settings_rename_column_cascade(Oid parent_relid, const char *old,
															   const char *new);
TSDLLEXPORT void ts_convert_sparse_index_config_to_jsonb(JsonbParseState *parse_state,
														 SparseIndexConfigBase *config);

TSDLLEXPORT
bool ts_contains_sparse_index_config(CompressionSettings *settings, const char *attname,
									 const char *sparse_index_type, bool skip_column_arrays);
TSDLLEXPORT Jsonb *ts_add_orderby_sparse_index(CompressionSettings *settings);

TSDLLEXPORT Jsonb *ts_remove_orderby_sparse_index(CompressionSettings *settings);

extern TSDLLEXPORT ParsedCompressionSettings *
ts_convert_to_parsed_compression_settings(Jsonb *jsonb);
extern TSDLLEXPORT Jsonb *
ts_convert_from_parsed_compression_settings(ParsedCompressionSettings *settings);
extern TSDLLEXPORT void ts_free_parsed_compression_settings(ParsedCompressionSettings *settings);
extern TSDLLEXPORT const char *
ts_parsed_compression_settings_to_cstring(const ParsedCompressionSettings *settings);
extern TSDLLEXPORT char *ts_parsed_compression_settings_pstrdup(ParsedCompressionSettings *settings,
																const char *str);
extern TSDLLEXPORT List *
ts_get_per_column_compression_settings(const ParsedCompressionSettings *settings);
extern TSDLLEXPORT PerColumnCompressionSettings *
ts_get_per_column_compression_settings_by_column_name(List *per_column_settings,
													  const char *column_name);
extern TSDLLEXPORT List *
ts_get_column_names_from_parsed_object(ParsedCompressionSettingsObject *obj);

extern TSDLLEXPORT TsBmsList
ts_resolve_columns_to_attnos_from_parsed_settings(ParsedCompressionSettings *settings, Oid relid);

extern TSDLLEXPORT List *
ts_get_values_by_key_from_parsed_object(ParsedCompressionSettingsObject *obj, const char *key);
