/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <catalog/pg_type.h>

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

typedef struct SparseIndexBase
{
	SparseIndexTypeEnum type;
	char *col;
	SparseIndexSourceEnum source;
} SparseIndexBase;

typedef struct SparseIndexConfig
{
	SparseIndexBase base;
} SparseIndexConfig;

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

TSDLLEXPORT int ts_compression_settings_update(CompressionSettings *settings);
TSDLLEXPORT void ts_compression_settings_rename_column_cascade(Oid parent_relid, const char *old,
															   const char *new);
TSDLLEXPORT void ts_convert_sparse_index_config_to_jsonb(JsonbParseState *parse_state,
														 SparseIndexConfig *config);
TSDLLEXPORT
bool ts_contains_sparse_index_config(CompressionSettings *settings, const char *attname,
									 const char *sparse_index_type);
TSDLLEXPORT Jsonb *ts_add_orderby_sparse_index(CompressionSettings *settings);

TSDLLEXPORT Jsonb *ts_remove_orderby_sparse_index(CompressionSettings *settings);
