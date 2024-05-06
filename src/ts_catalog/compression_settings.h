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

TSDLLEXPORT CompressionSettings *ts_compression_settings_create(Oid relid, Oid compress_relid,
																ArrayType *segmentby,
																ArrayType *orderby,
																ArrayType *orderby_desc,
																ArrayType *orderby_nullsfirst);
TSDLLEXPORT CompressionSettings *ts_compression_settings_get(Oid relid);
TSDLLEXPORT CompressionSettings *ts_compression_settings_get_by_compress_relid(Oid relid);
TSDLLEXPORT CompressionSettings *ts_compression_settings_materialize(const CompressionSettings *src,
																	 Oid relid, Oid compress_relid);
TSDLLEXPORT bool ts_compression_settings_delete(Oid relid);
TSDLLEXPORT bool ts_compression_settings_delete_by_compress_relid(Oid relid);
TSDLLEXPORT bool ts_compression_settings_equal(const CompressionSettings *left,
											   const CompressionSettings *right);

TSDLLEXPORT int ts_compression_settings_update(CompressionSettings *settings);
TSDLLEXPORT void ts_compression_settings_rename_column_recurse(Oid parent_relid, const char *old,
															   const char *new);
