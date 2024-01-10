/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <catalog/pg_type.h>

#include "ts_catalog/catalog.h"
#include "hypertable.h"

typedef struct CompressionSettings
{
	FormData_compression_settings fd;
} CompressionSettings;

TSDLLEXPORT CompressionSettings *ts_compression_settings_create(Oid relid, ArrayType *segmentby,
																ArrayType *orderby,
																ArrayType *orderby_desc,
																ArrayType *orderby_nullsfirst);
TSDLLEXPORT CompressionSettings *ts_compression_settings_get(Oid relid);
TSDLLEXPORT CompressionSettings *ts_compression_settings_materialize(Oid ht_relid, Oid dst_relid);
TSDLLEXPORT bool ts_compression_settings_delete(Oid relid);

TSDLLEXPORT int ts_compression_settings_update(CompressionSettings *settings);

TSDLLEXPORT void ts_compression_settings_rename_column(Oid relid, char *old, char *new);
TSDLLEXPORT void ts_compression_settings_rename_column_hypertable(Hypertable *ht, char *old,
																  char *new);
