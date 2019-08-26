/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TYPE_CACHE_H
#define TIMESCALEDB_TYPE_CACHE_H

#include <postgres.h>
#include "compat.h"

typedef enum CustomType
{
	CUSTOM_TYPE_TS_INTERVAL = 0,
	CUSTOM_TYPE_COMPRESSED_DATA,
	CUSTOM_TYPE_SEGMENT_META_MIN_MAX,

	_CUSTOM_TYPE_MAX_INDEX
} CustomType;

typedef struct CustomTypeInfo
{
	const char *schema_name;
	const char *type_name;
	Oid type_oid;
} CustomTypeInfo;

extern TSDLLEXPORT CustomTypeInfo *ts_custom_type_cache_get(CustomType type);

#endif /* TIMESCALEDB_TYPE_CACHE_H */
