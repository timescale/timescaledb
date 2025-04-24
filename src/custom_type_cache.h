/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include "compat/compat.h"
#include <postgres.h>

typedef enum CustomType
{
	CUSTOM_TYPE_COMPRESSED_DATA = 0,

	_CUSTOM_TYPE_MAX_INDEX
} CustomType;

typedef struct CustomTypeInfo
{
	const char *schema_name;
	const char *type_name;
	Oid type_oid;
} CustomTypeInfo;

extern TSDLLEXPORT CustomTypeInfo *ts_custom_type_cache_get(CustomType type);
