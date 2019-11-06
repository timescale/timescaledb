/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/syscache.h>
#include <catalog/namespace.h>

#include "custom_type_cache.h"
#include "catalog.h"
#include "extension_constants.h"

/* Information about functions that we put in the cache */
static CustomTypeInfo typeinfo[_CUSTOM_TYPE_MAX_INDEX] = {
	[CUSTOM_TYPE_TS_INTERVAL] = {
		.schema_name = INTERNAL_SCHEMA_NAME,
		.type_name = "ts_interval",
		.type_oid = InvalidOid,
	},
	[CUSTOM_TYPE_COMPRESSED_DATA] = {
		.schema_name = INTERNAL_SCHEMA_NAME,
		.type_name = "compressed_data",
		.type_oid = InvalidOid,
	},
	[CUSTOM_TYPE_SEGMENT_META_MIN_MAX] = {
		.schema_name = INTERNAL_SCHEMA_NAME,
		.type_name = "segment_meta_min_max",
		.type_oid = InvalidOid,
	}
};

extern CustomTypeInfo *
ts_custom_type_cache_get(CustomType type)
{
	CustomTypeInfo *tinfo;

	if (type >= _CUSTOM_TYPE_MAX_INDEX)
		elog(ERROR, "invalid timescaledb type %d", type);

	tinfo = &typeinfo[type];

	if (tinfo->type_oid == InvalidOid)
	{
		Oid schema_oid = LookupExplicitNamespace(tinfo->schema_name, false);
		Oid type_oid = GetSysCacheOid2Compat(TYPENAMENSP,
											 Anum_pg_type_oid,
											 CStringGetDatum(tinfo->type_name),
											 ObjectIdGetDatum(schema_oid));
		if (!OidIsValid(type_oid))
			elog(ERROR, "unknown timescaledb type %s", tinfo->type_name);

		tinfo->type_oid = type_oid;
	}

	return tinfo;
}
