/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <stdlib.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <catalog/pg_type.h>
#include <access/htup_details.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>
#include <utils/datum.h>
#include <utils/uuid.h>

#include "ts_catalog/catalog.h"
#include "ts_catalog/metadata.h"
#include "scanner.h"
#include "uuid.h"

#include "compat/compat.h"

#define TYPE_ERROR(inout, typeid)                                                                  \
	elog(ERROR, "ts_metadata: no %s function for type %u", inout, typeid);

static Datum
convert_type_to_text(Datum value, Oid from_type)
{
	bool is_varlena;
	Oid outfunc;

	getTypeOutputInfo(from_type, &outfunc, &is_varlena);

	if (!OidIsValid(outfunc))
		TYPE_ERROR("output", from_type);

	return DirectFunctionCall1(textin, CStringGetDatum(OidFunctionCall1(outfunc, value)));
}

static Datum
convert_text_to_type(Datum value, Oid to_type)
{
	Oid value_in;
	Oid value_ioparam;

	getTypeInputInfo(to_type, &value_in, &value_ioparam);

	if (!OidIsValid(value_in))
		TYPE_ERROR("input", to_type);

	value = OidFunctionCall3(value_in,
							 CStringGetDatum(TextDatumGetCString(value)),
							 ObjectIdGetDatum(InvalidOid),
							 Int32GetDatum(-1));
	return value;
}

typedef struct DatumValue
{
	/*
	 * This form is not used for anything. It is here to reference the type so
	 * that pgindent works. It can be removed from this struct in case we
	 * actually use the form type in code
	 */
	FormData_metadata *form;
	Datum value;
	Oid typeid;
	bool isnull;
} DatumValue;

static ScanTupleResult
metadata_tuple_get_value(TupleInfo *ti, void *data)
{
	DatumValue *dv = data;

	dv->value = slot_getattr(ti->slot, Anum_metadata_value, &dv->isnull);

	if (!dv->isnull)
		dv->value = convert_text_to_type(dv->value, dv->typeid);

	return SCAN_DONE;
}

static Datum
metadata_get_value_internal(const char *key, Oid value_type, bool *isnull, LOCKMODE lockmode)
{
	ScanKeyData scankey[1];
	DatumValue dv = {
		.typeid = value_type,
		.isnull = true,
	};
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, METADATA),
		.index = catalog_get_index(catalog, METADATA, METADATA_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = metadata_tuple_get_value,
		.data = &dv,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0],
				Anum_metadata_key,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(key));

	ts_scanner_scan(&scanctx);

	if (NULL != isnull)
		*isnull = dv.isnull;

	return dv.value;
}

Datum
ts_metadata_get_value(const char *metadata_key, Oid value_type, bool *isnull)
{
	return metadata_get_value_internal(metadata_key, value_type, isnull, AccessShareLock);
}

/*
 *  Insert a row into the metadata table. Acquires a lock in
 *  SHARE ROW EXCLUSIVE mode to conflict with itself, and then verifies that
 *  the desired metadata KV pair still does not exist. Otherwise, exits
 *  without inserting to avoid underlying database error on PK conflict.
 *  Returns the value of the key; this is either the requested insert value or
 *  the existing value if nothing was inserted.
 */
Datum
ts_metadata_insert(const char *metadata_key, Datum metadata_value, Oid value_type,
				   bool include_in_telemetry)
{
	Datum existing_value;
	Datum values[Natts_metadata];
	bool nulls[Natts_metadata] = { false };
	bool isnull = false;
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	char key_data[NAMEDATALEN];

	rel = table_open(catalog_get_table_id(catalog, METADATA), ShareRowExclusiveLock);

	/* Check for row existence while we have the lock */
	existing_value =
		metadata_get_value_internal(metadata_key, value_type, &isnull, ShareRowExclusiveLock);

	if (!isnull)
	{
		table_close(rel, ShareRowExclusiveLock);
		return existing_value;
	}

	/* We have to copy the key here because heap_form_tuple will copy NAMEDATALEN
	 * into the tuple instead of checking length. */
	strlcpy(key_data, metadata_key, NAMEDATALEN);

	/* Insert into the catalog table for persistence */
	values[AttrNumberGetAttrOffset(Anum_metadata_key)] = CStringGetDatum(key_data);
	values[AttrNumberGetAttrOffset(Anum_metadata_value)] =
		convert_type_to_text(metadata_value, value_type);
	values[AttrNumberGetAttrOffset(Anum_metadata_include_in_telemetry)] =
		BoolGetDatum(include_in_telemetry);

	ts_catalog_insert_values(rel, RelationGetDescr(rel), values, nulls);

	table_close(rel, ShareRowExclusiveLock);

	return metadata_value;
}

static ScanTupleResult
metadata_tuple_delete(TupleInfo *ti, void *data)
{
	ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));

	return SCAN_CONTINUE;
}

void
ts_metadata_drop(const char *metadata_key)
{
	ScanKeyData scankey[1];
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, METADATA),
		.index = catalog_get_index(catalog, METADATA, METADATA_PKEY_IDX),
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = metadata_tuple_delete,
		.data = NULL,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};

	ScanKeyInit(&scankey[0],
				Anum_metadata_key,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(metadata_key));

	ts_scanner_scan(&scanctx);
}

static Datum
get_uuid_by_key(const char *key)
{
	bool isnull;
	Datum uuid;

	uuid = ts_metadata_get_value(key, UUIDOID, &isnull);

	if (isnull)
		uuid = ts_metadata_insert(key, UUIDPGetDatum(ts_uuid_create()), UUIDOID, true);
	return uuid;
}

Datum
ts_metadata_get_uuid(void)
{
	return get_uuid_by_key(METADATA_UUID_KEY_NAME);
}

Datum
ts_metadata_get_exported_uuid(void)
{
	return get_uuid_by_key(METADATA_EXPORTED_UUID_KEY_NAME);
}

Datum
ts_metadata_get_install_timestamp(void)
{
	bool isnull;
	Datum timestamp;

	timestamp = ts_metadata_get_value(METADATA_TIMESTAMP_KEY_NAME, TIMESTAMPTZOID, &isnull);

	if (isnull)
		timestamp = ts_metadata_insert(METADATA_TIMESTAMP_KEY_NAME,
									   TimestampTzGetDatum(GetCurrentTimestamp()),
									   TIMESTAMPTZOID,
									   true);

	return timestamp;
}
