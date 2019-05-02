/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>

#include "catalog.h"
#include "metadata.h"
#include "telemetry/uuid.h"
#include "telemetry/telemetry_metadata.h"
#include "scan_iterator.h"
#include "jsonb_utils.h"

#define METADATA_UUID_KEY_NAME "uuid"
#define METADATA_EXPORTED_UUID_KEY_NAME "exported_uuid"
#define METADATA_TIMESTAMP_KEY_NAME "install_timestamp"

/*
 * add all entries from _timescaledb_catalog.metadata
 */
void
ts_telemetry_metadata_add_values(JsonbParseState *state)
{
	Datum key, value;
	bool key_isnull, value_isnull, include_entry;
	ScanIterator iterator =
		ts_scan_iterator_create(METADATA, AccessShareLock, CurrentMemoryContext);
	iterator.ctx.index = catalog_get_index(ts_catalog_get(), METADATA, METADATA_PKEY_IDX);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = iterator.tinfo;

		key = heap_getattr(ti->tuple, Anum_metadata_key, ti->desc, &key_isnull);
		include_entry =
			!key_isnull &&
			DatumGetBool(
				heap_getattr(ti->tuple, Anum_metadata_include_in_telemetry, ti->desc, &key_isnull));

		if (include_entry)
		{
			Name key_name = DatumGetName(key);

			/* skip keys included as toplevel items */
			if (namestrcmp(key_name, METADATA_UUID_KEY_NAME) != 0 &&
				namestrcmp(key_name, METADATA_EXPORTED_UUID_KEY_NAME) != 0 &&
				namestrcmp(key_name, METADATA_TIMESTAMP_KEY_NAME) != 0)
			{
				value = heap_getattr(ti->tuple, Anum_metadata_value, ti->desc, &value_isnull);

				if (!value_isnull)
					ts_jsonb_add_str(state, DatumGetCString(key), TextDatumGetCString(value));
			}
		}
	}
}

static Datum
get_uuid_by_key(const char *key)
{
	bool isnull;
	Datum uuid;

	uuid = ts_metadata_get_value(CStringGetDatum(key), CSTRINGOID, UUIDOID, &isnull);

	if (isnull)
		uuid = ts_metadata_insert(CStringGetDatum(key),
								  CSTRINGOID,
								  UUIDPGetDatum(ts_uuid_create()),
								  UUIDOID,
								  true);
	return uuid;
}

Datum
ts_telemetry_metadata_get_uuid(void)
{
	return get_uuid_by_key(METADATA_UUID_KEY_NAME);
}

Datum
ts_telemetry_metadata_get_exported_uuid(void)
{
	return get_uuid_by_key(METADATA_EXPORTED_UUID_KEY_NAME);
}

Datum
ts_telemetry_metadata_get_install_timestamp(void)
{
	bool isnull;
	Datum timestamp;

	timestamp = ts_metadata_get_value(CStringGetDatum(METADATA_TIMESTAMP_KEY_NAME),
									  CSTRINGOID,
									  TIMESTAMPTZOID,
									  &isnull);

	if (isnull)
		timestamp = ts_metadata_insert(CStringGetDatum(METADATA_TIMESTAMP_KEY_NAME),
									   CSTRINGOID,
									   TimestampTzGetDatum(GetCurrentTimestamp()),
									   TIMESTAMPTZOID,
									   true);

	return timestamp;
}
