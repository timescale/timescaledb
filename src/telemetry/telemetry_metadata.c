/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/timestamp.h>

#include "ts_catalog/catalog.h"
#include "ts_catalog/metadata.h"
#include "uuid.h"
#include "telemetry/telemetry_metadata.h"
#include "scan_iterator.h"
#include "jsonb_utils.h"

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

		key = slot_getattr(ti->slot, Anum_metadata_key, &key_isnull);

		include_entry =
			!key_isnull &&
			DatumGetBool(slot_getattr(ti->slot, Anum_metadata_include_in_telemetry, &key_isnull));

		if (include_entry)
		{
			Name key_name = DatumGetName(key);

			/* skip keys included as toplevel items */
			if (namestrcmp(key_name, METADATA_UUID_KEY_NAME) != 0 &&
				namestrcmp(key_name, METADATA_EXPORTED_UUID_KEY_NAME) != 0 &&
				namestrcmp(key_name, METADATA_TIMESTAMP_KEY_NAME) != 0)
			{
				value = slot_getattr(ti->slot, Anum_metadata_value, &value_isnull);

				if (!value_isnull)
					ts_jsonb_add_str(state, DatumGetCString(key), TextDatumGetCString(value));
			}
		}
	}
}
