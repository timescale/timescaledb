/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>
#include <commands/tablecmds.h>

#include "ts_catalog/catalog.h"
#include "ts_catalog/metadata.h"
#include "uuid.h"
#include "telemetry/telemetry_metadata.h"
#include "scan_iterator.h"
#include "jsonb_utils.h"

#if PG14_LT
/* Copied from jsonb_util.c */
static void
JsonbToJsonbValue(Jsonb *jsonb, JsonbValue *val)
{
	val->type = jbvBinary;
	val->val.binary.data = &jsonb->root;
	val->val.binary.len = VARSIZE(jsonb) - VARHDRSZ;
}
#endif

void
ts_telemetry_event_truncate(void)
{
	RangeVar rv = {
		.schemaname = CATALOG_SCHEMA_NAME,
		.relname = TELEMETRY_EVENT_TABLE_NAME,
	};
	ExecuteTruncate(&(TruncateStmt){
		.type = T_TruncateStmt,
		.relations = list_make1(&rv),
		.behavior = DROP_RESTRICT,
	});
}

void
ts_telemetry_events_add(JsonbParseState *state)
{
	ScanIterator iterator =
		ts_scan_iterator_create(TELEMETRY_EVENT, AccessShareLock, CurrentMemoryContext);
	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = iterator.tinfo;
		TupleDesc tupdesc = ti->slot->tts_tupleDescriptor;
		bool created_isnull, tag_isnull, value_isnull;
		Datum created = slot_getattr(ti->slot, Anum_telemetry_event_created, &created_isnull);
		Datum tag = slot_getattr(ti->slot, Anum_telemetry_event_tag, &tag_isnull);
		Datum body = slot_getattr(ti->slot, Anum_telemetry_event_body, &value_isnull);

		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);
		if (!created_isnull)
			ts_jsonb_add_str(state,
							 NameStr(
								 TupleDescAttr(tupdesc, Anum_telemetry_event_created - 1)->attname),
							 DatumGetCString(DirectFunctionCall1(timestamptz_out, created)));

		if (!tag_isnull)
			ts_jsonb_add_str(state,
							 NameStr(TupleDescAttr(tupdesc, Anum_telemetry_event_tag - 1)->attname),
							 pstrdup(NameStr(*DatumGetName(tag))));

		if (!value_isnull)
		{
			JsonbValue jsonb_value;
			JsonbToJsonbValue(DatumGetJsonbPCopy(body), &jsonb_value);
			ts_jsonb_add_value(state,
							   NameStr(
								   TupleDescAttr(tupdesc, Anum_telemetry_event_body - 1)->attname),
							   &jsonb_value);
		}
		pushJsonbValue(&state, WJB_END_OBJECT, NULL);
	}
	pushJsonbValue(&state, WJB_END_ARRAY, NULL);
}

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
					ts_jsonb_add_str(state,
									 pstrdup(NameStr(*key_name)),
									 pstrdup(TextDatumGetCString(value)));
			}
		}
	}
}
