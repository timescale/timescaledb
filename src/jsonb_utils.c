/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>

#include <utils/builtins.h>
#include <utils/json.h>
#include <utils/jsonb.h>
#include <utils/jsonapi.h>

#include "compat.h"
#include "export.h"

#include "jsonb_utils.h"

static void ts_jsonb_add_pair(JsonbParseState *state, JsonbValue *key, JsonbValue *value);

TSDLLEXPORT void
ts_jsonb_add_bool(JsonbParseState *state, const char *key, bool boolean)
{
	JsonbValue json_value;

	json_value.type = jbvBool;
	json_value.val.boolean = boolean;

	ts_jsonb_add_value(state, key, &json_value);
}

TSDLLEXPORT void
ts_jsonb_add_str(JsonbParseState *state, const char *key, const char *value)
{
	JsonbValue json_value;

	/* If there is a null entry, don't add it to the JSON */
	if (value == NULL)
		return;

	json_value.type = jbvString;
	json_value.val.string.val = (char *) value;
	json_value.val.string.len = strlen(value);

	ts_jsonb_add_value(state, key, &json_value);
}

void
ts_jsonb_add_value(JsonbParseState *state, const char *key, JsonbValue *value)
{
	JsonbValue json_key;

	Assert(key != NULL);
	if (value == NULL)
		return;

	json_key.type = jbvString;
	json_key.val.string.val = (char *) key;
	json_key.val.string.len = strlen(key);

	ts_jsonb_add_pair(state, &json_key, value);
}

static void
ts_jsonb_add_pair(JsonbParseState *state, JsonbValue *key, JsonbValue *value)
{
	Assert(state != NULL);
	Assert(key != NULL);
	if (value == NULL)
		return;

	pushJsonbValue(&state, WJB_KEY, key);
	pushJsonbValue(&state, WJB_VALUE, value);
}

TSDLLEXPORT text *
ts_jsonb_get_text_field(Jsonb *json, text *field_name)
{
	/*
	 * `jsonb_object_field_text` returns NULL when the field is not found so
	 * we cannot use `DirectFunctionCall`
	 */
	LOCAL_FCINFO(fcinfo, 2);
	Datum result;

	InitFunctionCallInfoData(*fcinfo, NULL, 2, InvalidOid, NULL, NULL);

	FC_SET_ARG(fcinfo, 0, PointerGetDatum(json));
	FC_SET_ARG(fcinfo, 1, PointerGetDatum(field_name));

	result = jsonb_object_field_text(fcinfo);

	if (fcinfo->isnull)
		return NULL;

	return DatumGetTextP(result);
}

TSDLLEXPORT char *
ts_jsonb_get_str_field(Jsonb *license, text *field_name)
{
	text *text_str = ts_jsonb_get_text_field(license, field_name);

	if (text_str == NULL)
		return NULL;

	return text_to_cstring(text_str);
}

TSDLLEXPORT TimestampTz
ts_jsonb_get_time_field(Jsonb *license, text *field_name, bool *field_found)
{
	Datum time_datum;
	text *time_str = ts_jsonb_get_text_field(license, field_name);

	if (time_str == NULL)
	{
		*field_found = false;
		return DT_NOBEGIN;
	}

	time_datum = DirectFunctionCall3(timestamptz_in,
									 /* str= */ CStringGetDatum(text_to_cstring(time_str)),
									 /* unused */ Int32GetDatum(-1),
									 /* typmod= */ Int32GetDatum(-1));

	*field_found = true;
	return DatumGetTimestampTz(time_datum);
}
