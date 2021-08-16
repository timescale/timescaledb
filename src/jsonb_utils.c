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

#include "compat/compat.h"

#if PG13_LT
#include <utils/jsonapi.h>
#else
#include <common/jsonapi.h>
#endif

#include "export.h"

#include "jsonb_utils.h"

static void ts_jsonb_add_pair(JsonbParseState *state, JsonbValue *key, JsonbValue *value);

void
ts_jsonb_add_null(JsonbParseState *state, const char *key)
{
	JsonbValue json_value;

	json_value.type = jbvNull;
	ts_jsonb_add_value(state, key, &json_value);
}

void
ts_jsonb_add_bool(JsonbParseState *state, const char *key, bool boolean)
{
	JsonbValue json_value;

	json_value.type = jbvBool;
	json_value.val.boolean = boolean;

	ts_jsonb_add_value(state, key, &json_value);
}

void
ts_jsonb_add_str(JsonbParseState *state, const char *key, const char *value)
{
	JsonbValue json_value;

	Assert(value != NULL);
	/* If there is a null entry, don't add it to the JSON */
	if (value == NULL)
		return;

	json_value.type = jbvString;
	json_value.val.string.val = (char *) value;
	json_value.val.string.len = strlen(value);

	ts_jsonb_add_value(state, key, &json_value);
}

void
ts_jsonb_add_int32(JsonbParseState *state, const char *key, const int32 int_value)
{
	Numeric value;

	value = DatumGetNumeric(DirectFunctionCall1(int4_numeric, Int32GetDatum(int_value)));

	ts_jsonb_add_numeric(state, key, value);
}

void
ts_jsonb_add_int64(JsonbParseState *state, const char *key, const int64 int_value)
{
	Numeric value;

	value = DatumGetNumeric(DirectFunctionCall1(int8_numeric, Int64GetDatum(int_value)));

	ts_jsonb_add_numeric(state, key, value);
}

void
ts_jsonb_add_interval(JsonbParseState *state, const char *key, Interval *interval)
{
	char *value;

	value = DatumGetCString(DirectFunctionCall1(interval_out, IntervalPGetDatum(interval)));

	ts_jsonb_add_str(state, key, value);
}

void
ts_jsonb_add_numeric(JsonbParseState *state, const char *key, const Numeric value)
{
	JsonbValue json_value = { .type = jbvNumeric, .val.numeric = value };

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

char *
ts_jsonb_get_str_field(const Jsonb *jsonb, const char *key)
{
	/*
	 * `jsonb_object_field_text` returns NULL when the field is not found so
	 * we cannot use `DirectFunctionCall`
	 */
	LOCAL_FCINFO(fcinfo, 2);
	Datum result;

	InitFunctionCallInfoData(*fcinfo, NULL, 2, InvalidOid, NULL, NULL);

	FC_SET_ARG(fcinfo, 0, PointerGetDatum(jsonb));
	FC_SET_ARG(fcinfo, 1, PointerGetDatum(cstring_to_text(key)));

	result = jsonb_object_field_text(fcinfo);

	if (fcinfo->isnull)
		return NULL;

	return text_to_cstring(DatumGetTextP(result));
}

TimestampTz
ts_jsonb_get_time_field(const Jsonb *jsonb, const char *key, bool *field_found)
{
	Datum time_datum;
	char *time_str = ts_jsonb_get_str_field(jsonb, key);

	if (time_str == NULL)
	{
		*field_found = false;
		return DT_NOBEGIN;
	}

	time_datum = DirectFunctionCall3(timestamptz_in,
									 /* str= */ CStringGetDatum(time_str),
									 /* unused */ Int32GetDatum(-1),
									 /* typmod= */ Int32GetDatum(-1));

	*field_found = true;
	return DatumGetTimestampTz(time_datum);
}

bool
ts_jsonb_get_bool_field(const Jsonb *json, const char *key, bool *field_found)
{
	Datum bool_datum;
	char *bool_str = ts_jsonb_get_str_field(json, key);

	if (bool_str == NULL)
	{
		*field_found = false;
		return false;
	}

	bool_datum = DirectFunctionCall1(boolin, CStringGetDatum(bool_str));

	*field_found = true;
	return DatumGetBool(bool_datum);
}

int32
ts_jsonb_get_int32_field(const Jsonb *json, const char *key, bool *field_found)
{
	Datum int_datum;
	char *int_str = ts_jsonb_get_str_field(json, key);

	if (int_str == NULL)
	{
		*field_found = false;
		return 0;
	}

	int_datum = DirectFunctionCall1(int4in, CStringGetDatum(int_str));

	*field_found = true;
	return DatumGetInt32(int_datum);
}

int64
ts_jsonb_get_int64_field(const Jsonb *json, const char *key, bool *field_found)
{
	Datum int_datum;
	char *int_str = ts_jsonb_get_str_field(json, key);

	if (int_str == NULL)
	{
		*field_found = false;
		return 0;
	}

	int_datum = DirectFunctionCall1(int8in, CStringGetDatum(int_str));

	*field_found = true;
	return DatumGetInt64(int_datum);
}

Interval *
ts_jsonb_get_interval_field(const Jsonb *json, const char *key)
{
	Datum interval_datum;
	char *interval_str = ts_jsonb_get_str_field(json, key);

	if (interval_str == NULL)
		return NULL;

	interval_datum =
		DirectFunctionCall3(interval_in, CStringGetDatum(interval_str), InvalidOid, -1);

	return DatumGetIntervalP(interval_datum);
}
