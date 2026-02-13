/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <common/jsonapi.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/json.h>
#include <utils/jsonb.h>

#include "compat/compat.h"
#include "export.h"
#include "jsonb_utils.h"
#include "utils.h"

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

static void
ts_jsonb_add_str_element(JsonbParseState *state, const char *elem)
{
	JsonbValue json_value;

	Assert(elem != NULL);
	/* If there is a null entry, don't add it to the JSON */
	if (elem == NULL)
		return;

	json_value.type = jbvString;
	json_value.val.string.val = (char *) elem;
	json_value.val.string.len = strlen(elem);

	pushJsonbValue(&state, WJB_ELEM, &json_value);
}

void
ts_jsonb_add_str_array(JsonbParseState *state, const char *key, const char **values, int num_values)
{
	JsonbValue json_key;
	Assert(key != NULL);
	Assert(values != NULL);
	Assert(num_values > 0);
	Assert(key[0] != '\0');
	if (key == NULL || values == NULL || num_values <= 0 || key[0] == '\0')
		return;

	json_key.type = jbvString;
	json_key.val.string.val = (char *) key;
	json_key.val.string.len = strlen(key);
	pushJsonbValue(&state, WJB_KEY, &json_key);

	pushJsonbValue(&state, WJB_BEGIN_ARRAY, NULL);
	for (int i = 0; i < num_values; i++)
	{
		if (values[i] == NULL || values[i][0] == '\0')
			continue;
		ts_jsonb_add_str_element(state, values[i]);
	}
	pushJsonbValue(&state, WJB_END_ARRAY, NULL);
}

static PGFunction
get_convert_func(Oid typeid)
{
	switch (typeid)
	{
		case INT2OID:
			return int2_numeric;
		case INT4OID:
			return int4_numeric;
		case INT8OID:
			return int8_numeric;
		default:
			return NULL;
	}
}

void
ts_jsonb_set_value_by_type(JsonbValue *value, Oid typeid, Datum datum)
{
	switch (typeid)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case NUMERICOID:
		{
			PGFunction func = get_convert_func(typeid);
			value->type = jbvNumeric;
			value->val.numeric = DatumGetNumeric(func ? DirectFunctionCall1(func, datum) : datum);
			break;
		}
		default:
		{
			char *str = ts_datum_to_string(datum, typeid);
			value->type = jbvString;
			value->val.string.val = str;
			value->val.string.len = strlen(str);
			break;
		}
	}
}

void
ts_jsonb_add_int32(JsonbParseState *state, const char *key, const int32 int_value)
{
	JsonbValue json_value;

	ts_jsonb_set_value_by_type(&json_value, INT4OID, Int32GetDatum(int_value));
	ts_jsonb_add_value(state, key, &json_value);
}

void
ts_jsonb_add_int64(JsonbParseState *state, const char *key, const int64 int_value)
{
	JsonbValue json_value;

	ts_jsonb_set_value_by_type(&json_value, INT8OID, Int64GetDatum(int_value));
	ts_jsonb_add_value(state, key, &json_value);
}

void
ts_jsonb_add_interval(JsonbParseState *state, const char *key, Interval *interval)
{
	JsonbValue json_value;

	ts_jsonb_set_value_by_type(&json_value, INTERVALOID, IntervalPGetDatum(interval));
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

bool
ts_jsonb_equal(const Jsonb *left, const Jsonb *right)
{
	/* Quick exit if both are NULL or point to same thing. */
	if (left == right)
		return true;

	if (left == NULL || right == NULL)
		return false;

	Assert(left != NULL && right != NULL);

	Datum result = DirectFunctionCall2(jsonb_eq, PointerGetDatum(left), PointerGetDatum(right));

	return DatumGetBool(result);
}

/*
 * searches for any occurrences of a matching key value pair. compatible with nested and
 * array jsonbs
 */
bool
ts_jsonb_has_key_value_str_field(Jsonb *jb, const char *key, const char *value)
{
	JsonbIterator *it;
	JsonbValue v;
	JsonbIteratorToken r;

	if (jb == NULL || JB_ROOT_COUNT(jb) == 0)
		return false;

	if (JB_ROOT_IS_SCALAR(jb))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("cannot find from scalar")));

	it = JsonbIteratorInit(&jb->root);

	while ((r = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		if (r == WJB_KEY && v.type == jbvString && ((int) strlen(key) == v.val.string.len) &&
			strncmp(key, v.val.string.val, v.val.string.len) == 0)
		{
			r = JsonbIteratorNext(&it, &v, false);
			Assert(r == WJB_VALUE || r == WJB_BEGIN_ARRAY);

			if (v.type == jbvArray)
			{
				/* iterate over the array members and consume them all as this function should only
				 * match single values and not arrays */
				int i = 0;
				int n_elems = v.val.array.nElems;
				while (i < n_elems && (r = JsonbIteratorNext(&it, &v, false)) == WJB_ELEM)
				{
					++i;
				}
				continue;
			}
			else if (v.type != jbvString)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("Jsonb value is of type \"%s\", but expected type \"string\"",
								JsonbTypeName(&v))));
			}

			if (v.type == jbvString && ((int) strlen(value) == v.val.string.len) &&
				strncmp(value, v.val.string.val, v.val.string.len) == 0)
			{
				return true;
			}
		}
	}

	return false;
}
