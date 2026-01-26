/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include "jsonb_utils.h"
#include "test_utils.h"
#include "utils/jsonb.h"
#include <fmgr.h>
#include <funcapi.h>

// Declare jsonb_in explicitly
extern Datum jsonb_in(PG_FUNCTION_ARGS);

const char *
jsonb_to_cstring(Jsonb *jsonb)
{
	StringInfoData buf;
	initStringInfo(&buf);
	JsonbToCString(&buf, &jsonb->root, 0);
	return buf.data;
}

Jsonb *
cstring_to_jsonb(const char *cstring)
{
	Datum jsonb_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(cstring));
	Jsonb *jsonb = DatumGetJsonbP(jsonb_datum);
	return jsonb;
}

static void
test_get_str_field()
{
	{
		/* Empty JSONB  doesn't have the key */
		Jsonb *jb = cstring_to_jsonb("{}");
		TestAssertCStringEq(ts_jsonb_get_str_field(jb, "key"), NULL);
		TestAssertJsonbEqCstring(jb, "{}");
		pfree(jb);
	}

	{
		/* JSONB with the key, string value */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": \"value\" }");
		TestAssertCStringEq(ts_jsonb_get_str_field(jb, "key"), "value");
		pfree(jb);
	}

	{
		/* JSONB with the key, integer value */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": 1 }");
		TestAssertCStringEq(ts_jsonb_get_str_field(jb, "key"), "1");
		pfree(jb);
	}

	{
		/* JSONB with the key, empty object value */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": {} }");
		TestAssertCStringEq(ts_jsonb_get_str_field(jb, "key"), "{}");
		pfree(jb);
	}

	{
		/* JSONB with the key, array value */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": [1, 2, 3] }");
		TestAssertCStringEq(ts_jsonb_get_str_field(jb, "key"), "[1, 2, 3]");
		pfree(jb);
	}
}

static void
test_get_bool_field()
{
	{
		/* JSONB with missing key */
		Jsonb *jb = cstring_to_jsonb("{ \"something_else\": {} }");
		bool found;
		TestAssertBoolEq(ts_jsonb_get_bool_field(jb, "key", &found), false);
		TestAssertBoolEq(found, false);
		pfree(jb);
	}

	{
		/* JSONB with the key, true value */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": true }");
		bool found;
		TestAssertBoolEq(ts_jsonb_get_bool_field(jb, "key", &found), true);
		TestAssertBoolEq(found, true);
		pfree(jb);
	}

	{
		/* JSONB with the key, false value */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": false }");
		bool found;
		TestAssertBoolEq(ts_jsonb_get_bool_field(jb, "key", &found), false);
		TestAssertBoolEq(found, true);
		pfree(jb);
	}
}

static void
test_has_key_value_str_field()
{
	{
		/* JSONB with missing key */
		Jsonb *jb = cstring_to_jsonb("{ \"something_else\": {} }");
		TestAssertBoolEq(ts_jsonb_has_key_value_str_field(jb, "key", "value"), false);
		pfree(jb);
	}

	{
		/* JSONB with the key, string value */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": \"value\" }");
		TestAssertBoolEq(ts_jsonb_has_key_value_str_field(jb, "key", "value"), true);
		pfree(jb);
	}

	{
		/* JSONB with the key, array value, only string key=value pairs should be matched */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": [\"value\", \"value2\"] }");
		TestAssertBoolEq(ts_jsonb_has_key_value_str_field(jb, "key", "value"), false);
		pfree(jb);
	}

	{
		/* JSONB with the key, array value, only string key=value pairs should be matched */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": [\"value2\", \"value\"] }");
		TestAssertBoolEq(ts_jsonb_has_key_value_str_field(jb, "key", "value"), false);
		pfree(jb);
	}

	{
		/* Key value pair nested in an object */
		Jsonb *jb = cstring_to_jsonb("{ \"key\": [\"value\", \"value2\"], \"x\": {\"y\": \"z\", "
									 "\"key\": \"value\"}, \"z\": [{\"key\": \"value\"}] }");
		TestAssertBoolEq(ts_jsonb_has_key_value_str_field(jb, "key", "value"), true);
		TestAssertBoolEq(ts_jsonb_has_key_value_str_field(jb, "y", "z"), true);
		TestAssertBoolEq(ts_jsonb_has_key_value_str_field(jb, "z", "value"), false);
		TestAssertBoolEq(ts_jsonb_has_key_value_str_field(jb, "z", "key"), false);
		pfree(jb);
	}
}

static void
test_contains_sparse_index_config()
{
	/* TODO (dbeck): ts_contains_sparse_index_config */
	/* TODO (dbeck): make sure that this function only works with single column sparse index
	 * configurations */
}

TS_TEST_FN(ts_test_jsonb_utils)
{
	test_get_str_field();
	test_get_bool_field();
	test_has_key_value_str_field();
	test_contains_sparse_index_config();
	PG_RETURN_VOID();
}
