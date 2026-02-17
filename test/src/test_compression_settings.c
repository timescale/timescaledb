/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
#include <ts_catalog/compression_settings.h>
#include <utils/jsonb.h>

/* Include test_utils.h after all other headers */
#include <test_utils.h>

#define TestAssertParsedCompressionSettingsEqCstring(a, b)                                         \
	do                                                                                             \
	{                                                                                              \
		ParsedCompressionSettings *a_ps = (a);                                                     \
		Assert(a_ps != NULL);                                                                      \
		const char *a_i =                                                                          \
			(a) == NULL ? "<null>" : ts_parsed_compression_settings_to_cstring(a_ps);              \
		const char *b_i = (b) == NULL ? "<null>" : (b);                                            \
		if (strcmp(a_i, b_i) != 0)                                                                 \
			TestFailure("(%s == %s)", a_i, b_i);                                                   \
	} while (0)

static void
test_alter_table_rename_column_effect_jsonb()
{
	const char *jsonb_str =
		"[{\"type\": \"bloom\", \"column\": \"big1\", \"source\": \"config\"}, "
		"{\"type\": \"bloom\", \"column\": \"big2\", \"source\": \"config\"}, "
		"{\"type\": \"bloom\", \"column\": \"value\", \"source\": \"config\"}, "
		"{\"type\": \"bloom\", \"column\": [\"value\", \"big1\", \"big2\"], \"source\": "
		"\"config\"}, "
		"{\"type\": \"bloom\", \"column\": [\"o\", \"big2\"], \"source\": \"config\"}, "
		"{\"type\": \"minmax\", \"column\": \"ts\", \"source\": \"orderby\"}]";

	const char *jsonb_str_expected =
		"[{\"type\": \"bloom\", \"column\": \"big1\", \"source\": \"config\"}, "
		"{\"type\": \"bloom\", \"column\": \"xxl\", \"source\": \"config\"}, "
		"{\"type\": \"bloom\", \"column\": \"value\", \"source\": \"config\"}, "
		"{\"type\": \"bloom\", \"column\": [\"value\", \"big1\", \"xxl\"], \"source\": "
		"\"config\"}, "
		"{\"type\": \"bloom\", \"column\": [\"o\", \"xxl\"], \"source\": \"config\"}, "
		"{\"type\": \"minmax\", \"column\": \"ts\", \"source\": \"orderby\"}]";

	Jsonb *jb = cstring_to_jsonb(jsonb_str);
	ParsedCompressionSettings *parsed_settings = ts_convert_to_parsed_compression_settings(jb);

	TestAssertInt64Eq(list_length(parsed_settings->objects), 6);
	ListCell *obj_cell = NULL;
	foreach (obj_cell, parsed_settings->objects)
	{
		ParsedCompressionSettingsObject *obj = (ParsedCompressionSettingsObject *) lfirst(obj_cell);
		Assert(obj != NULL);
		TestAssertInt64Eq(list_length(obj->pairs), 3);

		ListCell *pair_cell = NULL;
		foreach (pair_cell, obj->pairs)
		{
			ParsedCompressionSettingsPair *pair =
				(ParsedCompressionSettingsPair *) lfirst(pair_cell);
			Assert(pair != NULL);
			if (strcmp(pair->key, "column") != 0)
			{
				continue;
			}
			ListCell *value_cell = NULL;
			foreach (value_cell, pair->values)
			{
				const char *value = (const char *) lfirst(value_cell);
				Assert(value != NULL);
				if (strcmp(value, "big2") == 0)
				{
					/* Replace the value with the new one, allocate from the parsed settings context
					 */
					value_cell->ptr_value =
						ts_parsed_compression_settings_pstrdup(parsed_settings, "xxl");
				}
			}
		}
	}

	Jsonb *result = ts_convert_from_parsed_compression_settings(parsed_settings);
	TestAssertJsonbEqCstring(result, jsonb_str_expected);
	TestAssertParsedCompressionSettingsEqCstring(parsed_settings, jsonb_str_expected);

	/* test the per column settings */
	List *per_column_settings = ts_get_per_column_compression_settings(parsed_settings);
	Assert(per_column_settings != NIL);
	TestAssertInt64Eq(list_length(per_column_settings), 5);
	PerColumnCompressionSettings *per_column_setting = NULL;
	{
		per_column_setting =
			(PerColumnCompressionSettings *) lfirst(list_head(per_column_settings));
		Assert(per_column_setting != NULL);
		TestAssertCStringEq(per_column_setting->column_name, "big1");
		TestAssertInt64Eq(per_column_setting->single_bloom_obj_id, 0);
		TestAssertInt64Eq(per_column_setting->minmax_obj_id, -1);
		/* only part of a single composite bloom index */
		TestAssertInt64Eq(bms_num_members(per_column_setting->composite_bloom_index_obj_ids), 1);
		TestAssertBoolEq(bms_is_member(3, per_column_setting->composite_bloom_index_obj_ids), true);
	}

	{
		per_column_setting = (PerColumnCompressionSettings *) lsecond(per_column_settings);
		Assert(per_column_setting != NULL);
		TestAssertCStringEq(per_column_setting->column_name, "xxl");
		TestAssertInt64Eq(per_column_setting->single_bloom_obj_id, 1);
		TestAssertInt64Eq(per_column_setting->minmax_obj_id, -1);
		/* part of two composite bloom indices */
		TestAssertInt64Eq(bms_num_members(per_column_setting->composite_bloom_index_obj_ids), 2);
		TestAssertBoolEq(bms_is_member(3, per_column_setting->composite_bloom_index_obj_ids), true);
		TestAssertBoolEq(bms_is_member(4, per_column_setting->composite_bloom_index_obj_ids), true);
	}

	{
		per_column_setting = (PerColumnCompressionSettings *) lthird(per_column_settings);
		Assert(per_column_setting != NULL);
		TestAssertCStringEq(per_column_setting->column_name, "value");
		TestAssertInt64Eq(per_column_setting->single_bloom_obj_id, 2);
		TestAssertInt64Eq(per_column_setting->minmax_obj_id, -1);
		/* part of a single composite bloom index */
		TestAssertInt64Eq(bms_num_members(per_column_setting->composite_bloom_index_obj_ids), 1);
		TestAssertBoolEq(bms_is_member(3, per_column_setting->composite_bloom_index_obj_ids), true);
	}

	{
		per_column_setting = (PerColumnCompressionSettings *) lfourth(per_column_settings);
		Assert(per_column_setting != NULL);
		TestAssertCStringEq(per_column_setting->column_name, "o");
		TestAssertInt64Eq(per_column_setting->single_bloom_obj_id, -1);
		TestAssertInt64Eq(per_column_setting->minmax_obj_id, -1);
		/* part of a single composite bloom index */
		TestAssertInt64Eq(bms_num_members(per_column_setting->composite_bloom_index_obj_ids), 1);
		TestAssertBoolEq(bms_is_member(4, per_column_setting->composite_bloom_index_obj_ids), true);
	}

	{
		per_column_setting = (PerColumnCompressionSettings *) lfifth(per_column_settings);
		Assert(per_column_setting != NULL);
		TestAssertCStringEq(per_column_setting->column_name, "ts");
		TestAssertInt64Eq(per_column_setting->single_bloom_obj_id, -1);
		TestAssertInt64Eq(per_column_setting->minmax_obj_id, 5);
		TestAssertPtrEq(per_column_setting->composite_bloom_index_obj_ids, NULL);
	}

	pfree(result);
	ts_free_parsed_compression_settings(parsed_settings);
	pfree(jb);
}

static void
test_alter_table_drop_column_effect_jsonb()
{
	const char *jsonb_str =
		"[{\"type\": \"bloom\", \"column\": \"big1\", \"source\": \"config\"}, "
		"{\"type\": \"bloom\", \"column\": \"big2\", \"source\": \"config\"}, "
		"{\"type\": \"bloom\", \"column\": \"value\", \"source\": \"config\"}, "
		"{\"type\": \"bloom\", \"column\": [\"value\", \"big1\", \"big2\"], \"source\": "
		"\"config\"}, "
		"{\"type\": \"bloom\", \"column\": [\"o\", \"big2\"], \"source\": \"config\"}, "
		"{\"type\": \"minmax\", \"column\": \"ts\", \"source\": \"orderby\"}]";

	const char *jsonb_str_expected =
		"[{\"type\": \"bloom\", \"column\": \"big1\", \"source\": \"config\"}, "
		/* DROP: "{\"type\": \"bloom\", \"column\": \"big2\", \"source\": \"config\"}, " */
		"{\"type\": \"bloom\", \"column\": \"value\", \"source\": \"config\"}, "
		/* DROP: "{\"type\": \"bloom\", \"column\": [\"value\", \"big1\", \"big2\"], \"source\":
		   \"config\"}, */
		/* DROP: "{\"type\": \"bloom\", \"column\": [\"o\", \"big2\"], \"source\": \"config\"}, " */
		"{\"type\": \"minmax\", \"column\": \"ts\", \"source\": \"orderby\"}]";

	Jsonb *jb = cstring_to_jsonb(jsonb_str);
	ParsedCompressionSettings *parsed_settings = ts_convert_to_parsed_compression_settings(jb);

	TestAssertInt64Eq(list_length(parsed_settings->objects), 6);
	ListCell *obj_cell = NULL;
	foreach (obj_cell, parsed_settings->objects)
	{
		ParsedCompressionSettingsObject *obj = (ParsedCompressionSettingsObject *) lfirst(obj_cell);
		Assert(obj != NULL);
		TestAssertInt64Eq(list_length(obj->pairs), 3);
		bool to_remove = false;
		ListCell *pair_cell = NULL;
		foreach (pair_cell, obj->pairs)
		{
			ParsedCompressionSettingsPair *pair =
				(ParsedCompressionSettingsPair *) lfirst(pair_cell);
			Assert(pair != NULL);
			if (strcmp(pair->key, "column") != 0)
			{
				continue;
			}
			ListCell *value_cell = NULL;
			foreach (value_cell, pair->values)
			{
				const char *value = (const char *) lfirst(value_cell);
				Assert(value != NULL);
				if (strcmp(value, "big2") == 0)
				{
					to_remove = true;
					break;
				}
			}
			if (to_remove)
			{
				break;
			}
		}
		if (to_remove)
		{
			/* Remove the object from the list of objects */
			parsed_settings->objects = foreach_delete_current(parsed_settings->objects, obj_cell);
		}
	}

	TestAssertInt64Eq(list_length(parsed_settings->objects), 3);
	Jsonb *result = ts_convert_from_parsed_compression_settings(parsed_settings);
	TestAssertJsonbEqCstring(result, jsonb_str_expected);
	TestAssertParsedCompressionSettingsEqCstring(parsed_settings, jsonb_str_expected);
	ts_free_parsed_compression_settings(parsed_settings);
	pfree(result);
	pfree(jb);
}

static void
test_convert_to_parsed_compression_settings()
{
	{
		/* Objects with a single pair are converted to ParsedCompressionSettings */
		Jsonb *jb = cstring_to_jsonb("{\"key\": \"value\"}");
		ParsedCompressionSettings *parsed_settings = ts_convert_to_parsed_compression_settings(jb);
		TestAssertInt64Eq(list_length(parsed_settings->objects), 1);
		TestAssertParsedCompressionSettingsEqCstring(parsed_settings, "[{\"key\": \"value\"}]");
		Jsonb *result = ts_convert_from_parsed_compression_settings(parsed_settings);
		TestAssertJsonbEqCstring(result, "[{\"key\": \"value\"}]");
		/* per column should be empty because there is no column and no index type */
		List *per_column_settings = ts_get_per_column_compression_settings(parsed_settings);
		TestAssertPtrEq(per_column_settings, NIL);
		ts_free_parsed_compression_settings(parsed_settings);
		pfree(jb);
		pfree(result);
	}

	{
		/* Objects with an array value are converted to ParsedCompressionSettings */
		Jsonb *jb = cstring_to_jsonb("{\"key\": [\"value\", \"value2\"]}");
		ParsedCompressionSettings *parsed_settings = ts_convert_to_parsed_compression_settings(jb);
		TestAssertInt64Eq(list_length(parsed_settings->objects), 1);
		TestAssertParsedCompressionSettingsEqCstring(parsed_settings,
													 "[{\"key\": [\"value\", \"value2\"]}]");
		Jsonb *result = ts_convert_from_parsed_compression_settings(parsed_settings);
		TestAssertJsonbEqCstring(result, "[{\"key\": [\"value\", \"value2\"]}]");
		ts_free_parsed_compression_settings(parsed_settings);
		pfree(jb);
		pfree(result);
	}

	{
		/* Objects with multiple pairs are converted to ParsedCompressionSettings */
		Jsonb *jb = cstring_to_jsonb("{\"key\": [\"value\", \"value2\"], \"key2\": \"value3\"}");
		ParsedCompressionSettings *parsed_settings = ts_convert_to_parsed_compression_settings(jb);
		TestAssertInt64Eq(list_length(parsed_settings->objects), 1);
		TestAssertParsedCompressionSettingsEqCstring(parsed_settings,
													 "[{\"key\": [\"value\", \"value2\"], "
													 "\"key2\": \"value3\"}]");
		Jsonb *result = ts_convert_from_parsed_compression_settings(parsed_settings);
		TestAssertJsonbEqCstring(result,
								 "[{\"key\": [\"value\", \"value2\"], \"key2\": \"value3\"}]");
		ts_free_parsed_compression_settings(parsed_settings);
		pfree(jb);
		pfree(result);
	}

	{
		/* Empty objects are converted to NULL */
		Jsonb *jb = cstring_to_jsonb("{}");
		ParsedCompressionSettings *parsed_settings = ts_convert_to_parsed_compression_settings(jb);
		TestAssertPtrEq(parsed_settings, NULL);
		pfree(jb);
	}

	{
		/* Empty arrays are ignored and converted to NULL */
		Jsonb *jb = cstring_to_jsonb("[]");
		ParsedCompressionSettings *parsed_settings = ts_convert_to_parsed_compression_settings(jb);
		TestAssertPtrEq(parsed_settings, NULL);
		pfree(jb);
	}

	{
		/* Empty objects are ignored */
		Jsonb *jb = cstring_to_jsonb("[{}, {\"key\": \"value\"}, {}, {}]");
		ParsedCompressionSettings *parsed_settings = ts_convert_to_parsed_compression_settings(jb);
		TestAssertInt64Eq(list_length(parsed_settings->objects), 1);
		TestAssertParsedCompressionSettingsEqCstring(parsed_settings, "[{\"key\": \"value\"}]");
		Jsonb *result = ts_convert_from_parsed_compression_settings(parsed_settings);
		TestAssertJsonbEqCstring(result, "[{\"key\": \"value\"}]");
		ts_free_parsed_compression_settings(parsed_settings);
		pfree(jb);
		pfree(result);
	}

	{
		/* Unexpected nesting of objects return an error */
		Jsonb *jb = cstring_to_jsonb("{\"key\": [{\"key2\": \"value2\"}]}");
		TestEnsureError(ts_convert_to_parsed_compression_settings(jb));
		pfree(jb);
	}

	{
		/* Unexpected nesting of objects return an error */
		Jsonb *jb = cstring_to_jsonb("{\"key\": {\"key2\": \"value2\"}}");
		TestEnsureError(ts_convert_to_parsed_compression_settings(jb));
		pfree(jb);
	}

	{
		/* Unexpected nesting of objects return an error */
		Jsonb *jb = cstring_to_jsonb("{\"key\": [{\"key2\": \"value2\"}, {\"key3\": \"value3\"}]}");
		TestEnsureError(ts_convert_to_parsed_compression_settings(jb));
		pfree(jb);
	}

	{
		/* Unexpected nesting of arrays return an error */
		Jsonb *jb = cstring_to_jsonb("{\"key\": [[\"value2\", \"value3\"]]}");
		TestEnsureError(ts_convert_to_parsed_compression_settings(jb));
		pfree(jb);
	}

	{
		/* Unexpected nesting of arrays return an error */
		Jsonb *jb = cstring_to_jsonb("[[{\"key\": [\"value2\", \"value3\"]}]]");
		TestEnsureError(ts_convert_to_parsed_compression_settings(jb));
		pfree(jb);
	}

	{
		/* Unexpected nesting of arrays return an error */
		Jsonb *jb = cstring_to_jsonb("[{\"key\": [\"value2\", [\"value3\"]]}]");
		TestEnsureError(ts_convert_to_parsed_compression_settings(jb));
		pfree(jb);
	}
}

TS_TEST_FN(ts_test_compression_settings)
{
	test_alter_table_rename_column_effect_jsonb();
	test_alter_table_drop_column_effect_jsonb();
	test_convert_to_parsed_compression_settings();
	PG_RETURN_VOID();
}

