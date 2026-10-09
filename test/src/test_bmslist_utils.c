/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include "bmslist_utils.h"
#include "test_utils.h"
#include <fmgr.h>
#include <funcapi.h>
#include <ts_catalog/compression_settings.h>

static void
test_empty_bmslist_contains_items(void)
{
	TsBmsList bmslist = ts_bmslist_create();
	int items[] = { 1, 2, 3 };
	bool result = ts_bmslist_contains_items(bmslist, items, 3);
	TestAssertBoolEq(result, false);
}

static void
test_non_empty_bmslist_contains_items(void)
{
	TsBmsList bmslist = ts_bmslist_create();
	int items[] = { 1, 2, 3 };
	bmslist = ts_bmslist_add_member(bmslist, items, 1);
	bmslist = ts_bmslist_add_member(bmslist, items, 2);
	bmslist = ts_bmslist_add_member(bmslist, items, 3);

	bool result = ts_bmslist_contains_items(bmslist, items, 3);
	TestAssertBoolEq(result, true);

	result = ts_bmslist_contains_items(bmslist, items, 2);
	TestAssertBoolEq(result, true);

	result = ts_bmslist_contains_items(bmslist, items, 1);
	TestAssertBoolEq(result, true);

	items[0] = 4;
	result = ts_bmslist_contains_items(bmslist, items, 3);
	TestAssertBoolEq(result, false);

	result = ts_bmslist_contains_items(bmslist, items, 2);
	TestAssertBoolEq(result, false);

	result = ts_bmslist_contains_items(bmslist, items, 1);
	TestAssertBoolEq(result, false);

	ts_bmslist_free(bmslist);
}

static void
test_bmslist_with_empty_set(void)
{
	TsBmsList bmslist = ts_bmslist_create();
	int items[] = { 1, 2 };
	bmslist = ts_bmslist_add_member(bmslist, items, 2);
	bmslist = lappend(bmslist, NULL);
	bmslist = ts_bmslist_add_member(bmslist, items, 1);

	TestAssertInt64Eq(list_length(bmslist), 3);
	TestAssertBoolEq(ts_bmslist_contains_items(bmslist, items, 1), true);

	ts_bmslist_free(bmslist);
}

TS_TEST_FN(ts_test_bmslist_utils)
{
	test_empty_bmslist_contains_items();
	test_non_empty_bmslist_contains_items();
	test_bmslist_with_empty_set();
	PG_RETURN_VOID();
}
