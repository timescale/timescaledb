/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include "bmslist_utils.h"

TsBmsList
ts_bmslist_create(void)
{
	/* Create a new empty list is NULL */
	return NIL;
}

TsBmsList
ts_bmslist_add_member(TsBmsList bmslist, const int *items, int num_items)
{
	Assert(items != NULL);
	Assert(num_items > 0);

	if (items == NULL || num_items == 0)
		return bmslist;

	/* Create a new Bitmapset for the items */
	int first_item = items[0];
	Bitmapset *set = bms_make_singleton(first_item);
	for (int i = 1; i < num_items; i++)
	{
		int item = items[i];
		set = bms_add_member(set, item);
	}

	/* Add the new set to the list */
	return lappend(bmslist, set);
}

TsBmsList
ts_bmslist_add_set(TsBmsList bmslist, Bitmapset *set)
{
	Assert(set != NULL);
	return lappend(bmslist, set);
}

bool
ts_bmslist_contains_items(TsBmsList bmslist, const int *items, int num_items)
{
	bool result = false;

	Assert(items != NULL);
	Assert(num_items > 0);

	if (items == NULL || num_items == 0)
		return false;

	/* Create a new Bitmapset for the items */
	int first_item = items[0];
	Bitmapset *set = bms_make_singleton(first_item);
	for (int i = 1; i < num_items; i++)
	{
		int item = items[i];
		set = bms_add_member(set, item);
	}

	result = ts_bmslist_contains_set(bmslist, set);

	bms_free(set);
	return result;
}

bool
ts_bmslist_contains_set(TsBmsList bmslist, Bitmapset *set)
{
	ListCell *lc;
	Assert(set != NULL);

	if (set == NULL)
		return false;

	foreach (lc, bmslist)
	{
		Bitmapset *item = (Bitmapset *) lfirst(lc);
		if (bms_equal(item, set))
			return true;
	}

	return false;
}

void
ts_bmslist_free(TsBmsList bmslist)
{
	list_free_deep(bmslist);
}
