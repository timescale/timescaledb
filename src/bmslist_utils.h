/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

/*
 * Utility functions to simplify working with lists of Bitmapsets.
 * It builds on the Bitmapset and List implementations in Postgres.
 * It is merely a convenience layer on top of the Postgres functionality.
 */

#include "nodes/bitmapset.h"
#include "nodes/pg_list.h"
#include "postgres.h"

#include "export.h"

typedef List *TsBmsList;

/* Just a wrapper around the List type */
extern TSDLLEXPORT TsBmsList ts_bmslist_create(void);

/* Convert the items to a Bitmapset and add it to the list. It doesn't verify if
 * the items are already in the list, so it may add duplicates. */
extern TSDLLEXPORT TsBmsList ts_bmslist_add_member(TsBmsList bmslist, const int *items,
												   int num_items);
extern TSDLLEXPORT TsBmsList ts_bmslist_add_set(TsBmsList bmslist, Bitmapset *set);

/* Checks if the list contains the given items or set. */
extern TSDLLEXPORT bool ts_bmslist_contains_items(TsBmsList bmslist, const int *items,
												  int num_items);
extern TSDLLEXPORT bool ts_bmslist_contains_set(TsBmsList bmslist, Bitmapset *set);

/* Frees the list and all the Bitmapsets it contains. */
extern TSDLLEXPORT void ts_bmslist_free(TsBmsList bmslist);
