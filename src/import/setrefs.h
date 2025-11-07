/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "export.h"
#include <access/attnum.h>
#include <nodes/nodeFuncs.h>
/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */

typedef struct
{
	int varno;			 /* RT index of Var */
	AttrNumber varattno; /* attr number of Var */
	AttrNumber resno;	 /* TLE position of Var */
} ts_tlist_vinfo;

typedef struct
{
	List *tlist;								/* underlying target list */
	int num_vars;								/* number of plain Var tlist entries */
	bool has_non_vars;							/* are there other entries? */
	ts_tlist_vinfo vars[FLEXIBLE_ARRAY_MEMBER]; /* has num_vars entries */
} ts_indexed_tlist;

extern TSDLLEXPORT ts_indexed_tlist *ts_build_tlist_index(List *tlist);
extern TSDLLEXPORT Node *ts_replace_tlist_expr(Node *node, ts_indexed_tlist *subplan_itlist,
											   int newvarno, int rtoffset);
