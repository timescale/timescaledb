/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#pragma once

#include <postgres.h>


extern TSDLLEXPORT Sort *ts_make_sort_from_pathkeys(Plan *lefttree, List *pathkeys, Relids relids);

extern TSDLLEXPORT Sort *ts_make_sort(Plan *lefttree, int numCols, AttrNumber *sortColIdx,
									  Oid *sortOperators, Oid *collations, bool *nullsFirst);

extern TSDLLEXPORT Plan *
ts_prepare_sort_from_pathkeys(Plan *lefttree, List *pathkeys, Relids relids,
							  const AttrNumber *reqColIdx, bool adjust_tlist_in_place,
							  int *p_numsortkeys, AttrNumber **p_sortColIdx, Oid **p_sortOperators,
							  Oid **p_collations, bool **p_nullsFirst);

extern TSDLLEXPORT Node *ts_replace_nestloop_params(PlannerInfo *root, Node *expr);

extern TSDLLEXPORT List *ts_build_path_tlist(PlannerInfo *root, Path *path);

extern TSDLLEXPORT void
ts_label_sort_with_costsize(PlannerInfo *root, Sort *plan, double limit_tuples);
