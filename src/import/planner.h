/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 *
 * These function were copied from the PostgreSQL core planner, since
 * they were declared static in the core planner, but we need them for
 * our manipulations.
 */

#include <postgres.h>
#include <nodes/execnodes.h>
#include <utils/rel.h>
#include <utils/selfuncs.h>

#include "export.h"

extern TSDLLEXPORT void ts_make_inh_translation_list(Relation oldrelation, Relation newrelation,
													 Index newvarno, List **translated_vars);

extern struct PathTarget *ts_make_partial_grouping_target(struct PlannerInfo *root,
														  PathTarget *grouping_target);

extern bool ts_get_variable_range(PlannerInfo *root, VariableStatData *vardata, Oid sortop,
								  Datum *min, Datum *max);

extern TSDLLEXPORT Plan *
ts_prepare_sort_from_pathkeys(Plan *lefttree, List *pathkeys, Relids relids,
							  const AttrNumber *reqColIdx, bool adjust_tlist_in_place,
							  int *p_numsortkeys, AttrNumber **p_sortColIdx, Oid **p_sortOperators,
							  Oid **p_collations, bool **p_nullsFirst);

extern TSDLLEXPORT Sort *ts_make_sort_from_pathkeys(Plan *lefttree, List *pathkeys, Relids relids);

extern TSDLLEXPORT Sort *ts_make_sort(Plan *lefttree, int numCols, AttrNumber *sortColIdx,
									  Oid *sortOperators, Oid *collations, bool *nullsFirst);

extern TSDLLEXPORT PathKey *ts_make_pathkey_from_sortop(PlannerInfo *root, Expr *expr,
														Relids nullable_relids, Oid ordering_op,
														bool nulls_first, Index sortref,
														bool create_it);

extern TSDLLEXPORT List *ts_build_path_tlist(PlannerInfo *root, Path *path);

extern void ts_ExecSetTupleBound(int64 tuples_needed, PlanState *child_node);
