/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/primnodes.h>
#include <utils/lsyscache.h>

#include "export.h"

bool TSDLLEXPORT ts_extract_expr_args(Expr *expr, Var **var, Expr **arg_value, Oid *opno,
									  Oid *opcode);

TSDLLEXPORT List *ts_build_trivial_custom_output_targetlist(List *scan_targetlist);
TSDLLEXPORT Node *ts_resolve_outer_special_vars(Node *node, Plan *childplan);

typedef Plan *(*ts_plan_tree_walkerfunc)(Plan *, void *);
extern TSDLLEXPORT Plan *ts_plan_tree_walker(Plan *plan, ts_plan_tree_walkerfunc func,
											 void *context);
