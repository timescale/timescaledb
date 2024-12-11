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
