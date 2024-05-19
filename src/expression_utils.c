/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <nodes/primnodes.h>
#include <utils/lsyscache.h>

#include "export.h"
#include "expression_utils.h"

/*
 * This function is meant to extract the expression components to be used in a ScanKey.
 *
 * It will work on the following expression types:
 * - Var OP Expr
 *
 * Var OP Var is not supported as that will not work with scankeys.
 *
 */
bool TSDLLEXPORT
ts_extract_expr_args(Expr *expr, Var **var, Expr **arg_value, Oid *opno, Oid *opcode)
{
	List *args;
	Oid expr_opno, expr_opcode;

	switch (nodeTag(expr))
	{
		case T_OpExpr:
		{
			OpExpr *opexpr = castNode(OpExpr, expr);
			args = opexpr->args;
			expr_opno = opexpr->opno;
			expr_opcode = opexpr->opfuncid;

			if (opexpr->opresulttype != BOOLOID)
				return false;

			break;
		}
		case T_ScalarArrayOpExpr:
		{
			ScalarArrayOpExpr *sa_opexpr = castNode(ScalarArrayOpExpr, expr);
			args = sa_opexpr->args;
			expr_opno = sa_opexpr->opno;
			expr_opcode = sa_opexpr->opfuncid;
			break;
		}
		default:
			return false;
	}

	if (list_length(args) != 2)
		return false;

	Expr *leftop = linitial(args);
	Expr *rightop = lsecond(args);

	if (IsA(leftop, RelabelType))
		leftop = castNode(RelabelType, leftop)->arg;
	if (IsA(rightop, RelabelType))
		rightop = castNode(RelabelType, rightop)->arg;

	if (IsA(leftop, Var) && !IsA(rightop, Var))
	{
		/* ignore system columns */
		if (castNode(Var, leftop)->varattno <= 0)
			return false;

		*var = castNode(Var, leftop);

		*arg_value = rightop;
		*opno = expr_opno;
		if (opcode)
			*opcode = expr_opcode;
		return true;
	}
	else if (IsA(rightop, Var) && !IsA(leftop, Var))
	{
		/* ignore system columns */
		if (castNode(Var, rightop)->varattno <= 0)
			return false;

		*var = castNode(Var, rightop);
		*arg_value = leftop;
		expr_opno = get_commutator(expr_opno);
		if (!OidIsValid(expr_opno))
			return false;

		if (opcode)
		{
			expr_opcode = get_opcode(expr_opno);
			if (!OidIsValid(expr_opcode))
				return false;
			*opcode = expr_opcode;
		}

		*opno = expr_opno;

		return true;
	}

	return false;
}
