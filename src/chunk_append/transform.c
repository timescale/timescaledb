/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <utils/lsyscache.h>

#include "compat.h"
#ifdef PG12_LT
#include <optimizer/clauses.h>
#else
#include <optimizer/optimizer.h>
#endif

#include "chunk_append/transform.h"
#include "utils.h"

#define DATATYPE_PAIR(left, right, type1, type2)                                                   \
	((left == type1 && right == type2) || (left == type2 && right == type1))

/*
 * Cross datatype comparisons between DATE/TIMESTAMP/TIMESTAMPTZ
 * are not immutable which prevents their usage for chunk exclusion.
 * Unfortunately estimate_expression_value will not estimate those
 * expressions which makes them unusable for execution time chunk
 * exclusion with constraint aware append.
 * To circumvent this we inject casts and use an operator
 * with the same datatype on both sides when constifying
 * restrictinfo. This allows estimate_expression_value
 * to evaluate those expressions and makes them accessible for
 * execution time chunk exclusion.
 *
 * The following transformations are done:
 * TIMESTAMP OP TIMESTAMPTZ => TIMESTAMP OP (TIMESTAMPTZ::TIMESTAMP)
 * TIMESTAMPTZ OP DATE => TIMESTAMPTZ OP (DATE::TIMESTAMPTZ)
 *
 * No transformation is required for TIMESTAMP OP DATE because
 * those operators are marked immutable.
 */
Expr *
ts_transform_cross_datatype_comparison(Expr *clause)
{
	clause = copyObject(clause);
	if (IsA(clause, OpExpr) && list_length(castNode(OpExpr, clause)->args) == 2)
	{
		OpExpr *op = castNode(OpExpr, clause);
		Oid left_type = exprType(linitial(op->args));
		Oid right_type = exprType(lsecond(op->args));

		if (op->opresulttype != BOOLOID || op->opretset == true)
			return clause;

		if (!IsA(linitial(op->args), Var) && !IsA(lsecond(op->args), Var))
			return clause;

		if (DATATYPE_PAIR(left_type, right_type, TIMESTAMPOID, TIMESTAMPTZOID) ||
			DATATYPE_PAIR(left_type, right_type, TIMESTAMPTZOID, DATEOID))
		{
			char *opname = get_opname(op->opno);
			Oid source_type, target_type, opno, cast_oid;

			/*
			 * if Var is on left side we put cast on right side otherwise
			 * it will be left
			 */
			if (IsA(linitial(op->args), Var))
			{
				source_type = right_type;
				target_type = left_type;
			}
			else
			{
				source_type = left_type;
				target_type = right_type;
			}

			opno = ts_get_operator(opname, PG_CATALOG_NAMESPACE, target_type, target_type);
			cast_oid = ts_get_cast_func(source_type, target_type);

			if (OidIsValid(opno) && OidIsValid(cast_oid))
			{
				Expr *left = linitial(op->args);
				Expr *right = lsecond(op->args);

				if (source_type == left_type)
					left = (Expr *) makeFuncExpr(cast_oid,
												 target_type,
												 list_make1(left),
												 InvalidOid,
												 InvalidOid,
												 0);
				else
					right = (Expr *) makeFuncExpr(cast_oid,
												  target_type,
												  list_make1(right),
												  InvalidOid,
												  InvalidOid,
												  0);

				clause = make_opclause(opno, BOOLOID, false, left, right, InvalidOid, InvalidOid);
			}
		}
	}
	return clause;
}
