/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 *
 * make_SAOP_expr was added in PG18 and is copied here for older versions.
 */
#include <postgres.h>

#include <nodes/makefuncs.h>
#include <utils/array.h>
#include <utils/lsyscache.h>

#include "compat/compat.h"
#include "foreach_ptr.h"
#include "import/optimizer/clauses.h"

#if PG18_LT

/*
 * Build ScalarArrayOpExpr on top of 'exprs.' 'haveNonConst' indicates
 * whether at least one of the expressions is not Const.  When it's false,
 * the array constant is built directly; otherwise, we have to build a child
 * ArrayExpr. The 'exprs' list gets freed if not directly used in the output
 * expression tree.
 */
ScalarArrayOpExpr *
make_SAOP_expr(Oid oper, Node *leftexpr, Oid coltype, Oid arraycollid,
			   Oid inputcollid, List *exprs, bool haveNonConst)
{
	Node	   *arrayNode = NULL;
	ScalarArrayOpExpr *saopexpr = NULL;
	Oid			arraytype = get_array_type(coltype);

	if (!OidIsValid(arraytype))
		return NULL;

	/*
	 * Assemble an array from the list of constants.  It seems more profitable
	 * to build a const array.  But in the presence of other nodes, we don't
	 * have a specific value here and must employ an ArrayExpr instead.
	 */
	if (haveNonConst)
	{
		ArrayExpr  *arrayExpr = makeNode(ArrayExpr);

		/* array_collid will be set by parse_collate.c */
		arrayExpr->element_typeid = coltype;
		arrayExpr->array_typeid = arraytype;
		arrayExpr->multidims = false;
		arrayExpr->elements = exprs;
		arrayExpr->location = -1;

		arrayNode = (Node *) arrayExpr;
	}
	else
	{
		int16		typlen;
		bool		typbyval;
		char		typalign;
		Datum	   *elems;
		bool	   *nulls;
		int			i = 0;
		ArrayType  *arrayConst;
		int			dims[1] = {list_length(exprs)};
		int			lbs[1] = {1};

		get_typlenbyvalalign(coltype, &typlen, &typbyval, &typalign);

		elems = (Datum *) palloc(sizeof(Datum) * list_length(exprs));
		nulls = (bool *) palloc(sizeof(bool) * list_length(exprs));
		foreach_node(Const, value, exprs)
		{
			elems[i] = value->constvalue;
			nulls[i++] = value->constisnull;
		}

		arrayConst = construct_md_array(elems, nulls, 1, dims, lbs,
										coltype, typlen, typbyval, typalign);
		arrayNode = (Node *) makeConst(arraytype, -1, arraycollid,
									   -1, PointerGetDatum(arrayConst),
									   false, false);

		pfree(elems);
		pfree(nulls);
		list_free(exprs);
	}

	/* Build the SAOP expression node */
	saopexpr = makeNode(ScalarArrayOpExpr);
	saopexpr->opno = oper;
	saopexpr->opfuncid = get_opcode(oper);
	saopexpr->hashfuncid = InvalidOid;
	saopexpr->negfuncid = InvalidOid;
	saopexpr->useOr = true;
	saopexpr->inputcollid = inputcollid;
	saopexpr->args = list_make2(leftexpr, arrayNode);
	saopexpr->location = -1;

	return saopexpr;
}

#endif
