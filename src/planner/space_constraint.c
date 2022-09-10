/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <datatype/timestamp.h>
#include <nodes/makefuncs.h>
#include <nodes/pg_list.h>
#include <optimizer/optimizer.h>
#include <parser/parse_func.h>
#include <utils/fmgroids.h>
#include <utils/typcache.h>

#include "cache.h"
#include "dimension.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "partitioning.h"
#include "planner.h"

/*
 * Returns space dimension for a specific column. Returns NULL
 * if the column is not a space dimension.
 */
static Dimension *
get_space_dimension(Oid relid, Index varattno)
{
	Hypertable *ht = ts_planner_get_hypertable(relid, CACHE_FLAG_CHECK);
	if (!ht)
		return NULL;

	for (uint16 i = 0; i < ht->space->num_dimensions; i++)
	{
		Dimension *dim = &ht->space->dimensions[i];
		if (dim->type == DIMENSION_TYPE_CLOSED && dim->column_attno == varattno)
		{
			return dim;
		}
	}
	return NULL;
}

/*
 * Check if this operator is compatible with the constraints on
 * the space dimension. This is the equality operator between
 * left and right in the btree operator family.
 */
static bool
is_valid_space_operator(Oid opno, Oid left, Oid right)
{
	TypeCacheEntry *tce;

	if (left == right)
	{
		/*
		 * When left and right match lookup_type_cache can
		 * directly return the equality operator saving us
		 * one roundtrip.
		 */
		tce = lookup_type_cache(left, TYPECACHE_EQ_OPR);

		return tce && opno == tce->eq_opr;
	}
	else
	{
		/*
		 * The left and right type might not match when comparing
		 * different integer types eg comparing int2 or int8
		 * columns with integer literals which default to int4.
		 */
		tce = lookup_type_cache(left, TYPECACHE_BTREE_OPFAMILY);
		if (!tce)
			return false;

		Oid eqop = get_opfamily_member(tce->btree_opf, left, right, BTEqualStrategyNumber);
		return opno == eqop;
	}
}

/*
 * Valid constraints are: Var = Const
 * Var has to refer to a space partitioning column
 */
static bool
is_valid_space_constraint(OpExpr *op, List *rtable)
{
	Assert(IsA(op, OpExpr));
	if (!IsA(linitial(op->args), Var) || !IsA(lsecond(op->args), Const))
		return false;

	Var *var = linitial_node(Var, op->args);
	if (var->varlevelsup != 0)
		return false;

	Const *value = lsecond_node(Const, op->args);
	if (!is_valid_space_operator(op->opno, var->vartype, value->consttype))
		return false;

	/*
	 * Check that the constraint is actually on a partitioning column.
	 */
	Assert(var->varno <= list_length(rtable));
	RangeTblEntry *rte = list_nth(rtable, var->varno - 1);
	Dimension *dim = get_space_dimension(rte->relid, var->varattno);

	if (!dim)
		return false;

	return true;
}

/*
 * Valid constraints are:
 *   Var = ANY(ARRAY[Const,Const])
 *   Var IN (Const,Const)
 * Var has to refer to a space partitioning column
 */
static bool
is_valid_scalar_space_constraint(ScalarArrayOpExpr *op, List *rtable)
{
	Assert(IsA(op, ScalarArrayOpExpr));
	if (!IsA(linitial(op->args), Var) || !IsA(lsecond(op->args), ArrayExpr))
		return false;

	Var *var = linitial_node(Var, op->args);
	ArrayExpr *arr = castNode(ArrayExpr, lsecond(op->args));
	if (arr->multidims || !op->useOr || var->varlevelsup != 0)
		return false;

	if (!is_valid_space_operator(op->opno, var->vartype, arr->element_typeid))
		return false;

	/*
	 * Check that the constraint is actually on a partitioning column.
	 */
	Assert(var->varno <= list_length(rtable));
	RangeTblEntry *rte = list_nth(rtable, var->varno - 1);
	Dimension *dim = get_space_dimension(rte->relid, var->varattno);

	if (!dim)
		return false;

	ListCell *lc;
	foreach (lc, arr->elements)
	{
		switch (nodeTag(lfirst(lc)))
		{
			case T_Const:
				break;
			case T_FuncExpr:
			{
				FuncExpr *element = lfirst_node(FuncExpr, lc);
				if (element->funcformat != COERCE_IMPLICIT_CAST ||
					!IsA(linitial(element->args), Const))
					return false;

				break;
			}
			default:
				return false;
				break;
		}
	}
	return true;
}

static FuncExpr *
make_partfunc_call(Oid funcid, Oid rettype, List *args, Oid inputcollid)
{
	/* build FuncExpr to use in eval_const_expressions */
	return makeFuncExpr(funcid /* funcid */,
						rettype /* rettype */,
						args /* args */,
						InvalidOid /* funccollid */,
						inputcollid /* inputcollid */,
						COERCE_EXPLICIT_CALL /* fformat */);
}

/*
 * Transform a constraint like: device_id = 1
 * into
 * ((device_id = 1) AND (_timescaledb_internal.get_partition_hash(device_id) = 242423622))
 */
static OpExpr *
transform_space_constraint(PlannerInfo *root, List *rtable, OpExpr *op)
{
	Var *var = linitial_node(Var, op->args);
	Const *value = lsecond_node(Const, op->args);
	Const *part_value;
	RangeTblEntry *rte = list_nth(rtable, var->varno - 1);
	Dimension *dim = get_space_dimension(rte->relid, var->varattno);
	Oid rettype = dim->partitioning->partfunc.rettype;
	TypeCacheEntry *tce = lookup_type_cache(rettype, TYPECACHE_EQ_OPR);

	/* build FuncExpr to use in eval_const_expressions */
	FuncExpr *partcall = make_partfunc_call(dim->partitioning->partfunc.func_fmgr.fn_oid,
											rettype,
											list_make1(value),
											var->varcollid);

	/*
	 * We should always be able to constify here
	 */
	part_value = castNode(Const, eval_const_expressions(root, (Node *) partcall));

	/* build FuncExpr with column reference to use in constraint */
	partcall->args = list_make1(copyObject(var));

	OpExpr *ret = (OpExpr *) make_opclause(tce->eq_opr /* opno */,
										   BOOLOID /*opresulttype */,
										   false /* opretset */,
										   (Expr *) partcall /* left */,
										   (Expr *) part_value /* right */,
										   InvalidOid /* opcollid */,
										   InvalidOid /* inputcollid */);
	ret->location = PLANNER_LOCATION_MAGIC;
	return ret;
}

/*
 * Transforms a constraint like: s1 = ANY ('{s1_2,s1_2}'::text[])
 * into
 * ((s1 = ANY ('{s1_2,s1_2}'::text[])) AND (_timescaledb_internal.get_partition_hash(s1) = ANY
 * ('{1583420735,1583420735}'::integer[])))
 */
static ScalarArrayOpExpr *
transform_scalar_space_constraint(PlannerInfo *root, List *rtable, ScalarArrayOpExpr *op)
{
	Var *var = linitial_node(Var, op->args);
	RangeTblEntry *rte = list_nth(rtable, var->varno - 1);
	Dimension *dim = get_space_dimension(rte->relid, var->varattno);
	Oid rettype = dim->partitioning->partfunc.rettype;
	TypeCacheEntry *tce = lookup_type_cache(rettype, TYPECACHE_EQ_OPR);
	List *part_values = NIL;
	ListCell *lc;

	/* build FuncExpr to use in eval_const_expressions */
	FuncExpr *partcall = make_partfunc_call(dim->partitioning->partfunc.func_fmgr.fn_oid,
											rettype,
											NIL,
											var->varcollid);

	foreach (lc, lsecond_node(ArrayExpr, op->args)->elements)
	{
		Assert(IsA(lfirst(lc), Const) ||
			   (IsA(lfirst(lc), FuncExpr) &&
				lfirst_node(FuncExpr, lc)->funcformat == COERCE_IMPLICIT_CAST));

		/*
		 * We can skip NULL here as elements are ORed and partitioning dimensions
		 * have NOT NULL constraint.
		 */
		if (IsA(lfirst(lc), Const) && lfirst_node(Const, lc)->constisnull)
			continue;

		List *args = list_make1(lfirst(lc));
		partcall->args = args;
		part_values =
			lappend(part_values, castNode(Const, eval_const_expressions(root, (Node *) partcall)));
	}
	/* build FuncExpr with column reference to use in constraint */
	partcall->args = list_make1(copyObject(var));

	ArrayExpr *arr2 = makeNode(ArrayExpr);
	arr2->array_collid = InvalidOid;
	arr2->array_typeid = get_array_type(rettype);
	arr2->element_typeid = rettype;
	arr2->multidims = false;
	arr2->location = -1;
	arr2->elements = part_values;

	ScalarArrayOpExpr *op2 = makeNode(ScalarArrayOpExpr);
	op2->opno = tce->eq_opr;
	op2->args = list_make2(partcall, arr2);
	op2->inputcollid = InvalidOid;
	op2->useOr = true;
	op2->location = PLANNER_LOCATION_MAGIC;

	return op2;
}

/*
 * Transform constraints for hash-based partitioning columns to make
 * them usable by postgres constraint exclusion.
 *
 * If we have an equality condition on a space partitioning column, we add
 * a corresponding condition on get_partition_hash on this column. These
 * conditions match the constraints on chunks, so postgres' constraint
 * exclusion is able to use them and exclude the chunks.
 *
 */
Node *
ts_add_space_constraints(PlannerInfo *root, List *rtable, Node *node)
{
	Assert(node);

	switch (nodeTag(node))
	{
		case T_ScalarArrayOpExpr:
		{
			if (is_valid_scalar_space_constraint(castNode(ScalarArrayOpExpr, node), rtable))
			{
				List *args =
					list_make2(node,
							   transform_scalar_space_constraint(root,
																 rtable,
																 castNode(ScalarArrayOpExpr,
																		  node)));
				return (Node *) makeBoolExpr(AND_EXPR, args, -1);
			}

			break;
		}
		case T_OpExpr:
			if (is_valid_space_constraint(castNode(OpExpr, node), rtable))
			{
				List *args =
					list_make2(node,
							   transform_space_constraint(root, rtable, castNode(OpExpr, node)));
				return (Node *) makeBoolExpr(AND_EXPR, args, -1);
			}
			break;
		case T_BoolExpr:
		{
			ListCell *lc;
			BoolExpr *be = castNode(BoolExpr, node);

			if (be->boolop == AND_EXPR)
			{
				List *additions = NIL;
				/*
				 * If this is a top-level AND we can just append our transformed constraints
				 * to the list of ANDed expressions.
				 */
				foreach (lc, be->args)
				{
					switch (nodeTag(lfirst(lc)))
					{
						case T_OpExpr:
						{
							OpExpr *op = lfirst_node(OpExpr, lc);
							if (is_valid_space_constraint(op, rtable))
								additions = lappend(additions,
													transform_space_constraint(root, rtable, op));
							break;
						}
						case T_ScalarArrayOpExpr:
						{
							ScalarArrayOpExpr *op = lfirst_node(ScalarArrayOpExpr, lc);
							if (is_valid_scalar_space_constraint(op, rtable))
								additions =
									lappend(additions,
											transform_scalar_space_constraint(root, rtable, op));
							break;
						}
						default:
							break;
					}
				}

				if (additions)
					be->args = list_concat(be->args, additions);
			}
			break;
		}
		default:
			break;
	}

	return node;
}
