/* * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <optimizer/restrictinfo.h>
#include <parser/parse_func.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compression/create.h"
#include "custom_type_cache.h"
#include "decompress_chunk.h"
#include "guc.h"
#include "ts_catalog/array_utils.h"

#include "qual_pushdown.h"

typedef struct QualPushdownContext
{
	RelOptInfo *chunk_rel;
	RelOptInfo *compressed_rel;
	RangeTblEntry *chunk_rte;
	RangeTblEntry *compressed_rte;
	bool can_pushdown;
	bool needs_recheck;
	CompressionSettings *settings;
} QualPushdownContext;

static Node *modify_expression(Node *node, QualPushdownContext *context);

static List *
deconstruct_array_const(Const *array_const)
{
	/*
	 * No way to represent that as a list (NIL is an empty array), so has to be
	 * handled by the caller.
	 */
	Assert(!array_const->constisnull);

	Oid array_type = array_const->consttype;
	Datum array_datum = array_const->constvalue;

	Oid element_type = get_element_type(array_type);

	int16 typlen;
	bool typbyval;
	char typalign;
	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);

	int nelems;
	Datum *elem_values;
	bool *elem_nulls;
	deconstruct_array(DatumGetArrayTypeP(array_datum),
					  element_type,
					  typlen,
					  typbyval,
					  typalign,
					  &elem_values,
					  &elem_nulls,
					  &nelems);

	List *const_list = NIL;
	for (int i = 0; i < nelems; i++)
	{
		Const *elem_const = makeConst(element_type,
									  array_const->consttypmod,
									  array_const->constcollid,
									  typlen,
									  elem_values[i],
									  elem_nulls[i],
									  typbyval);
		const_list = lappend(const_list, elem_const);
	}

	return const_list;
}

void
pushdown_quals(PlannerInfo *root, CompressionSettings *settings, RelOptInfo *chunk_rel,
			   RelOptInfo *compressed_rel, bool chunk_partial)
{
	ListCell *lc;
	List *decompress_clauses = NIL;
	QualPushdownContext context = {
		.chunk_rel = chunk_rel,
		.compressed_rel = compressed_rel,
		.chunk_rte = planner_rt_fetch(chunk_rel->relid, root),
		.compressed_rte = planner_rt_fetch(compressed_rel->relid, root),
		.settings = settings,
	};

	foreach (lc, chunk_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst(lc);
		Expr *expr;

		/* pushdown is not safe for volatile expressions */
		if (contain_volatile_functions((Node *) ri->clause))
		{
			decompress_clauses = lappend(decompress_clauses, ri);
			continue;
		}

		context.can_pushdown = true;
		context.needs_recheck = false;
		expr = (Expr *) modify_expression((Node *) ri->clause, &context);

		if (context.can_pushdown)
		{
			/*
			 * We have to call eval_const_expressions after pushing down
			 * the quals, to normalize the bool expressions. Namely, we might add an
			 * AND boolexpr on minmax metadata columns, but the normal form is not
			 * allowed to have nested AND boolexprs. They break some functions like
			 * generate_bitmap_or_paths().
			 */
			expr = (Expr *) eval_const_expressions(root, (Node *) expr);

			if (IsA(expr, BoolExpr) && ((BoolExpr *) expr)->boolop == AND_EXPR)
			{
				/* have to separate out and expr into different restrict infos */
				ListCell *lc_and;
				BoolExpr *bool_expr = (BoolExpr *) expr;
				foreach (lc_and, bool_expr->args)
				{
					compressed_rel->baserestrictinfo =
						lappend(compressed_rel->baserestrictinfo,
								make_simple_restrictinfo(root, lfirst(lc_and)));
				}
			}
			else
				compressed_rel->baserestrictinfo =
					lappend(compressed_rel->baserestrictinfo, make_simple_restrictinfo(root, expr));
		}
		/* We need to check the restriction clause on the decompress node if the clause can't be
		 * pushed down or needs re-checking */
		if (!context.can_pushdown || context.needs_recheck || chunk_partial)
		{
			decompress_clauses = lappend(decompress_clauses, ri);
		}
	}
	chunk_rel->baserestrictinfo = decompress_clauses;
}

static OpExpr *
make_segment_meta_opexpr(QualPushdownContext *context, Oid opno, AttrNumber meta_column_attno,
						 Var *uncompressed_var, Expr *compare_to_expr, StrategyNumber strategy)
{
	Var *meta_var = makeVar(context->compressed_rel->relid,
							meta_column_attno,
							uncompressed_var->vartype,
							-1,
							InvalidOid,
							0);

	return (OpExpr *) make_opclause(opno,
									BOOLOID,
									false,
									(Expr *) meta_var,
									copyObject(compare_to_expr),
									InvalidOid,
									uncompressed_var->varcollid);
}

static Expr *
get_pushdownsafe_expr(const QualPushdownContext *input_context, Expr *input)
{
	/* do not mess up the input_context, so create a new one */
	QualPushdownContext test_context;
	Expr *expr;

	memcpy(&test_context, input_context, sizeof(test_context));
	test_context.can_pushdown = true;
	expr = (Expr *) modify_expression((Node *) input, &test_context);
	if (test_context.can_pushdown)
		return expr;
	return NULL;
}

static void
expr_fetch_minmax_metadata(QualPushdownContext *context, Expr *expr, AttrNumber *min_attno,
						   AttrNumber *max_attno)
{
	*min_attno = InvalidAttrNumber;
	*max_attno = InvalidAttrNumber;

	if (!IsA(expr, Var))
		return;

	Var *var = castNode(Var, expr);

	/*
	 * Not on the chunk we expect. This doesn't really happen because we don't
	 * push down the join quals, only the baserestrictinfo.
	 */
	if ((Index) var->varno != context->chunk_rel->relid)
		return;

	/* ignore system attributes or whole row references */
	if (var->varattno <= 0)
		return;

	*min_attno = compressed_column_metadata_attno(context->settings,
												  context->chunk_rte->relid,
												  var->varattno,
												  context->compressed_rte->relid,
												  "min");
	*max_attno = compressed_column_metadata_attno(context->settings,
												  context->chunk_rte->relid,
												  var->varattno,
												  context->compressed_rte->relid,
												  "max");
}

static Expr *
pushdown_op_to_segment_meta_min_max(QualPushdownContext *context, List *expr_args, Oid op_oid,
									Oid op_collation)
{
	Expr *leftop, *rightop;
	TypeCacheEntry *tce;
	int strategy;
	Oid expr_type_id;

	if (list_length(expr_args) != 2)
		return NULL;

	leftop = linitial(expr_args);
	rightop = lsecond(expr_args);

	if (IsA(leftop, RelabelType))
		leftop = ((RelabelType *) leftop)->arg;
	if (IsA(rightop, RelabelType))
		rightop = ((RelabelType *) rightop)->arg;

	/* Find the side that has var with segment meta set expr to the other side */
	AttrNumber min_attno;
	AttrNumber max_attno;
	expr_fetch_minmax_metadata(context, leftop, &min_attno, &max_attno);
	if (min_attno == InvalidAttrNumber || max_attno == InvalidAttrNumber)
	{
		/* No metadata for the left operand, try to commute the operator. */
		op_oid = get_commutator(op_oid);
		Expr *tmp = leftop;
		leftop = rightop;
		rightop = tmp;

		expr_fetch_minmax_metadata(context, leftop, &min_attno, &max_attno);
	}

	if (min_attno == InvalidAttrNumber || max_attno == InvalidAttrNumber)
	{
		/* No metadata for either operand. */
		return NULL;
	}

	Var *var_with_segment_meta = castNode(Var, leftop);
	Expr *expr = rightop;

	/* May be able to allow non-strict operations as well.
	 * Next steps: Think through edge cases, either allow and write tests or figure out why we must
	 * block strict operations
	 */
	if (!OidIsValid(op_oid) || !op_strict(op_oid))
		return NULL;

	/* If the collation to be used by the OP doesn't match the column's collation do not push down
	 * as the materialized min/max value do not match the semantics of what we need here */
	if (var_with_segment_meta->varcollid != op_collation)
		return NULL;

	tce = lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_BTREE_OPFAMILY);

	strategy = get_op_opfamily_strategy(op_oid, tce->btree_opf);
	if (strategy == InvalidStrategy)
		return NULL;

	Expr *new_expr = get_pushdownsafe_expr(context, expr);

	if (new_expr == NULL)
	{
		return NULL;
	}

	expr = new_expr;

	expr_type_id = exprType((Node *) expr);

	switch (strategy)
	{
		case BTEqualStrategyNumber:
		{
			/* var = expr implies min < expr and max > expr */
			Oid opno_le = get_opfamily_member(tce->btree_opf,
											  tce->type_id,
											  expr_type_id,
											  BTLessEqualStrategyNumber);
			Oid opno_ge = get_opfamily_member(tce->btree_opf,
											  tce->type_id,
											  expr_type_id,
											  BTGreaterEqualStrategyNumber);

			if (!OidIsValid(opno_le) || !OidIsValid(opno_ge))
				return NULL;

			return make_andclause(
				list_make2(make_segment_meta_opexpr(context,
													opno_le,
													min_attno,
													var_with_segment_meta,
													expr,
													BTLessEqualStrategyNumber),
						   make_segment_meta_opexpr(context,
													opno_ge,
													max_attno,
													var_with_segment_meta,
													expr,
													BTGreaterEqualStrategyNumber)));
		}
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			/* var < expr  implies min < expr */
			{
				Oid opno =
					get_opfamily_member(tce->btree_opf, tce->type_id, expr_type_id, strategy);

				if (!OidIsValid(opno))
					return NULL;

				return (Expr *) make_segment_meta_opexpr(context,
														 opno,
														 min_attno,
														 var_with_segment_meta,
														 expr,
														 strategy);
			}

		case BTGreaterStrategyNumber:
		case BTGreaterEqualStrategyNumber:
			/* var > expr  implies max > expr */
			{
				Oid opno =
					get_opfamily_member(tce->btree_opf, tce->type_id, expr_type_id, strategy);

				if (!OidIsValid(opno))
					return NULL;

				return (Expr *) make_segment_meta_opexpr(context,
														 opno,
														 max_attno,
														 var_with_segment_meta,
														 expr,
														 strategy);
			}
		default:
			return NULL;
	}
}

static void
expr_fetch_bloom1_metadata(QualPushdownContext *context, Expr *expr, AttrNumber *bloom1_attno)
{
	*bloom1_attno = InvalidAttrNumber;

	if (!IsA(expr, Var))
		return;

	Var *var = castNode(Var, expr);

	/*
	 * Not on the chunk we expect. This doesn't really happen because we don't
	 * push down the join quals, only the baserestrictinfo.
	 */
	if ((Index) var->varno != context->chunk_rel->relid)
		return;

	/* ignore system attributes or whole row references */
	if (var->varattno <= 0)
		return;

	*bloom1_attno = compressed_column_metadata_attno(context->settings,
													 context->chunk_rte->relid,
													 var->varattno,
													 context->compressed_rte->relid,
													 "bloom1");
}

static Expr *
pushdown_op_to_segment_meta_bloom1(QualPushdownContext *context, List *expr_args, Oid op_oid,
								   Oid op_collation)
{
	Expr *original_leftop;
	Expr *original_rightop;
	TypeCacheEntry *tce;
	int strategy;

	if (list_length(expr_args) != 2)
		return NULL;

	original_leftop = linitial(expr_args);
	original_rightop = lsecond(expr_args);

	if (IsA(original_leftop, RelabelType))
		original_leftop = ((RelabelType *) original_leftop)->arg;
	if (IsA(original_rightop, RelabelType))
		original_rightop = ((RelabelType *) original_rightop)->arg;

	/* Find the side that has var with segment meta set expr to the other side */
	AttrNumber bloom1_attno = InvalidAttrNumber;
	expr_fetch_bloom1_metadata(context, original_leftop, &bloom1_attno);
	if (bloom1_attno == InvalidAttrNumber)
	{
		/* No metadata for the left operand, try to commute the operator. */
		op_oid = get_commutator(op_oid);
		Expr *tmp = original_leftop;
		original_leftop = original_rightop;
		original_rightop = tmp;

		expr_fetch_bloom1_metadata(context, original_leftop, &bloom1_attno);
	}

	if (bloom1_attno == InvalidAttrNumber)
	{
		/* No metadata for either operand. */
		return NULL;
	}

	Var *var_with_segment_meta = castNode(Var, original_leftop);

	/*
	 * Play it safe and don't push down if the operator collation doesn't match
	 * the column collation.
	 */
	if (var_with_segment_meta->varcollid != op_collation)
	{
		return NULL;
	}

	/*
	 * We cannot use bloom filters for non-deterministic collations.
	 */
	if (OidIsValid(op_collation) && !get_collation_isdeterministic(op_collation))
	{
		return NULL;
	}

	/*
	 * We only support hashable equality operators.
	 */
	tce = lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_HASH_OPFAMILY);
	strategy = get_op_opfamily_strategy(op_oid, tce->hash_opf);
	if (strategy != HTEqualStrategyNumber)
	{
		return NULL;
	}

	/*
	 * The hash equality operators are supposed to be strict.
	 */
	Assert(op_strict(op_oid));

	/*
	 * Check if the righthand expression is safe to push down.
	 * functions.
	 */
	Expr *pushed_down_rightop = get_pushdownsafe_expr(context, original_rightop);
	if (pushed_down_rightop == NULL)
	{
		return NULL;
	}

	/*
	 * var = expr implies bloom1_contains(var_bloom, expr).
	 */
	Var *bloom_var = makeVar(context->compressed_rel->relid,
							 bloom1_attno,
							 ts_custom_type_cache_get(CUSTOM_TYPE_BLOOM1)->type_oid,
							 -1,
							 InvalidOid,
							 0);

	Oid func = LookupFuncName(list_make2(makeString("_timescaledb_functions"),
										 makeString("bloom1_contains")),
							  /* nargs = */ -1,
							  /* argtypes = */ (void *) -1,
							  /* missing_ok = */ false);

	return (Expr *) makeFuncExpr(func,
								 BOOLOID,
								 list_make2(bloom_var, pushed_down_rightop),
								 /* funccollid = */ InvalidOid,
								 /* inputcollid = */ InvalidOid,
								 COERCE_EXPLICIT_CALL);
}

static Expr *
pushdown_op_to_segment_meta_bloom1_saop(QualPushdownContext *context, List *expr_args, Oid op_oid,
										Oid op_collation)
{
	Expr *original_leftop;
	Expr *original_rightop;
	TypeCacheEntry *tce;
	int strategy;

	if (list_length(expr_args) != 2)
		return NULL;

	original_leftop = linitial(expr_args);
	original_rightop = lsecond(expr_args);

	if (IsA(original_leftop, RelabelType))
		original_leftop = ((RelabelType *) original_leftop)->arg;
	if (IsA(original_rightop, RelabelType))
		original_rightop = ((RelabelType *) original_rightop)->arg;

	/*
	 * For scalar array operation, we expect a var on the left side.
	 */
	AttrNumber bloom1_attno = InvalidAttrNumber;
	expr_fetch_bloom1_metadata(context, original_leftop, &bloom1_attno);
	if (bloom1_attno == InvalidAttrNumber)
	{
		/* No metadata for left operand. */
		return NULL;
	}

	Var *var_with_segment_meta = castNode(Var, original_leftop);

	/*
	 * Play it safe and don't push down if the operator collation doesn't match
	 * the column collation.
	 */
	if (var_with_segment_meta->varcollid != op_collation)
	{
		return NULL;
	}

	/*
	 * We cannot use bloom filters for non-deterministic collations.
	 */
	if (OidIsValid(op_collation) && !get_collation_isdeterministic(op_collation))
	{
		return NULL;
	}

	/*
	 * We only support hashable equality operators.
	 */
	tce = lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_HASH_OPFAMILY);
	strategy = get_op_opfamily_strategy(op_oid, tce->hash_opf);
	if (strategy != HTEqualStrategyNumber)
	{
		return NULL;
	}

	/*
	 * The hash equality operators are supposed to be strict.
	 */
	Assert(op_strict(op_oid));

	/*
	 * Check if the righthand expression is safe to push down.
	 */
	Expr *pushed_down_rightop = get_pushdownsafe_expr(context, original_rightop);
	if (pushed_down_rightop == NULL)
	{
		return NULL;
	}

	/*
	 * var = any(array) implies bloom1_contains_any(var_bloom, array).
	 */
	Var *bloom_var = makeVar(context->compressed_rel->relid,
							 bloom1_attno,
							 ts_custom_type_cache_get(CUSTOM_TYPE_BLOOM1)->type_oid,
							 -1,
							 InvalidOid,
							 0);

	Oid func = LookupFuncName(list_make2(makeString("_timescaledb_functions"),
										 makeString("bloom1_contains_any")),
							  /* nargs = */ -1,
							  /* argtypes = */ (void *) -1,
							  /* missing_ok = */ false);

	return (Expr *) makeFuncExpr(func,
								 BOOLOID,
								 list_make2(bloom_var, pushed_down_rightop),
								 /* funccollid = */ InvalidOid,
								 /* inputcollid = */ InvalidOid,
								 COERCE_EXPLICIT_CALL);
}

static Node *
modify_expression(Node *node, QualPushdownContext *context)
{
	if (node == NULL)
		return NULL;

	switch (nodeTag(node))
	{
		case T_OpExpr:
		{
			OpExpr *opexpr = (OpExpr *) node;
			if (opexpr->opresulttype == BOOLOID)
			{
				Expr *pd = NULL;

				if (ts_guc_enable_sparse_index_bloom)
				{
					/*
					 * Try bloom1 sparse index.
					 */
					pd = pushdown_op_to_segment_meta_bloom1(context,
															opexpr->args,
															opexpr->opno,

															opexpr->inputcollid);
				}

				if (pd != NULL)
				{
					context->needs_recheck = true;
					/* pd is on the compressed table so do not mutate further */
					return (Node *) pd;
				}

				/*
				 * Try minmax sparse index.
				 */
				pd = pushdown_op_to_segment_meta_min_max(context,
														 opexpr->args,
														 opexpr->opno,
														 opexpr->inputcollid);
				if (pd != NULL)
				{
					context->needs_recheck = true;
					/* pd is on the compressed table so do not mutate further */
					return (Node *) pd;
				}
			}
			/* opexpr will still be checked for segment by columns */
			break;
		}
		case T_ScalarArrayOpExpr:
		{
			/*
			 * It can be possible to push down the SAOP as is, if it references
			 * only the segmentby columns. Check this case first.
			 */
			QualPushdownContext tmp_context = *context;
			void *pushed_down = expression_tree_mutator(node, modify_expression, &tmp_context);
			if (pushed_down != NULL && tmp_context.can_pushdown)
			{
				context->needs_recheck |= tmp_context.needs_recheck;
				return pushed_down;
			}

			/*
			 * Try to transform x = any(array[]) into
			 * bloom1_contains_any(bloom_x, array[]).
			 */
			tmp_context = *context;
			ScalarArrayOpExpr *saop = castNode(ScalarArrayOpExpr, node);
			if (saop->useOr)
			{
				void *pushed_down = pushdown_op_to_segment_meta_bloom1_saop(&tmp_context,
																			saop->args,
																			saop->opno,
																			saop->inputcollid);

				if (pushed_down != NULL && tmp_context.can_pushdown)
				{
					context->needs_recheck = true;
					return pushed_down;
				}
			}

			/*
			 * Generic code for scalar array operation pushdown that transforms
			 * them into a series of OR/AND clauses.
			 */
			OpExpr *opexpr = makeNode(OpExpr);
			opexpr->opno = saop->opno;
			opexpr->opfuncid = saop->opfuncid;
			opexpr->opresulttype = BOOLOID;
			opexpr->inputcollid = saop->inputcollid;
			// ArrayExpr *array_expr = castNode(ArrayExpr, list_nth(saop->args, 1));

			void *scalar_arg = linitial(saop->args);
			void *array_arg = list_nth(saop->args, 1);
			List *array_elements;
			if (IsA(array_arg, Const) && !castNode(Const, array_arg)->constisnull)
			{
				array_elements = deconstruct_array_const(castNode(Const, array_arg));
			}
			else if (IsA(array_arg, ArrayExpr))
			{
				array_elements = castNode(ArrayExpr, array_arg)->elements;
			}
			else
			{
				/* Not sure anything else is allowed, but just skip it. */
				break;
			}

			List *transformed_ops = NIL;
			ListCell *lc;
			foreach (lc, array_elements)
			{
				opexpr->args = list_make2(scalar_arg, lfirst(lc));
				void *transformed = modify_expression((Node *) opexpr, context);
				if (transformed == NULL)
				{
					break;
				}
				transformed_ops = lappend(transformed_ops, transformed);
			}

			if (saop->useOr)
			{
				return (Node *) make_orclause(transformed_ops);
			}
			else
			{
				return (Node *) make_andclause(transformed_ops);
			}

			break;
		}
		case T_BoolExpr:
		case T_CoerceViaIO:
		case T_RelabelType:
		case T_List:
		case T_Const:
		case T_NullTest:
		case T_Param:
		case T_SQLValueFunction:
		case T_CaseExpr:
		case T_CaseWhen:
		case T_ArrayExpr:
			break;
		case T_FuncExpr:
			/*
			 * The caller should have checked that we don't have volatile
			 * functions in this qual.
			 */
			Assert(!contain_volatile_functions(node));
			break;
		case T_Var:
		{
			Var *var = castNode(Var, node);
			Assert((Index) var->varno == context->chunk_rel->relid);

			if (var->varattno <= 0)
			{
				/* Can't do this for system columns such as whole-row var. */
				context->can_pushdown = false;
				return NULL;
			}

			char *attname = get_attname(context->chunk_rte->relid, var->varattno, false);
			/* we can only push down quals for segmentby columns */
			if (!ts_array_is_member(context->settings->fd.segmentby, attname))
			{
				context->can_pushdown = false;
				return NULL;
			}

			var = copyObject(var);
			var->varno = context->compressed_rel->relid;
			var->varattno = get_attnum(context->compressed_rte->relid, attname);

			return (Node *) var;
		}
		default:
			context->can_pushdown = false;
			return NULL;
			break;
	}

	return expression_tree_mutator(node, modify_expression, context);
}
