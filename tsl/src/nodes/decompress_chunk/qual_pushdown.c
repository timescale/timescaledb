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
	if (!array_const || !IsA(array_const, Const) || array_const->consttype == InvalidOid ||
		array_const->constisnull)
		return NIL;

	Oid array_type = array_const->consttype;
	Datum array_datum = array_const->constvalue;

	Oid element_type;
	int16 typlen;
	bool typbyval;
	char typalign;

	element_type = get_element_type(array_type);
	if (!OidIsValid(element_type))
		elog(ERROR, "Invalid array type: %u", array_type);

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

static Node *
make_segment_meta_opexpr(QualPushdownContext *context, Node *orig_expr, Oid opno,
						 AttrNumber meta_column_attno, Var *uncompressed_var, Expr *compare_to_expr,
						 StrategyNumber strategy)
{
	Var *meta_var = makeVar(context->compressed_rel->relid,
							meta_column_attno,
							uncompressed_var->vartype,
							-1,
							InvalidOid,
							0);

//	if (IsA(orig_expr, OpExpr))
//	{
		return (Node *) make_opclause(opno,
									  BOOLOID,
									  false,
									  (Expr *) meta_var,
									  copyObject(compare_to_expr),
									  InvalidOid,
									  uncompressed_var->varcollid);
//	}

//	if (IsA(orig_expr, ScalarArrayOpExpr))
//	{
//		ScalarArrayOpExpr *saop = castNode(ScalarArrayOpExpr, orig_expr);
//		ScalarArrayOpExpr *newopexpr = makeNode(ScalarArrayOpExpr);

//		newopexpr->opno = opno;
//		newopexpr->opfuncid = InvalidOid;
//		newopexpr->hashfuncid = InvalidOid;
//		newopexpr->negfuncid = InvalidOid;
//		newopexpr->useOr = saop->useOr;
//		newopexpr->inputcollid = saop->inputcollid;
//		newopexpr->args = list_make2(meta_var, copyObject(compare_to_expr));
//		newopexpr->location = saop->location;

//		return (Node *) newopexpr;
//	}

	Assert(false);
	return NULL;
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

static void
array_const_minmax(Const *array_const, Const **out_min, Const **out_max)
{
	Assert(array_const && !array_const->constisnull);

	ArrayType *arr = DatumGetArrayTypeP(array_const->constvalue);
	Oid elemtype = ARR_ELEMTYPE(arr);

	int16 elmlen;
	bool elmbyval;
	char elmalign;
	get_typlenbyvalalign(elemtype, &elmlen, &elmbyval, &elmalign);

	Datum *elems;
	bool *nulls;
	int nelems;
	deconstruct_array(arr, elemtype, elmlen, elmbyval, elmalign, &elems, &nulls, &nelems);

	if (nelems == 0)
	{
		*out_min = makeNullConst(elemtype, -1, array_const->constcollid);
		*out_max = makeNullConst(elemtype, -1, array_const->constcollid);
		return;
	}

	TypeCacheEntry *tc = lookup_type_cache(elemtype, TYPECACHE_CMP_PROC_FINFO);
	if (!OidIsValid(tc->cmp_proc_finfo.fn_oid))
		elog(ERROR, "no btree comparison function for type %u", elemtype);

	Datum best_min;
	Datum best_max;
	bool have_val = false;

	for (int i = 0; i < nelems; ++i)
	{
		if (nulls[i])
		{
			continue;
		}

		if (!have_val)
		{
			best_min = best_max = elems[i];
			have_val = true;
			continue;
		}

		if (DatumGetInt32(FunctionCall2Coll(&tc->cmp_proc_finfo,
											array_const->constcollid,
											elems[i],
											best_min)) < 0)
			best_min = elems[i];

		if (DatumGetInt32(FunctionCall2Coll(&tc->cmp_proc_finfo,
											array_const->constcollid,
											elems[i],
											best_max)) > 0)
			best_max = elems[i];
	}

	if (!have_val)
	{
		*out_min = makeNullConst(elemtype, -1, array_const->constcollid);
		*out_max = makeNullConst(elemtype, -1, array_const->constcollid);
		return;
	}

	*out_min = makeConst(elemtype, -1, array_const->constcollid, elmlen, best_min, false, elmbyval);

	*out_max = makeConst(elemtype, -1, array_const->constcollid, elmlen, best_max, false, elmbyval);
}

static Expr *
pushdown_op_to_segment_meta_min_max(QualPushdownContext *context, Node *orig_expr, List *expr_args,
									Oid op_oid, Oid op_collation)
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
	if ((min_attno == InvalidAttrNumber || max_attno == InvalidAttrNumber) &&
		IsA(orig_expr, OpExpr))
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
		fprintf(stderr, "no metadata for either operand\n");
		return NULL;
	}

	Var *var_with_segment_meta = castNode(Var, leftop);

	/* May be able to allow non-strict operations as well.
	 * Next steps: Think through edge cases, either allow and write tests or figure out why we must
	 * block strict operations
	 */
	if (!OidIsValid(op_oid) || !op_strict(op_oid))
	{
		fprintf(stderr, "not strict\n");
		return NULL;
	}

	/* If the collation to be used by the OP doesn't match the column's collation do not push down
	 * as the materialized min/max value do not match the semantics of what we need here */
	if (var_with_segment_meta->varcollid != op_collation)
	{
		fprintf(stderr, "wrong collation\n");
		return NULL;
	}

	tce = lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_BTREE_OPFAMILY);

	strategy = get_op_opfamily_strategy(op_oid, tce->btree_opf);
	if (strategy == InvalidStrategy)
	{
		fprintf(stderr, "invalid strategy\n");
		return NULL;
	}

	fprintf(stderr, "strategy is %d\n", strategy);

	if (IsA(orig_expr, ScalarArrayOpExpr) && strategy != BTEqualStrategyNumber)
	{
		/*
		 * Only support the simplest case of x = any(array[1, 2, 3]) for scalar
		 * array operations.
		 */
		return NULL;
	}

	Expr *pushed_down_rightop = get_pushdownsafe_expr(context, rightop);
	if (pushed_down_rightop == NULL)
	{
		return NULL;
	}

	expr_type_id = exprType((Node *) pushed_down_rightop);
	if (IsA(orig_expr, ScalarArrayOpExpr))
	{
		expr_type_id = get_element_type(expr_type_id);
	}

	Expr *min_search_value = pushed_down_rightop;
	Expr *max_search_value = pushed_down_rightop;
	if (IsA(orig_expr, ScalarArrayOpExpr))
	{ if (IsA(pushed_down_rightop, Const))
	{
		fprintf(stderr, "have const array\n");
		array_const_minmax(castNode(Const, pushed_down_rightop),
			(Const **) &min_search_value, (Const **) &max_search_value);
		my_print(min_search_value);
		my_print(max_search_value);
	}
	 else
	 {
		return NULL;
	 }
	}

	switch (strategy)
	{
		case BTEqualStrategyNumber:
		{
			/*
			 * var = expr implies min < expr and max > expr.
			 */
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
													orig_expr,
													opno_le,
													min_attno,
													var_with_segment_meta,
													min_search_value,
													BTLessEqualStrategyNumber),
						   make_segment_meta_opexpr(context,
													orig_expr,
													opno_ge,
													max_attno,
													var_with_segment_meta,
													max_search_value,
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
														 orig_expr,
														 opno,
														 min_attno,
														 var_with_segment_meta,
														 pushed_down_rightop,
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
														 orig_expr,
														 opno,
														 max_attno,
														 var_with_segment_meta,
														 pushed_down_rightop,
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

static Node *
modify_expression(Node *node, QualPushdownContext *context)
{
	if (node == NULL)
		return NULL;

	switch (nodeTag(node))
	{
		case T_ScalarArrayOpExpr:
		case T_OpExpr:
		{
			Oid opno = InvalidOid;
			List *args = NIL;
			Oid collation = InvalidOid;
			if (IsA(node, OpExpr))
			{
				OpExpr *opexpr = (OpExpr *) node;
				if (opexpr->opresulttype != BOOLOID)
				{
					break;
				}
				opno = opexpr->opno;
				args = opexpr->args;
				collation = opexpr->inputcollid;
			}
			else if (IsA(node, ScalarArrayOpExpr))
			{
				ScalarArrayOpExpr *saop = castNode(ScalarArrayOpExpr, node);

				/*
				 * Things like x = all(array[1, 2, 3]) are borderline
				 * useless, but generic optimizations for them are very
				 * complicated, so we only support the most useful case of
				 * x = any(array[1, 2, 3]).
				 */
				if (!saop->useOr)
				{
					break;
				}

				opno = saop->opno;
				args = saop->args;
				collation = saop->inputcollid;
			}
			else
			{
				Assert(false);
				break;
			}

			Expr *pd = NULL;

			if (ts_guc_enable_sparse_index_bloom && IsA(node, OpExpr))
			{
				/*
				 * Try bloom1 sparse index.
				 */
				pd = pushdown_op_to_segment_meta_bloom1(context, args, opno, collation);
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
			fprintf(stderr, "opno %d collation %d for orig:\n", opno, collation);
			my_print(node);
			pd = pushdown_op_to_segment_meta_min_max(context, node, args, opno, collation);
			if (pd != NULL)
			{
				context->needs_recheck = true;
				/* pd is on the compressed table so do not mutate further */
				return (Node *) pd;
			}

			/* opexpr will still be checked for segment by columns */
			break;
		}
			{
				fprintf(stderr, "saop!!!\n");

				/*
				 * It can be possible to push down the SAOP as is, if it references
				 * only the segmentby columns. Check this case first.
				 */
				QualPushdownContext tmp_context = *context;
				void *pushed_down = expression_tree_mutator(node, modify_expression, &tmp_context);
				if (pushed_down != NULL && tmp_context.can_pushdown)
				{
					fprintf(stderr, "pushed down normally\n");
					my_print(pushed_down);
					context->needs_recheck |= tmp_context.needs_recheck;
					return pushed_down;
				}

				ScalarArrayOpExpr *saop = castNode(ScalarArrayOpExpr, node);
				OpExpr *opexpr = makeNode(OpExpr);
				opexpr->opno = saop->opno;
				opexpr->opfuncid = saop->opfuncid;
				opexpr->opresulttype = BOOLOID;
				// ArrayExpr *array_expr = castNode(ArrayExpr, list_nth(saop->args, 1));

				my_print(saop);

				void *scalar_arg = linitial(saop->args);
				void *array_arg = list_nth(saop->args, 1);
				List *array_elements;
				if (IsA(array_arg, Const))
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
					fprintf(stderr, "wow! got this:\n");
					my_print(array_arg);
					break;
				}

				my_print(array_elements);

				List *transformed_ops = NIL;
				ListCell *lc;
				foreach (lc, array_elements)
				{
					opexpr->args = list_make2(scalar_arg, lfirst(lc));
					fprintf(stderr, "before transformation:\n");
					my_print(opexpr);
					void *transformed = modify_expression((Node *) opexpr, context);
					if (transformed == NULL)
					{
						fprintf(stderr, "cannot transform:\n");
						break;
					}
					else
					{
						fprintf(stderr, "transformed into:\n");
						my_print(transformed);
					}
					transformed_ops = lappend(transformed_ops, transformed);
				}
				fprintf(stderr, "transformed:\n");
				my_print(transformed_ops);

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
