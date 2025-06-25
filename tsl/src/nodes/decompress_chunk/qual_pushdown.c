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

static Node *qual_pushdown_mutator(Node *node, QualPushdownContext *context);

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

		context.can_pushdown = true;
		context.needs_recheck = false;
		Node *pushed_down = qual_pushdown_mutator((Node *) ri->clause, &context);

		if (context.can_pushdown)
		{
			/*
			 * We have to call eval_const_expressions after pushing down
			 * the quals, to normalize the bool expressions. Namely, we might add an
			 * AND boolexpr on minmax metadata columns, but the normal form is not
			 * allowed to have nested AND boolexprs. They break some functions like
			 * generate_bitmap_or_paths().
			 */
			pushed_down = eval_const_expressions(root, pushed_down);

			if (IsA(pushed_down, BoolExpr) && castNode(BoolExpr, pushed_down)->boolop == AND_EXPR)
			{
				/* have to separate out and expr into different restrict infos */
				ListCell *lc_and;
				BoolExpr *bool_expr = castNode(BoolExpr, pushed_down);
				foreach (lc_and, bool_expr->args)
				{
					compressed_rel->baserestrictinfo =
						lappend(compressed_rel->baserestrictinfo,
								make_simple_restrictinfo(root, lfirst(lc_and)));
				}
			}
			else
				compressed_rel->baserestrictinfo =
					lappend(compressed_rel->baserestrictinfo,
							make_simple_restrictinfo(root, (Expr *) pushed_down));
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

static void *
pushdown_op_to_segment_meta_min_max(QualPushdownContext *context, OpExpr *orig_opexpr)
{
	/*
	 * This always requires rechecking the decompressed data.
	 */
	context->needs_recheck = true;

	Expr *leftop, *rightop;
	TypeCacheEntry *tce;
	int strategy;
	Oid expr_type_id;

	List *expr_args = orig_opexpr->args;
	Assert(list_length(expr_args) == 2);
	leftop = linitial(expr_args);
	rightop = lsecond(expr_args);

	if (IsA(leftop, RelabelType))
		leftop = ((RelabelType *) leftop)->arg;
	if (IsA(rightop, RelabelType))
		rightop = ((RelabelType *) rightop)->arg;

	/* Find the side that has var with segment meta set expr to the other side */
	Oid op_oid = orig_opexpr->opno;
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
		context->can_pushdown = false;
		return orig_opexpr;
	}

	Var *var_with_segment_meta = castNode(Var, leftop);

	/* May be able to allow non-strict operations as well.
	 * Next steps: Think through edge cases, either allow and write tests or figure out why we must
	 * block strict operations
	 */
	if (!OidIsValid(op_oid) || !op_strict(op_oid))
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	/* If the collation to be used by the OP doesn't match the column's collation do not push down
	 * as the materialized min/max value do not match the semantics of what we need here */
	Oid op_collation = orig_opexpr->inputcollid;
	if (var_with_segment_meta->varcollid != op_collation)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	tce = lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_BTREE_OPFAMILY);

	strategy = get_op_opfamily_strategy(op_oid, tce->btree_opf);
	if (strategy == InvalidStrategy)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	Expr *pushed_down_rightop = (Expr *) qual_pushdown_mutator((Node *) rightop, context);
	if (!context->can_pushdown)
	{
		return orig_opexpr;
	}
	Assert(pushed_down_rightop != NULL);

	expr_type_id = exprType((Node *) pushed_down_rightop);

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
			{
				context->can_pushdown = false;
				return orig_opexpr;
			}

			return make_andclause(
				list_make2(make_segment_meta_opexpr(context,
													opno_le,
													min_attno,
													var_with_segment_meta,
													pushed_down_rightop,
													BTLessEqualStrategyNumber),
						   make_segment_meta_opexpr(context,
													opno_ge,
													max_attno,
													var_with_segment_meta,
													pushed_down_rightop,
													BTGreaterEqualStrategyNumber)));
		}
		case BTLessStrategyNumber:
		case BTLessEqualStrategyNumber:
			/* var < expr  implies min < expr */
			{
				Oid opno =
					get_opfamily_member(tce->btree_opf, tce->type_id, expr_type_id, strategy);

				if (!OidIsValid(opno))
				{
					context->can_pushdown = false;
					return orig_opexpr;
				}

				return (Expr *) make_segment_meta_opexpr(context,
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
				{
					context->can_pushdown = false;
					return orig_opexpr;
				}

				return (Expr *) make_segment_meta_opexpr(context,
														 opno,
														 max_attno,
														 var_with_segment_meta,
														 pushed_down_rightop,
														 strategy);
			}
		default:
			context->can_pushdown = false;
			return orig_opexpr;
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

static void *
pushdown_op_to_segment_meta_bloom1(QualPushdownContext *context, OpExpr *orig_opexpr)
{
	/*
	 * This always requires rechecking the decompressed data.
	 */
	context->needs_recheck = true;

	Expr *original_leftop;
	Expr *original_rightop;
	TypeCacheEntry *tce;
	int strategy;

	List *expr_args = orig_opexpr->args;
	original_leftop = linitial(expr_args);
	original_rightop = lsecond(expr_args);

	if (IsA(original_leftop, RelabelType))
		original_leftop = ((RelabelType *) original_leftop)->arg;
	if (IsA(original_rightop, RelabelType))
		original_rightop = ((RelabelType *) original_rightop)->arg;

	/* Find the side that has var with segment meta set expr to the other side */
	Oid op_oid = orig_opexpr->opno;
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
		context->can_pushdown = false;
		return orig_opexpr;
	}

	Var *var_with_segment_meta = castNode(Var, original_leftop);

	/*
	 * Play it safe and don't push down if the operator collation doesn't match
	 * the column collation.
	 */
	Oid op_collation = orig_opexpr->inputcollid;
	if (var_with_segment_meta->varcollid != op_collation)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	/*
	 * We cannot use bloom filters for non-deterministic collations.
	 */
	if (OidIsValid(op_collation) && !get_collation_isdeterministic(op_collation))
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	/*
	 * We only support hashable equality operators.
	 */
	tce = lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_HASH_OPFAMILY);
	strategy = get_op_opfamily_strategy(op_oid, tce->hash_opf);
	if (strategy != HTEqualStrategyNumber)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	/*
	 * The hash equality operators are supposed to be strict.
	 */
	Assert(op_strict(op_oid));

	/*
	 * Check if the righthand expression is safe to push down.
	 * functions.
	 */
	Expr *pushed_down_rightop = (Expr *) qual_pushdown_mutator((Node *) original_rightop, context);
	if (!context->can_pushdown)
	{
		return orig_opexpr;
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

/*
 * Try to transform x = any(array[]) into bloom1_contains_any(bloom_x, array[]).
 */
static void *
pushdown_saop_bloom1(QualPushdownContext *context, ScalarArrayOpExpr *orig_saop)
{
	/*
	 * This always requires rechecking the decompressed data.
	 */
	context->needs_recheck = true;

	Expr *original_leftop;
	Expr *original_rightop;
	TypeCacheEntry *tce;
	int strategy;

	if (!orig_saop->useOr)
	{
		context->can_pushdown = false;
		return orig_saop;
	}

	List *expr_args = orig_saop->args;
	if (list_length(expr_args) != 2)
	{
		context->can_pushdown = false;
		return orig_saop;
	}

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
		context->can_pushdown = false;
		return orig_saop;
	}

	Var *var_with_segment_meta = castNode(Var, original_leftop);

	/*
	 * Play it safe and don't push down if the operator collation doesn't match
	 * the column collation.
	 */
	Oid op_collation = orig_saop->inputcollid;
	if (var_with_segment_meta->varcollid != op_collation)
	{
		context->can_pushdown = false;
		return orig_saop;
	}

	/*
	 * We cannot use bloom filters for non-deterministic collations.
	 */
	if (OidIsValid(op_collation) && !get_collation_isdeterministic(op_collation))
	{
		context->can_pushdown = false;
		return orig_saop;
	}

	/*
	 * We only support hashable equality operators.
	 */
	Oid op_oid = orig_saop->opno;
	tce = lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_HASH_OPFAMILY);
	strategy = get_op_opfamily_strategy(op_oid, tce->hash_opf);
	if (strategy != HTEqualStrategyNumber)
	{
		context->can_pushdown = false;
		return orig_saop;
	}

	/*
	 * The hash equality operators are supposed to be strict.
	 */
	Assert(op_strict(op_oid));

	/*
	 * Check if the righthand expression is safe to push down.
	 */
	Expr *pushed_down_rightop = (Expr *) qual_pushdown_mutator((Node *) original_rightop, context);
	if (!context->can_pushdown)
	{
		return orig_saop;
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

	return makeFuncExpr(func,
						BOOLOID,
						list_make2(bloom_var, pushed_down_rightop),
						/* funccollid = */ InvalidOid,
						/* inputcollid = */ InvalidOid,
						COERCE_EXPLICIT_CALL);
}

/*
 * Push down the scalar array operation by transforming it into a series of
 * OR/AND clauses.
 */
static Expr *
pushdown_saop_boolexpr(QualPushdownContext *context, ScalarArrayOpExpr *saop)
{
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
		/*
		 * We can encounter an array-type Param here, and maybe something else.
		 * This function has to deconstruct the array into elements now, so
		 * these types of array argument are not suitable.
		 */
		context->can_pushdown = false;
		return (Expr *) saop;
	}

	/*
	 * This will be the operation on the scalar value and an individual array
	 * element.
	 */
	OpExpr *opexpr = makeNode(OpExpr);
	opexpr->opno = saop->opno;
	opexpr->opfuncid = saop->opfuncid;
	opexpr->opresulttype = BOOLOID;
	opexpr->inputcollid = saop->inputcollid;

	/*
	 * Try to apply the above operation for each array element.
	 */
	List *pushed_down_ops = NIL;
	ListCell *lc;
	foreach (lc, array_elements)
	{
		opexpr->args = list_make2(scalar_arg, lfirst(lc));

		QualPushdownContext tmp_context = *context;
		void *transformed = qual_pushdown_mutator((Node *) opexpr, &tmp_context);

		/*
		 * If the scalar array operation uses AND, it's correct and useful to
		 * push down the check only for some array elements.
		 *
		 * For OR, we must be able to push down the checks for every element.
		 */
		if (!tmp_context.can_pushdown)
		{
			if (saop->useOr)
			{
				context->can_pushdown = false;
				return (Expr *) saop;
			}

			continue;
		}
		context->needs_recheck |= tmp_context.needs_recheck;
		pushed_down_ops = lappend(pushed_down_ops, transformed);
	}

	/*
	 * We can have no pushed down clauses if:
	 * 1) we had an AND scalar array operation, but failed to push down every
	 * individual clause.
	 * 2) we had an empty array argument, apparently it's not simplified by
	 * Postgres' eval_const_expressions().
	 */
	if (pushed_down_ops == NIL)
	{
		context->can_pushdown = false;
		return (Expr *) saop;
	}

	if (saop->useOr)
	{
		return make_orclause(pushed_down_ops);
	}
	else
	{
		return make_andclause(pushed_down_ops);
	}
}

static bool
contain_volatile_functions_checker(Oid func_id, void *context)
{
	return (func_volatile(func_id) == PROVOLATILE_VOLATILE);
}

/*
 * Push down the given expression node.
 *
 * This is used as a mutator for expression_tree_mutator().
 *
 * We return the original node if we cannot push it down, to be consistent with
 * the expression_tree_mutator behavior. The caller must check
 * context.can_pushdown.
 */
static Node *
qual_pushdown_mutator(Node *orig_node, QualPushdownContext *context)
{
	if (orig_node == NULL)
	{
		/*
		 * An expression node can have a NULL field and the mutator will be
		 * still called for it, so we have to handle this.
		 */
		return NULL;
	}

	if (!context->can_pushdown)
	{
		/*
		 * Stop early if we already know we can't push down this filter.
		 */
		return orig_node;
	}

	if (check_functions_in_node(orig_node,
								contain_volatile_functions_checker,
								/* context = */ NULL))
	{
		/* pushdown is not safe for volatile expressions */
		context->can_pushdown = false;
		return orig_node;
	}

	switch (nodeTag(orig_node))
	{
		case T_Var:
		{
			Var *var = castNode(Var, orig_node);
			Assert((Index) var->varno == context->chunk_rel->relid);

			if (var->varattno <= 0)
			{
				/* Can't do this for system columns such as whole-row var. */
				context->can_pushdown = false;
				return orig_node;
			}

			char *attname = get_attname(context->chunk_rte->relid, var->varattno, false);
			/* we can only push down quals for segmentby columns */
			if (!ts_array_is_member(context->settings->fd.segmentby, attname))
			{
				context->can_pushdown = false;
				return orig_node;
			}

			var = copyObject(var);
			var->varno = context->compressed_rel->relid;
			var->varattno = get_attnum(context->compressed_rte->relid, attname);

			return (Node *) var;
		}
		case T_OpExpr:
		{
			OpExpr *opexpr = (OpExpr *) orig_node;

			/*
			 * It might be possible to push down the OpExpr as is, if it
			 * references only the segmentby columns. Check this case first.
			 */
			QualPushdownContext tmp_context = *context;
			void *pushed_down =
				expression_tree_mutator((Node *) orig_node, qual_pushdown_mutator, &tmp_context);
			if (tmp_context.can_pushdown)
			{
				context->needs_recheck |= tmp_context.needs_recheck;
				return pushed_down;
			}

			if (opexpr->opresulttype != BOOLOID)
			{
				/*
				 * The following pushdown options only support operators that
				 * return bool.
				 */
				context->can_pushdown = false;
				return orig_node;
			}

			if (list_length(opexpr->args) != 2)
			{
				/*
				 * The following pushdown options only support operators with
				 * two operands.
				 */
				context->can_pushdown = false;
				return orig_node;
			}

			/*
			 * Try bloom1 sparse index.
			 */
			if (ts_guc_enable_sparse_index_bloom)
			{
				tmp_context = *context;
				pushed_down = pushdown_op_to_segment_meta_bloom1(&tmp_context, opexpr);
				if (tmp_context.can_pushdown)
				{
					context->needs_recheck |= tmp_context.needs_recheck;
					return pushed_down;
				}
			}

			/*
			 * Try minmax sparse index.
			 */
			tmp_context = *context;
			pushed_down = pushdown_op_to_segment_meta_min_max(&tmp_context, opexpr);
			if (tmp_context.can_pushdown)
			{
				context->needs_recheck |= tmp_context.needs_recheck;
				return pushed_down;
			}

			/*
			 * No other options to push down the OpExpr.
			 */
			context->can_pushdown = false;
			return orig_node;
		}
		case T_ScalarArrayOpExpr:
		{
			/*
			 * It can be possible to push down the SAOP as is, if it references
			 * only the segmentby columns. Check this case first.
			 */
			QualPushdownContext tmp_context = *context;
			void *pushed_down =
				expression_tree_mutator((Node *) orig_node, qual_pushdown_mutator, &tmp_context);
			if (tmp_context.can_pushdown)
			{
				context->needs_recheck |= tmp_context.needs_recheck;
				return pushed_down;
			}

			/*
			 * Try to transform x = any(array[]) into
			 * bloom1_contains_any(bloom_x, array[]).
			 */
			tmp_context = *context;
			ScalarArrayOpExpr *saop = castNode(ScalarArrayOpExpr, orig_node);
			pushed_down = pushdown_saop_bloom1(&tmp_context, saop);
			if (tmp_context.can_pushdown)
			{
				context->needs_recheck |= tmp_context.needs_recheck;
				return pushed_down;
			}

			/*
			 * Generic code for scalar array operation pushdown that transforms
			 * them into a series of OR/AND clauses.
			 */
			tmp_context = *context;
			pushed_down = pushdown_saop_boolexpr(&tmp_context, saop);
			if (tmp_context.can_pushdown)
			{
				context->needs_recheck |= tmp_context.needs_recheck;
				return pushed_down;
			}

			/*
			 * No other ways to push it down, so consider it failed.
			 */
			context->can_pushdown = false;
			return orig_node;
		}
		case T_BoolExpr:
		{
			BoolExpr *orig_boolexpr = castNode(BoolExpr, orig_node);
			List *pushed_down_args = NIL;
			ListCell *lc;
			foreach (lc, orig_boolexpr->args)
			{
				QualPushdownContext tmp_context = *context;
				void *pushed_down = qual_pushdown_mutator(lfirst(lc), &tmp_context);

				/*
				 * If the bool operation uses AND, it's correct and useful to
				 * push down only some arguments.
				 *
				 * For OR, we must be able to push down every argument.
				 */
				if (!tmp_context.can_pushdown)
				{
					if (orig_boolexpr->boolop != AND_EXPR)
					{
						context->can_pushdown = false;
						return orig_node;
					}

					continue;
				}

				context->needs_recheck |= tmp_context.needs_recheck;
				pushed_down_args = lappend(pushed_down_args, pushed_down);
			}

			/*
			 * We can have no pushed down arguments if we had an AND bool
			 * operation, but failed to push down every individual argument.
			 */
			if (pushed_down_args == NIL)
			{
				context->can_pushdown = false;
				return orig_node;
			}

			BoolExpr *boolexpr_copy = makeNode(BoolExpr);
			*boolexpr_copy = *orig_boolexpr;
			boolexpr_copy->args = pushed_down_args;
			return (Node *) boolexpr_copy;
		}

		/*
		 * These nodes do not influence the pushdown by themselves, so we
		 * recurse.
		 */
		case T_FuncExpr:
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
		{
			Node *pushed_down =
				expression_tree_mutator((Node *) orig_node, qual_pushdown_mutator, context);
			return pushed_down;
		}

		/*
		 * We don't know how to work with other nodes.
		 */
		default:
			context->can_pushdown = false;
			return orig_node;
	}
}
