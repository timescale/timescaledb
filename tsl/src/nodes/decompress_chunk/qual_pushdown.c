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
#include "compression/sparse_index_bloom1.h"
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
	CompressionSettings *settings;

	/*
	 * This is actually the result, not the static input context like above, but
	 * there's no way to separate this properly using the expression tree mutator
	 * interface.
	 */
	bool can_pushdown;
	bool needs_recheck;
} QualPushdownContext;

static QualPushdownContext
copy_context(const QualPushdownContext *source)
{
	QualPushdownContext copy;
	copy = *source;
	copy.can_pushdown = true;
	copy.needs_recheck = false;
	return copy;
}

static Node *qual_pushdown_mutator(Node *node, QualPushdownContext *context);

void
pushdown_quals(PlannerInfo *root, CompressionSettings *settings, RelOptInfo *chunk_rel,
			   RelOptInfo *compressed_rel, bool chunk_partial)
{
	ListCell *lc;
	List *decompress_clauses = NIL;
	QualPushdownContext base_context = {
		.chunk_rel = chunk_rel,
		.compressed_rel = compressed_rel,
		.chunk_rte = planner_rt_fetch(chunk_rel->relid, root),
		.compressed_rte = planner_rt_fetch(compressed_rel->relid, root),
		.settings = settings,
	};

	foreach (lc, chunk_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst(lc);

		QualPushdownContext clause_context = copy_context(&base_context);
		Node *pushed_down = qual_pushdown_mutator((Node *) ri->clause, &clause_context);

		if (clause_context.can_pushdown)
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

		/*
		 * We need to check the restriction clause on the decompress node if the clause can't be
		 * pushed down or needs re-checking.
		 */
		if (!clause_context.can_pushdown || clause_context.needs_recheck || chunk_partial)
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

	List *expr_args = orig_opexpr->args;
	Assert(list_length(expr_args) == 2);
	Expr *orig_leftop = linitial(expr_args);
	Expr *orig_rightop = lsecond(expr_args);

	if (IsA(orig_leftop, RelabelType))
		orig_leftop = ((RelabelType *) orig_leftop)->arg;
	if (IsA(orig_rightop, RelabelType))
		orig_rightop = ((RelabelType *) orig_rightop)->arg;

	/* Find the side that has var with segment meta set expr to the other side */
	Oid op_oid = orig_opexpr->opno;
	AttrNumber min_attno;
	AttrNumber max_attno;
	expr_fetch_minmax_metadata(context, orig_leftop, &min_attno, &max_attno);
	if (min_attno == InvalidAttrNumber || max_attno == InvalidAttrNumber)
	{
		/* No metadata for the left operand, try to commute the operator. */
		op_oid = get_commutator(op_oid);
		Expr *tmp = orig_leftop;
		orig_leftop = orig_rightop;
		orig_rightop = tmp;

		expr_fetch_minmax_metadata(context, orig_leftop, &min_attno, &max_attno);
	}

	if (min_attno == InvalidAttrNumber || max_attno == InvalidAttrNumber)
	{
		/* No metadata for either operand. */
		context->can_pushdown = false;
		return orig_opexpr;
	}

	Var *var_with_segment_meta = castNode(Var, orig_leftop);

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

	TypeCacheEntry *tce =
		lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_BTREE_OPFAMILY);

	const int strategy = get_op_opfamily_strategy(op_oid, tce->btree_opf);
	if (strategy == InvalidStrategy)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}

	/*
	 * Check if the righthand expression is safe to push down. We cannot combine
	 * it with the original operator if there can be false negatives.
	 */
	QualPushdownContext tmp_context = copy_context(context);
	Expr *pushed_down_rightop = (Expr *) qual_pushdown_mutator((Node *) orig_rightop, &tmp_context);
	if (!tmp_context.can_pushdown || tmp_context.needs_recheck)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}
	Assert(pushed_down_rightop != NULL);

	const Oid expr_type_id = exprType((Node *) pushed_down_rightop);

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
				/*
				 * Shouldn't be possible if we managed to create the min/max
				 * sparse index, but defend against catalog corruption.
				 */
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
					/*
					 * Shouldn't be possible if we managed to create the min/max
					 * sparse index, but defend against catalog corruption.
					 */
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
					/*
					 * Shouldn't be possible if we managed to create the min/max
					 * sparse index, but defend against catalog corruption.
					 */
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
													 BLOOM1_COLUMN_PREFIX);
}

static void *
pushdown_op_to_segment_meta_bloom1(QualPushdownContext *context, OpExpr *orig_opexpr)
{
	/*
	 * This always requires rechecking the decompressed data.
	 */
	context->needs_recheck = true;

	List *expr_args = orig_opexpr->args;
	Assert(list_length(expr_args) == 2);
	Expr *orig_leftop = linitial(expr_args);
	Expr *orig_rightop = lsecond(expr_args);

	if (IsA(orig_leftop, RelabelType))
		orig_leftop = ((RelabelType *) orig_leftop)->arg;
	if (IsA(orig_rightop, RelabelType))
		orig_rightop = ((RelabelType *) orig_rightop)->arg;

	/* Find the side that has var with segment meta set expr to the other side */
	Oid op_oid = orig_opexpr->opno;
	AttrNumber bloom1_attno = InvalidAttrNumber;
	expr_fetch_bloom1_metadata(context, orig_leftop, &bloom1_attno);
	if (bloom1_attno == InvalidAttrNumber)
	{
		/* No metadata for the left operand, try to commute the operator. */
		op_oid = get_commutator(op_oid);
		Expr *tmp = orig_leftop;
		orig_leftop = orig_rightop;
		orig_rightop = tmp;

		expr_fetch_bloom1_metadata(context, orig_leftop, &bloom1_attno);
	}

	if (bloom1_attno == InvalidAttrNumber)
	{
		/* No metadata for either operand. */
		context->can_pushdown = false;
		return orig_opexpr;
	}

	Var *var_with_segment_meta = castNode(Var, orig_leftop);

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
	TypeCacheEntry *tce =
		lookup_type_cache(var_with_segment_meta->vartype, TYPECACHE_HASH_OPFAMILY);
	const int strategy = get_op_opfamily_strategy(op_oid, tce->hash_opf);
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
	 * Check if the righthand expression is safe to push down. We cannot combine
	 * it with the original operator if there can be false negatives.
	 */
	QualPushdownContext tmp_context = copy_context(context);
	Expr *pushed_down_rightop = (Expr *) qual_pushdown_mutator((Node *) orig_rightop, &tmp_context);
	if (!tmp_context.can_pushdown || tmp_context.needs_recheck)
	{
		context->can_pushdown = false;
		return orig_opexpr;
	}
	Assert(pushed_down_rightop != NULL);

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
			 *
			 * Note that we can't push down the entire operator if we pushed
			 * down both sides inexactly, i.e. they require recheck. This means
			 * we can have false positives there, and combining false positives
			 * with the original operator could lead to false negatives, which
			 * would be a bug. Consider for example (x = 1) = (y = 1) in case
			 * where both sides are false, but there's a false posistive for the
			 * pushed down version of the left side but not the right side.
			 */
			QualPushdownContext tmp_context = copy_context(context);
			void *pushed_down =
				expression_tree_mutator((Node *) orig_node, qual_pushdown_mutator, &tmp_context);
			if (tmp_context.can_pushdown && !tmp_context.needs_recheck)
			{
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
				tmp_context = copy_context(context);
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
			tmp_context = copy_context(context);
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
			 * It can be possible to push down the scalar array operation as is,
			 * if it references only the segmentby columns. Check this case
			 * first.
			 *
			 * See the comment for OpExpr about needs_recheck handling.
			 */
			QualPushdownContext tmp_context = copy_context(context);
			void *pushed_down =
				expression_tree_mutator((Node *) orig_node, qual_pushdown_mutator, &tmp_context);
			if (tmp_context.can_pushdown && !tmp_context.needs_recheck)
			{
				return pushed_down;
			}

			/*
			 * No other ways to push it down, so consider it failed.
			 */
			context->can_pushdown = false;
			return orig_node;
		}
		/*
		 * These nodes do not influence the pushdown by themselves, so we
		 * recurse.
		 */
		case T_BoolExpr:
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
