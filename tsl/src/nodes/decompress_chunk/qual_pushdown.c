/* * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/optimizer.h>
#include <optimizer/restrictinfo.h>
#include <parser/parsetree.h>
#include <parser/parse_func.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "decompress_chunk.h"
#include "qual_pushdown.h"
#include "ts_catalog/array_utils.h"
#include "compression/create.h"
#include "custom_type_cache.h"
#include "compression/segment_meta.h"

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
			if (IsA(expr, BoolExpr) && ((BoolExpr *) expr)->boolop == AND_EXPR)
			{
				/* have to separate out and expr into different restrict infos */
				ListCell *lc_and;
				BoolExpr *bool_expr = (BoolExpr *) expr;
				foreach (lc_and, bool_expr->args)
				{
					compressed_rel->baserestrictinfo =
						lappend(compressed_rel->baserestrictinfo,
								make_simple_restrictinfo_compat(root, lfirst(lc_and)));
				}
			}
			else
				compressed_rel->baserestrictinfo =
					lappend(compressed_rel->baserestrictinfo,
							make_simple_restrictinfo_compat(root, expr));
		}
		/* We need to check the restriction clause on the decompress node if the clause can't be
		 * pushed down or needs re-checking */
		if (!context.can_pushdown || context.needs_recheck || chunk_partial)
		{
			decompress_clauses = lappend(decompress_clauses, ri);
		}

		if (context.needs_recheck)
		{
			/*
			 * If we managed to push down the comparison of orderby column
			 * to the compressed scan, most matched batches are likely to
			 * match entirely, so the selectivity of the recheck will be
			 * close to 1.
			 */
			ri->norm_selec = 1;
			Assert(context.can_pushdown);
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

static AttrNumber
get_segment_meta_min_attr_number(int16 orderby_column_index, Oid compressed_relid)
{
	char *meta_col_name = column_segment_min_name(orderby_column_index);

	if (meta_col_name == NULL)
		elog(ERROR, "could not find meta column");

	return get_attnum(compressed_relid, meta_col_name);
}

static AttrNumber
get_segment_meta_max_attr_number(int16 orderby_column_index, Oid compressed_relid)
{
	char *meta_col_name = column_segment_max_name(orderby_column_index);

	if (meta_col_name == NULL)
		elog(ERROR, "could not find meta column");

	return get_attnum(compressed_relid, meta_col_name);
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

static bool
expr_has_metadata(QualPushdownContext *context, Expr *expr, int16 *index)
{
	if (!IsA(expr, Var))
		return false;

	Var *var = castNode(Var, expr);

	/* Not on the chunk we expect */
	if ((Index) var->varno != context->chunk_rel->relid)
		return false;

	/* ignore system attributes or whole row references */
	if (var->varattno <= 0)
		return false;

	char *attname = get_attname(context->chunk_rte->relid, var->varattno, false);
	int16 pos = ts_array_position(context->settings->fd.orderby, attname);

	if (pos > 0)
	{
		*index = pos;
	}

	return pos > 0;
}

static Expr *
pushdown_op_to_segment_meta_min_max(QualPushdownContext *context, List *expr_args, Oid op_oid,
									Oid op_collation)
{
	Expr *leftop, *rightop, *expr;
	Var *var_with_segment_meta;
	TypeCacheEntry *tce;
	int strategy;
	Oid expr_type_id;
	int16 index = 0;

	if (list_length(expr_args) != 2)
		return NULL;

	leftop = linitial(expr_args);
	rightop = lsecond(expr_args);

	if (IsA(leftop, RelabelType))
		leftop = ((RelabelType *) leftop)->arg;
	if (IsA(rightop, RelabelType))
		rightop = ((RelabelType *) rightop)->arg;

	/* Find the side that has var with segment meta set expr to the other side */
	if (expr_has_metadata(context, leftop, &index))
	{
		var_with_segment_meta = (Var *) leftop;
		expr = rightop;
	}
	else if (expr_has_metadata(context, rightop, &index))
	{
		var_with_segment_meta = (Var *) rightop;
		expr = leftop;
		op_oid = get_commutator(op_oid);
	}
	else
	{
		return NULL;
	}

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

	expr = get_pushdownsafe_expr(context, expr);

	if (expr == NULL)
		return NULL;
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

			return make_andclause(list_make2(
				make_segment_meta_opexpr(context,
										 opno_le,
										 get_segment_meta_min_attr_number(index,
																		  context->compressed_rte
																			  ->relid),
										 var_with_segment_meta,
										 expr,
										 BTLessEqualStrategyNumber),
				make_segment_meta_opexpr(context,
										 opno_ge,
										 get_segment_meta_max_attr_number(index,
																		  context->compressed_rte
																			  ->relid),
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

				return (Expr *)
					make_segment_meta_opexpr(context,
											 opno,
											 get_segment_meta_min_attr_number(index,
																			  context
																				  ->compressed_rte
																				  ->relid),
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

				return (Expr *)
					make_segment_meta_opexpr(context,
											 opno,
											 get_segment_meta_max_attr_number(index,
																			  context
																				  ->compressed_rte
																				  ->relid),
											 var_with_segment_meta,
											 expr,
											 strategy);
			}
		default:
			return NULL;
	}
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
				Expr *pd = pushdown_op_to_segment_meta_min_max(context,
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
		case T_CoerceViaIO:
		case T_RelabelType:
		case T_ScalarArrayOpExpr:
		case T_List:
		case T_Const:
		case T_NullTest:
		case T_Param:
		case T_SQLValueFunction:
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
