/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <parser/parse_func.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compat.h"

#if PG12_LT /* nodes/relation.h renamed in fa2cf16 */
#include <nodes/relation.h>
#include <optimizer/var.h> /* f09346a */
#else
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#endif

#include "decompress_chunk.h"
#include "qual_pushdown.h"
#include "hypertable_compression.h"
#include "compression/create.h"
#include "custom_type_cache.h"
#include "compression/segment_meta.h"

typedef struct QualPushdownContext
{
	RelOptInfo *chunk_rel;
	RelOptInfo *compressed_rel;
	RangeTblEntry *chunk_rte;
	RangeTblEntry *compressed_rte;
	List *compression_info;
	bool can_pushdown;
	bool needs_recheck;
} QualPushdownContext;

static Node *modify_expression(Node *node, QualPushdownContext *context);

void
pushdown_quals(PlannerInfo *root, RelOptInfo *chunk_rel, RelOptInfo *compressed_rel,
			   List *compression_info)
{
	ListCell *lc;
	List *decompress_clauses = NIL;
	QualPushdownContext context = {
		.chunk_rel = chunk_rel,
		.compressed_rel = compressed_rel,
		.chunk_rte = planner_rt_fetch(chunk_rel->relid, root),
		.compressed_rte = planner_rt_fetch(compressed_rel->relid, root),
		.compression_info = compression_info,
	};

	foreach (lc, chunk_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst(lc);
		Expr *expr;

		/* pushdown is not safe for volatile expressions */
		if (contain_volatile_functions((Node *) ri->clause))
			continue;

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
								make_simple_restrictinfo(lfirst(lc_and)));
				}
			}
			else
				compressed_rel->baserestrictinfo =
					lappend(compressed_rel->baserestrictinfo, make_simple_restrictinfo(expr));
		}
		/* We need to check the restriction clause on the decompress node if the clause can't be
		 * pushed down or needs re-checking */
		if (!context.can_pushdown || context.needs_recheck)
		{
			decompress_clauses = lappend(decompress_clauses, ri);
		}
	}
	chunk_rel->baserestrictinfo = decompress_clauses;
}

static inline FormData_hypertable_compression *
get_compression_info_from_var(QualPushdownContext *context, Var *var)
{
	char *column_name;
	/* Not on the chunk we expect */
	if (var->varno != context->chunk_rel->relid)
		return NULL;

	/* ignore system attibutes or whole row references */
	if (var->varattno <= 0)
		return NULL;

	column_name = get_attname_compat(context->chunk_rte->relid, var->varattno, false);
	return get_column_compressioninfo(context->compression_info, column_name);
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
get_segment_meta_min_attr_number(FormData_hypertable_compression *compression_info,
								 Oid compressed_relid)
{
	char *meta_col_name = compression_column_segment_min_name(compression_info);

	if (meta_col_name == NULL)
		elog(ERROR, "could not find meta column");

	return get_attnum(compressed_relid, meta_col_name);
}

static AttrNumber
get_segment_meta_max_attr_number(FormData_hypertable_compression *compression_info,
								 Oid compressed_relid)
{
	char *meta_col_name = compression_column_segment_max_name(compression_info);

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

static inline FormData_hypertable_compression *
get_compression_info_for_column_with_segment_meta(QualPushdownContext *context, Expr *expr)
{
	Var *v;
	FormData_hypertable_compression *compression_info;

	if (!IsA(expr, Var))
		return NULL;

	v = (Var *) expr;

	compression_info = get_compression_info_from_var(context, v);
	/* Only order by vars have segment meta */
	if (compression_info == NULL || compression_info->orderby_column_index <= 0)
		return NULL;

	return compression_info;
}

static Expr *
pushdown_op_to_segment_meta_min_max(QualPushdownContext *context, List *expr_args, Oid op_oid,
									Oid op_collation)
{
	Expr *leftop, *rightop, *expr;
	Var *var_with_segment_meta;
	TypeCacheEntry *tce;
	int strategy;
	FormData_hypertable_compression *compression_info;

	if (list_length(expr_args) != 2)
		return NULL;

	leftop = linitial(expr_args);
	rightop = lsecond(expr_args);

	if (IsA(leftop, RelabelType))
		leftop = ((RelabelType *) leftop)->arg;
	if (IsA(rightop, RelabelType))
		rightop = ((RelabelType *) rightop)->arg;

	/* Find the side that has var with segment meta set expr to the other side */
	if ((compression_info = get_compression_info_for_column_with_segment_meta(context, leftop)) !=
		NULL)
	{
		var_with_segment_meta = (Var *) leftop;
		expr = rightop;
	}
	else if ((compression_info =
				  get_compression_info_for_column_with_segment_meta(context, rightop)) != NULL)
	{
		var_with_segment_meta = (Var *) rightop;
		expr = leftop;
		op_oid = get_commutator(op_oid);
	}
	else
		return NULL;

	/* todo think through the strict case */
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

	switch (strategy)
	{
		case BTEqualStrategyNumber:
		{
			/* var = expr implies min < expr and max > expr */
			Oid opno_le = get_opfamily_member(tce->btree_opf,
											  tce->type_id,
											  tce->type_id,
											  BTLessEqualStrategyNumber);
			Oid opno_ge = get_opfamily_member(tce->btree_opf,
											  tce->type_id,
											  tce->type_id,
											  BTGreaterEqualStrategyNumber);

			if (!OidIsValid(opno_le) || !OidIsValid(opno_ge))
				return NULL;

			return make_andclause(list_make2(
				make_segment_meta_opexpr(context,
										 opno_le,
										 get_segment_meta_min_attr_number(compression_info,
																		  context->compressed_rte
																			  ->relid),
										 var_with_segment_meta,
										 expr,
										 BTLessEqualStrategyNumber),
				make_segment_meta_opexpr(context,
										 opno_ge,
										 get_segment_meta_max_attr_number(compression_info,
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
					get_opfamily_member(tce->btree_opf, tce->type_id, tce->type_id, strategy);

				if (!OidIsValid(opno))
					return NULL;

				return (Expr *)
					make_segment_meta_opexpr(context,
											 opno,
											 get_segment_meta_min_attr_number(compression_info,
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
					get_opfamily_member(tce->btree_opf, tce->type_id, tce->type_id, strategy);

				if (!OidIsValid(opno))
					return NULL;

				return (Expr *)
					make_segment_meta_opexpr(context,
											 opno,
											 get_segment_meta_max_attr_number(compression_info,
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
		case T_ScalarArrayOpExpr:
		case T_List:
		case T_Const:
		case T_NullTest:
		case T_Param:
			break;
		case T_Var:
		{
			Var *var = castNode(Var, node);
			FormData_hypertable_compression *compressioninfo =
				get_compression_info_from_var(context, var);
			AttrNumber compressed_attno;

			if (compressioninfo == NULL)
			{
				context->can_pushdown = false;
				return NULL;
			}

			/* we can only push down quals for segmentby columns */
			if (compressioninfo->segmentby_column_index <= 0)
			{
				context->can_pushdown = false;
				return NULL;
			}

			var = copyObject(var);
			compressed_attno =
				get_attnum(context->compressed_rte->relid, compressioninfo->attname.data);
			var->varno = context->compressed_rel->relid;
			var->varattno = compressed_attno;

			return (Node *) var;
		}
		default:
			context->can_pushdown = false;
			return NULL;
			break;
	}

	return expression_tree_mutator(node, modify_expression, context);
}
