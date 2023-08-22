/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */

/*-------------------------------------------------------------------------
 *
 * deparse.c
 *		  Query deparser for postgres_fdw
 *
 * This file includes functions that examine query WHERE clauses to see
 * whether they're safe to send to the data node for execution, as
 * well as functions to construct the query text to be sent.  The latter
 * functionality is annoyingly duplicative of ruleutils.c, but there are
 * enough special considerations that it seems best to keep this separate.
 * One saving grace is that we only need deparse logic for node types that
 * we consider safe to send.
 *
 * We assume that the remote session's search_path is exactly "pg_catalog",
 * and thus we need schema-qualify all and only names outside pg_catalog.
 *
 * We consider collations and COLLATE expressions safe to send since we assume
 * that all nodes of a distributed hypertable has the same configuration
 * w.r.t. collations.
 *
 * Portions Copyright (c) 2012-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		  contrib/postgres_fdw/deparse.c
 *
 *-------------------------------------------------------------------------
 */
#include <postgres.h>
#include <math.h>

#include <access/heapam.h>
#include <access/htup_details.h>
#include <access/sysattr.h>
#include <catalog/pg_aggregate.h>
#include <catalog/pg_collation.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <commands/defrem.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pathnodes.h>
#include <nodes/plannodes.h>
#include <optimizer/appendinfo.h>
#include <optimizer/clauses.h>
#include <optimizer/optimizer.h>
#include <optimizer/paths.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include <func_cache.h>
#include <remote/utils.h>

#include "relinfo.h"
#include "deparse.h"
#include "shippable.h"
#include "utils.h"
#include "scan_plan.h"
#include "extension_constants.h"
#include "partialize_finalize.h"
#include "nodes/gapfill/gapfill.h"
#include "planner/planner.h"

/*
 * Global context for foreign_expr_walker's search of an expression tree.
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;		/* global planner state */
	RelOptInfo *foreignrel; /* the foreign relation we are planning for */
	Relids relids;			/* relids of base relations in the underlying
							 * scan */
} foreign_glob_cxt;

/*
 * Context for deparseExpr
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;		/* global planner state */
	RelOptInfo *foreignrel; /* the foreign relation we are planning for */
	RelOptInfo *scanrel;	/* the underlying scan relation. Same as
							 * foreignrel, when that represents a join or
							 * a base relation. */
	StringInfo buf;			/* output buffer to append to */
	List **params_list;		/* exprs that will become remote Params */
	DataNodeChunkAssignment *sca;
} deparse_expr_cxt;

#define REL_ALIAS_PREFIX "r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno) appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))
#define SUBQUERY_REL_ALIAS_PREFIX "s"
#define SUBQUERY_COL_ALIAS_PREFIX "c"

/* Oids of mutable functions determined to safe to pushdown to data nodes */
static Oid PushdownSafeFunctionOIDs[] = {
	F_DATE_CMP_TIMESTAMPTZ,
	F_DATE_IN,
	F_DATE_OUT,
	F_INTERVAL_IN,
	F_INTERVAL_PL,
	F_NOW, /* Special case, this will be evaluated prior to pushdown */
	F_TIMESTAMPTZ_CMP_DATE,
	F_TIMESTAMPTZ_CMP_TIMESTAMP,
	F_TIMESTAMPTZ_DATE,
	F_TIMESTAMPTZ_EQ_DATE,
	F_TIMESTAMPTZ_EQ_TIMESTAMP,
	F_TIMESTAMPTZ_GE_DATE,
	F_TIMESTAMPTZ_GE_TIMESTAMP,
	F_TIMESTAMPTZ_GT_DATE,
	F_TIMESTAMPTZ_GT_TIMESTAMP,
	F_TIMESTAMPTZ_IN,
	F_TIMESTAMPTZ_LE_DATE,
	F_TIMESTAMPTZ_LE_TIMESTAMP,
	F_TIMESTAMPTZ_LT_DATE,
	F_TIMESTAMPTZ_LT_TIMESTAMP,
	F_TIMESTAMPTZ_MI_INTERVAL,
	F_TIMESTAMPTZ_NE_DATE,
	F_TIMESTAMPTZ_NE_TIMESTAMP,
	F_TIMESTAMPTZ_OUT,
	F_TIMESTAMPTZ_PL_INTERVAL,
	F_TIMESTAMPTZ_TIMESTAMP,
	F_TIMESTAMP_CMP_TIMESTAMPTZ,
	F_TIMESTAMP_EQ_TIMESTAMPTZ,
	F_TIMESTAMP_GE_TIMESTAMPTZ,
	F_TIMESTAMP_GT_TIMESTAMPTZ,
	F_TIMESTAMP_LE_TIMESTAMPTZ,
	F_TIMESTAMP_LT_TIMESTAMPTZ,
	F_TIMESTAMP_MI_INTERVAL,
	F_TIMESTAMP_NE_TIMESTAMPTZ,
	F_TIMESTAMP_PL_INTERVAL,
	F_TIMESTAMP_TIMESTAMPTZ,
	F_TIMETZ_IN,
	F_TIME_IN,
	F_TIME_TIMETZ,
#if PG14_LT
	F_INTERVAL_PART,
	F_MAKE_TIMESTAMPTZ,
	F_MAKE_TIMESTAMPTZ_AT_TIMEZONE,
	F_TIMESTAMPTZ_PART,
	F_TIMESTAMPTZ_TIME,
	F_TIMESTAMPTZ_TIMETZ,
	F_TIMESTAMPTZ_TRUNC,
	F_TIMESTAMPTZ_TRUNC_ZONE,
	F_TO_TIMESTAMP,
#elif PG14_GE
	F_DATE_PART_TEXT_INTERVAL,
	F_MAKE_TIMESTAMPTZ_INT4_INT4_INT4_INT4_INT4_FLOAT8,
	F_MAKE_TIMESTAMPTZ_INT4_INT4_INT4_INT4_INT4_FLOAT8_TEXT,
	F_DATE_PART_TEXT_TIMESTAMPTZ,
	F_TIME_TIMESTAMPTZ,
	F_TIMETZ_TIMESTAMPTZ,
	F_DATE_TRUNC_TEXT_TIMESTAMPTZ,
	F_DATE_TRUNC_TEXT_TIMESTAMPTZ_TEXT,
	F_TO_TIMESTAMP_TEXT_TEXT,
#endif
};
static const int NumPushdownSafeOIDs =
	sizeof(PushdownSafeFunctionOIDs) / sizeof(PushdownSafeFunctionOIDs[0]);

/*
 * Functions to determine whether an expression can be evaluated safely on
 * data node.
 */
static bool foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt);
static char *deparse_type_name(Oid type_oid, int32 typemod);

/*
 * Functions to construct string representation of a node tree.
 */
static void deparseTargetList(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
							  bool is_returning, Bitmapset *attrs_used, bool qualify_col,
							  List **retrieved_attrs);
static void deparseExplicitTargetList(List *tlist, bool is_returning, List **retrieved_attrs,
									  deparse_expr_cxt *context);
static void deparseSubqueryTargetList(deparse_expr_cxt *context);
static void deparseReturningList(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
								 bool trig_after_row, List *returningList, List **retrieved_attrs);
static void deparseColumnRef(StringInfo buf, int varno, int varattno, RangeTblEntry *rte,
							 bool qualify_col);
static void deparseRelation(StringInfo buf, Relation rel);
static void deparseExpr(Expr *node, deparse_expr_cxt *context);
static void deparseVar(Var *node, deparse_expr_cxt *context);
static void deparseConst(Const *node, deparse_expr_cxt *context, int showtype);
static void deparseParam(Param *node, deparse_expr_cxt *context);
static void deparseSubscriptingRef(SubscriptingRef *node, deparse_expr_cxt *context);
static void deparseFuncExpr(FuncExpr *node, deparse_expr_cxt *context);
static void deparseOpExpr(OpExpr *node, deparse_expr_cxt *context);
static void deparseOperatorName(StringInfo buf, Form_pg_operator opform);
static void deparseDistinctExpr(DistinctExpr *node, deparse_expr_cxt *context);
static void deparseScalarArrayOpExpr(ScalarArrayOpExpr *node, deparse_expr_cxt *context);
static void deparseRelabelType(RelabelType *node, deparse_expr_cxt *context);
static void deparseBoolExpr(BoolExpr *node, deparse_expr_cxt *context);
static void deparseNullTest(NullTest *node, deparse_expr_cxt *context);
static void deparseArrayExpr(ArrayExpr *node, deparse_expr_cxt *context);
static void printRemoteParam(int paramindex, Oid paramtype, int32 paramtypmod,
							 deparse_expr_cxt *context);
static void printRemotePlaceholder(Oid paramtype, int32 paramtypmod, deparse_expr_cxt *context);
static void deparseSelectSql(List *tlist, bool is_subquery, List **retrieved_attrs,
							 deparse_expr_cxt *context, List *pathkeys);
static void deparseLockingClause(deparse_expr_cxt *context);
static void appendOrderByClause(List *pathkeys, deparse_expr_cxt *context);
static void appendLimit(deparse_expr_cxt *context, List *pathkeys);

static void append_chunk_exclusion_condition(deparse_expr_cxt *context, bool use_alias);
static void appendConditions(List *exprs, deparse_expr_cxt *context, bool is_first);
static void deparseFromExprForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
								  bool use_alias, Index ignore_rel, List **ignore_conds,
								  List **params_list, DataNodeChunkAssignment *sca);
static void deparseFromExpr(List *quals, deparse_expr_cxt *context);
static void deparseRangeTblRef(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
							   bool make_subquery, Index ignore_rel, List **ignore_conds,
							   List **params_list, DataNodeChunkAssignment *sca);
static void deparseAggref(Aggref *node, deparse_expr_cxt *context);
static void appendGroupByClause(List *tlist, deparse_expr_cxt *context);
static void appendAggOrderBy(List *orderList, List *targetList, deparse_expr_cxt *context);
static void appendFunctionName(Oid funcid, deparse_expr_cxt *context);
static Node *deparseSortGroupClause(Index ref, List *tlist, bool force_colno,
									deparse_expr_cxt *context);
static bool column_qualification_needed(deparse_expr_cxt *context);

/*
 * Helper functions
 */
static bool is_subquery_var(Var *node, RelOptInfo *foreignrel, int *relno, int *colno);
static void get_relation_column_alias_ids(Var *node, RelOptInfo *foreignrel, int *relno,
										  int *colno);

/*
 * Examine each qual clause in input_conds, and classify them into two groups,
 * which are returned as two lists:
 *	- remote_conds contains expressions that can be evaluated remotely
 *	- local_conds contains expressions that can't be evaluated remotely
 */
void
classify_conditions(PlannerInfo *root, RelOptInfo *baserel, List *input_conds, List **remote_conds,
					List **local_conds)
{
	ListCell *lc;

	*remote_conds = NIL;
	*local_conds = NIL;

	foreach (lc, input_conds)
	{
		RestrictInfo *ri = lfirst_node(RestrictInfo, lc);

		if (ts_is_foreign_expr(root, baserel, ri->clause))
			*remote_conds = lappend(*remote_conds, ri);
		else
			*local_conds = lappend(*local_conds, ri);
	}
}

static int
oid_comparator(const void *a, const void *b)
{
	if (*(Oid *) a == *(Oid *) b)
		return 0;
	else if (*(Oid *) a < *(Oid *) b)
		return -1;
	else
		return 1;
}

static bool
function_is_whitelisted(Oid func_id)
{
	static bool PushdownOIDsSorted = false;

	if (!PushdownOIDsSorted)
	{
		qsort(PushdownSafeFunctionOIDs, NumPushdownSafeOIDs, sizeof(Oid), oid_comparator);
		PushdownOIDsSorted = true;
	}

	return bsearch(&func_id,
				   PushdownSafeFunctionOIDs,
				   NumPushdownSafeOIDs,
				   sizeof(Oid),
				   oid_comparator) != NULL;
}

/*
 * Check for mutable functions in an expression.
 *
 * This code is based on the corresponding PostgreSQL function, but with extra
 * handling to whitelist some bucketing functions that we know are safe to
 * push down despite mutability.
 */
static bool
contain_mutable_functions_checker(Oid func_id, void *context)
{
	FuncInfo *finfo = ts_func_cache_get_bucketing_func(func_id);

	/* We treat all bucketing functions as shippable, even date_trunc(text,
	 * timestamptz). We do this special case for bucketing functions until we
	 * can figure out a more consistent way to deal with functions taking,
	 * e.g., timestamptz parameters since we ensure that all connections to
	 * other nodes have the access node's timezone setting. */
	if (NULL != finfo)
		return false;

	if (func_volatile(func_id) == PROVOLATILE_IMMUTABLE)
		return false;

	/* Certain functions are mutable but are known to safe to push down to the data node. */
	if (function_is_whitelisted(func_id))
		return false;

#ifndef NDEBUG
	/* Special debug functions that we want to ship to data nodes. */
	const char debug_func_prefix[] = "ts_debug_shippable_";
	if (strncmp(get_func_name(func_id), debug_func_prefix, strlen(debug_func_prefix)) == 0)
	{
		return false;
	}
#endif

	return true;
}

/*
 * Expression walker based on the corresponding PostgreSQL function. We're
 * using a custom checker function, so need a modifed version of this walker.
 */
static bool
contain_mutable_functions_walker(Node *node, void *context)
{
	if (node == NULL)
		return false;
	/* Check for mutable functions in node itself */
	if (check_functions_in_node(node, contain_mutable_functions_checker, context))
		return true;

	if (IsA(node, SQLValueFunction))
	{
		/* all variants of SQLValueFunction are stable */
		return true;
	}

	if (IsA(node, NextValueExpr))
	{
		/* NextValueExpr is volatile */
		return true;
	}

	/*
	 * It should be safe to treat MinMaxExpr as immutable, because it will
	 * depend on a non-cross-type btree comparison function, and those should
	 * always be immutable.  Treating XmlExpr as immutable is more dubious,
	 * and treating CoerceToDomain as immutable is outright dangerous.  But we
	 * have done so historically, and changing this would probably cause more
	 * problems than it would fix.  In practice, if you have a non-immutable
	 * domain constraint you are in for pain anyhow.
	 */

	/* Recurse to check arguments */
	if (IsA(node, Query))
	{
		/* Recurse into subselects */
		return query_tree_walker((Query *) node, contain_mutable_functions_walker, context, 0);
	}
	return expression_tree_walker(node, contain_mutable_functions_walker, context);
}

static bool
foreign_expr_contains_mutable_functions(Node *clause)
{
	return contain_mutable_functions_walker(clause, NULL);
}

/*
 * Returns true if given expr is safe to evaluate on the data node.
 */
bool
ts_is_foreign_expr(PlannerInfo *root, RelOptInfo *baserel, Expr *expr)
{
	foreign_glob_cxt glob_cxt;
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(baserel);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;

	/*
	 * For an upper relation, use relids from its underneath scan relation,
	 * because the upperrel's own relids currently aren't set to anything
	 * meaningful by the core code.  For other relation, use their own relids.
	 */
	if (IS_UPPER_REL(baserel))
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;

	if (!foreign_expr_walker((Node *) expr, &glob_cxt))
		return false;

	/*
	 * It is not supported to execute time_bucket_gapfill on data node.
	 */
	if (gapfill_in_expression(expr))
		return false;

	/*
	 * An expression which includes any mutable functions can't be sent over
	 * because its result is not stable.  For example, sending now() remote
	 * side could cause confusion from clock offsets.  Future versions might
	 * be able to make this choice with more granularity.  (We check this last
	 * because it requires a lot of expensive catalog lookups.)
	 */
	if (foreign_expr_contains_mutable_functions((Node *) expr))
		return false;

	/* OK to evaluate on the data node */
	return true;
}

/*
 * Check if expression is safe to execute remotely, and return true if so.
 *
 * We must check that the expression contains only node types we can deparse,
 * that all types/functions/operators are safe to send (they are "shippable"),
 * and that we aren't sending Var references to system columns.
 *
 * We do not care about collations because we assume that data nodes have
 * identical configuration as the access node.
 *
 * Note function mutability is not currently considered here.
 */
static bool
foreign_expr_walker(Node *node, foreign_glob_cxt *glob_cxt)
{
	bool check_type = true;
	TsFdwRelInfo *fpinfo;

	/* Need do nothing for empty subexpressions */
	if (node == NULL)
		return true;

	fpinfo = fdw_relinfo_get(glob_cxt->foreignrel);

	switch (nodeTag(node))
	{
		case T_Var:
		{
			Var *var = castNode(Var, node);

			if (bms_is_member(var->varno, glob_cxt->relids) && var->varlevelsup == 0)
			{
				/* Var belongs to foreign table */

				/*
				 * System columns other than ctid and oid should not be
				 * sent to the remote, since we don't make any effort to
				 * ensure that local and remote values match (tableoid, in
				 * particular, almost certainly doesn't match).
				 */
				if (var->varattno < 0 && var->varattno != SelfItemPointerAttributeNumber)
					return false;
			}
		}
		break;
		case T_Const:
			/* Consts are OK to execute remotely */
			break;
		case T_Param:
			/* Params are also OK to execute remotely */
			break;
		case T_SubscriptingRef:
		{
			SubscriptingRef *ar = castNode(SubscriptingRef, node);

			/* Assignment should not be in restrictions. */
			if (ar->refassgnexpr != NULL)
				return false;

			/*
			 * Recurse to remaining subexpressions.  Since the array
			 * subscripts must yield (noncollatable) integers, they won't
			 * affect the inner_cxt state.
			 */
			if (!foreign_expr_walker((Node *) ar->refupperindexpr, glob_cxt))
				return false;
			if (!foreign_expr_walker((Node *) ar->reflowerindexpr, glob_cxt))
				return false;
			if (!foreign_expr_walker((Node *) ar->refexpr, glob_cxt))
				return false;
		}
		break;
		case T_FuncExpr:
		{
			FuncExpr *fe = castNode(FuncExpr, node);

			/*
			 * If function used by the expression is not shippable, it
			 * can't be sent to remote because it might have incompatible
			 * semantics on remote side.
			 */
			if (!is_shippable(fe->funcid, ProcedureRelationId, fpinfo))
				return false;

			/*
			 * Recurse to input subexpressions.
			 */
			if (!foreign_expr_walker((Node *) fe->args, glob_cxt))
				return false;
		}
		break;
		case T_OpExpr:
		case T_DistinctExpr: /* struct-equivalent to OpExpr */
		{
			OpExpr *oe = (OpExpr *) node;

			/*
			 * Similarly, only shippable operators can be sent to remote.
			 * (If the operator is shippable, we assume its underlying
			 * function is too.)
			 */
			if (!is_shippable(oe->opno, OperatorRelationId, fpinfo))
				return false;

			/*
			 * Recurse to input subexpressions.
			 */
			if (!foreign_expr_walker((Node *) oe->args, glob_cxt))
				return false;
		}
		break;
		case T_ScalarArrayOpExpr:
		{
			ScalarArrayOpExpr *oe = castNode(ScalarArrayOpExpr, node);

			/*
			 * Again, only shippable operators can be sent to remote.
			 */
			if (!is_shippable(oe->opno, OperatorRelationId, fpinfo))
				return false;

			/*
			 * Recurse to input subexpressions.
			 */
			if (!foreign_expr_walker((Node *) oe->args, glob_cxt))
				return false;
		}
		break;
		case T_RelabelType:
		{
			RelabelType *r = castNode(RelabelType, node);

			/*
			 * Recurse to input subexpression.
			 */
			if (!foreign_expr_walker((Node *) r->arg, glob_cxt))
				return false;
		}
		break;
		case T_BoolExpr:
		{
			BoolExpr *b = castNode(BoolExpr, node);

			/*
			 * Recurse to input subexpressions.
			 */
			if (!foreign_expr_walker((Node *) b->args, glob_cxt))
				return false;
		}
		break;
		case T_NullTest:
		{
			NullTest *nt = castNode(NullTest, node);

			/*
			 * Recurse to input subexpressions.
			 */
			if (!foreign_expr_walker((Node *) nt->arg, glob_cxt))
				return false;
		}
		break;
		case T_ArrayExpr:
		{
			ArrayExpr *a = castNode(ArrayExpr, node);

			/*
			 * Recurse to input subexpressions.
			 */
			if (!foreign_expr_walker((Node *) a->elements, glob_cxt))
				return false;
		}
		break;
		case T_List:
		{
			List *l = castNode(List, node);
			ListCell *lc;

			/*
			 * Recurse to component subexpressions.
			 */
			foreach (lc, l)
			{
				if (!foreign_expr_walker((Node *) lfirst(lc), glob_cxt))
					return false;
			}
			/* Don't apply exprType() to the list. */
			check_type = false;
		}
		break;
		case T_Aggref:
		{
			Aggref *agg = castNode(Aggref, node);
			ListCell *lc;

			/* Not safe to pushdown when not in grouping context */
			if (!IS_UPPER_REL(glob_cxt->foreignrel))
				return false;

			/* As usual, it must be shippable. */
			if (!is_shippable(agg->aggfnoid, ProcedureRelationId, fpinfo))
				return false;

			/*
			 * Recurse to input args. aggdirectargs, aggorder and
			 * aggdistinct are all present in args, so no need to check
			 * their shippability explicitly.
			 */
			foreach (lc, agg->args)
			{
				Node *n = (Node *) lfirst(lc);

				/* If TargetEntry, extract the expression from it */
				if (IsA(n, TargetEntry))
				{
					TargetEntry *tle = castNode(TargetEntry, n);

					n = (Node *) tle->expr;
				}

				if (!foreign_expr_walker(n, glob_cxt))
					return false;
			}

			/*
			 * For aggorder elements, check whether the sort operator, if
			 * specified, is shippable or not.
			 */
			if (agg->aggorder)
			{
				ListCell *lc;

				foreach (lc, agg->aggorder)
				{
					SortGroupClause *srt = lfirst_node(SortGroupClause, lc);
					Oid sortcoltype;
					TypeCacheEntry *typentry;
					TargetEntry *tle;

					tle = get_sortgroupref_tle(srt->tleSortGroupRef, agg->args);
					sortcoltype = exprType((Node *) tle->expr);
					typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
					/* Check shippability of non-default sort operator. */
					if (srt->sortop != typentry->lt_opr && srt->sortop != typentry->gt_opr &&
						!is_shippable(srt->sortop, OperatorRelationId, fpinfo))
						return false;
				}
			}

			/* Check aggregate filter */
			if (!foreign_expr_walker((Node *) agg->aggfilter, glob_cxt))
				return false;
		}
		break;
		default:

			/*
			 * If it's anything else, assume it's unsafe.  This list can be
			 * expanded later, but don't forget to add deparse support below.
			 */
			return false;
	}

	/*
	 * If result type of given expression is not shippable, it can't be sent
	 * to remote because it might have incompatible semantics on remote side.
	 */
	if (check_type && !is_shippable(exprType(node), TypeRelationId, fpinfo))
		return false;

	/* It looks OK */
	return true;
}

/*
 * Convert type OID + typmod info into a type name we can ship to the data
 * node.  Someplace else had better have verified that this type name is
 * expected to be known on the remote end.
 *
 * This is almost just format_type_with_typemod(), except that if left to its
 * own devices, that function will make schema-qualification decisions based
 * on the local search_path, which is wrong.  We must schema-qualify all
 * type names that are not in pg_catalog.  We assume here that built-in types
 * are all in pg_catalog and need not be qualified; otherwise, qualify.
 */
static char *
deparse_type_name(Oid type_oid, int32 typemod)
{
	bits16 flags = FORMAT_TYPE_TYPEMOD_GIVEN;

	if (!is_builtin(type_oid))
		flags |= FORMAT_TYPE_FORCE_QUALIFY;

	return format_type_extended(type_oid, typemod, flags);
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * data node for the given relation.  If foreignrel is an upper relation,
 * then the output targetlist can also contain expressions to be evaluated on
 * data node.
 */
List *
build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	List *tlist = NIL;
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(foreignrel);
	ListCell *lc;

	/*
	 * For an upper relation, we have already built the target list while
	 * checking shippability, so just return that.
	 */
	if (IS_UPPER_REL(foreignrel))
		return fpinfo->grouped_tlist;
	/*
	 * We require columns specified in foreignrel->reltarget->exprs and those
	 * required for evaluating the local conditions.
	 */

	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) foreignrel->reltarget->exprs,
											  PVC_RECURSE_PLACEHOLDERS));
	foreach (lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		tlist =
			add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) rinfo->clause, PVC_RECURSE_PLACEHOLDERS));
	}

	return tlist;
}

/*
 * Deparse SELECT statement for given relation into buf.
 *
 * tlist contains the list of desired columns to be fetched from data node.
 * For a base relation fpinfo->attrs_used is used to construct SELECT clause,
 * hence the tlist is ignored for a base relation.
 *
 * remote_where is the list of conditions to be deparsed into the WHERE clause,
 * and remote_having into the HAVING clause (this is useful for upper relations).
 *
 * If params_list is not NULL, it receives a list of Params and other-relation
 * Vars used in the clauses; these values must be transmitted to the data
 * node as parameter values.
 *
 * If params_list is NULL, we're generating the query for EXPLAIN purposes,
 * so Params and other-relation Vars should be replaced by dummy values.
 *
 * pathkeys is the list of pathkeys to order the result by.
 *
 * is_subquery is the flag to indicate whether to deparse the specified
 * relation as a subquery.
 *
 * List of columns selected is returned in retrieved_attrs.
 */
void
deparseSelectStmtForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, List *tlist,
						List *remote_where, List *remote_having, List *pathkeys, bool is_subquery,
						List **retrieved_attrs, List **params_list, DataNodeChunkAssignment *sca)
{
	deparse_expr_cxt context;
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);

	/*
	 * We handle relations for foreign tables, joins between those and upper
	 * relations.
	 */
	Assert(IS_JOIN_REL(rel) || IS_SIMPLE_REL(rel) || IS_UPPER_REL(rel));

	/* Fill portions of context common to upper, join and base relation */
	context.buf = buf;
	context.root = root;
	context.foreignrel = rel;
	context.scanrel = IS_UPPER_REL(rel) ? fpinfo->outerrel : rel;
	context.params_list = params_list;
	context.sca = sca;

	/* Construct SELECT clause */
	deparseSelectSql(tlist, is_subquery, retrieved_attrs, &context, pathkeys);

	/* Construct FROM and WHERE clauses */
	deparseFromExpr(remote_where, &context);

	if (IS_UPPER_REL(rel))
	{
		/* Append GROUP BY clause */
		appendGroupByClause(tlist, &context);

		/* Append HAVING clause */
		if (remote_having)
		{
			appendStringInfoString(buf, " HAVING ");
			appendConditions(remote_having, &context, true);
		}
	}

	/* Add ORDER BY clause if we found any useful pathkeys */
	if (pathkeys)
		appendOrderByClause(pathkeys, &context);

	/* Add LIMIT if it is set and can be pushed */
	if (context.root->limit_tuples > 0.0)
		appendLimit(&context, pathkeys);

	/* Add any necessary FOR UPDATE/SHARE. */
	deparseLockingClause(&context);
}

/*
 * Construct "SELECT DISTINCT target_list" or "SELECT DISTINCT ON (col1, col..)
 * target_list" statement to push down the DISTINCT clause to the remote side.
 *
 * We only allow references to basic "Vars" or constants in the DISTINCT exprs
 *
 * So, "SELECT DISTINCT col1" is fine but "SELECT DISTINCT 2*col1" is not.
 *
 * "SELECT DISTINCT col1, 'const1', NULL, col2" which is a mix of column
 * references and constants is also supported. Everything else is not supported.
 *
 * It should be noted that "SELECT DISTINCT col1, col2" will return the same
 * set of values as "SELECT DISTINCT col2, col1". So nothing additional needs
 * to be done here as the upper projection will take care of any ordering
 * between the attributes.
 *
 * We also explicitly deparse the distinctClause entries only for the
 * "DISTINCT ON (col..)" case. For regular DISTINCT the targetlist
 * deparsing which happens later is good enough
 */
static void
deparseDistinctClause(StringInfo buf, deparse_expr_cxt *context, List *pathkeys)
{
	PlannerInfo *root = context->root;
	Query *query = root->parse;
	ListCell *l, *dc_l;
	bool first = true, varno_assigned = false;
	Index varno = 0; /* mostly to quell compiler warning, handled via varno_assigned */
	RangeTblEntry *dc_rte;
	RangeTblEntry *rte;

	if (query->distinctClause == NIL)
		return;

	foreach (l, query->distinctClause)
	{
		SortGroupClause *sgc = lfirst_node(SortGroupClause, l);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

		/*
		 * We only send basic attributes to the remote side. So we can
		 * pushdown DISTINCT only if the tle is a simple one
		 * referring to the "Var" directly. Also all varno entries
		 * need to point to the same relid.
		 *
		 * Also handle "DISTINCT col1, CONST1, NULL" types cases
		 */
		if (IsA(tle->expr, Var))
		{
			Var *var = castNode(Var, tle->expr);

			if (first)
			{
				varno = var->varno;
				first = false;
				varno_assigned = true;
			}

			if (varno != (Index) var->varno)
				return;
		}
		/* We only allow constants apart from vars, but we ignore them */
		else if (!IsA(tle->expr, Const))
			return;
	}

	if (query->hasDistinctOn)
	{
		/*
		 * Pushing down DISTINCT ON is more complex than plain DISTINCT.
		 * The DISTINCT ON columns must be a prefix of the ORDER BY columns.
		 * Without this, the DISTINCT ON would return an unpredictable row
		 * each time. There is a diagnostic for the case where the ORDER BY
		 * clause doesn't match the DISTINCT ON clause, so in this case we
		 * would get an error on the data node. There is no diagnostic for
		 * the case where the ORDER BY is absent, so in this case we would
		 * get a wrong result.
		 * The remote ORDER BY clause is created from the pathkeys of the
		 * corresponding relation. If the DISTINCT ON columns are not a prefix
		 * of these pathkeys, we cannot push it down.
		 */
		ListCell *distinct_cell, *pathkey_cell;
		forboth (distinct_cell, query->distinctClause, pathkey_cell, pathkeys)
		{
			SortGroupClause *sgc = lfirst_node(SortGroupClause, distinct_cell);
			TargetEntry *tle = get_sortgroupclause_tle(sgc, query->targetList);

			PathKey *pk = lfirst_node(PathKey, pathkey_cell);
			EquivalenceClass *ec = pk->pk_eclass;

			/*
			 * The find_ec_member_matching_expr() has many checks that don't seem
			 * to be relevant here. Enumerate the pathkey EquivalenceMembers by
			 * hand and find the one that matches the DISTINCT ON expression.
			 */
			ListCell *ec_member_cell;
			foreach (ec_member_cell, ec->ec_members)
			{
				EquivalenceMember *ec_member = lfirst_node(EquivalenceMember, ec_member_cell);
				if (equal(ec_member->em_expr, tle->expr))
					break;
			}

			if (ec_member_cell == NULL)
			{
				/*
				 * Went through all the equivalence class members and didn't
				 * find a match.
				 */
				return;
			}
		}

		if (pathkey_cell == NULL && distinct_cell != NULL)
		{
			/* Ran out of pathkeys before we matched all the DISTINCT ON columns. */
			return;
		}
	}

	/* If there are no varno entries in the distinctClause, we are done */
	if (!varno_assigned)
		return;

	/*
	 * If all distinctClause entries point to our rte->relid then it's
	 * safe to push down to the datanode
	 *
	 * The only other case we allow is if the dc_rte->relid has the
	 * rte->relid as a child
	 */
	dc_rte = planner_rt_fetch(varno, root);
	rte = planner_rt_fetch(context->foreignrel->relid, root);

	if (dc_rte->relid != rte->relid && ts_inheritance_parent_relid(rte->relid) != dc_rte->relid)
		return;

	/*
	 * Ok to pushdown!
	 *
	 * The distinctClause entries will be referring to the
	 * varno pulled above, so adjust the scanrel temporarily
	 * for the deparsing of the distint clauses
	 *
	 * Note that we deparse the targetlist below only for the
	 * "DISTINCT ON" case. For DISTINCT, the regular targetlist
	 * deparsing later works
	 */
	if (query->hasDistinctOn)
	{
		char *sep = "";
		RelOptInfo *scanrel = context->scanrel;

		Assert(varno > 0 && varno < (Index) root->simple_rel_array_size);
		context->scanrel = root->simple_rel_array[varno];

		appendStringInfoString(buf, "DISTINCT ON (");

		foreach (dc_l, query->distinctClause)
		{
			SortGroupClause *srt = lfirst_node(SortGroupClause, dc_l);

			appendStringInfoString(buf, sep);
			deparseSortGroupClause(srt->tleSortGroupRef, query->targetList, false, context);
			sep = ", ";
		}

		appendStringInfoString(buf, ") ");

		/* reset scanrel to the earlier value now */
		context->scanrel = scanrel;
	}
	else
		appendStringInfoString(buf, "DISTINCT ");
}

/*
 * Construct a simple SELECT statement that retrieves desired columns
 * of the specified foreign table, and append it to "buf".  The output
 * contains just "SELECT ... ".
 *
 * We also create an integer List of the columns being retrieved, which is
 * returned to *retrieved_attrs, unless we deparse the specified relation
 * as a subquery.
 *
 * tlist is the list of desired columns.  is_subquery is the flag to
 * indicate whether to deparse the specified relation as a subquery.
 * Read prologue of deparseSelectStmtForRel() for details.
 */
static void
deparseSelectSql(List *tlist, bool is_subquery, List **retrieved_attrs, deparse_expr_cxt *context,
				 List *pathkeys)
{
	StringInfo buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	PlannerInfo *root = context->root;
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(foreignrel);

	/*
	 * Construct SELECT list
	 */
	appendStringInfoString(buf, "SELECT ");

	if (is_subquery)
	{
		/*
		 * For a relation that is deparsed as a subquery, emit expressions
		 * specified in the relation's reltarget.  Note that since this is for
		 * the subquery, no need to care about *retrieved_attrs.
		 */
		deparseSubqueryTargetList(context);
	}
	else if (tlist != NIL || IS_JOIN_REL(foreignrel))
	{
		/*
		 * For a join, hypertable-data node or upper relation the input tlist gives the list of
		 * columns required to be fetched from the data node.
		 */
		deparseExplicitTargetList(tlist, false, retrieved_attrs, context);
	}
	else
	{
		/*
		 * For a base relation fpinfo->attrs_used gives the list of columns
		 * required to be fetched from the data node.
		 */
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		Relation rel = table_open(rte->relid, NoLock);

		if (root->parse->distinctClause != NIL)
			deparseDistinctClause(buf, context, pathkeys);

		deparseTargetList(buf,
						  rte,
						  foreignrel->relid,
						  rel,
						  false,
						  fpinfo->attrs_used,
						  false,
						  retrieved_attrs);
		table_close(rel, NoLock);
	}
}

/*
 * Construct a FROM clause and, if needed, a WHERE clause, and append those to
 * "buf".
 *
 * quals is the list of clauses to be included in the WHERE clause.
 * (These may or may not include RestrictInfo decoration.)
 */
static void
deparseFromExpr(List *quals, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	RelOptInfo *scanrel = context->scanrel;
	/* Use alias if scan is on multiple rels, unless a per-data node scan */
	bool use_alias = column_qualification_needed(context);

	/* For upper relations, scanrel must be either a joinrel or a baserel */
	Assert(!IS_UPPER_REL(context->foreignrel) || IS_JOIN_REL(scanrel) || IS_SIMPLE_REL(scanrel));

	/* Construct FROM clause */
	appendStringInfoString(buf, " FROM ");
	deparseFromExprForRel(buf,
						  context->root,
						  scanrel,
						  use_alias,
						  (Index) 0,
						  NULL,
						  context->params_list,
						  context->sca);

	/* Construct WHERE clause */
	if (quals != NIL || context->sca != NULL)
		appendStringInfoString(buf, " WHERE ");

	if (context->sca != NULL)
		append_chunk_exclusion_condition(context, use_alias);

	if (quals != NIL)
		appendConditions(quals, context, (context->sca == NULL));
}

/*
 * Emit a target list that retrieves the columns specified in attrs_used.
 * This is used for both SELECT and RETURNING targetlists; the is_returning
 * parameter is true only for a RETURNING targetlist.
 *
 * The tlist text is appended to buf, and we also create an integer List
 * of the columns being retrieved, which is returned to *retrieved_attrs.
 *
 * If qualify_col is true, add relation alias before the column name.
 */
static void
deparseTargetList(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
				  bool is_returning, Bitmapset *attrs_used, bool qualify_col,
				  List **retrieved_attrs)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	bool have_wholerow;
	bool first;
	int i;

	*retrieved_attrs = NIL;

	/* If there's a whole-row reference, we'll need all the columns. */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber, attrs_used);

	first = true;
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* Ignore dropped attributes. */
		if (attr->attisdropped)
			continue;

		if (have_wholerow || bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			else if (is_returning)
				appendStringInfoString(buf, " RETURNING ");
			first = false;

			deparseColumnRef(buf, rtindex, i, rte, qualify_col);

			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/*
	 * Add ctid and oid if needed.  We currently don't support retrieving any
	 * other system columns.
	 */
	if (bms_is_member(SelfItemPointerAttributeNumber - FirstLowInvalidHeapAttributeNumber,
					  attrs_used))
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		else if (is_returning)
			appendStringInfoString(buf, " RETURNING ");
		first = false;

		if (qualify_col)
			ADD_REL_QUALIFIER(buf, rtindex);
		appendStringInfoString(buf, "ctid");

		*retrieved_attrs = lappend_int(*retrieved_attrs, SelfItemPointerAttributeNumber);
	}
	/* Don't generate bad syntax if no undropped columns */
	if (first && !is_returning)
		appendStringInfoString(buf, "NULL");
}

/*
 * Deparse the appropriate locking clause (FOR UPDATE or FOR SHARE) for a
 * given relation (context->scanrel).
 */
static void
deparseLockingClause(deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	PlannerInfo *root = context->root;
	RelOptInfo *rel = context->scanrel;
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);
	int relid = -1;

	while ((relid = bms_next_member(rel->relids, relid)) >= 0)
	{
		/*
		 * Ignore relation if it appears in a lower subquery.  Locking clause
		 * for such a relation is included in the subquery if necessary.
		 */
		if (bms_is_member(relid, fpinfo->lower_subquery_rels))
			continue;

		/*
		 * Add FOR UPDATE/SHARE if appropriate.  We apply locking during the
		 * initial row fetch, rather than later on as is done for local
		 * tables. The extra roundtrips involved in trying to duplicate the
		 * local semantics exactly don't seem worthwhile (see also comments
		 * for RowMarkType).
		 *
		 * Note: because we actually run the query as a cursor, this assumes
		 * that DECLARE CURSOR ... FOR UPDATE is supported, which it isn't
		 * before 8.3.
		 */
		if (relid == root->parse->resultRelation &&
			(root->parse->commandType == CMD_UPDATE || root->parse->commandType == CMD_DELETE))
		{
			/* Relation is UPDATE/DELETE target, so use FOR UPDATE */
			appendStringInfoString(buf, " FOR UPDATE");

			/* Add the relation alias if we are here for a join relation */
			if (IS_JOIN_REL(rel))
				appendStringInfo(buf, " OF %s%d", REL_ALIAS_PREFIX, relid);
		}
		else
		{
			PlanRowMark *rc = get_plan_rowmark(root->rowMarks, relid);

			if (rc)
			{
				/*
				 * Relation is specified as a FOR UPDATE/SHARE target, so
				 * handle that.  (But we could also see LCS_NONE, meaning this
				 * isn't a target relation after all.)
				 *
				 * For now, just ignore any [NO] KEY specification, since (a)
				 * it's not clear what that means for a remote table that we
				 * don't have complete information about, and (b) it wouldn't
				 * work anyway on older data nodes.  Likewise, we don't
				 * worry about NOWAIT.
				 */
				switch (rc->strength)
				{
					case LCS_NONE:
						/* No locking needed */
						break;
					case LCS_FORKEYSHARE:
					case LCS_FORSHARE:
						appendStringInfoString(buf, " FOR SHARE");
						break;
					case LCS_FORNOKEYUPDATE:
					case LCS_FORUPDATE:
						appendStringInfoString(buf, " FOR UPDATE");
						break;
				}

				/* Add the relation alias if we are here for a join relation */
				if (bms_membership(rel->relids) == BMS_MULTIPLE && rc->strength != LCS_NONE)
					appendStringInfo(buf, " OF %s%d", REL_ALIAS_PREFIX, relid);
			}
		}
	}
}

static void
append_chunk_exclusion_condition(deparse_expr_cxt *context, bool use_alias)
{
	StringInfo buf = context->buf;
	DataNodeChunkAssignment *sca = context->sca;
	RelOptInfo *scanrel = context->scanrel;
	ListCell *lc;
	bool first = true;

	appendStringInfoString(buf, FUNCTIONS_SCHEMA_NAME "." CHUNK_EXCL_FUNC_NAME "(");

	if (use_alias)
		appendStringInfo(buf, "%s%d, ", REL_ALIAS_PREFIX, scanrel->relid);
	else
	{
		/* use a qualfied relation name */
		RangeTblEntry *rte = planner_rt_fetch(scanrel->relid, context->root);
		Relation rel = table_open(rte->relid, NoLock);
		deparseRelation(buf, rel);
		table_close(rel, NoLock);
		/* We explicitly append expand operator `.*` to prevent
		 * confusing parser when using qualified name (otherwise parser believes that schema name is
		 * relation name) */
		appendStringInfoString(buf, ".*, ");
	}

	appendStringInfo(buf, "ARRAY[");
	foreach (lc, sca->remote_chunk_ids)
	{
		int remote_chunk_id = lfirst_int(lc);

		if (!first)
			appendStringInfo(buf, ", ");
		appendStringInfo(buf, "%d", remote_chunk_id);

		first = false;
	}
	appendStringInfo(buf, "])"); /* end array and function call */
}

/*
 * Deparse conditions from the provided list and append them to buf.
 *
 * The conditions in the list are assumed to be ANDed. This function is used to
 * deparse WHERE clauses, JOIN .. ON clauses and HAVING clauses.
 *
 * Depending on the caller, the list elements might be either RestrictInfos
 * or bare clauses.
 */
static void
appendConditions(List *exprs, deparse_expr_cxt *context, bool is_first)
{
	int nestlevel;
	ListCell *lc;
	StringInfo buf = context->buf;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = set_transmission_modes();

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *) lfirst(lc);

		/* Extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo))
			expr = ((RestrictInfo *) expr)->clause;

		/* Connect expressions with "AND" and parenthesize each condition. */
		if (!is_first)
			appendStringInfoString(buf, " AND ");

		appendStringInfoChar(buf, '(');
		deparseExpr(expr, context);
		appendStringInfoChar(buf, ')');

		is_first = false;
	}

	reset_transmission_modes(nestlevel);
}

/*
 * Deparse given targetlist and append it to context->buf.
 *
 * tlist is list of TargetEntry's which in turn contain Var nodes.
 *
 * retrieved_attrs is the list of continuously increasing integers starting
 * from 1. It has same number of entries as tlist.
 *
 * This is used for both SELECT and RETURNING targetlists; the is_returning
 * parameter is true only for a RETURNING targetlist.
 */
static void
deparseExplicitTargetList(List *tlist, bool is_returning, List **retrieved_attrs,
						  deparse_expr_cxt *context)
{
	ListCell *lc;
	StringInfo buf = context->buf;
	int i = 0;

	*retrieved_attrs = NIL;

	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (i > 0)
			appendStringInfoString(buf, ", ");
		else if (is_returning)
			appendStringInfoString(buf, " RETURNING ");

		deparseExpr((Expr *) tle->expr, context);

		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
		i++;
	}

	if (i == 0 && !is_returning)
		appendStringInfoString(buf, "NULL");
}

/*
 * Emit expressions specified in the given relation's reltarget.
 *
 * This is used for deparsing the given relation as a subquery.
 */
static void
deparseSubqueryTargetList(deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	RelOptInfo *foreignrel = context->foreignrel;
	bool first;
	ListCell *lc;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	first = true;
	foreach (lc, foreignrel->reltarget->exprs)
	{
		Node *node = (Node *) lfirst(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		deparseExpr((Expr *) node, context);
	}

	/* Don't generate bad syntax if no expressions */
	if (first)
		appendStringInfoString(buf, "NULL");
}
/* Output join name for given join type */
const char *
get_jointype_name(JoinType jointype)
{
	switch (jointype)
	{
		case JOIN_INNER:
			return "INNER";

		case JOIN_LEFT:
			return "LEFT";

		case JOIN_RIGHT:
			return "RIGHT";

		case JOIN_FULL:
			return "FULL";

		default:
			/* Shouldn't come here, but protect from buggy code. */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Keep compiler happy */
	return NULL;
}

/*
 * Construct FROM clause for given relation
 *
 * The function constructs ... JOIN ... ON ... for join relation. For a base
 * relation it just returns schema-qualified tablename, with the appropriate
 * alias if so requested.
 *
 * 'ignore_rel' is either zero or the RT index of a target relation.  In the
 * latter case the function constructs FROM clause of UPDATE or USING clause
 * of DELETE; it deparses the join relation as if the relation never contained
 * the target relation, and creates a List of conditions to be deparsed into
 * the top-level WHERE clause, which is returned to *ignore_conds.
 */
static void
deparseFromExprForRel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel, bool use_alias,
					  Index ignore_rel, List **ignore_conds, List **params_list,
					  DataNodeChunkAssignment *sca)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(foreignrel);

	if (IS_JOIN_REL(foreignrel))
	{
		StringInfoData join_sql_o;
		StringInfoData join_sql_i;
		RelOptInfo *outerrel = fpinfo->outerrel;
		RelOptInfo *innerrel = fpinfo->innerrel;
		bool outerrel_is_target = false;
		bool innerrel_is_target = false;

		if (ignore_rel > 0 && bms_is_member(ignore_rel, foreignrel->relids))
		{
			/*
			 * If this is an inner join, add joinclauses to *ignore_conds and
			 * set it to empty so that those can be deparsed into the WHERE
			 * clause.  Note that since the target relation can never be
			 * within the nullable side of an outer join, those could safely
			 * be pulled up into the WHERE clause (see foreign_join_ok()).
			 * Note also that since the target relation is only inner-joined
			 * to any other relation in the query, all conditions in the join
			 * tree mentioning the target relation could be deparsed into the
			 * WHERE clause by doing this recursively.
			 */
			if (fpinfo->jointype == JOIN_INNER)
			{
				*ignore_conds = list_concat(*ignore_conds, fpinfo->joinclauses);
				fpinfo->joinclauses = NIL;
			}

			/*
			 * Check if either of the input relations is the target relation.
			 */
			if (outerrel->relid == ignore_rel)
				outerrel_is_target = true;
			else if (innerrel->relid == ignore_rel)
				innerrel_is_target = true;
		}

		/* Deparse outer relation if not the target relation. */
		if (!outerrel_is_target)
		{
			initStringInfo(&join_sql_o);
			deparseRangeTblRef(&join_sql_o,
							   root,
							   outerrel,
							   fpinfo->make_outerrel_subquery,
							   ignore_rel,
							   ignore_conds,
							   params_list,
							   sca);

			/*
			 * If inner relation is the target relation, skip deparsing it.
			 * Note that since the join of the target relation with any other
			 * relation in the query is an inner join and can never be within
			 * the nullable side of an outer join, the join could be
			 * interchanged with higher-level joins (cf. identity 1 on outer
			 * join reordering shown in src/backend/optimizer/README), which
			 * means it's safe to skip the target-relation deparsing here.
			 */
			if (innerrel_is_target)
			{
				Assert(fpinfo->jointype == JOIN_INNER);
				Assert(fpinfo->joinclauses == NIL);
				appendBinaryStringInfo(buf, join_sql_o.data, join_sql_o.len);
				return;
			}
		}

		/* Deparse inner relation if not the target relation. */
		if (!innerrel_is_target)
		{
			initStringInfo(&join_sql_i);
			deparseRangeTblRef(&join_sql_i,
							   root,
							   innerrel,
							   fpinfo->make_innerrel_subquery,
							   ignore_rel,
							   ignore_conds,
							   params_list,
							   sca);

			/*
			 * If outer relation is the target relation, skip deparsing it.
			 * See the above note about safety.
			 */
			if (outerrel_is_target)
			{
				Assert(fpinfo->jointype == JOIN_INNER);
				Assert(fpinfo->joinclauses == NIL);
				appendBinaryStringInfo(buf, join_sql_i.data, join_sql_i.len);
				return;
			}
		}

		/* Neither of the relations is the target relation. */
		Assert(!outerrel_is_target && !innerrel_is_target);

		/*
		 * For a join relation FROM clause entry is deparsed as
		 *
		 * ((outer relation) <join type> (inner relation) ON (joinclauses))
		 */
		appendStringInfo(buf,
						 "(%s %s JOIN %s ON ",
						 join_sql_o.data,
						 get_jointype_name(fpinfo->jointype),
						 join_sql_i.data);

		/* Append join clause; (TRUE) if no join clause */
		if (fpinfo->joinclauses)
		{
			deparse_expr_cxt context;

			context.buf = buf;
			context.foreignrel = foreignrel;
			context.scanrel = foreignrel;
			context.root = root;
			context.params_list = params_list;

			appendStringInfoChar(buf, '(');
			appendConditions(fpinfo->joinclauses, &context, true);
			appendStringInfoChar(buf, ')');
		}
		else
			appendStringInfoString(buf, "(TRUE)");

		/* End the FROM clause entry. */
		appendStringInfoChar(buf, ')');
	}
	else
	{
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		/*
		 * Core code already has some lock on each rel being planned, so we
		 * can use NoLock here.
		 */
		Relation rel = table_open(rte->relid, NoLock);

		deparseRelation(buf, rel);

		/*
		 * Add a unique alias to avoid any conflict in relation names due to
		 * pulled up subqueries in the query being built for a pushed down
		 * join.
		 */
		if (use_alias)
			appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);

		table_close(rel, NoLock);
	}
}

/*
 * Append FROM clause entry for the given relation into buf.
 */
static void
deparseRangeTblRef(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel, bool make_subquery,
				   Index ignore_rel, List **ignore_conds, List **params_list,
				   DataNodeChunkAssignment *sca)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(foreignrel);

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	Assert(fpinfo->local_conds == NIL);

	/* If make_subquery is true, deparse the relation as a subquery. */
	if (make_subquery)
	{
		List *retrieved_attrs;
		int ncols;

		/*
		 * The given relation shouldn't contain the target relation, because
		 * this should only happen for input relations for a full join, and
		 * such relations can never contain an UPDATE/DELETE target.
		 */
		Assert(ignore_rel == 0 || !bms_is_member(ignore_rel, foreignrel->relids));

		/* Deparse the subquery representing the relation. */
		appendStringInfoChar(buf, '(');
		deparseSelectStmtForRel(buf,
								root,
								foreignrel,
								NIL,
								fpinfo->remote_conds,
								NIL /* remote_having */,
								NIL /* pathkeys */,
								true /* is_subquery */,
								&retrieved_attrs,
								params_list,
								sca);
		appendStringInfoChar(buf, ')');

		/* Append the relation alias. */
		appendStringInfo(buf, " %s%d", SUBQUERY_REL_ALIAS_PREFIX, fpinfo->relation_index);

		/*
		 * Append the column aliases if needed.  Note that the subquery emits
		 * expressions specified in the relation's reltarget (see
		 * deparseSubqueryTargetList).
		 */
		ncols = list_length(foreignrel->reltarget->exprs);
		if (ncols > 0)
		{
			int i;

			appendStringInfoChar(buf, '(');
			for (i = 1; i <= ncols; i++)
			{
				if (i > 1)
					appendStringInfoString(buf, ", ");

				appendStringInfo(buf, "%s%d", SUBQUERY_COL_ALIAS_PREFIX, i);
			}
			appendStringInfoChar(buf, ')');
		}
	}
	else
		deparseFromExprForRel(buf,
							  root,
							  foreignrel,
							  true,
							  ignore_rel,
							  ignore_conds,
							  params_list,
							  sca);
}

/*
 * deparse remote INSERT statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
deparse_insert_stmt(DeparsedInsertStmt *stmt, RangeTblEntry *rte, Index rtindex, Relation rel,
					List *target_attrs, bool do_nothing, List *returning_list)
{
	bool first;
	ListCell *lc;
	StringInfoData buf;

	memset(stmt, 0, sizeof(DeparsedInsertStmt));
	initStringInfo(&buf);

	appendStringInfoString(&buf, "INSERT INTO ");
	deparseRelation(&buf, rel);

	stmt->target = buf.data;
	stmt->num_target_attrs = list_length(target_attrs);

	initStringInfo(&buf);

	if (target_attrs != NIL)
	{
		appendStringInfoChar(&buf, '(');

		first = true;
		foreach (lc, target_attrs)
		{
			int attnum = lfirst_int(lc);

			if (!first)
				appendStringInfoString(&buf, ", ");
			first = false;

			deparseColumnRef(&buf, rtindex, attnum, rte, false);
		}

		appendStringInfoString(&buf, ") VALUES ");

		stmt->target_attrs = buf.data;

		initStringInfo(&buf);
	}

	stmt->do_nothing = do_nothing;

	deparseReturningList(&buf,
						 rte,
						 rtindex,
						 rel,
						 rel->trigdesc && rel->trigdesc->trig_insert_after_row,
						 returning_list,
						 &stmt->retrieved_attrs);

	if (stmt->retrieved_attrs == NIL)
		stmt->returning = NULL;
	else
		stmt->returning = buf.data;
}

static int
append_values_params(DeparsedInsertStmt *stmt, StringInfo buf, int pindex)
{
	bool first = true;

	appendStringInfoChar(buf, '(');

	for (unsigned int i = 0; i < stmt->num_target_attrs; i++)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		else
			first = false;

		appendStringInfo(buf, "$%d", pindex);
		pindex++;
	}

	appendStringInfoChar(buf, ')');

	return pindex;
}

static const char *
deparsed_insert_stmt_get_sql_internal(DeparsedInsertStmt *stmt, StringInfo buf, int64 num_rows,
									  bool abbrev)
{
	appendStringInfoString(buf, stmt->target);

	if (stmt->num_target_attrs > 0)
	{
		appendStringInfoString(buf, stmt->target_attrs);

		if (abbrev)
		{
			append_values_params(stmt, buf, 1);

			if (num_rows > 1)
			{
				appendStringInfo(buf, ", ..., ");
				append_values_params(stmt,
									 buf,
									 (stmt->num_target_attrs * num_rows) - stmt->num_target_attrs +
										 1);
			}
		}
		else
		{
			int pindex = 1;
			int64 i;

			for (i = 0; i < num_rows; i++)
			{
				pindex = append_values_params(stmt, buf, pindex);

				if (i < (num_rows - 1))
					appendStringInfoString(buf, ", ");
			}
		}
	}
	else
		appendStringInfoString(buf, " DEFAULT VALUES");

	if (stmt->do_nothing)
		appendStringInfoString(buf, " ON CONFLICT DO NOTHING");

	if (NULL != stmt->returning)
		appendStringInfoString(buf, stmt->returning);

	return buf->data;
}

const char *
deparsed_insert_stmt_get_sql(DeparsedInsertStmt *stmt, int64 num_rows)
{
	StringInfoData buf;

	initStringInfo(&buf);

	return deparsed_insert_stmt_get_sql_internal(stmt, &buf, num_rows, false);
}

const char *
deparsed_insert_stmt_get_sql_explain(DeparsedInsertStmt *stmt, int64 num_rows)
{
	StringInfoData buf;

	initStringInfo(&buf);

	return deparsed_insert_stmt_get_sql_internal(stmt, &buf, num_rows, true);
}

enum DeparsedInsertStmtIndex
{
	DeparsedInsertStmtTarget,
	DeparsedInsertStmtNumTargetAttrs,
	DeparsedInsertStmtTargetAttrs,
	DeparsedInsertStmtDoNothing,
	DeparsedInsertStmtRetrievedAttrs,
	DeparsedInsertStmtReturning,
};

List *
deparsed_insert_stmt_to_list(DeparsedInsertStmt *stmt)
{
	List *stmt_list =
		list_make5(makeString(pstrdup(stmt->target)),
				   makeInteger(stmt->num_target_attrs),
				   makeString(stmt->target_attrs != NULL ? pstrdup(stmt->target_attrs) : ""),
				   makeInteger(stmt->do_nothing ? 1 : 0),
				   stmt->retrieved_attrs);

	if (NULL != stmt->returning)
		stmt_list = lappend(stmt_list, makeString(pstrdup(stmt->returning)));

	return stmt_list;
}

void
deparsed_insert_stmt_from_list(DeparsedInsertStmt *stmt, List *list_stmt)
{
	stmt->target = strVal(list_nth(list_stmt, DeparsedInsertStmtTarget));
	stmt->num_target_attrs = intVal(list_nth(list_stmt, DeparsedInsertStmtNumTargetAttrs));
	stmt->target_attrs = (stmt->num_target_attrs > 0) ?
							 strVal(list_nth(list_stmt, DeparsedInsertStmtTargetAttrs)) :
							 NULL;
	stmt->do_nothing = intVal(list_nth(list_stmt, DeparsedInsertStmtDoNothing));
	stmt->retrieved_attrs = list_nth(list_stmt, DeparsedInsertStmtRetrievedAttrs);

	if (list_length(list_stmt) > DeparsedInsertStmtReturning)
	{
		Assert(stmt->retrieved_attrs != NIL);
		stmt->returning = strVal(list_nth(list_stmt, DeparsedInsertStmtReturning));
	}
	else
		stmt->returning = NULL;
}

void
deparseInsertSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel, List *targetAttrs,
				 int64 num_rows, bool doNothing, List *returningList, List **retrieved_attrs)
{
	DeparsedInsertStmt stmt;

	deparse_insert_stmt(&stmt, rte, rtindex, rel, targetAttrs, doNothing, returningList);

	deparsed_insert_stmt_get_sql_internal(&stmt, buf, num_rows, false);

	if (NULL != retrieved_attrs)
		*retrieved_attrs = stmt.retrieved_attrs;
}

/*
 * deparse remote UPDATE statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
deparseUpdateSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel, List *targetAttrs,
				 List *returningList, List **retrieved_attrs)
{
	AttrNumber pindex;
	bool first;
	ListCell *lc;

	appendStringInfoString(buf, "UPDATE ");
	deparseRelation(buf, rel);
	appendStringInfoString(buf, " SET ");

	pindex = 2; /* ctid is always the first param */
	first = true;
	foreach (lc, targetAttrs)
	{
		int attnum = lfirst_int(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		deparseColumnRef(buf, rtindex, attnum, rte, false);
		appendStringInfo(buf, " = $%d", pindex);
		pindex++;
	}
	appendStringInfoString(buf, " WHERE ctid = $1");

	deparseReturningList(buf,
						 rte,
						 rtindex,
						 rel,
						 rel->trigdesc && rel->trigdesc->trig_update_after_row,
						 returningList,
						 retrieved_attrs);
}

/*
 * deparse remote DELETE statement
 *
 * The statement text is appended to buf, and we also create an integer List
 * of the columns being retrieved by RETURNING (if any), which is returned
 * to *retrieved_attrs.
 */
void
deparseDeleteSql(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
				 List *returningList, List **retrieved_attrs)
{
	appendStringInfoString(buf, "DELETE FROM ");
	deparseRelation(buf, rel);
	appendStringInfoString(buf, " WHERE ctid = $1");

	deparseReturningList(buf,
						 rte,
						 rtindex,
						 rel,
						 rel->trigdesc && rel->trigdesc->trig_delete_after_row,
						 returningList,
						 retrieved_attrs);
}

/*
 * Add a RETURNING clause, if needed, to an INSERT/UPDATE/DELETE.
 */
static void
deparseReturningList(StringInfo buf, RangeTblEntry *rte, Index rtindex, Relation rel,
					 bool trig_after_row, List *returningList, List **retrieved_attrs)
{
	Bitmapset *attrs_used = NULL;

	/* We currently do not handle triggers (trig_after_row == TRUE) given that
	 * we know these triggers should exist also on data nodes and
	 * can/should be executed there. The only reason to handle triggers on the
	 * frontend is to (1) more efficiently handle BEFORE triggers (executing
	 * them on the frontend before sending tuples), or (2) have triggers that
	 * do not exist on data nodes.
	 *
	 * Note that, for a hypertable, trig_after_row is always true because of
	 * the insert blocker trigger.
	 */

	if (returningList != NIL)
	{
		/*
		 * We need the attrs, non-system and system, mentioned in the local
		 * query's RETURNING list.
		 */
		pull_varattnos((Node *) returningList, rtindex, &attrs_used);
	}

	if (attrs_used != NULL)
		deparseTargetList(buf, rte, rtindex, rel, true, attrs_used, false, retrieved_attrs);
	else
		*retrieved_attrs = NIL;
}

/*
 * Construct SELECT statement to acquire size in blocks of given relation.
 *
 * Note: we use local definition of block size, not remote definition.
 * This is perhaps debatable.
 *
 * Note: pg_relation_size() exists in 8.1 and later.
 */
void
deparseAnalyzeSizeSql(StringInfo buf, Relation rel)
{
	StringInfoData relname;

	/* We'll need the remote relation name as a literal. */
	initStringInfo(&relname);
	deparseRelation(&relname, rel);

	appendStringInfoString(buf, "SELECT pg_catalog.pg_relation_size(");
	deparseStringLiteral(buf, relname.data);
	appendStringInfo(buf, "::pg_catalog.regclass) / %d", BLCKSZ);
}

/*
 * Construct SELECT statement to acquire sample rows of given relation.
 *
 * SELECT command is appended to buf, and list of columns retrieved
 * is returned to *retrieved_attrs.
 */
void
deparseAnalyzeSql(StringInfo buf, Relation rel, List **retrieved_attrs)
{
	Oid relid = RelationGetRelid(rel);
	TupleDesc tupdesc = RelationGetDescr(rel);
	int i;
	char *colname;
	List *options;
	ListCell *lc;
	bool first = true;

	*retrieved_attrs = NIL;

	appendStringInfoString(buf, "SELECT ");
	for (i = 0; i < tupdesc->natts; i++)
	{
		/* Ignore dropped columns. */
		if (TupleDescAttr(tupdesc, i)->attisdropped)
			continue;

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		/* Use attribute name or column_name option. */
		colname = NameStr(TupleDescAttr(tupdesc, i)->attname);
		options = GetForeignColumnOptions(relid, i + 1);

		foreach (lc, options)
		{
			DefElem *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "column_name") == 0)
			{
				colname = defGetString(def);
				break;
			}
		}

		appendStringInfoString(buf, quote_identifier(colname));

		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
	}

	/* Don't generate bad syntax for zero-column relation. */
	if (first)
		appendStringInfoString(buf, "NULL");

	/*
	 * Construct FROM clause
	 */
	appendStringInfoString(buf, " FROM ");
	deparseRelation(buf, rel);
}

/*
 * Construct name to use for given column, and emit it into buf.
 * If it has a column_name FDW option, use that instead of attribute name.
 *
 * If qualify_col is true, qualify column name with the alias of relation.
 */
static void
deparseColumnRef(StringInfo buf, int varno, int varattno, RangeTblEntry *rte, bool qualify_col)
{
	/* We support fetching the remote side's CTID and OID. */
	if (varattno == SelfItemPointerAttributeNumber)
	{
		if (qualify_col)
			ADD_REL_QUALIFIER(buf, varno);
		appendStringInfoString(buf, "ctid");
	}
	else if (varattno < 0)
	{
		/*
		 * All other system attributes are fetched as 0, except for table OID,
		 * which is fetched as the local table OID.  However, we must be
		 * careful; the table could be beneath an outer join, in which case it
		 * must go to NULL whenever the rest of the row does.
		 */
		Oid fetchval = 0;

		if (varattno == TableOidAttributeNumber)
			fetchval = rte->relid;

		if (qualify_col)
		{
			appendStringInfoString(buf, "CASE WHEN (");
			ADD_REL_QUALIFIER(buf, varno);
			appendStringInfo(buf, "*)::text IS NOT NULL THEN %u END", fetchval);
		}
		else
			appendStringInfo(buf, "%u", fetchval);
	}
	else if (varattno == 0)
	{
		/* Whole row reference */
		Relation rel;
		Bitmapset *attrs_used;

		/* Required only to be passed down to deparseTargetList(). */
		List *retrieved_attrs;

		/*
		 * The lock on the relation will be held by upper callers, so it's
		 * fine to open it with no lock here.
		 */
		rel = table_open(rte->relid, NoLock);

		/*
		 * The local name of the foreign table can not be recognized by the
		 * data node and the table it references on data node might
		 * have different column ordering or different columns than those
		 * declared locally. Hence we have to deparse whole-row reference as
		 * ROW(columns referenced locally). Construct this by deparsing a
		 * "whole row" attribute.
		 */
		attrs_used = bms_add_member(NULL, 0 - FirstLowInvalidHeapAttributeNumber);

		/*
		 * In case the whole-row reference is under an outer join then it has
		 * to go NULL whenever the rest of the row goes NULL. Deparsing a join
		 * query would always involve multiple relations, thus qualify_col
		 * would be true.
		 */
		if (qualify_col)
		{
			appendStringInfoString(buf, "CASE WHEN (");
			ADD_REL_QUALIFIER(buf, varno);
			appendStringInfoString(buf, "*)::text IS NOT NULL THEN ");
		}

		appendStringInfoString(buf, "ROW(");
		deparseTargetList(buf, rte, varno, rel, false, attrs_used, qualify_col, &retrieved_attrs);
		appendStringInfoChar(buf, ')');

		/* Complete the CASE WHEN statement started above. */
		if (qualify_col)
			appendStringInfoString(buf, " END");

		table_close(rel, NoLock);
		bms_free(attrs_used);
	}
	else
	{
		char *colname = NULL;
		List *options;
		ListCell *lc;

		/* varno must not be any of OUTER_VAR, INNER_VAR and INDEX_VAR. */
		Assert(!IS_SPECIAL_VARNO(varno));

		/*
		 * If it's a column of a foreign table, and it has the column_name FDW
		 * option, use that value.
		 */
		options = GetForeignColumnOptions(rte->relid, varattno);
		foreach (lc, options)
		{
			DefElem *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "column_name") == 0)
			{
				colname = defGetString(def);
				break;
			}
		}

		/*
		 * If it's a column of a regular table or it doesn't have column_name
		 * FDW option, use attribute name.
		 */
		if (colname == NULL)
			colname = get_attname(rte->relid, varattno, false);

		if (qualify_col)
			ADD_REL_QUALIFIER(buf, varno);

		appendStringInfoString(buf, quote_identifier(colname));
	}
}

/*
 * Append name of table being queried.
 *
 * Note, we enforce that table names are the same across nodes.
 */
static void
deparseRelation(StringInfo buf, Relation rel)
{
	const char *nspname;
	const char *relname;

	Assert(rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE ||
		   rel->rd_rel->relkind == RELKIND_RELATION);

	nspname = get_namespace_name(RelationGetNamespace(rel));
	relname = RelationGetRelationName(rel);

	appendStringInfo(buf, "%s.%s", quote_identifier(nspname), quote_identifier(relname));
}

/*
 * Append a SQL string literal representing "val" to buf.
 */
void
deparseStringLiteral(StringInfo buf, const char *val)
{
	const char *valptr;

	/*
	 * Rather than making assumptions about the data node's value of
	 * standard_conforming_strings, always use E'foo' syntax if there are any
	 * backslashes.  This will fail on data nodes before 8.1, but those
	 * are long out of support.
	 */
	if (strchr(val, '\\') != NULL)
		appendStringInfoChar(buf, ESCAPE_STRING_SYNTAX);
	appendStringInfoChar(buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch);
		appendStringInfoChar(buf, ch);
	}
	appendStringInfoChar(buf, '\'');
}

/*
 * Deparse given expression into context->buf.
 *
 * This function must support all the same node types that foreign_expr_walker
 * accepts.
 *
 * Note: unlike ruleutils.c, we just use a simple hard-wired parenthesization
 * scheme: anything more complex than a Var, Const, function call or cast
 * should be self-parenthesized.
 */
static void
deparseExpr(Expr *node, deparse_expr_cxt *context)
{
	if (node == NULL)
		return;

	switch (nodeTag(node))
	{
		case T_Var:
			deparseVar(castNode(Var, node), context);
			break;
		case T_Const:
			deparseConst(castNode(Const, node), context, 0);
			break;
		case T_Param:
			deparseParam(castNode(Param, node), context);
			break;
		case T_SubscriptingRef:
			deparseSubscriptingRef(castNode(SubscriptingRef, node), context);
			break;
		case T_FuncExpr:
			deparseFuncExpr(castNode(FuncExpr, node), context);
			break;
		case T_OpExpr:
			deparseOpExpr(castNode(OpExpr, node), context);
			break;
		case T_DistinctExpr:
			deparseDistinctExpr(castNode(DistinctExpr, node), context);
			break;
		case T_ScalarArrayOpExpr:
			deparseScalarArrayOpExpr(castNode(ScalarArrayOpExpr, node), context);
			break;
		case T_RelabelType:
			deparseRelabelType(castNode(RelabelType, node), context);
			break;
		case T_BoolExpr:
			deparseBoolExpr(castNode(BoolExpr, node), context);
			break;
		case T_NullTest:
			deparseNullTest(castNode(NullTest, node), context);
			break;
		case T_ArrayExpr:
			deparseArrayExpr(castNode(ArrayExpr, node), context);
			break;
		case T_Aggref:
			deparseAggref(castNode(Aggref, node), context);
			break;
		default:
			elog(ERROR, "unsupported expression type for deparse: %d", (int) nodeTag(node));
			break;
	}
}

/*
 * Is it requeired to qualify the column name (e.g., multiple
 * tables are part of the query).
 */
static bool
column_qualification_needed(deparse_expr_cxt *context)
{
	Relids relids = context->scanrel->relids;

	if (bms_membership(relids) == BMS_MULTIPLE)
	{
		if (IS_JOIN_REL(context->scanrel))
			return true;
		else if (context->sca == NULL)
			return true;
	}

	return false;
}

/*
 * Deparse given Var node into context->buf.
 *
 * If the Var belongs to the foreign relation, just print its remote name.
 * Otherwise, it's effectively a Param (and will in fact be a Param at
 * run time).  Handle it the same way we handle plain Params --- see
 * deparseParam for comments.
 */
static void
deparseVar(Var *node, deparse_expr_cxt *context)
{
	Relids relids = context->scanrel->relids;
	int relno;
	int colno;

	/* Qualify columns when multiple relations are involved, unless it is a
	 * per-data node scan or a join. */
	bool qualify_col = column_qualification_needed(context);

	/*
	 * If the Var belongs to the foreign relation that is deparsed as a
	 * subquery, use the relation and column alias to the Var provided by the
	 * subquery, instead of the remote name.
	 */
	if (is_subquery_var(node, context->scanrel, &relno, &colno))
	{
		appendStringInfo(context->buf,
						 "%s%d.%s%d",
						 SUBQUERY_REL_ALIAS_PREFIX,
						 relno,
						 SUBQUERY_COL_ALIAS_PREFIX,
						 colno);
		return;
	}

	if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
		deparseColumnRef(context->buf,
						 node->varno,
						 node->varattno,
						 planner_rt_fetch(node->varno, context->root),
						 qualify_col);
	else
	{
		/* Treat like a Param */
		if (context->params_list)
		{
			int pindex = 0;
			ListCell *lc;

			/* find its index in params_list */
			foreach (lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *) lfirst(lc)))
					break;
			}
			if (lc == NULL)
			{
				/* not in list, so add it */
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}

			printRemoteParam(pindex, node->vartype, node->vartypmod, context);
		}
		else
		{
			printRemotePlaceholder(node->vartype, node->vartypmod, context);
		}
	}
}

/*
 * Deparse given constant value into context->buf.
 *
 * This function has to be kept in sync with ruleutils.c's get_const_expr.
 * As for that function, showtype can be -1 to never show "::typename" decoration,
 * or +1 to always show it, or 0 to show it only if the constant wouldn't be assumed
 * to be the right type by default.
 */
static void
deparseConst(Const *node, deparse_expr_cxt *context, int showtype)
{
	StringInfo buf = context->buf;
	Oid typoutput;
	bool typIsVarlena;
	char *extval;
	bool isfloat = false;
	bool needlabel;

	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		if (showtype >= 0)
			appendStringInfo(buf, "::%s", deparse_type_name(node->consttype, node->consttypmod));
		return;
	}

	getTypeOutputInfo(node->consttype, &typoutput, &typIsVarlena);
	extval = OidOutputFunctionCall(typoutput, node->constvalue);

	switch (node->consttype)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
		{
			/*
			 * No need to quote unless it's a special value such as 'NaN'.
			 * See comments in get_const_expr().
			 */
			if (strspn(extval, "0123456789+-eE.") == strlen(extval))
			{
				if (extval[0] == '+' || extval[0] == '-')
					appendStringInfo(buf, "(%s)", extval);
				else
					appendStringInfoString(buf, extval);
				if (strcspn(extval, "eE.") != strlen(extval))
					isfloat = true; /* it looks like a float */
			}
			else
				appendStringInfo(buf, "'%s'", extval);
		}
		break;
		case BITOID:
		case VARBITOID:
			appendStringInfo(buf, "B'%s'", extval);
			break;
		case BOOLOID:
			if (strcmp(extval, "t") == 0)
				appendStringInfoString(buf, "true");
			else
				appendStringInfoString(buf, "false");
			break;
		default:
			deparseStringLiteral(buf, extval);
			break;
	}

	pfree(extval);

	if (showtype < 0)
		return;

	/*
	 * For showtype == 0, append ::typename unless the constant will be
	 * implicitly typed as the right type when it is read in.
	 *
	 * XXX this code has to be kept in sync with the behavior of the parser,
	 * especially make_const.
	 */
	switch (node->consttype)
	{
		case BOOLOID:
		case INT4OID:
		case UNKNOWNOID:
			needlabel = false;
			break;
		case NUMERICOID:
			needlabel = !isfloat || (node->consttypmod >= 0);
			break;
		default:
			needlabel = true;
			break;
	}
	if (needlabel || showtype > 0)
		appendStringInfo(buf, "::%s", deparse_type_name(node->consttype, node->consttypmod));
}

/*
 * Deparse given Param node.
 *
 * If we're generating the query "for real", add the Param to
 * context->params_list if it's not already present, and then use its index
 * in that list as the remote parameter number.  During EXPLAIN, there's
 * no need to identify a parameter number.
 */
static void
deparseParam(Param *node, deparse_expr_cxt *context)
{
	if (context->params_list)
	{
		int pindex = 0;
		ListCell *lc;

		/* find its index in params_list */
		foreach (lc, *context->params_list)
		{
			pindex++;
			if (equal(node, (Node *) lfirst(lc)))
				break;
		}
		if (lc == NULL)
		{
			/* not in list, so add it */
			pindex++;
			*context->params_list = lappend(*context->params_list, node);
		}

		printRemoteParam(pindex, node->paramtype, node->paramtypmod, context);
	}
	else
	{
		printRemotePlaceholder(node->paramtype, node->paramtypmod, context);
	}
}

/*
 * Deparse a subscripting expression.
 */
static void
deparseSubscriptingRef(SubscriptingRef *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	ListCell *lowlist_item;
	ListCell *uplist_item;

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/*
	 * Deparse referenced container expression first.  If that expression includes
	 * a cast, we have to parenthesize to prevent the array subscript from
	 * being taken as typename decoration.  We can avoid that in the typical
	 * case of subscripting a Var, but otherwise do it.
	 */
	if (IsA(node->refexpr, Var))
		deparseExpr(node->refexpr, context);
	else
	{
		appendStringInfoChar(buf, '(');
		deparseExpr(node->refexpr, context);
		appendStringInfoChar(buf, ')');
	}

	/* Deparse subscript expressions. */
	lowlist_item = list_head(node->reflowerindexpr); /* could be NULL */
	foreach (uplist_item, node->refupperindexpr)
	{
		appendStringInfoChar(buf, '[');
		if (lowlist_item)
		{
			deparseExpr(lfirst(lowlist_item), context);
			appendStringInfoChar(buf, ':');
			lowlist_item = lnext(node->reflowerindexpr, lowlist_item);
		}
		deparseExpr(lfirst(uplist_item), context);
		appendStringInfoChar(buf, ']');
	}

	appendStringInfoChar(buf, ')');
}

/*
 * Deparse a function call.
 */
static void
deparseFuncExpr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	bool use_variadic;
	bool first;
	ListCell *arg;

	/*
	 * If the function call came from an implicit coercion, then just show the
	 * first argument.
	 */
	if (node->funcformat == COERCE_IMPLICIT_CAST)
	{
		deparseExpr((Expr *) linitial(node->args), context);
		return;
	}

	/*
	 * If the function call came from a cast, then show the first argument
	 * plus an explicit cast operation.
	 */
	if (node->funcformat == COERCE_EXPLICIT_CAST)
	{
		Oid rettype = node->funcresulttype;
		int32 coercedTypmod;

		/* Get the typmod if this is a length-coercion function */
		(void) exprIsLengthCoercion((Node *) node, &coercedTypmod);

		deparseExpr((Expr *) linitial(node->args), context);
		appendStringInfo(buf, "::%s", deparse_type_name(rettype, coercedTypmod));
		return;
	}

	/* Check if need to print VARIADIC (cf. ruleutils.c) */
	use_variadic = node->funcvariadic;

	/*
	 * Normal function: display as proname(args).
	 */
	appendFunctionName(node->funcid, context);
	appendStringInfoChar(buf, '(');

	/* ... and all the arguments */
	first = true;
	foreach (arg, node->args)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		if (use_variadic && lnext(node->args, arg) == NULL)
			appendStringInfoString(buf, "VARIADIC ");
		deparseExpr((Expr *) lfirst(arg), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given operator expression.   To avoid problems around
 * priority of operations, we always parenthesize the arguments.
 */
static void
deparseOpExpr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	HeapTuple tuple;
	Form_pg_operator form;
	char oprkind;
	ListCell *arg;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);
	oprkind = form->oprkind;

	/* Sanity check. */
	Assert((oprkind == 'r' && list_length(node->args) == 1) ||
		   (oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	if (oprkind == 'r' || oprkind == 'b')
	{
		arg = list_head(node->args);
		deparseExpr(lfirst(arg), context);
		appendStringInfoChar(buf, ' ');
	}

	/* Deparse operator name. */
	deparseOperatorName(buf, form);

	/* Deparse right operand. */
	if (oprkind == 'l' || oprkind == 'b')
	{
		arg = list_tail(node->args);
		appendStringInfoChar(buf, ' ');
		deparseExpr(lfirst(arg), context);
	}

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Print the name of an operator.
 */
static void
deparseOperatorName(StringInfo buf, Form_pg_operator opform)
{
	char *opname;

	/* opname is not a SQL identifier, so we should not quote it. */
	opname = NameStr(opform->oprname);

	/* Print schema name only if it's not pg_catalog */
	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname;

		opnspname = get_namespace_name(opform->oprnamespace);
		/* Print fully qualified operator name. */
		appendStringInfo(buf, "OPERATOR(%s.%s)", quote_identifier(opnspname), opname);
	}
	else
	{
		/* Just print operator name. */
		appendStringInfoString(buf, opname);
	}
}

/*
 * Deparse IS DISTINCT FROM.
 */
static void
deparseDistinctExpr(DistinctExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;

	Assert(list_length(node->args) == 2);

	appendStringInfoChar(buf, '(');
	deparseExpr(linitial(node->args), context);
	appendStringInfoString(buf, " IS DISTINCT FROM ");
	deparseExpr(lsecond(node->args), context);
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse given ScalarArrayOpExpr expression.  To avoid problems
 * around priority of operations, we always parenthesize the arguments.
 */
static void
deparseScalarArrayOpExpr(ScalarArrayOpExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	HeapTuple tuple;
	Form_pg_operator form;
	Expr *arg1;
	Expr *arg2;

	/* Retrieve information about the operator from system catalog. */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator) GETSTRUCT(tuple);

	/* Sanity check. */
	Assert(list_length(node->args) == 2);

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, '(');

	/* Deparse left operand. */
	arg1 = linitial(node->args);
	deparseExpr(arg1, context);
	appendStringInfoChar(buf, ' ');

	/* Deparse operator name plus decoration. */
	deparseOperatorName(buf, form);
	appendStringInfo(buf, " %s (", node->useOr ? "ANY" : "ALL");

	/* Deparse right operand. */
	arg2 = lsecond(node->args);
	deparseExpr(arg2, context);

	appendStringInfoChar(buf, ')');

	/* Always parenthesize the expression. */
	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

/*
 * Deparse a RelabelType (binary-compatible cast) node.
 */
static void
deparseRelabelType(RelabelType *node, deparse_expr_cxt *context)
{
	deparseExpr(node->arg, context);
	if (node->relabelformat != COERCE_IMPLICIT_CAST)
		appendStringInfo(context->buf,
						 "::%s",
						 deparse_type_name(node->resulttype, node->resulttypmod));
}

/*
 * Deparse a BoolExpr node.
 */
static void
deparseBoolExpr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	const char *op = NULL; /* keep compiler quiet */
	bool first;
	ListCell *lc;

	switch (node->boolop)
	{
		case AND_EXPR:
			op = "AND";
			break;
		case OR_EXPR:
			op = "OR";
			break;
		case NOT_EXPR:
			appendStringInfoString(buf, "(NOT ");
			deparseExpr(linitial(node->args), context);
			appendStringInfoChar(buf, ')');
			return;
	}

	appendStringInfoChar(buf, '(');
	first = true;
	foreach (lc, node->args)
	{
		if (!first)
			appendStringInfo(buf, " %s ", op);
		deparseExpr((Expr *) lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Deparse IS [NOT] NULL expression.
 */
static void
deparseNullTest(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;

	appendStringInfoChar(buf, '(');
	deparseExpr(node->arg, context);

	/*
	 * For scalar inputs, we prefer to print as IS [NOT] NULL, which is
	 * shorter and traditional.  If it's a rowtype input but we're applying a
	 * scalar test, must print IS [NOT] DISTINCT FROM NULL to be semantically
	 * correct.
	 */
	if (node->argisrow || !type_is_rowtype(exprType((Node *) node->arg)))
	{
		if (node->nulltesttype == IS_NULL)
			appendStringInfoString(buf, " IS NULL)");
		else
			appendStringInfoString(buf, " IS NOT NULL)");
	}
	else
	{
		if (node->nulltesttype == IS_NULL)
			appendStringInfoString(buf, " IS NOT DISTINCT FROM NULL)");
		else
			appendStringInfoString(buf, " IS DISTINCT FROM NULL)");
	}
}

/*
 * Deparse ARRAY[...] construct.
 */
static void
deparseArrayExpr(ArrayExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	bool first = true;
	ListCell *lc;

	appendStringInfoString(buf, "ARRAY[");
	foreach (lc, node->elements)
	{
		if (!first)
			appendStringInfoString(buf, ", ");
		deparseExpr(lfirst(lc), context);
		first = false;
	}
	appendStringInfoChar(buf, ']');

	/* If the array is empty, we need an explicit cast to the array type. */
	if (node->elements == NIL)
		appendStringInfo(buf, "::%s", deparse_type_name(node->array_typeid, -1));
}

/*
 * Deparse an Aggref node.
 */
static void
deparseAggref(Aggref *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	bool use_variadic;
	bool partial_agg = node->aggsplit != AGGSPLIT_SIMPLE;
	Assert(node->aggsplit == AGGSPLIT_SIMPLE || node->aggsplit == AGGSPLIT_INITIAL_SERIAL);

	/* Check if need to print VARIADIC (cf. ruleutils.c) */
	use_variadic = node->aggvariadic;

	/* Find aggregate name from aggfnoid which is a pg_proc entry */
	if (partial_agg)
		appendStringInfoString(buf, INTERNAL_SCHEMA_NAME "." PARTIALIZE_FUNC_NAME "(");

	appendFunctionName(node->aggfnoid, context);
	appendStringInfoChar(buf, '(');

	/* Add DISTINCT */
	appendStringInfoString(buf, (node->aggdistinct != NIL) ? "DISTINCT " : "");

	if (AGGKIND_IS_ORDERED_SET(node->aggkind))
	{
		/* Add WITHIN GROUP (ORDER BY ..) */
		ListCell *arg;
		bool first = true;

		Assert(!node->aggvariadic);
		Assert(node->aggorder != NIL);

		foreach (arg, node->aggdirectargs)
		{
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			deparseExpr((Expr *) lfirst(arg), context);
		}

		appendStringInfoString(buf, ") WITHIN GROUP (ORDER BY ");
		appendAggOrderBy(node->aggorder, node->args, context);
	}
	else
	{
		/* aggstar can be set only in zero-argument aggregates */
		if (node->aggstar)
			appendStringInfoChar(buf, '*');
		else
		{
			ListCell *arg;
			bool first = true;

			/* Add all the arguments */
			foreach (arg, node->args)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(arg);
				Node *n = (Node *) tle->expr;

				if (tle->resjunk)
					continue;

				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				/* Add VARIADIC */
				if (use_variadic && lnext(node->args, arg) == NULL)
					appendStringInfoString(buf, "VARIADIC ");

				deparseExpr((Expr *) n, context);
			}
		}

		/* Add ORDER BY */
		if (node->aggorder != NIL)
		{
			appendStringInfoString(buf, " ORDER BY ");
			appendAggOrderBy(node->aggorder, node->args, context);
		}
	}

	/* Add FILTER (WHERE ..) */
	if (node->aggfilter != NULL)
	{
		appendStringInfoString(buf, ") FILTER (WHERE ");
		deparseExpr((Expr *) node->aggfilter, context);
	}

	appendStringInfoString(buf, partial_agg ? "))" : ")");
}

/*
 * Append ORDER BY within aggregate function.
 */
static void
appendAggOrderBy(List *orderList, List *targetList, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	ListCell *lc;
	bool first = true;

	foreach (lc, orderList)
	{
		SortGroupClause *srt = (SortGroupClause *) lfirst(lc);
		Node *sortexpr;
		Oid sortcoltype;
		TypeCacheEntry *typentry;

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		sortexpr = deparseSortGroupClause(srt->tleSortGroupRef, targetList, false, context);
		sortcoltype = exprType(sortexpr);
		/* See whether operator is default < or > for datatype */
		typentry = lookup_type_cache(sortcoltype, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);
		if (srt->sortop == typentry->lt_opr)
			appendStringInfoString(buf, " ASC");
		else if (srt->sortop == typentry->gt_opr)
			appendStringInfoString(buf, " DESC");
		else
		{
			HeapTuple opertup;
			Form_pg_operator operform;

			appendStringInfoString(buf, " USING ");

			/* Append operator name. */
			opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(srt->sortop));
			if (!HeapTupleIsValid(opertup))
				elog(ERROR, "cache lookup failed for operator %u", srt->sortop);
			operform = (Form_pg_operator) GETSTRUCT(opertup);
			deparseOperatorName(buf, operform);
			ReleaseSysCache(opertup);
		}

		if (srt->nulls_first)
			appendStringInfoString(buf, " NULLS FIRST");
		else
			appendStringInfoString(buf, " NULLS LAST");
	}
}

/*
 * Print the representation of a parameter to be sent to the remote side.
 *
 * Note: we always label the Param's type explicitly rather than relying on
 * transmitting a numeric type OID in PQexecParams().  This allows us to
 * avoid assuming that types have the same OIDs on the remote side as they
 * do locally --- they need only have the same names.
 */
static void
printRemoteParam(int paramindex, Oid paramtype, int32 paramtypmod, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	char *ptypename = deparse_type_name(paramtype, paramtypmod);

	appendStringInfo(buf, "$%d::%s", paramindex, ptypename);
}

/*
 * Print the representation of a placeholder for a parameter that will be
 * sent to the remote side at execution time.
 *
 * This is used when we're just trying to EXPLAIN the remote query.
 * We don't have the actual value of the runtime parameter yet, and we don't
 * want the remote planner to generate a plan that depends on such a value
 * anyway.  Thus, we can't do something simple like "$1::paramtype".
 * Instead, we emit "((SELECT null::paramtype)::paramtype)".
 * In all extant versions of Postgres, the planner will see that as an unknown
 * constant value, which is what we want.  This might need adjustment if we
 * ever make the planner flatten scalar subqueries.  Note: the reason for the
 * apparently useless outer cast is to ensure that the representation as a
 * whole will be parsed as an a_expr and not a select_with_parens; the latter
 * would do the wrong thing in the context "x = ANY(...)".
 */
static void
printRemotePlaceholder(Oid paramtype, int32 paramtypmod, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	char *ptypename = deparse_type_name(paramtype, paramtypmod);

	appendStringInfo(buf, "((SELECT null::%s)::%s)", ptypename, ptypename);
}

/*
 * Deparse GROUP BY clause.
 */
static void
appendGroupByClause(List *tlist, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	Query *query = context->root->parse;
	ListCell *lc;
	bool first = true;

	/* Nothing to be done, if there's no GROUP BY clause in the query. */
	if (!query->groupClause)
		return;

	appendStringInfoString(buf, " GROUP BY ");

	/*
	 * Queries with grouping sets are not pushed down, so we don't expect
	 * grouping sets here.
	 */
	Assert(!query->groupingSets);

	foreach (lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *) lfirst(lc);

		if (!first)
			appendStringInfoString(buf, ", ");
		first = false;

		deparseSortGroupClause(grp->tleSortGroupRef, tlist, true, context);
	}
}

/*
 * Deparse ORDER BY clause according to the given pathkeys for given base
 * relation. From given pathkeys expressions belonging entirely to the given
 * base relation are obtained and deparsed.
 */
static void
appendOrderByClause(List *pathkeys, deparse_expr_cxt *context)
{
	ListCell *lcell;
	int nestlevel;
	char *delim = " ";
	RelOptInfo *baserel = context->scanrel;
	StringInfo buf = context->buf;

	/* Make sure any constants in the exprs are printed portably */
	nestlevel = set_transmission_modes();

	appendStringInfoString(buf, " ORDER BY");
	foreach (lcell, pathkeys)
	{
		PathKey *pathkey = lfirst(lcell);
		Expr *em_expr;

		em_expr = find_em_expr_for_rel(pathkey->pk_eclass, baserel);
		Assert(em_expr != NULL);

		appendStringInfoString(buf, delim);
		deparseExpr(em_expr, context);
		if (pathkey->pk_strategy == BTLessStrategyNumber)
			appendStringInfoString(buf, " ASC");
		else
			appendStringInfoString(buf, " DESC");

		if (pathkey->pk_nulls_first)
			appendStringInfoString(buf, " NULLS FIRST");
		else
			appendStringInfoString(buf, " NULLS LAST");

		delim = ", ";
	}
	reset_transmission_modes(nestlevel);
}

static void
appendLimit(deparse_expr_cxt *context, List *pathkeys)
{
	Query *query = context->root->parse;

	/* Limit is always set to value greater than zero, even for
	 * the LIMIT 0 case.
	 *
	 * We do not explicitly push OFFSET clause, since PostgreSQL
	 * treats limit_tuples as a sum of original
	 * LIMIT + OFFSET.
	 */
	Assert(context->root->limit_tuples >= 1.0);

	/* Do LIMIT deparsing only for supported clauses.
	 *
	 * Current implementation does not handle aggregates with LIMIT
	 * pushdown. It should have different deparsing logic because
	 * at this point PostgreSQL already excluded LIMIT for the most of
	 * the incompatible features during group planning:
	 * distinct, aggs, window functions, group by and having.
	 *
	 * See: grouping_planner() backend/optimizer/plan/planner.c
	 *
	 * Just make sure this is true.
	 */
	Assert(!(query->groupClause || query->groupingSets || query->distinctClause || query->hasAggs ||
			 query->hasWindowFuncs || query->hasTargetSRFs || context->root->hasHavingQual));

	/* JOIN restrict to only one table */
	if (!(list_length(query->jointree->fromlist) == 1 &&
		  IsA(linitial(query->jointree->fromlist), RangeTblRef)))
		return;

	/* ORDER BY is used but not pushed down */
	if (pathkeys == NULL && context->root->query_pathkeys)
		return;

	/* Use format to round float value */
	appendStringInfo(context->buf, " LIMIT %d", (int) ceil(context->root->limit_tuples));
}

/*
 * appendFunctionName
 *		Deparses function name from given function oid.
 */
static void
appendFunctionName(Oid funcid, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	HeapTuple proctup;
	Form_pg_proc procform;
	const char *proname;

	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", funcid);
	procform = (Form_pg_proc) GETSTRUCT(proctup);

	/* Print schema name only if it's not pg_catalog */
	if (procform->pronamespace != PG_CATALOG_NAMESPACE)
	{
		const char *schemaname;

		schemaname = get_namespace_name(procform->pronamespace);
		appendStringInfo(buf, "%s.", quote_identifier(schemaname));
	}

	proname = NameStr(procform->proname);

	/* Always print the function name */
	appendStringInfoString(buf, quote_identifier(proname));

	ReleaseSysCache(proctup);
}

/*
 * Appends a sort or group clause.
 *
 * Like get_rule_sortgroupclause(), returns the expression tree, so caller
 * need not find it again.
 */
static Node *
deparseSortGroupClause(Index ref, List *tlist, bool force_colno, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;
	TargetEntry *tle;
	Expr *expr;

	tle = get_sortgroupref_tle(ref, tlist);
	expr = tle->expr;

	if (force_colno)
	{
		/* Use column-number form when requested by caller. */
		Assert(!tle->resjunk);
		appendStringInfo(buf, "%d", tle->resno);
	}
	else if (expr && IsA(expr, Const))
	{
		/*
		 * Force a typecast here so that we don't emit something like "GROUP
		 * BY 2", which will be misconstrued as a column position rather than
		 * a constant.
		 */
		deparseConst((Const *) expr, context, 1);
	}
	else if (!expr || IsA(expr, Var))
		deparseExpr(expr, context);
	else
	{
		/* Always parenthesize the expression. */
		appendStringInfoChar(buf, '(');
		deparseExpr(expr, context);
		appendStringInfoChar(buf, ')');
	}

	return (Node *) expr;
}

/*
 * Returns true if given Var is deparsed as a subquery output column, in
 * which case, *relno and *colno are set to the IDs for the relation and
 * column alias to the Var provided by the subquery.
 */
static bool
is_subquery_var(Var *node, RelOptInfo *foreignrel, int *relno, int *colno)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(foreignrel);
	RelOptInfo *outerrel = fpinfo->outerrel;
	RelOptInfo *innerrel = fpinfo->innerrel;

	/* Should only be called in these cases. */
	Assert(IS_SIMPLE_REL(foreignrel) || IS_JOIN_REL(foreignrel));

	/*
	 * If the given relation isn't a join relation, it doesn't have any lower
	 * subqueries, so the Var isn't a subquery output column.
	 */
	if (!IS_JOIN_REL(foreignrel))
		return false;

	/*
	 * If the Var doesn't belong to any lower subqueries, it isn't a subquery
	 * output column.
	 */
	if (!bms_is_member(node->varno, fpinfo->lower_subquery_rels))
		return false;

	if (bms_is_member(node->varno, outerrel->relids))
	{
		/*
		 * If outer relation is deparsed as a subquery, the Var is an output
		 * column of the subquery; get the IDs for the relation/column alias.
		 */
		if (fpinfo->make_outerrel_subquery)
		{
			get_relation_column_alias_ids(node, outerrel, relno, colno);
			return true;
		}

		/* Otherwise, recurse into the outer relation. */
		return is_subquery_var(node, outerrel, relno, colno);
	}
	else
	{
		Assert(bms_is_member(node->varno, innerrel->relids));

		/*
		 * If inner relation is deparsed as a subquery, the Var is an output
		 * column of the subquery; get the IDs for the relation/column alias.
		 */
		if (fpinfo->make_innerrel_subquery)
		{
			get_relation_column_alias_ids(node, innerrel, relno, colno);
			return true;
		}

		/* Otherwise, recurse into the inner relation. */
		return is_subquery_var(node, innerrel, relno, colno);
	}
}

/*
 * Get the IDs for the relation and column alias to given Var belonging to
 * given relation, which are returned into *relno and *colno.
 */
static void
get_relation_column_alias_ids(Var *node, RelOptInfo *foreignrel, int *relno, int *colno)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(foreignrel);
	int i;
	ListCell *lc;

	/* Get the relation alias ID */
	*relno = fpinfo->relation_index;

	/* Get the column alias ID */
	i = 1;
	foreach (lc, foreignrel->reltarget->exprs)
	{
		if (equal(lfirst(lc), (Node *) node))
		{
			*colno = i;
			return;
		}
		i++;
	}

	/* Shouldn't get here */
	elog(ERROR, "unexpected expression in subquery output");
}
