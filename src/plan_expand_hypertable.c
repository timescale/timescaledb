/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_inherits.h>
#include <catalog/pg_namespace.h>
#include <catalog/pg_type.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <optimizer/pathnode.h>
#include <optimizer/prep.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parse_func.h>
#include <parser/parsetree.h>
#include <utils/date.h>
#include <utils/errcodes.h>
#include <utils/syscache.h>

#include "compat.h"
#if PG11_LT /* PG11 consolidates pg_foo_fn.h -> pg_foo.h */
#include <catalog/pg_constraint_fn.h>
#include <catalog/pg_inherits_fn.h>
#endif
#if PG11_GE
#include <partitioning/partbounds.h>
#include <optimizer/cost.h>
#endif

#if PG12_LT
#include <optimizer/clauses.h>
#include <optimizer/var.h>
#elif PG12_GE
#include <optimizer/optimizer.h>
#endif

#include "import/planner.h"
#include "plan_expand_hypertable.h"
#include "hypertable.h"
#include "hypertable_restrict_info.h"
#include "planner.h"
#include "chunk_append/chunk_append.h"
#include "guc.h"
#include "extension.h"
#include "chunk.h"
#include "extension_constants.h"
#include "partitioning.h"

typedef struct CollectQualCtx
{
	PlannerInfo *root;
	RelOptInfo *rel;
	List *restrictions;
	FuncExpr *chunk_exclusion_func;
	List *join_conditions;
	List *propagate_conditions;
	List *all_quals;
} CollectQualCtx;

static void propagate_join_quals(PlannerInfo *root, RelOptInfo *rel, CollectQualCtx *ctx);

static Oid chunk_exclusion_func = InvalidOid;
#define CHUNK_EXCL_FUNC_NAME "chunks_in"
static Oid ts_chunks_arg_types[] = { RECORDOID, INT4ARRAYOID };

static void
init_chunk_exclusion_func()
{
	if (chunk_exclusion_func == InvalidOid)
	{
		List *l = list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(CHUNK_EXCL_FUNC_NAME));
		chunk_exclusion_func =
			LookupFuncName(l, lengthof(ts_chunks_arg_types), ts_chunks_arg_types, false);
	}
	Assert(chunk_exclusion_func != InvalidOid);
}

static bool
is_chunk_exclusion_func(Expr *node)
{
	if (IsA(node, FuncExpr) && castNode(FuncExpr, node)->funcid == chunk_exclusion_func)
		return true;

	return false;
}

static bool
is_time_bucket_function(Expr *node)
{
	if (IsA(node, FuncExpr) &&
		strncmp(get_func_name(castNode(FuncExpr, node)->funcid), "time_bucket", NAMEDATALEN) == 0)
		return true;

	return false;
}

static int64
const_datum_get_int(Const *cnst)
{
	Assert(!cnst->constisnull);

	switch (cnst->consttype)
	{
		case INT2OID:
			return (int64)(DatumGetInt16(cnst->constvalue));
		case INT4OID:
			return (int64)(DatumGetInt32(cnst->constvalue));
		case INT8OID:
			return DatumGetInt64(cnst->constvalue);
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("can only use const_datum_get_int with integer types")));

	pg_unreachable();
}

/*
 * Transform time_bucket calls of the following form in WHERE clause:
 *
 * time_bucket(width, column) OP value
 *
 * Since time_bucket always returns the lower bound of the bucket
 * for lower bound comparisons the width is not relevant and the
 * following transformation can be applied:
 *
 * time_bucket(width, column) > value
 * column > value
 *
 * Example with values:
 *
 * time_bucket(10, column) > 109
 * column > 109
 *
 * For upper bound comparisons width needs to be taken into account
 * and we need to extend the upper bound by width to capture all
 * possible values.
 *
 * time_bucket(width, column) < value
 * column < value + width
 *
 * Example with values:
 *
 * time_bucket(10, column) < 100
 * column < 100 + 10
 *
 * Expressions with value on the left side will be switched around
 * when building the expression for RestrictInfo.
 *
 * Caller must ensure that only 2 argument time_bucket versions
 * are used.
 */
static OpExpr *
transform_time_bucket_comparison(PlannerInfo *root, OpExpr *op)
{
	Expr *left = linitial(op->args);
	Expr *right = lsecond(op->args);

	FuncExpr *time_bucket = castNode(FuncExpr, (IsA(left, FuncExpr) ? left : right));
	Expr *value = IsA(right, Const) ? right : left;

	Const *width = linitial(time_bucket->args);
	Oid opno = op->opno;
	TypeCacheEntry *tce;
	int strategy;

	/* caller must ensure time_bucket only has 2 arguments */
	Assert(list_length(time_bucket->args) == 2);

	/*
	 * if time_bucket call is on wrong side we switch operator
	 */
	if (IsA(right, FuncExpr))
	{
		opno = get_commutator(op->opno);

		if (!OidIsValid(opno))
			return op;
	}

	tce = lookup_type_cache(exprType((Node *) time_bucket), TYPECACHE_BTREE_OPFAMILY);
	strategy = get_op_opfamily_strategy(opno, tce->btree_opf);

	if (strategy == BTGreaterStrategyNumber || strategy == BTGreaterEqualStrategyNumber)
	{
		/* column > value */
		op = copyObject(op);
		op->args = list_make2(lsecond(time_bucket->args), value);

		/*
		 * if we switched operator we need to adjust OpExpr as well
		 */
		if (IsA(right, FuncExpr))
		{
			op->opno = opno;
			op->opfuncid = InvalidOid;
		}

		return op;
	}
	else if (strategy == BTLessStrategyNumber || strategy == BTLessEqualStrategyNumber)
	{
		/* column < value + width */
		Expr *subst;
		Datum datum;
		int64 integralValue, integralWidth;

		/*
		 * caller should make sure value and width are Const
		 */
		Assert(IsA(value, Const) && IsA(width, Const));

		if (castNode(Const, value)->constisnull || width->constisnull)
			return op;

		switch (tce->type_id)
		{
			case INT2OID:
				integralValue = const_datum_get_int(castNode(Const, value));
				integralWidth = const_datum_get_int(width);

				if (integralValue >= PG_INT16_MAX - integralWidth)
					return op;

				datum = Int16GetDatum(integralValue + integralWidth);
				subst = (Expr *) makeConst(tce->type_id,
										   -1,
										   InvalidOid,
										   tce->typlen,
										   datum,
										   false,
										   tce->typbyval);
				break;

			case INT4OID:
				integralValue = const_datum_get_int(castNode(Const, value));
				integralWidth = const_datum_get_int(width);

				if (integralValue >= PG_INT32_MAX - integralWidth)
					return op;

				datum = Int32GetDatum(integralValue + integralWidth);
				subst = (Expr *) makeConst(tce->type_id,
										   -1,
										   InvalidOid,
										   tce->typlen,
										   datum,
										   false,
										   tce->typbyval);
				break;
			case INT8OID:
				integralValue = const_datum_get_int(castNode(Const, value));
				integralWidth = const_datum_get_int(width);

				if (integralValue >= PG_INT64_MAX - integralWidth)
					return op;

				datum = Int64GetDatum(integralValue + integralWidth);
				subst = (Expr *) makeConst(tce->type_id,
										   -1,
										   InvalidOid,
										   tce->typlen,
										   datum,
										   false,
										   tce->typbyval);

				break;
			case DATEOID:
			{
				Interval *interval = DatumGetIntervalP(width->constvalue);

				if (interval->month != 0)
					return op;

				/* bail out if interval->time can't be exactly represented as a double */
				if (interval->time >= 0x3FFFFFFFFFFFFFll)
					return op;

				if (DatumGetDateADT(castNode(Const, value)->constvalue) >=
					DATEVAL_NOEND - interval->day +
						ceil((double) interval->time / (double) USECS_PER_DAY))
					return op;

				datum = DateADTGetDatum(DatumGetDateADT(castNode(Const, value)->constvalue) +
										interval->day +
										ceil((double) interval->time / (double) USECS_PER_DAY));
				subst = (Expr *) makeConst(tce->type_id,
										   -1,
										   InvalidOid,
										   tce->typlen,
										   datum,
										   false,
										   tce->typbyval);

				break;
			}
			case TIMESTAMPTZOID:
			{
				Interval *interval = DatumGetIntervalP(width->constvalue);

				Assert(width->consttype == INTERVALOID);

				/*
				 * intervals with month component are not supported by time_bucket
				 */
				if (interval->month != 0)
					return op;

				/*
				 * If width interval has day component we merge it with time component
				 */
				if (interval->day != 0)
				{
					width = copyObject(width);
					interval = DatumGetIntervalP(width->constvalue);

					/*
					 * if our transformed restriction would overflow we skip adding it
					 */
					if (interval->time >= PG_INT64_MAX - interval->day * USECS_PER_DAY)
						return op;

					interval->time += interval->day * USECS_PER_DAY;
					interval->day = 0;
				}

				if (DatumGetTimestampTz(castNode(Const, value)->constvalue) >=
					DT_NOEND - interval->time)
					return op;

				datum = TimestampTzGetDatum(
					DatumGetTimestampTz(castNode(Const, value)->constvalue) + interval->time);
				subst = (Expr *) makeConst(tce->type_id,
										   -1,
										   InvalidOid,
										   tce->typlen,
										   datum,
										   false,
										   tce->typbyval);

				break;
			}

			case TIMESTAMPOID:
			{
				Interval *interval = DatumGetIntervalP(width->constvalue);

				Assert(width->consttype == INTERVALOID);

				/*
				 * intervals with month component are not supported by time_bucket
				 */
				if (interval->month != 0)
					return op;

				/*
				 * If width interval has day component we merge it with time component
				 */
				if (interval->day != 0)
				{
					width = copyObject(width);
					interval = DatumGetIntervalP(width->constvalue);

					/*
					 * if our merged value overflows we skip adding it
					 */
					if (interval->time >= PG_INT64_MAX - interval->day * USECS_PER_DAY)
						return op;

					interval->time += interval->day * USECS_PER_DAY;
					interval->day = 0;
				}

				if (DatumGetTimestamp(castNode(Const, value)->constvalue) >=
					DT_NOEND - interval->time)
					return op;

				datum = TimestampGetDatum(DatumGetTimestamp(castNode(Const, value)->constvalue) +
										  interval->time);
				subst = (Expr *) makeConst(tce->type_id,
										   -1,
										   InvalidOid,
										   tce->typlen,
										   datum,
										   false,
										   tce->typbyval);

				break;
			}
			default:
				return op;
				break;
		}

		/*
		 * adjust toplevel expression if datatypes changed
		 * this can happen when comparing int4 values against int8 time_bucket
		 */
		if (tce->type_id != castNode(Const, value)->consttype)
		{
			opno =
				ts_get_operator(get_opname(opno), PG_CATALOG_NAMESPACE, tce->type_id, tce->type_id);

			if (!OidIsValid(opno))
				return op;
		}

		op = copyObject(op);

		/*
		 * if we changed operator we need to adjust OpExpr as well
		 */
		if (op->opno != opno)
		{
			op->opno = opno;
			op->opfuncid = get_opcode(opno);
		}

		op->args = list_make2(lsecond(time_bucket->args), subst);
	}

	return op;
}

/* Since baserestrictinfo is not yet set by the planner, we have to derive
 * it ourselves. It's safe for us to miss some restrict info clauses (this
 * will just result in more chunks being included) so this does not need
 * to be as comprehensive as the PG native derivation. This is inspired
 * by the derivation in `deconstruct_recurse` in PG
 *
 * When we detect explicit chunk exclusion with the chunks_in function
 * we stop further processing and do an early exit.
 *
 * This function removes chunks_in from the list of quals, because chunks_in is
 * just used as marker function to trigger explicit chunk exclusion and the function
 * will throw an error when executed.
 */
static Node *
process_quals(Node *quals, CollectQualCtx *ctx, bool is_outer_join)
{
	ListCell *lc;

	ListCell *prev pg_attribute_unused() = NULL;
	List *additional_quals = NIL;

	for (lc = list_head((List *) quals); lc != NULL; prev = lc, lc = lnext(lc))
	{
		Expr *qual = lfirst(lc);
		Relids relids = pull_varnos((Node *) qual);
		int num_rels = bms_num_members(relids);

		/* stop processing if not for current rel */
		if (num_rels != 1 || !bms_is_member(ctx->rel->relid, relids))
			continue;

		if (is_chunk_exclusion_func(qual))
		{
			FuncExpr *func_expr = (FuncExpr *) qual;

			/* validation */
			Assert(func_expr->args->length == 2);
			if (!IsA(linitial(func_expr->args), Var))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("first parameter for chunks_in function needs to be record")));

			ctx->chunk_exclusion_func = func_expr;
			ctx->restrictions = NIL;
			/* In PG12 removing the chunk_exclusion function here causes issues
			 * when the first/last optimization fires, as those subqueries
			 * will not see the function. Fortunately, in pg12 we do not the
			 * baserestrictinfo is already populated by the time this function
			 * is called, so we can remove the functions from that directly
			 */
#if PG12_LT
			quals = (Node *) list_delete_cell((List *) quals, lc, prev);
#endif
			return quals;
		}

		if (IsA(qual, OpExpr) && list_length(castNode(OpExpr, qual)->args) == 2)
		{
			OpExpr *op = castNode(OpExpr, qual);
			Expr *left = linitial(op->args);
			Expr *right = lsecond(op->args);

			/*
			 * check for time_bucket comparisons
			 * time_bucket(Const, time_colum) > Const
			 */
			if ((IsA(left, FuncExpr) && IsA(right, Const) &&
				 list_length(castNode(FuncExpr, left)->args) == 2 &&
				 is_time_bucket_function(left)) ||
				(IsA(left, Const) && IsA(right, FuncExpr) &&
				 list_length(castNode(FuncExpr, right)->args) == 2 &&
				 is_time_bucket_function(right)))
			{
				qual = (Expr *) transform_time_bucket_comparison(ctx->root, op);
				/*
				 * if we could transform the expression we add it to the list of
				 * quals so it can be used as an index condition
				 */
				if (qual != (Expr *) op)
					additional_quals = lappend(additional_quals, qual);
			}
		}

		/* Do not include this restriction if this is an outer join. Including
		 * the restriction would exclude chunks and thus rows of the outer
		 * relation when it should show all rows */
		if (!is_outer_join)
			ctx->restrictions = lappend(ctx->restrictions, make_simple_restrictinfo(qual));
	}
	return (Node *) list_concat((List *) quals, additional_quals);
}

#if PG12_GE
static List *
remove_exclusion_fns(List *restrictinfo)
{
	ListCell *prev = NULL;
	ListCell *lc = list_head(restrictinfo);

	while (lc != NULL)
	{
		RestrictInfo *rinfo = lfirst(lc);
		Expr *qual = rinfo->clause;

		if (is_chunk_exclusion_func(qual))
		{
			FuncExpr *func_expr = (FuncExpr *) qual;

			/* validation */
			Assert(func_expr->args->length == 2);
			if (!IsA(linitial(func_expr->args), Var))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("first parameter for chunks_in function needs to be record")));

			restrictinfo = list_delete_cell((List *) restrictinfo, lc, prev);
			return restrictinfo;
		}
		prev = lc;
		lc = lnext(lc);
	}
	return restrictinfo;
}
#endif

static Node *
timebucket_annotate(Node *quals, CollectQualCtx *ctx)
{
	ListCell *lc;
	List *additional_quals = NIL;

	foreach (lc, castNode(List, quals))
	{
		Expr *qual = lfirst(lc);
		Relids relids = pull_varnos((Node *) qual);
		int num_rels = bms_num_members(relids);

		/* stop processing if not for current rel */
		if (num_rels != 1 || !bms_is_member(ctx->rel->relid, relids))
			continue;

		if (IsA(qual, OpExpr) && list_length(castNode(OpExpr, qual)->args) == 2)
		{
			OpExpr *op = castNode(OpExpr, qual);
			Expr *left = linitial(op->args);
			Expr *right = lsecond(op->args);

			/*
			 * check for time_bucket comparisons
			 * time_bucket(Const, time_colum) > Const
			 */
			if ((IsA(left, FuncExpr) && IsA(right, Const) &&
				 list_length(castNode(FuncExpr, left)->args) == 2 &&
				 is_time_bucket_function(left)) ||
				(IsA(left, Const) && IsA(right, FuncExpr) &&
				 list_length(castNode(FuncExpr, right)->args) == 2 &&
				 is_time_bucket_function(right)))
			{
				qual = (Expr *) transform_time_bucket_comparison(ctx->root, op);
				/*
				 * if we could transform the expression we add it to the list of
				 * quals so it can be used as an index condition
				 */
				if (qual != (Expr *) op)
					additional_quals = lappend(additional_quals, qual);
			}
		}

		ctx->restrictions = lappend(ctx->restrictions, make_simple_restrictinfo(qual));
	}
	return (Node *) list_concat((List *) quals, additional_quals);
}

/*
 * collect JOIN information
 *
 * This function adds information to two lists in the CollectQualCtx
 *
 * join_conditions
 *
 * This list contains all equality join conditions and is used by
 * ChunkAppend to decide whether the ordered append optimization
 * can be applied.
 *
 * propagate_conditions
 *
 * This list contains toplevel or INNER JOIN equality conditions.
 * This list is used for propagating quals to the other side of
 * a JOIN.
 */
static void
collect_join_quals(Node *quals, CollectQualCtx *ctx, bool is_outer_join)
{
	ListCell *lc;

	foreach (lc, (List *) quals)
	{
		Expr *qual = lfirst(lc);
		Relids relids = pull_varnos((Node *) qual);
		int num_rels = bms_num_members(relids);

		/*
		 * collect quals to propagate to join relations
		 */
		if (num_rels == 1 && !is_outer_join && IsA(qual, OpExpr) &&
			list_length(castNode(OpExpr, qual)->args) == 2)
			ctx->all_quals = lappend(ctx->all_quals, qual);

		if (!bms_is_member(ctx->rel->relid, relids))
			continue;

		/* collect equality JOIN conditions for current rel */
		if (num_rels == 2 && IsA(qual, OpExpr) && list_length(castNode(OpExpr, qual)->args) == 2)
		{
			OpExpr *op = castNode(OpExpr, qual);
			Expr *left = linitial(op->args);
			Expr *right = lsecond(op->args);

			if (IsA(left, Var) && IsA(right, Var))
			{
				Var *ht_var =
					castNode(Var, castNode(Var, left)->varno == ctx->rel->relid ? left : right);
				TypeCacheEntry *tce = lookup_type_cache(ht_var->vartype, TYPECACHE_EQ_OPR);

				if (op->opno == tce->eq_opr)
				{
					ctx->join_conditions = lappend(ctx->join_conditions, op);

					if (!is_outer_join)
						ctx->propagate_conditions = lappend(ctx->propagate_conditions, op);
				}
			}
			continue;
		}
	}
}

static bool
collect_quals_walker(Node *node, CollectQualCtx *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, FromExpr))
	{
		FromExpr *f = castNode(FromExpr, node);
		f->quals = process_quals(f->quals, ctx, false);
		collect_join_quals(f->quals, ctx, false);
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr *j = castNode(JoinExpr, node);
		j->quals = process_quals(j->quals, ctx, IS_OUTER_JOIN(j->jointype));
		collect_join_quals(j->quals, ctx, IS_OUTER_JOIN(j->jointype));
	}

	/* skip processing if we found a chunks_in call for current relation */
	if (ctx->chunk_exclusion_func != NULL)
		return true;

	return expression_tree_walker(node, collect_quals_walker, ctx);
}

static List *
find_children_oids(HypertableRestrictInfo *hri, Hypertable *ht, LOCKMODE lockmode)
{
	/*
	 * Using the HRI only makes sense if we are not using all the chunks,
	 * otherwise using the cached inheritance hierarchy is faster.
	 */
	if (!ts_hypertable_restrict_info_has_restrictions(hri))
		return find_inheritance_children(ht->main_table_relid, lockmode);

	/*
	 * Unlike find_all_inheritors we do not include parent because if there
	 * are restrictions the parent table cannot fulfill them and since we do
	 * have a trigger blocking inserts on the parent table it cannot contain
	 * any rows.
	 */
	return ts_hypertable_restrict_info_get_chunk_oids(hri, ht, lockmode);
}

static bool
should_order_append(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht, List *join_conditions,
					int *order_attno, bool *reverse)
{
	/* check if optimizations are enabled */
	if (ts_guc_disable_optimizations || !ts_guc_enable_ordered_append ||
		!ts_guc_enable_chunk_append)
		return false;

	/*
	 * only do this optimization for hypertables with 1 dimension and queries
	 * with an ORDER BY clause
	 */
	if (root->parse->sortClause == NIL)
		return false;

	return ts_ordered_append_should_optimize(root, rel, ht, join_conditions, order_attno, reverse);
}

/*  get chunk oids specified by explicit chunk exclusion function */
static List *
get_explicit_chunk_oids(CollectQualCtx *ctx, Hypertable *ht)
{
	List *chunk_oids = NIL;
	Const *chunks_arg;
	ArrayIterator chunk_id_iterator;
	Datum elem = (Datum) NULL;
	bool isnull;
	Expr *expr;

	Assert(ctx->chunk_exclusion_func->args->length == 2);
	expr = lsecond(ctx->chunk_exclusion_func->args);
	if (!IsA(expr, Const))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("second argument to chunk_in should contain only integer consts")));

	chunks_arg = (Const *) expr;

	/* function marked as STRICT so argument can't be NULL */
	Assert(!chunks_arg->constisnull);

	chunk_id_iterator = array_create_iterator(DatumGetArrayTypeP(chunks_arg->constvalue), 0, NULL);

	while (array_iterate(chunk_id_iterator, &elem, &isnull))
	{
		if (!isnull)
		{
			int32 chunk_id = DatumGetInt32(elem);
			Chunk *chunk = ts_chunk_get_by_id(chunk_id, 0, false);

			if (chunk == NULL)
				ereport(ERROR, (errmsg("chunk id %d not found", chunk_id)));

			if (chunk->fd.hypertable_id != ht->fd.id)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("chunk id %d does not belong to hypertable \"%s\"",
								chunk_id,
								NameStr(ht->fd.table_name))));

			chunk_oids = lappend_int(chunk_oids, chunk->table_id);
		}
		else
			elog(ERROR, "chunk id can't be NULL");
	}
	array_free_iterator(chunk_id_iterator);
	return chunk_oids;
}

/**
 * Get chunk oids from either restrict info or explicit chunk exclusion. Explicit chunk exclusion
 * takes precedence.
 *
 * If appends are returned in order appends_ordered on rel->fdw_private is set to true.
 * To make verifying pathkeys easier in set_rel_pathlist the attno of the column ordered by
 * is
 * If the hypertable uses space partitioning the nested oids are stored in nested_oids
 * on rel->fdw_private when appends are ordered.
 */
static List *
get_chunk_oids(CollectQualCtx *ctx, PlannerInfo *root, RelOptInfo *rel, Hypertable *ht)
{
	bool reverse;
	int order_attno;

	if (ctx->chunk_exclusion_func == NULL)
	{
		HypertableRestrictInfo *hri = ts_hypertable_restrict_info_create(rel, ht);

		/*
		 * This is where the magic happens: use our HypertableRestrictInfo
		 * infrastructure to deduce the appropriate chunks using our range
		 * exclusion
		 */
		ts_hypertable_restrict_info_add(hri, root, ctx->restrictions);

		/*
		 * If fdw_private has not been setup by caller there is no point checking
		 * for ordered append as we can't pass the required metadata in fdw_private
		 * to signal that this is safe to transform in ordered append plan in
		 * set_rel_pathlist.
		 */
		if (rel->fdw_private != NULL &&
			should_order_append(root, rel, ht, ctx->join_conditions, &order_attno, &reverse))
		{
			TimescaleDBPrivate *priv = ts_get_private_reloptinfo(rel);
			List **nested_oids = NULL;

			priv->appends_ordered = true;
			priv->order_attno = order_attno;

			/*
			 * for space partitioning we need extra information about the
			 * time slices of the chunks
			 */
			if (ht->space->num_dimensions > 1)
				nested_oids = &priv->nested_oids;

			return ts_hypertable_restrict_info_get_chunk_oids_ordered(hri,
																	  ht,
																	  AccessShareLock,
																	  nested_oids,
																	  reverse);
		}
		return find_children_oids(hri, ht, AccessShareLock);
	}
	else
		return get_explicit_chunk_oids(ctx, ht);
}

#if PG11_GE

/*
 * Create partition expressions for a hypertable.
 *
 * Build an array of partition expressions where each element represents valid
 * expressions on a particular partitioning key.
 *
 * The partition expressions are used by, e.g., group_by_has_partkey() to check
 * whether a GROUP BY clause covers all partitioning dimensions.
 *
 * For dimensions with a partitioning function, we can support either
 * expressions on the plain key (column) or the partitioning function applied
 * to the key. For instance, the queries
 *
 * SELECT time, device, avg(temp)
 * FROM hypertable
 * GROUP BY 1, 2;
 *
 * and
 *
 * SELECT time_func(time), device, avg(temp)
 * FROM hypertable
 * GROUP BY 1, 2;
 *
 * are both amenable to aggregate push down if "time" is supported by the
 * partitioning function "time_func" and "device" is also a partitioning
 * dimension.
 */
static List **
get_hypertable_partexprs(Hypertable *ht, Query *parse, Index varno)
{
	int i;
	List **partexprs;

	Assert(NULL != ht->space);

	partexprs = palloc0(sizeof(List *) * ht->space->num_dimensions);

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		Dimension *dim = &ht->space->dimensions[i];
		Expr *expr;
		HeapTuple tuple = SearchSysCacheAttNum(ht->main_table_relid, dim->column_attno);
		Form_pg_attribute att;

		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for attribute");

		att = (Form_pg_attribute) GETSTRUCT(tuple);

		expr = (Expr *)
			makeVar(varno, dim->column_attno, att->atttypid, att->atttypmod, att->attcollation, 0);

		ReleaseSysCache(tuple);

		/* The expression on the partitioning key can be the raw key or the
		 * partitioning function on the key */
		if (NULL != dim->partitioning)
			partexprs[i] = list_make2(expr, dim->partitioning->partfunc.func_fmgr.fn_expr);
		else
			partexprs[i] = list_make1(expr);
	}

	return partexprs;
}

#define PARTITION_STRATEGY_MULTIDIM 'm'

/*
 * Partition info for hypertables.
 *
 * Build a "fake" partition scheme for a hypertable that makes the planner
 * believe this is a PostgreSQL partitioned table for planning purposes. In
 * particular, this will make the planner consider partitionwise aggregations
 * when applicable.
 *
 * Partitionwise aggregation can either be FULL or PARTIAL. The former means
 * that the aggregation can be performed independently on each partition
 * (chunk) without a finalize step which is needed in PARTIAL. FULL requires
 * that the GROUP BY clause contains all hypertable partitioning
 * dimensions. This requirement is enforced by creating a partitioning scheme
 * that covers multiple attributes, i.e., one per dimension. This works well
 * since the "shallow" (one-level hierarchy) of a multi-dimensional hypertable
 * is similar to a one-level partitioned PostgreSQL table where the
 * partitioning key covers multiple attributes.
 *
 * Note that we use a partition scheme with a strategy that does not exist in
 * PostgreSQL. This makes PostgreSQL raise errors when this partition scheme is
 * used in places that require a valid partition scheme with a supported
 * strategy.
 */
static void
build_hypertable_partition_info(Hypertable *ht, PlannerInfo *root, RelOptInfo *hyper_rel,
								int nparts)
{
	PartitionScheme part_scheme = palloc0(sizeof(PartitionSchemeData));

	/* We only set the info needed for planning */
	part_scheme->partnatts = ht->space->num_dimensions;
	part_scheme->strategy = PARTITION_STRATEGY_MULTIDIM;
	hyper_rel->nparts = nparts;
	hyper_rel->part_scheme = part_scheme;
	hyper_rel->partexprs = get_hypertable_partexprs(ht, root->parse, hyper_rel->relid);
	hyper_rel->nullable_partexprs = (List **) palloc0(sizeof(List *) * part_scheme->partnatts);
	hyper_rel->boundinfo = palloc(sizeof(PartitionBoundInfoData));
	hyper_rel->part_rels = palloc0(sizeof(*hyper_rel->part_rels) * nparts);
}

#endif /* PG11_GE */

static bool
timebucket_annotate_walker(Node *node, CollectQualCtx *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, FromExpr))
	{
		FromExpr *f = castNode(FromExpr, node);
		f->quals = timebucket_annotate(f->quals, ctx);
		collect_join_quals(f->quals, ctx, true);
	}
	else if (IsA(node, JoinExpr))
	{
		JoinExpr *j = castNode(JoinExpr, node);
		j->quals = timebucket_annotate(j->quals, ctx);
		collect_join_quals(j->quals, ctx, !IS_OUTER_JOIN(j->jointype));
	}

	/* skip processing if we found a chunks_in call for current relation */
	if (ctx->chunk_exclusion_func != NULL)
		return true;

	return expression_tree_walker(node, timebucket_annotate_walker, ctx);
}

void
ts_plan_expand_timebucket_annotate(PlannerInfo *root, RelOptInfo *rel)
{
	CollectQualCtx ctx = {
		.root = root,
		.rel = rel,
		.restrictions = NIL,
		.chunk_exclusion_func = NULL,
		.all_quals = NIL,
		.join_conditions = NIL,
		.propagate_conditions = NIL,
	};

	init_chunk_exclusion_func();

	/* Walk the tree and find restrictions or chunk exclusion functions */
	timebucket_annotate_walker((Node *) root->parse->jointree, &ctx);

	if (ctx.propagate_conditions != NIL)
		propagate_join_quals(root, rel, &ctx);
}

/* Inspired by expand_inherited_rtentry but expands
 * a hypertable chunks into an append relationship */
void
ts_plan_expand_hypertable_chunks(Hypertable *ht, PlannerInfo *root, RelOptInfo *rel)
{
	RangeTblEntry *rte = rt_fetch(rel->relid, root->parse->rtable);
	Oid parent_oid = rte->relid;
	List *inh_oids;
	ListCell *l;
	Relation oldrelation = table_open(parent_oid, NoLock);
	Query *parse = root->parse;
	Index rti = rel->relid;
	List *appinfos = NIL;
	PlanRowMark *oldrc;
	CollectQualCtx ctx = {
		.root = root,
		.rel = rel,
		.restrictions = NIL,
		.chunk_exclusion_func = NULL,
		.all_quals = NIL,
		.join_conditions = NIL,
		.propagate_conditions = NIL,
	};
	Size old_rel_array_len;
	Index first_chunk_index = 0;
#if PG12_GE
	Index i;
#endif

	/* double check our permissions are valid */
	Assert(rti != parse->resultRelation);

	oldrc = get_plan_rowmark(root->rowMarks, rti);

	if (oldrc && RowMarkRequiresRowShareLock(oldrc->markType))
		elog(ERROR, "unexpected permissions requested");

		/* mark the parent as an append relation */
#if PG12_LT
	rte->inh = true;
#endif

	init_chunk_exclusion_func();

	/* Walk the tree and find restrictions or chunk exclusion functions */
	collect_quals_walker((Node *) root->parse->jointree, &ctx);

#if PG12_GE
	rel->baserestrictinfo = remove_exclusion_fns(rel->baserestrictinfo);
#endif

	if (ctx.propagate_conditions != NIL)
		propagate_join_quals(root, rel, &ctx);

	inh_oids = get_chunk_oids(&ctx, root, rel, ht);

	/*
	 * the simple_*_array structures have already been set, we need to add the
	 * children to them
	 */
	old_rel_array_len = root->simple_rel_array_size;
	root->simple_rel_array_size += list_length(inh_oids);
	root->simple_rel_array =
		repalloc(root->simple_rel_array, root->simple_rel_array_size * sizeof(RelOptInfo *));
	/* postgres expects these arrays to be 0'ed until intialized */
	memset(root->simple_rel_array + old_rel_array_len,
		   0,
		   list_length(inh_oids) * sizeof(*root->simple_rel_array));

	root->simple_rte_array =
		repalloc(root->simple_rte_array, root->simple_rel_array_size * sizeof(RangeTblEntry *));
	/* postgres expects these arrays to be 0'ed until intialized */
	memset(root->simple_rte_array + old_rel_array_len,
		   0,
		   list_length(inh_oids) * sizeof(*root->simple_rte_array));

#if PG11_GE
	/* Adding partition info will make PostgreSQL consider the inheritance
	 * children as part of a partitioned relation. This will enable
	 * partitionwise aggregation. */
	if (enable_partitionwise_aggregate)
		build_hypertable_partition_info(ht, root, rel, list_length(inh_oids));
#endif

	foreach (l, inh_oids)
	{
		Oid child_oid = lfirst_oid(l);
		Relation newrelation;
		RangeTblEntry *childrte;
		Index child_rtindex;
		AppendRelInfo *appinfo;
#if PG12_LT
		LOCKMODE chunk_lock = NoLock;
#else
		LOCKMODE chunk_lock = rte->rellockmode;
#endif

		/* Open rel if needed */

		if (child_oid != parent_oid)
			newrelation = table_open(child_oid, chunk_lock);
		else
			newrelation = oldrelation;

		/* chunks cannot be temp tables */
		Assert(!RELATION_IS_OTHER_TEMP(newrelation));

		/*
		 * Build an RTE for the child, and attach to query's rangetable list.
		 * We copy most fields of the parent's RTE, but replace relation OID
		 * and relkind, and set inh = false.  Also, set requiredPerms to zero
		 * since all required permissions checks are done on the original RTE.
		 * Likewise, set the child's securityQuals to empty, because we only
		 * want to apply the parent's RLS conditions regardless of what RLS
		 * properties individual children may have.  (This is an intentional
		 * choice to make inherited RLS work like regular permissions checks.)
		 * The parent securityQuals will be propagated to children along with
		 * other base restriction clauses, so we don't need to do it here.
		 */
		childrte = copyObject(rte);
		childrte->relid = child_oid;
		childrte->relkind = newrelation->rd_rel->relkind;
		childrte->inh = false;
		/* clear the magic bit */
		childrte->ctename = NULL;
		childrte->requiredPerms = 0;
		childrte->securityQuals = NIL;
		parse->rtable = lappend(parse->rtable, childrte);
		child_rtindex = list_length(parse->rtable);
		if (first_chunk_index == 0)
			first_chunk_index = child_rtindex;
		root->simple_rte_array[child_rtindex] = childrte;
#if PG12_LT
		root->simple_rel_array[child_rtindex] = NULL;
#endif

		appinfo = makeNode(AppendRelInfo);
		appinfo->parent_relid = rti;
		appinfo->child_relid = child_rtindex;
		appinfo->parent_reltype = oldrelation->rd_rel->reltype;
		appinfo->child_reltype = newrelation->rd_rel->reltype;
		ts_make_inh_translation_list(oldrelation,
									 newrelation,
									 child_rtindex,
									 &appinfo->translated_vars);
		appinfo->parent_reloid = parent_oid;
		appinfos = lappend(appinfos, appinfo);

		/* Close child relations, but keep locks */
		if (child_oid != parent_oid)
			table_close(newrelation, NoLock);
	}

	table_close(oldrelation, NoLock);

	root->append_rel_list = list_concat(root->append_rel_list, appinfos);

#if PG11_GE
	/*
	 * PG11 introduces a separate array to make looking up children faster, see:
	 * https://github.com/postgres/postgres/commit/7d872c91a3f9d49b56117557cdbb0c3d4c620687.
	 */
	setup_append_rel_array(root);
#endif

#if PG12_GE
	/* In pg12 postgres will not set up the child rels for use, due to the games
	 * we're playing with inheritance, so we must do it ourselves.
	 * build_simple_rel will look things up in the append_rel_array, so we can
	 * only use it after that array has been set up.
	 */
	i = 0;
	for (i = 0; i < list_length(inh_oids); i++)
	{
		Index child_rtindex = first_chunk_index + i;
		/* build_simple_rel will add the child to the relarray */
		RelOptInfo *child_rel = build_simple_rel(root, child_rtindex, rel);

		/* if we're performing partitionwise aggregation, we must populate part_rels */
		if (rel->part_rels != NULL)
			rel->part_rels[i] = child_rel;
	}
#endif
}

void
propagate_join_quals(PlannerInfo *root, RelOptInfo *rel, CollectQualCtx *ctx)
{
	ListCell *lc;

	/* propagate join constraints */
	foreach (lc, ctx->propagate_conditions)
	{
		ListCell *lc_qual;
		OpExpr *op = lfirst(lc);
		Var *rel_var, *other_var;

		/*
		 * join_conditions only has OpExpr with 2 Var as arguments
		 * this is enforced in process_quals
		 */
		Assert(IsA(op, OpExpr) && list_length(castNode(OpExpr, op)->args) == 2);
		Assert(IsA(linitial(op->args), Var) && IsA(lsecond(op->args), Var));

		/*
		 * check this join condition refers to current hypertable
		 * our Var might be on either side of the expression
		 */
		if (linitial_node(Var, op->args)->varno == rel->relid)
		{
			rel_var = linitial_node(Var, op->args);
			other_var = lsecond_node(Var, op->args);
		}
		else if (lsecond_node(Var, op->args)->varno == rel->relid)
		{
			rel_var = lsecond_node(Var, op->args);
			other_var = linitial_node(Var, op->args);
		}
		else
			continue;

		foreach (lc_qual, ctx->all_quals)
		{
			OpExpr *qual = lfirst(lc_qual);
			Expr *left = linitial(qual->args);
			Expr *right = lsecond(qual->args);
			OpExpr *propagated;
			ListCell *lc_ri;
			bool new_qual = true;

			/*
			 * check this is Var OP Expr / Expr OP Var
			 * Var needs to reference the relid of the JOIN condition and
			 * Expr must not contain volatile functions
			 */
			if (IsA(left, Var) && castNode(Var, left)->varno == other_var->varno &&
				castNode(Var, left)->varattno == other_var->varattno && !IsA(right, Var) &&
				!contain_volatile_functions((Node *) right))
			{
				propagated = copyObject(qual);
				propagated->args = list_make2(rel_var, lsecond(propagated->args));
			}
			else if (IsA(right, Var) && castNode(Var, right)->varno == other_var->varno &&
					 castNode(Var, right)->varattno == other_var->varattno && !IsA(left, Var) &&
					 !contain_volatile_functions((Node *) left))
			{
				propagated = copyObject(qual);
				propagated->args = list_make2(linitial(propagated->args), rel_var);
			}
			else
				continue;

			/*
			 * check if this is a new qual
			 */
			foreach (lc_ri, ctx->restrictions)
			{
				if (equal(castNode(RestrictInfo, lfirst(lc_ri))->clause, propagated))
				{
					new_qual = false;
					break;
				}
			}

			if (new_qual)
			{
				Relids relids = pull_varnos((Node *) propagated);
				RestrictInfo *restrictinfo;

#if PG96
				restrictinfo =
					make_restrictinfo((Expr *) propagated, true, false, false, relids, NULL, NULL);
#else
				restrictinfo = make_restrictinfo((Expr *) propagated,
												 true,
												 false,
												 false,
												 ctx->root->qual_security_level,
												 relids,
												 NULL,
												 NULL);
#endif
				ctx->restrictions = lappend(ctx->restrictions, restrictinfo);
				root->parse->jointree->quals =
					(Node *) lappend((List *) root->parse->jointree->quals, propagated);
			}
		}
	}
}
