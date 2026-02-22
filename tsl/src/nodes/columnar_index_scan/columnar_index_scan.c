/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/sysattr.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>

#include "columnar_index_scan.h"
#include "compression/create.h"
#include "expression_utils.h"
#include "func_cache.h"
#include "guc.h"
#include "nodes/columnar_scan/columnar_scan.h"
#include "nodes/columnar_scan/planner.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/compression_settings.h"
#include "utils.h"

static CustomScanMethods columnar_index_scan_plan_methods = {
	.CustomName = COLUMNAR_INDEX_SCAN_NAME,
	.CreateCustomScanState = columnar_index_scan_state_create,
};

void
_columnar_index_scan_init(void)
{
	TryRegisterCustomScanMethods(&columnar_index_scan_plan_methods);
}

/*
 * Check if an aggregate function can use compressed chunk sparse index.
 *
 * Currently supported aggregates are min, max, first, and last.
 */
static bool
is_supported_aggregate(Aggref *aggref, Var **arg_var_out, const char **meta_type_out)
{
	if (aggref->args == NIL)
		return false;

	/* Get the argument - must be a Var (possibly with implicit coercions) */
	TargetEntry *arg_te = linitial_node(TargetEntry, aggref->args);

	Node *arg_expr = strip_implicit_coercions((Node *) arg_te->expr);
	if (!IsA(arg_expr, Var))
		return false;

	Var *var = castNode(Var, arg_expr);

	/* Reject system columns except tableoid */
	if (var->varattno <= 0 && var->varattno != TableOidAttributeNumber)
		return false;

	switch (aggref->aggfnoid)
	{
		case F_MIN_ANYARRAY:
		case F_MIN_ANYENUM:
		case F_MIN_BPCHAR:
#if PG18_GE
		case F_MIN_BYTEA:
#endif
		case F_MIN_DATE:
		case F_MIN_FLOAT4:
		case F_MIN_FLOAT8:
		case F_MIN_INET:
		case F_MIN_INT2:
		case F_MIN_INT4:
		case F_MIN_INT8:
		case F_MIN_INTERVAL:
		case F_MIN_MONEY:
		case F_MIN_NUMERIC:
		case F_MIN_OID:
		case F_MIN_PG_LSN:
#if PG18_GE
		case F_MIN_RECORD:
#endif
		case F_MIN_TEXT:
		case F_MIN_TID:
		case F_MIN_TIME:
		case F_MIN_TIMESTAMP:
		case F_MIN_TIMESTAMPTZ:
		case F_MIN_TIMETZ:
		case F_MIN_XID8:
			*meta_type_out = "min";
			*arg_var_out = var;
			return true;
		case F_MAX_ANYARRAY:
		case F_MAX_ANYENUM:
		case F_MAX_BPCHAR:
#if PG18_GE
		case F_MAX_BYTEA:
#endif
		case F_MAX_DATE:
		case F_MAX_FLOAT4:
		case F_MAX_FLOAT8:
		case F_MAX_INET:
		case F_MAX_INT2:
		case F_MAX_INT4:
		case F_MAX_INT8:
		case F_MAX_INTERVAL:
		case F_MAX_MONEY:
		case F_MAX_NUMERIC:
		case F_MAX_OID:
		case F_MAX_PG_LSN:
#if PG18_GE
		case F_MAX_RECORD:
#endif
		case F_MAX_TEXT:
		case F_MAX_TID:
		case F_MAX_TIME:
		case F_MAX_TIMESTAMP:
		case F_MAX_TIMESTAMPTZ:
		case F_MAX_TIMETZ:
		case F_MAX_XID8:
			*meta_type_out = "max";
			*arg_var_out = var;
			return true;
		default:
			/* Check for first()/last() with both args referencing same column */
			if (!OidIsValid(ts_first_func_oid) || !OidIsValid(ts_last_func_oid))
				ts_func_cache_init();

			if (aggref->aggfnoid == ts_first_func_oid || aggref->aggfnoid == ts_last_func_oid)
			{
				if (list_length(aggref->args) != 2)
					return false;
				TargetEntry *tle2 = castNode(TargetEntry, lsecond(aggref->args));
				Node *arg2_expr = strip_implicit_coercions((Node *) tle2->expr);
				if (!equal(var, arg2_expr))
					return false;
				*meta_type_out = (aggref->aggfnoid == ts_first_func_oid) ? "min" : "max";
				*arg_var_out = var;
				return true;
			}
			break;
	}

	return false;
}

typedef struct SegmentbyCheckContext
{
	Oid relid;
	CompressionSettings *settings;
	List *custom_scan_tlist;
} SegmentbyCheckContext;

/*
 * Expression tree walker: returns true (abort) if any Var references a
 * non-segmentby column.  Vars are resolved through the custom_scan_tlist
 * to obtain the real table attribute number before checking.
 */
static bool
has_non_segmentby_var(Node *node, void *context)
{
	if (node == NULL)
		return false;
	if (IsA(node, Var))
	{
		SegmentbyCheckContext *ctx = (SegmentbyCheckContext *) context;
		Var *var = castNode(Var, node);
		if (var->varattno <= 0)
			return true;

		/* Resolve through custom_scan_tlist to get real column attno */
		AttrNumber real_attno = var->varattno;
		if (ctx->custom_scan_tlist != NIL)
		{
			TargetEntry *tle = get_tle_by_resno(ctx->custom_scan_tlist, var->varattno);
			if (tle == NULL || !IsA(tle->expr, Var))
				return true;
			real_attno = castNode(Var, tle->expr)->varattno;
		}
		if (real_attno <= 0)
			return true;

		char *col_name = get_attname(ctx->relid, real_attno, true);
		return col_name == NULL || !ts_array_is_member(ctx->settings->fd.segmentby, col_name);
	}
	return expression_tree_walker(node, has_non_segmentby_var, context);
}

/*
 * Check if all quals reference only segmentby columns.
 *
 * Vars in a ColumnarScan's plan->qual use custom_scan_tlist positions, so we
 * resolve through the tlist to get the real table column before checking.
 */
static bool
quals_only_reference_segmentby(List *quals, CustomScan *cscan, List *rtable)
{
	RangeTblEntry *rte = rt_fetch(cscan->scan.scanrelid, rtable);
	CompressionSettings *settings = ts_compression_settings_get(rte->relid);
	if (settings == NULL || settings->fd.segmentby == NULL)
		return false;

	SegmentbyCheckContext ctx = {
		.relid = rte->relid,
		.settings = settings,
		.custom_scan_tlist = cscan->custom_scan_tlist,
	};

	ListCell *lc;
	foreach (lc, quals)
	{
		if (has_non_segmentby_var(lfirst(lc), &ctx))
			return false;
	}
	return true;
}

/*
 * Check if the ColumnarScan's vectorized quals are empty (no filters applied).
 */
static bool
columnar_scan_has_no_vector_quals(CustomScan *cscan)
{
	/*
	 * The vectorized quals are stored in custom_exprs. If the list is empty
	 * or the first element is NIL, there are no vectorized quals.
	 */
	if (cscan->custom_exprs == NIL)
		return true;
	if (linitial(cscan->custom_exprs) == NIL)
		return true;
	return false;
}

/*
 * Find the resno in the leaf plan's targetlist that corresponds to a given
 * compressed chunk attribute number.
 */
static AttrNumber
find_resno_by_compressed_attno(Plan *leaf_plan, AttrNumber compressed_attno)
{
	ListCell *lc;
	foreach (lc, leaf_plan->targetlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		if (IsA(tle->expr, Var))
		{
			Var *var = castNode(Var, tle->expr);
			if (var->varattno == compressed_attno)
				return tle->resno;
		}
	}
	return InvalidAttrNumber;
}

/*
 * Context for validate_entries_walker.
 */
typedef struct ValidateContext
{
	Oid uncompressed_relid;
	Oid compressed_relid;
	Index uncompressed_scanrelid;
	Index compressed_scanrelid;
	CompressionSettings *settings;
	Plan *leaf_plan;
	List *custom_scan_tlist;
	List *output_map;
	AttrNumber next_resno;
} ValidateContext;

static void
add_scan_output(ValidateContext *ctx, AttrNumber child_resno, Index tlist_varno,
				AttrNumber tlist_attno, Oid col_type, int32 col_typmod, Oid col_collid)
{
	Var *tlist_var = makeVar(tlist_varno, tlist_attno, col_type, col_typmod, col_collid, 0);
	ctx->custom_scan_tlist =
		lappend(ctx->custom_scan_tlist,
				makeTargetEntry((Expr *) tlist_var, ctx->next_resno++, NULL, false));
	ctx->output_map = lappend(ctx->output_map, makeInteger(child_resno));
}

/*
 * Expression tree walker that validates Var and Aggref nodes in the resolved
 * Agg targetlist. Builds custom_scan_tlist and output_map entries in DFS order.
 * Returns true (abort) if any node cannot be handled.
 *
 * Aggrefs are validated but NOT recursed into — their args reference the
 * child plan being replaced, so we only care about the aggregate itself.
 */
static bool
validate_entries_walker(Node *node, void *context)
{
	ValidateContext *ctx = (ValidateContext *) context;

	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		if (var->varattno == TableOidAttributeNumber)
		{
			add_scan_output(ctx,
							TableOidAttributeNumber,
							ctx->uncompressed_scanrelid,
							TableOidAttributeNumber,
							OIDOID,
							-1,
							InvalidOid);
			return false;
		}

		if (var->varattno <= 0)
			return true;

		char *col_name = get_attname(ctx->uncompressed_relid, var->varattno, false);

		if (ctx->settings == NULL || !ts_array_is_member(ctx->settings->fd.segmentby, col_name))
			return true;

		AttrNumber compressed_attno = get_attnum(ctx->compressed_relid, col_name);
		if (compressed_attno == InvalidAttrNumber)
			return true;

		AttrNumber child_resno = find_resno_by_compressed_attno(ctx->leaf_plan, compressed_attno);
		if (child_resno == InvalidAttrNumber)
			return true;

		add_scan_output(ctx,
						child_resno,
						ctx->uncompressed_scanrelid,
						var->varattno,
						var->vartype,
						var->vartypmod,
						var->varcollid);
		return false;
	}

	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;

		/* No DISTINCT, ORDER BY, or FILTER on the Aggref */
		if (aggref->aggdistinct != NIL || aggref->aggorder != NIL || aggref->aggfilter != NULL)
			return true;

		if (aggref->aggfnoid == F_COUNT_)
		{
			AttrNumber meta_count_attno =
				get_attnum(ctx->compressed_relid, COMPRESSION_COLUMN_METADATA_COUNT_NAME);
			if (meta_count_attno == InvalidAttrNumber)
				return true;

			AttrNumber child_resno =
				find_resno_by_compressed_attno(ctx->leaf_plan, meta_count_attno);
			if (child_resno == InvalidAttrNumber)
				return true;

			add_scan_output(ctx,
							child_resno,
							ctx->compressed_scanrelid,
							meta_count_attno,
							INT4OID,
							-1,
							InvalidOid);
		}
		else
		{
			Var *arg_var = NULL;
			const char *meta_type = NULL;
			if (!is_supported_aggregate(aggref, &arg_var, &meta_type))
				return true;

			if (arg_var->varattno == TableOidAttributeNumber)
			{
				/*
				 * tableoid is constant within a single chunk scan, so
				 * min(tableoid) = max(tableoid) = tableoid.  Output the
				 * tableoid sentinel and let the Agg reduce it.
				 */
				add_scan_output(ctx,
								TableOidAttributeNumber,
								ctx->uncompressed_scanrelid,
								TableOidAttributeNumber,
								OIDOID,
								-1,
								InvalidOid);
			}
			else
			{
				AttrNumber meta_attno = compressed_column_metadata_attno(ctx->settings,
																		 ctx->uncompressed_relid,
																		 arg_var->varattno,
																		 ctx->compressed_relid,
																		 meta_type);
				if (meta_attno == InvalidAttrNumber)
					return true;

				AttrNumber child_resno = find_resno_by_compressed_attno(ctx->leaf_plan, meta_attno);
				if (child_resno == InvalidAttrNumber)
					return true;

				add_scan_output(ctx,
								child_resno,
								ctx->uncompressed_scanrelid,
								arg_var->varattno,
								get_atttype(ctx->compressed_relid, meta_attno),
								-1,
								arg_var->varcollid);
			}
		}

		/* Don't recurse into Aggref children */
		return false;
	}

	return expression_tree_walker(node, validate_entries_walker, context);
}

/*
 * Context for rewrite_agg_tlist_mutator.
 */
typedef struct RewriteContext
{
	Agg *agg;
	List *custom_scan_tlist;
	AttrNumber next_resno;
} RewriteContext;

/*
 * Expression tree mutator that rewrites Var and Aggref nodes in the Agg's
 * targetlist to reference ColumnarIndexScan output columns. Type information
 * for rewritten aggregate arguments is read from the pre-built custom_scan_tlist.
 */
static Node *
rewrite_agg_tlist_mutator(Node *node, void *context)
{
	RewriteContext *ctx = (RewriteContext *) context;

	if (node == NULL)
		return NULL;

	if (IsA(node, Var) && ((Var *) node)->varno == OUTER_VAR)
	{
		Var *var = (Var *) node;
		AttrNumber resno = ctx->next_resno++;

		/* Update grpColIdx for GROUP BY Vars */
		for (int k = 0; k < ctx->agg->numCols; k++)
		{
			if (ctx->agg->grpColIdx[k] == var->varattno)
				ctx->agg->grpColIdx[k] = resno;
		}

		return (Node *) makeVar(OUTER_VAR,
								resno,
								var->vartype,
								var->vartypmod,
								var->varcollid,
								var->varlevelsup);
	}

	if (IsA(node, Aggref))
	{
		Aggref *orig = (Aggref *) node;
		AttrNumber resno = ctx->next_resno++;

		if (orig->aggfnoid == F_COUNT_)
		{
			/*
			 * Rewrite count(*) → sum(_ts_meta_count).
			 *
			 * sum(int4) uses int4_sum as transition function with INT8
			 * transition type, same as count(*)'s int8inc. Both use
			 * int8pl as combine function, so the Finalize Agg above
			 * (which still has the original count(*) aggfnoid) can
			 * combine partial states from either without changes.
			 *
			 * For AGGSPLIT_SIMPLE (no Finalize), wrap in COALESCE to
			 * preserve count(*)'s 0-for-empty semantics since sum()
			 * returns NULL for no rows. For AGGSPLIT_INITIAL_SERIAL,
			 * the Finalize's strict combine function (int8pl) and
			 * count's initial value (0) handle NULL partial states.
			 */
			Aggref *aggref = copyObject(orig);
			aggref->aggfnoid = F_SUM_INT4;
			aggref->aggtranstype = INT8OID;
			aggref->aggstar = false;
			aggref->aggargtypes = list_make1_oid(INT4OID);
			Var *arg_var = makeVar(OUTER_VAR, resno, INT4OID, -1, InvalidOid, 0);
			aggref->args = list_make1(makeTargetEntry((Expr *) arg_var, 1, NULL, false));

			if (ctx->agg->aggsplit == AGGSPLIT_SIMPLE)
			{
				CoalesceExpr *coalesce = makeNode(CoalesceExpr);
				coalesce->coalescetype = aggref->aggtype;
				coalesce->args = list_make2(aggref,
											makeConst(INT8OID,
													  -1,
													  InvalidOid,
													  sizeof(int64),
													  Int64GetDatum(0),
													  false,
													  true));
				coalesce->location = -1;
				return (Node *) coalesce;
			}

			return (Node *) aggref;
		}

		/* min/max/first/last: rewrite args to point to metadata column */
		Aggref *aggref = copyObject(orig);
		TargetEntry *scan_tle = list_nth_node(TargetEntry, ctx->custom_scan_tlist, resno - 1);
		Var *scan_var = castNode(Var, scan_tle->expr);
		Var *new_arg = makeVar(OUTER_VAR,
							   resno,
							   scan_var->vartype,
							   scan_var->vartypmod,
							   scan_var->varcollid,
							   0);
		TargetEntry *new_arg_te = makeTargetEntry((Expr *) new_arg, 1, NULL, false);

		if (list_length(aggref->args) == 2)
		{
			/* first/last: both args point to same metadata column */
			Var *new_arg2 = copyObject(new_arg);
			TargetEntry *new_arg_te2 = makeTargetEntry((Expr *) new_arg2, 2, NULL, false);
			aggref->args = list_make2(new_arg_te, new_arg_te2);
		}
		else
		{
			aggref->args = list_make1(new_arg_te);
		}

		return (Node *) aggref;
	}

	return expression_tree_mutator(node, rewrite_agg_tlist_mutator, context);
}

/*
 * Create a ColumnarIndexScan plan node as a pure scan node that replaces the ColumnarScan
 * beneath the Agg. The Agg stays in the plan with rewritten Aggrefs:
 *   count(*)        → sum(_ts_meta_count)
 *   min(col)/max(col) → same aggfnoid, arg rewritten to metadata column
 *   first(col,col)/last(col,col) → same aggfnoid, both args rewritten
 *
 * Expressions that combine aggregates (e.g. 2*count(*), min(x)+max(y)) are
 * supported — the walker/mutator handles Var and Aggref nodes at any depth.
 */
static Plan *
columnar_index_scan_plan_create(Agg *agg, CustomScan *cscan, List *resolved_targetlist,
								List *rtable)
{
	Plan *compressed_scan_subtree = linitial(cscan->custom_plans);

	Plan *leaf_plan = compressed_scan_subtree;
	if (IsA(leaf_plan, Sort))
		leaf_plan = leaf_plan->lefttree;

	Scan *compressed_scan = (Scan *) leaf_plan;
	RangeTblEntry *compressed_rte = rt_fetch(compressed_scan->scanrelid, rtable);
	Oid compressed_relid = compressed_rte->relid;

	Oid uncompressed_relid = rt_fetch(cscan->scan.scanrelid, rtable)->relid;
	CompressionSettings *settings = ts_compression_settings_get(uncompressed_relid);

	/*
	 * Validation pass: walk the resolved targetlist to validate all Var and
	 * Aggref leaf nodes. Builds custom_scan_tlist and output_map directly.
	 * No modifications to the Agg happen here, so bailing out is safe.
	 */
	ValidateContext validate_ctx = {
		.uncompressed_relid = uncompressed_relid,
		.compressed_relid = compressed_relid,
		.uncompressed_scanrelid = cscan->scan.scanrelid,
		.compressed_scanrelid = compressed_scan->scanrelid,
		.settings = settings,
		.leaf_plan = leaf_plan,
		.custom_scan_tlist = NIL,
		.output_map = NIL,
		.next_resno = 1,
	};

	if (validate_entries_walker((Node *) resolved_targetlist, &validate_ctx))
		return NULL;

	List *custom_scan_tlist = validate_ctx.custom_scan_tlist;
	List *output_map = validate_ctx.output_map;

	if (custom_scan_tlist == NIL)
		return NULL;

	/*
	 * Rewrite pass: walk the Agg's targetlist with a mutator that rewrites
	 * Var and Aggref nodes to reference ColumnarIndexScan output columns.
	 */
	RewriteContext rewrite_ctx = {
		.agg = agg,
		.custom_scan_tlist = custom_scan_tlist,
		.next_resno = 1,
	};

	agg->plan.targetlist =
		(List *) rewrite_agg_tlist_mutator((Node *) agg->plan.targetlist, &rewrite_ctx);

	/* Build ColumnarIndexScan CustomScan */
	CustomScan *columnar_index_scan = (CustomScan *) makeNode(CustomScan);
	columnar_index_scan->custom_plans = list_make1(compressed_scan_subtree);
	columnar_index_scan->methods = &columnar_index_scan_plan_methods;

	columnar_index_scan->scan.scanrelid = cscan->scan.scanrelid;

	columnar_index_scan->custom_scan_tlist = custom_scan_tlist;
	columnar_index_scan->scan.plan.targetlist =
		ts_build_trivial_custom_output_targetlist(custom_scan_tlist);

	/* Copy cost/parallel/param fields from the ColumnarScan */
	columnar_index_scan->scan.plan.plan_rows = cscan->scan.plan.plan_rows;
	columnar_index_scan->scan.plan.plan_width = cscan->scan.plan.plan_width;
	columnar_index_scan->scan.plan.startup_cost = cscan->scan.plan.startup_cost;
	columnar_index_scan->scan.plan.total_cost = cscan->scan.plan.total_cost;

	columnar_index_scan->scan.plan.parallel_aware = false;
	columnar_index_scan->scan.plan.parallel_safe = cscan->scan.plan.parallel_safe;
	columnar_index_scan->scan.plan.async_capable = false;

	columnar_index_scan->scan.plan.plan_node_id = cscan->scan.plan.plan_node_id;

	columnar_index_scan->scan.plan.initPlan = cscan->scan.plan.initPlan;
	columnar_index_scan->scan.plan.extParam = bms_copy(cscan->scan.plan.extParam);
	columnar_index_scan->scan.plan.allParam = bms_copy(cscan->scan.plan.allParam);

	columnar_index_scan->custom_private = list_make1(output_map);

	/* Set ColumnarIndexScan as the Agg's child */
	agg->plan.lefttree = (Plan *) columnar_index_scan;

	return (Plan *) agg;
}

/*
 * Callback for ts_plan_tree_walker: try to insert a ColumnarIndexScan node between an Agg
 * and its ColumnarScan child.
 */
static Plan *
insert_columnar_index_scan(Plan *plan, void *context)
{
	List *rtable = (List *) context;

	if (plan->type != T_Agg)
		return plan;

	Agg *agg = castNode(Agg, plan);

	/*
	 * We are looking for the partial step (AGGSPLIT_INITIAL_SERIAL) of
	 * partial/finalize aggregate or non-partial aggregates (AGGSPLIT_SIMPLE).
	 */
	if (agg->aggsplit != AGGSPLIT_INITIAL_SERIAL && agg->aggsplit != AGGSPLIT_SIMPLE)
		return plan;

	/* bail out on HAVING */
	if (agg->plan.qual != NIL)
		return plan;

	Plan *childplan = agg->plan.lefttree;

	/*
	 * The child must be a ColumnarScan.
	 */
	if (!ts_is_columnar_scan_plan(childplan))
		return plan;

	CustomScan *cscan = castNode(CustomScan, childplan);

	/*
	 * No Postgres quals on the ColumnarScan, or quals only on segmentby
	 * columns. Segmentby filters are pushed to the compressed scan so
	 * ColumnarIndexScan can skip them.
	 */
	if (childplan->qual != NIL && !quals_only_reference_segmentby(childplan->qual, cscan, rtable))
		return plan;

	/*
	 * No vectorized quals on the ColumnarScan.
	 */
	if (!columnar_scan_has_no_vector_quals(cscan))
		return plan;

	/*
	 * Resolve OUTER_VAR references in the Agg targetlist.
	 */
	List *resolved_targetlist = ts_resolve_outer_special_vars(agg->plan.targetlist, childplan);

	Plan *result = columnar_index_scan_plan_create(agg, cscan, resolved_targetlist, rtable);
	if (result == NULL)
		return plan;

	return result;
}

/*
 * Where possible, replace Agg -> ColumnarScan with Agg -> ColumnarIndexScan.
 * The Agg stays in the plan with rewritten Aggrefs; ColumnarIndexScan replaces
 * the ColumnarScan as a pure scan node over compressed metadata.
 */
Plan *
try_insert_columnar_index_scan_node(Plan *plan, List *rtable)
{
	return ts_plan_tree_walker(plan, insert_columnar_index_scan, rtable);
}
