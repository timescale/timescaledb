/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <nodes/bitmapset.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#include <parser/parsetree.h>

#include "compression/create.h"
#include "debug_assert.h"
#include "func_cache.h"
#include "guc.h"
#include "nodes/columnar_index_scan/columnar_index_scan.h"

static CustomPathMethods columnar_index_scan_path_methods = {
	.CustomName = "ColumnarIndexScanPath",
	.PlanCustomPath = columnar_index_scan_plan_create,
};

static CustomScanMethods columnar_index_scan_plan_methods = {
	.CustomName = "ColumnarIndexScan",
	.CreateCustomScanState = columnar_index_scan_state_create,
};

bool
ts_is_columnar_index_scan_path(Path *path)
{
	return IsA(path, CustomPath) &&
		   castNode(CustomPath, path)->methods == &columnar_index_scan_path_methods;
}

bool
ts_is_columnar_index_scan_plan(Plan *plan)
{
	return IsA(plan, CustomScan) &&
		   castNode(CustomScan, plan)->methods == &columnar_index_scan_plan_methods;
}

void
_columnar_index_scan_init(void)
{
	TryRegisterCustomScanMethods(&columnar_index_scan_plan_methods);
}

/*
 * Check if an aggregate function can use compressed chunk sparse index.
 *
 * Currently supported aggregates are min and max
 */
static bool
is_supported_aggregate(const CompressionInfo *info, Aggref *aggref, AttrNumber *aggregate_attno,
					   AttrNumber *metadata_attno)
{
	/* No DISTINCT, ORDER BY, or FILTER */
	if (aggref->args == NIL || aggref->aggdistinct != NIL || aggref->aggorder != NIL ||
		aggref->aggfilter != NULL)
		return false;

	/* Get the argument - must be a Var referencing orderby column */
	TargetEntry *arg_te = linitial_node(TargetEntry, aggref->args);

	Node *arg_expr = strip_implicit_coercions((Node *) arg_te->expr);
	if (!IsA(arg_expr, Var))
		return false;

	Var *var = castNode(Var, arg_expr);

	/* Reject any system columns */
	if (var->varattno <= 0)
		return false;

	char *meta_type = NULL;

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
		case F_MIN_SCALE:
		case F_MIN_TEXT:
		case F_MIN_TID:
		case F_MIN_TIME:
		case F_MIN_TIMESTAMP:
		case F_MIN_TIMESTAMPTZ:
		case F_MIN_TIMETZ:
		case F_MIN_XID8:
			meta_type = "min";
			break;
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
			meta_type = "max";
			break;
		default:
			/* Initialize function cache for access to ts_first_func_oid and ts_last_func_oid */
			if (!OidIsValid(ts_first_func_oid) || !OidIsValid(ts_last_func_oid))
				ts_func_cache_get(InvalidOid);

			if (aggref->aggfnoid == ts_first_func_oid || aggref->aggfnoid == ts_last_func_oid)
			{
				/*
				 * Check for eligible first/last aggregate
				 * For now we only support first/last with both arguments referencing same column
				 */
				TargetEntry *tle2 = castNode(TargetEntry, lsecond(aggref->args));
				if (!equal(var, tle2->expr))
					return false;

				meta_type = (aggref->aggfnoid == ts_first_func_oid) ? "min" : "max";
			}
			break;
	}
	if (meta_type)
	{
		/* var references hypertable attnums so we have to use hypertable relid for column name
		 * lookup */
		AttrNumber chunk_attno =
			ts_map_attno(info->ht_rte->relid, info->chunk_rte->relid, var->varattno);
		AttrNumber meta_attno = compressed_column_metadata_attno(info->settings,
																 info->chunk_rte->relid,
																 chunk_attno,
																 info->compressed_rte->relid,
																 meta_type);
		if (meta_attno)
		{
			*aggregate_attno = chunk_attno;
			*metadata_attno = meta_attno;
			return true;
		}
	}
	return false;
}

/*
 * Check if we can use a ColumnarIndexScan for the given query.
 *
 * Requirements:
 * - Query has eligible aggregate on any column with metadata
 * - GROUP BY has only segmentby columns
 * - WHERE clause can be fully pushed down to compressed chunk
 * - No HAVING clause
 */
static bool
can_use_columnar_index_scan(PlannerInfo *root, const Chunk *chunk, RelOptInfo *chunk_rel,
							const CompressionInfo *info, AttrNumber *aggregate_attno,
							AttrNumber *metadata_attno)
{
	Assert(ts_guc_enable_columnarindexscan);

	Query *parse = root->parse;
	ListCell *lc;
	bool found_aggregate = false;

	/* Must have aggregates */
	if (!parse->hasAggs)
		return false;

	/*
	 * Punt on queries without GROUP BY for now
	 *
	 * We can support these but this has the side effect of disabling
	 * interaction with ordered append queries since those won't have
	 * GROUP BY. So for now we only support GROUP BY queries.
	 */
	if (!parse->groupClause)
		return false;

	/*
	 * Only quals on non-segmentby constraints should be left in chunk_rel.
	 * Segmentby constraints should be pushed into compressed chunk.
	 */
	if (chunk_rel->baserestrictinfo != NIL)
		return false;

	/* No HAVING clause allowed */
	if (parse->havingQual != NULL)
		return false;

	/*
	 * Check that only segmentby columns are in GROUP BY columns
	 */
	foreach (lc, parse->groupClause)
	{
		SortGroupClause *sgc = (SortGroupClause *) lfirst(lc);
		TargetEntry *tle = get_sortgroupclause_tle(sgc, parse->targetList);

		/* Must be a simple Var */
		Node *node = strip_implicit_coercions((Node *) tle->expr);
		if (!IsA(node, Var))
			return false;

		Var *var = castNode(Var, node);
		if (var->varattno <= 0)
		{
			/* Reject any system columns except tableoid */
			if (var->varattno != TableOidAttributeNumber)
				return false;
		}
		else
		{
			/* GROUP BY references hypertable attnums so we have to translate to chunk */
			AttrNumber chunk_attno =
				ts_map_attno(info->ht_rte->relid, info->chunk_rte->relid, var->varattno);
			if (!bms_is_member(chunk_attno, info->chunk_segmentby_attnos))
				return false;
		}
	}

	/*
	 * Check target list.
	 * The target list should contain:
	 * - A single eligible aggregate on a column with min/max index
	 * - Any segmentby columns
	 */
	foreach (lc, parse->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tle->resjunk)
			continue;

		if (IsA(tle->expr, Aggref))
		{
			/* Only one aggregate allowed */
			if (found_aggregate)
				return false;

			if (!is_supported_aggregate(info,
										castNode(Aggref, tle->expr),
										aggregate_attno,
										metadata_attno))
				return false;

			found_aggregate = true;
		}
		else
		{
			/* Non-aggregate must be a segmentby column (for GROUP BY) */
			Node *expr = strip_implicit_coercions((Node *) tle->expr);
			if (!IsA(expr, Var))
				return false;

			Var *var = castNode(Var, expr);
			if (var->varattno <= 0)
			{
				if (var->varattno != TableOidAttributeNumber)
					return false;
			}
			else if (!bms_is_member(var->varattno, info->chunk_segmentby_attnos))
			{
				return false;
			}
		}
	}

	return found_aggregate;
}

/*
 * Calculate cost for ColumnarIndexScan.
 */
static void
cost_columnar_index_scan(ColumnarIndexScanPath *path, Path *compressed_path)
{
	/* We return one row per compressed batch (same as compressed path rows) */
	path->custom_path.path.rows = compressed_path->rows;
	path->custom_path.path.startup_cost = compressed_path->startup_cost;
	path->custom_path.path.total_cost = compressed_path->total_cost;

#if PG18_GE
	path->custom_path.path.disabled_nodes = compressed_path->disabled_nodes;
#endif
}

ColumnarIndexScanPath *
ts_columnar_index_scan_path_create(PlannerInfo *root, const Chunk *chunk, RelOptInfo *chunk_rel,
								   const CompressionInfo *info, Path *compressed_path)
{
	if (!ts_guc_enable_columnarindexscan)
		return NULL;

	AttrNumber aggregate_attno = InvalidAttrNumber;
	AttrNumber metadata_attno = InvalidAttrNumber;
	if (!can_use_columnar_index_scan(root,
									 chunk,
									 chunk_rel,
									 info,
									 &aggregate_attno,
									 &metadata_attno))
		return NULL;

	ColumnarIndexScanPath *path =
		(ColumnarIndexScanPath *) newNode(sizeof(ColumnarIndexScanPath), T_CustomPath);

	path->custom_path.path.pathtype = T_CustomScan;
	path->custom_path.path.parent = chunk_rel;
	path->custom_path.path.pathtarget = chunk_rel->reltarget;
	path->custom_path.path.param_info = NULL;
	path->custom_path.path.parallel_aware = false;
	path->custom_path.path.parallel_safe = compressed_path->parallel_safe;
	path->custom_path.path.parallel_workers = compressed_path->parallel_workers;
	path->custom_path.path.pathkeys = NIL;
	path->custom_path.flags = 0;
	path->custom_path.custom_paths = list_make1(compressed_path);
	path->custom_path.methods = &columnar_index_scan_path_methods;

	path->info = info;
	path->aggregate_attno = aggregate_attno;
	path->metadata_attno = metadata_attno;

	cost_columnar_index_scan(path, compressed_path);

	return path;
}

/*
 * Build a physical targetlist for a relation.
 *
 * This is similar to build_physical_tlist in plancat.c, but
 * we do not punt on dropped columns or columns with missing values,
 * since the columns we are interested in should not be dropped or missing.
 */
static List *
build_tlist(PlannerInfo *root, RelOptInfo *rel)
{
	List *tlist = NIL;
	Index varno = rel->relid;
	RangeTblEntry *rte = planner_rt_fetch(varno, root);
	Var *var;

	/* Assume we already have adequate lock */
	Relation relation = table_open(rte->relid, NoLock);

	int numattrs = RelationGetNumberOfAttributes(relation);
	for (int attrno = 1; attrno <= numattrs; attrno++)
	{
		Form_pg_attribute att_tup = TupleDescAttr(relation->rd_att, attrno - 1);

		var =
			makeVar(varno, attrno, att_tup->atttypid, att_tup->atttypmod, att_tup->attcollation, 0);

		tlist = lappend(tlist, makeTargetEntry((Expr *) var, attrno, NULL, false));
	}

	table_close(relation, NoLock);

	return tlist;
}

Plan *
columnar_index_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
								List *output_targetlist, List *clauses, List *custom_plans)
{
	ColumnarIndexScanPath *cispath = (ColumnarIndexScanPath *) path;
	CustomScan *cscan = makeNode(CustomScan);
	Scan *compressed_scan = linitial(custom_plans);

	cscan->flags = path->flags;
	cscan->methods = &columnar_index_scan_plan_methods;
	cscan->scan.scanrelid = rel->relid;
	cscan->custom_plans = custom_plans;

	compressed_scan->plan.targetlist = build_tlist(root, cispath->info->compressed_rel);

	ListCell *lc;
	List *output_map = NIL;

	foreach (lc, output_targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Ensure(IsA(tle->expr, Var), "output targetlist entries must be Vars");

		Var *var = castNode(Var, tle->expr);
		Assert((Index) var->varno == rel->relid);

		if (var->varattno == cispath->aggregate_attno)
		{
			/* Aggregate column */
			output_map = lappend_int(output_map, cispath->metadata_attno);
		}
		else
		{
			/* Segmentby column */
			AttrNumber compressed_attno = ts_map_attno(cispath->info->chunk_rte->relid,
													   cispath->info->compressed_rte->relid,
													   var->varattno);
			Ensure(compressed_attno != InvalidAttrNumber,
				   "could not map chunk attno to compressed attno");
			output_map = lappend_int(output_map, compressed_attno);
		}
	}

	cscan->custom_private = list_make1(output_map);
	cscan->custom_scan_tlist = output_targetlist;
	cscan->scan.plan.targetlist = output_targetlist;

	return &cscan->scan.plan;
}
