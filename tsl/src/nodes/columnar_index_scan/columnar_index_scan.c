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
#include "expression_utils.h"
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
 * Currently supported aggregates are min, max, first, and last.
 * If supported, adds the chunk attno, metadata attno, and aggfnoid to the lists.
 */
static bool
is_supported_aggregate(const CompressionInfo *info, Aggref *aggref, List **aggregate_attnos,
					   List **metadata_attnos, List **aggregate_fnoids)
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
				ts_func_cache_init();

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
			*aggregate_attnos = lappend_int(*aggregate_attnos, chunk_attno);
			*metadata_attnos = lappend_int(*metadata_attnos, meta_attno);
			*aggregate_fnoids = lappend_oid(*aggregate_fnoids, aggref->aggfnoid);
			return true;
		}
	}
	return false;
}

/*
 * Check if we can use a ColumnarIndexScan for the given query.
 *
 * Requirements:
 * - Query has eligible aggregates on columns with metadata
 * - GROUP BY has only segmentby columns
 * - WHERE clause can be fully pushed down to compressed chunk
 * - No HAVING clause
 */
static bool
can_use_columnar_index_scan(PlannerInfo *root, const Chunk *chunk, RelOptInfo *chunk_rel,
							const CompressionInfo *info, List **aggregate_attnos,
							List **metadata_attnos, List **aggregate_fnoids)
{
	Assert(ts_guc_enable_columnarindexscan);

	Query *parse = root->parse;
	ListCell *lc;

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
	 * - One or more eligible aggregates on columns with min/max metadata
	 * - Any segmentby columns
	 */
	foreach (lc, parse->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		if (tle->resjunk)
			continue;

		if (IsA(tle->expr, Aggref))
		{
			if (!is_supported_aggregate(info,
										castNode(Aggref, tle->expr),
										aggregate_attnos,
										metadata_attnos,
										aggregate_fnoids))
				return false;
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
			else
			{
				/* Targetlist references hypertable attnums so we have to translate to chunk */
				AttrNumber chunk_attno =
					ts_map_attno(info->ht_rte->relid, info->chunk_rte->relid, var->varattno);
				if (!bms_is_member(chunk_attno, info->chunk_segmentby_attnos))
					return false;
			}
		}
	}

	/* Must have found at least one aggregate */
	if (*aggregate_fnoids == NIL)
		return false;

	return true;
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
	if (!ts_guc_enable_columnarindexscan || compressed_path->parallel_aware)
		return NULL;

	List *aggregate_attnos = NIL;
	List *metadata_attnos = NIL;
	List *aggregate_fnoids = NIL;
	if (!can_use_columnar_index_scan(root,
									 chunk,
									 chunk_rel,
									 info,
									 &aggregate_attnos,
									 &metadata_attnos,
									 &aggregate_fnoids))
		return NULL;

	ColumnarIndexScanPath *path =
		(ColumnarIndexScanPath *) newNode(sizeof(ColumnarIndexScanPath), T_CustomPath);

	path->custom_path.path.pathtype = T_CustomScan;
	path->custom_path.path.parent = chunk_rel;
	path->custom_path.path.pathtarget = chunk_rel->reltarget;
	path->custom_path.path.param_info = NULL;
	path->custom_path.path.parallel_aware = false;
	path->custom_path.path.parallel_safe = false;
	path->custom_path.path.parallel_workers = 0;
	path->custom_path.path.pathkeys = NIL;
	path->custom_path.flags = 0;
	path->custom_path.custom_paths = list_make1(compressed_path);
	path->custom_path.methods = &columnar_index_scan_path_methods;

	path->info = info;
	path->aggregate_attnos = aggregate_attnos;
	path->metadata_attnos = metadata_attnos;
	path->aggregate_fnoids = aggregate_fnoids;

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

/*
 * Build the output map, custom target list, and remap info for the ColumnarIndexScan.
 *
 * This builds custom_scan_tlist with one entry per aggregate (even if same column).
 * The output_map maps each entry to the right metadata/compressed column.
 * For multiple aggregates on the same column, we create multiple entries
 * as different aggregates may pull in values from different metadata columns.
 *
 * Returns the custom_scan_tlist. The output_map and remap_info are returned
 * via output parameters.
 */
static List *
build_output_map(ColumnarIndexScanPath *cispath, RelOptInfo *rel, List *output_targetlist,
				 List **output_map_out, List **remap_info_out)
{
	ListCell *lc;
	ListCell *agg_lc = list_head(cispath->aggregate_attnos);
	ListCell *meta_lc = list_head(cispath->metadata_attnos);
	ListCell *fnoid_lc = list_head(cispath->aggregate_fnoids);
	List *custom_tlist = NIL;
	List *output_map = NIL;

	/* Three parallel lists for remap_info */
	List *remap_original_positions = NIL;
	List *remap_target_positions = NIL;
	List *remap_aggfnoids = NIL;

	int resno = 0;

	foreach (lc, output_targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Ensure(IsA(tle->expr, Var), "output targetlist entries must be Vars");

		Var *var = castNode(Var, tle->expr);
		Assert((Index) var->varno == rel->relid);
		int original_pos = foreach_current_index(lc) + 1; /* 1-based position */

		/*
		 * Check if this column is used by aggregates. If multiple aggregates
		 * use the same column, we create multiple entries in custom_tlist.
		 */
		bool found_agg = false;
		while (agg_lc != NULL && var->varattno == lfirst_int(agg_lc))
		{
			Assert(meta_lc != NULL && fnoid_lc != NULL);

			AttrNumber metadata_attno = lfirst_int(meta_lc);
			Oid aggfnoid = lfirst_oid(fnoid_lc);

			resno++;

			/* Create a new TargetEntry for this aggregate */
			Var *new_var = copyObject(var);
			TargetEntry *new_tle = makeTargetEntry((Expr *) new_var, resno, NULL, false);
			custom_tlist = lappend(custom_tlist, new_tle);

			/* Map this entry to the metadata column */
			output_map = lappend_int(output_map, metadata_attno);

			/*
			 * Record remapping info when target position differs from original.
			 * After setrefs, Aggrefs reference original_pos (position in output_targetlist).
			 * We need to change them to reference resno (position in custom_tlist).
			 */
			if (resno != original_pos)
			{
				remap_original_positions = lappend_int(remap_original_positions, original_pos);
				remap_target_positions = lappend_int(remap_target_positions, resno);
				remap_aggfnoids = lappend_oid(remap_aggfnoids, aggfnoid);
			}
			found_agg = true;

			/* Advance to next aggregate */
			agg_lc = lnext(cispath->aggregate_attnos, agg_lc);
			meta_lc = lnext(cispath->metadata_attnos, meta_lc);
			fnoid_lc = lnext(cispath->aggregate_fnoids, fnoid_lc);
		}

		if (!found_agg)
		{
			/* Not an aggregate column - segmentby column */
			resno++;

			TargetEntry *new_tle = copyObject(tle);
			new_tle->resno = resno;
			custom_tlist = lappend(custom_tlist, new_tle);

			AttrNumber compressed_attno = ts_map_attno(cispath->info->chunk_rte->relid,
													   cispath->info->compressed_rte->relid,
													   var->varattno);
			Ensure(compressed_attno != InvalidAttrNumber,
				   "could not map chunk attno to compressed attno");
			output_map = lappend_int(output_map, compressed_attno);

			/*
			 * Non-aggregate columns may also need remapping if they shifted position
			 * due to aggregate expansion. Use aggfnoid=InvalidOid to indicate this is
			 * not an aggregate remap entry.
			 */
			if (resno != original_pos)
			{
				remap_original_positions = lappend_int(remap_original_positions, original_pos);
				remap_aggfnoids = lappend_oid(remap_aggfnoids, InvalidOid);
				remap_target_positions = lappend_int(remap_target_positions, resno);
			}
		}
	}

	*output_map_out = output_map;

	/* Store remap_info as three parallel lists, or NIL if no remapping needed */
	if (remap_original_positions != NIL)
		*remap_info_out =
			list_make3(remap_original_positions, remap_target_positions, remap_aggfnoids);
	else
		*remap_info_out = NIL;

	return custom_tlist;
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

	List *output_map = NIL;
	List *remap_info = NIL;
	List *custom_tlist =
		build_output_map(cispath, rel, output_targetlist, &output_map, &remap_info);

	/* Store output_map and remap_info in custom_private */
	cscan->custom_private = list_make2(output_map, remap_info);
	cscan->custom_scan_tlist = custom_tlist;

	/*
	 * Set scan.plan.targetlist to match output_targetlist, which is what the
	 * Aggregate node was planned with. PostgreSQL's setrefs pass will adjust
	 * Var references based on this structure.
	 *
	 * After setrefs, our fix_aggrefs hook will:
	 * 1. Update the Aggregate's references to point to expanded positions in custom_scan_tlist
	 * 2. Rebuild scan.plan.targetlist to match custom_scan_tlist
	 *
	 * Note: custom_scan_tlist may have more entries than output_targetlist when
	 * there are multiple aggregates on the same column (e.g., min(x) and max(x)).
	 */
	cscan->scan.plan.targetlist = copyObject(output_targetlist);

	return &cscan->scan.plan;
}

/*
 * Helper to fix Aggref args in an expression tree.
 * After setrefs, all Aggrefs referencing the same column have the same arg position.
 * We need to fix them based on aggfnoid to reference the correct position.
 *
 * remap_info is list_make3(original_positions, target_positions, aggfnoids)
 */
static bool
fix_aggref_walker(Node *node, List *remap_info)
{
	if (node == NULL)
		return false;

	/* Extract the three parallel lists from remap_info */
	List *original_positions = linitial(remap_info);
	List *target_positions = lsecond(remap_info);
	List *aggfnoids = lthird(remap_info);

	if (IsA(node, Aggref))
	{
		Aggref *aggref = (Aggref *) node;

		/*
		 * Remap all arguments of this aggregate. This is important for aggregates
		 * like first(val, time) which have multiple arguments that may all need
		 * remapping.
		 */
		ListCell *arg_lc;
		foreach (arg_lc, aggref->args)
		{
			TargetEntry *arg_te = lfirst_node(TargetEntry, arg_lc);
			if (!IsA(arg_te->expr, Var))
				continue;

			Var *var = (Var *) arg_te->expr;

			/* Look up remapping for this (position, aggfnoid) combination */
			ListCell *pos_lc = list_head(original_positions);
			ListCell *fnoid_lc = list_head(aggfnoids);
			ListCell *target_lc = list_head(target_positions);

			while (pos_lc != NULL)
			{
				Assert(fnoid_lc != NULL && target_lc != NULL);

				AttrNumber original_pos = lfirst_int(pos_lc);
				Oid entry_aggfnoid = lfirst_oid(fnoid_lc);
				AttrNumber target_pos = lfirst_int(target_lc);

				if (var->varattno == original_pos && aggref->aggfnoid == entry_aggfnoid)
				{
					/* Apply remapping */
					var->varattno = target_pos;
					break;
				}

				pos_lc = lnext(original_positions, pos_lc);
				fnoid_lc = lnext(aggfnoids, fnoid_lc);
				target_lc = lnext(target_positions, target_lc);
			}
		}
		/* Don't recurse into Aggref args, we've handled it */
		return false;
	}

	/*
	 * Fix plain Vars that reference shifted positions (non-aggregate columns).
	 * These are identified by aggfnoid=InvalidOid in remap_info.
	 */
	if (IsA(node, Var))
	{
		Var *var = (Var *) node;

		ListCell *pos_lc = list_head(original_positions);
		ListCell *fnoid_lc = list_head(aggfnoids);
		ListCell *target_lc = list_head(target_positions);

		while (pos_lc != NULL)
		{
			Assert(fnoid_lc != NULL && target_lc != NULL);

			AttrNumber original_pos = lfirst_int(pos_lc);
			Oid entry_aggfnoid = lfirst_oid(fnoid_lc);
			AttrNumber target_pos = lfirst_int(target_lc);

			/* Match non-aggregate remap entries (aggfnoid=InvalidOid) */
			if (!OidIsValid(entry_aggfnoid) && var->varattno == original_pos)
			{
				var->varattno = target_pos;
				break;
			}

			pos_lc = lnext(original_positions, pos_lc);
			fnoid_lc = lnext(aggfnoids, fnoid_lc);
			target_lc = lnext(target_positions, target_lc);
		}
		return false;
	}

	return expression_tree_walker(node, fix_aggref_walker, remap_info);
}

/*
 * Fix Aggref references in an Aggregate node's targetlist, qual, and grpColIdx.
 * remap_info is list_make3(original_positions, target_positions, aggfnoids)
 */
static void
fix_aggregate_aggrefs(Agg *agg, List *remap_info)
{
	fix_aggref_walker((Node *) agg->plan.targetlist, remap_info);
	fix_aggref_walker((Node *) agg->plan.qual, remap_info);

	/* Extract the three parallel lists from remap_info */
	List *original_positions = linitial(remap_info);
	List *target_positions = lsecond(remap_info);
	List *aggfnoids = lthird(remap_info);

	/*
	 * Fix grpColIdx - these are the input column positions for GROUP BY columns.
	 * When non-aggregate columns shift position due to aggregate expansion,
	 * grpColIdx needs to be updated accordingly.
	 */
	for (int i = 0; i < agg->numCols; i++)
	{
		AttrNumber old_attno = agg->grpColIdx[i];

		ListCell *pos_lc = list_head(original_positions);
		ListCell *fnoid_lc = list_head(aggfnoids);
		ListCell *target_lc = list_head(target_positions);

		while (pos_lc != NULL)
		{
			Assert(fnoid_lc != NULL && target_lc != NULL);

			AttrNumber original_pos = lfirst_int(pos_lc);
			Oid entry_aggfnoid = lfirst_oid(fnoid_lc);
			AttrNumber target_pos = lfirst_int(target_lc);

			/* Match non-aggregate remap entries (aggfnoid=InvalidOid) */
			if (!OidIsValid(entry_aggfnoid) && old_attno == original_pos)
			{
				agg->grpColIdx[i] = target_pos;
				break;
			}

			pos_lc = lnext(original_positions, pos_lc);
			fnoid_lc = lnext(aggfnoids, fnoid_lc);
			target_lc = lnext(target_positions, target_lc);
		}
	}
}

/*
 * Recursively process plan tree to fix Aggref references for ColumnarIndexScan.
 */
void
ts_columnar_index_scan_fix_aggrefs(Plan *plan)
{
	if (plan == NULL)
		return;

	/* Recurse to children first */
	ts_columnar_index_scan_fix_aggrefs(plan->lefttree);
	ts_columnar_index_scan_fix_aggrefs(plan->righttree);

	/* Check if this is an Aggregate over ColumnarIndexScan */
	if (IsA(plan, Agg))
	{
		Agg *agg = (Agg *) plan;
		Plan *subplan = agg->plan.lefttree;

		/* ColumnarIndexScan might be below Sort/IncrementalSort nodes */
		if (!ts_is_columnar_index_scan_plan(subplan))
		{
			switch (nodeTag(subplan))
			{
				case T_Sort:
				case T_IncrementalSort:
					subplan = subplan->lefttree;
					break;
				default:
					return;
					break;
			}
		}

		if (subplan && ts_is_columnar_index_scan_plan(subplan))
		{
			CustomScan *cscan = (CustomScan *) subplan;

			/* Get remap_info from custom_private */
			Assert(list_length(cscan->custom_private) == 2);
			List *remap_info = lsecond(cscan->custom_private);
			if (remap_info != NIL)
			{
				fix_aggregate_aggrefs(agg, remap_info);

				/*
				 * Rebuild CustomScan's scan.plan.targetlist to have
				 * entries for all positions in custom_scan_tlist. This is
				 * necessary because setrefs set it up based on the original
				 * plan, but after fix_aggregate_aggrefs, the Aggregate
				 * expects columns at expanded positions.
				 */
				cscan->scan.plan.targetlist =
					ts_build_trivial_custom_output_targetlist(cscan->custom_scan_tlist);

				/*
				 * Also fix targetlists of intermediate nodes (Sort, etc.)
				 * between ColumnarIndexScan and Aggregate. These need to
				 * pass through all columns from the expanded targetlist.
				 * Use OUTER_VAR since these nodes reference their lefttree.
				 */
				Plan *intermediate = agg->plan.lefttree;
				while (intermediate != (Plan *) cscan)
				{
					List *new_tlist = NIL;
					ListCell *lc;
					foreach (lc, cscan->custom_scan_tlist)
					{
						TargetEntry *scan_entry = (TargetEntry *) lfirst(lc);
						Var *var = makeVar(OUTER_VAR,
										   scan_entry->resno,
										   exprType((Node *) scan_entry->expr),
										   exprTypmod((Node *) scan_entry->expr),
										   exprCollation((Node *) scan_entry->expr),
										   0);
						TargetEntry *new_entry = makeTargetEntry((Expr *) var,
																 scan_entry->resno,
																 scan_entry->resname,
																 scan_entry->resjunk);
						new_tlist = lappend(new_tlist, new_entry);
					}
					intermediate->targetlist = new_tlist;
					intermediate = intermediate->lefttree;
				}
			}
		}
	}

	/* Handle other plan node types that might have subplans */
	if (IsA(plan, Append))
	{
		Append *append = (Append *) plan;
		ListCell *lc;
		foreach (lc, append->appendplans)
		{
			ts_columnar_index_scan_fix_aggrefs(lfirst(lc));
		}
	}
	else if (IsA(plan, MergeAppend))
	{
		MergeAppend *merge = (MergeAppend *) plan;
		ListCell *lc;
		foreach (lc, merge->mergeplans)
		{
			ts_columnar_index_scan_fix_aggrefs(lfirst(lc));
		}
	}
	else if (IsA(plan, SubqueryScan))
	{
		SubqueryScan *ss = (SubqueryScan *) plan;
		ts_columnar_index_scan_fix_aggrefs(ss->subplan);
	}
	else if (IsA(plan, CustomScan))
	{
		CustomScan *cs = (CustomScan *) plan;
		ListCell *lc;
		foreach (lc, cs->custom_plans)
		{
			ts_columnar_index_scan_fix_aggrefs(lfirst(lc));
		}
	}
	else if (IsA(plan, BitmapAnd))
	{
		BitmapAnd *ba = (BitmapAnd *) plan;
		ListCell *lc;
		foreach (lc, ba->bitmapplans)
		{
			ts_columnar_index_scan_fix_aggrefs(lfirst(lc));
		}
	}
	else if (IsA(plan, BitmapOr))
	{
		BitmapOr *bo = (BitmapOr *) plan;
		ListCell *lc;
		foreach (lc, bo->bitmapplans)
		{
			ts_columnar_index_scan_fix_aggrefs(lfirst(lc));
		}
	}
}
