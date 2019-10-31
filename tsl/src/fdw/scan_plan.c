/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <parser/parsetree.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/planmain.h>
#include <optimizer/cost.h>
#include <optimizer/clauses.h>
#include <optimizer/tlist.h>
#include <optimizer/paths.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/selfuncs.h>
#include <miscadmin.h>
#include <fmgr.h>

#include <export.h>
#include <planner.h>

#include "estimate.h"
#include "relinfo.h"
#include "utils.h"
#include "deparse.h"
#include "scan_plan.h"

/*
 * get_useful_pathkeys_for_relation
 *		Determine which orderings of a relation might be useful.
 *
 * Getting data in sorted order can be useful either because the requested
 * order matches the final output ordering for the overall query we're
 * planning, or because it enables an efficient merge join.  Here, we try
 * to figure out which pathkeys to consider.
 */
static List *
get_useful_pathkeys_for_relation(PlannerInfo *root, RelOptInfo *rel)
{
	List *useful_pathkeys_list = NIL;
	ListCell *lc;

	/*
	 * Pushing the query_pathkeys to the data node is always worth
	 * considering, because it might let us avoid a local sort.
	 */
	if (root->query_pathkeys)
	{
		bool query_pathkeys_ok = true;

		foreach (lc, root->query_pathkeys)
		{
			PathKey *pathkey = (PathKey *) lfirst(lc);
			EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
			Expr *em_expr;

			/*
			 * The planner and executor don't have any clever strategy for
			 * taking data sorted by a prefix of the query's pathkeys and
			 * getting it to be sorted by all of those pathkeys. We'll just
			 * end up resorting the entire data set.  So, unless we can push
			 * down all of the query pathkeys, forget it.
			 *
			 * is_foreign_expr would detect volatile expressions as well, but
			 * checking ec_has_volatile here saves some cycles.
			 */
			if (pathkey_ec->ec_has_volatile || !(em_expr = find_em_expr_for_rel(pathkey_ec, rel)) ||
				!is_foreign_expr(root, rel, em_expr))
			{
				query_pathkeys_ok = false;
				break;
			}
		}

		if (query_pathkeys_ok)
			useful_pathkeys_list = list_make1(list_copy(root->query_pathkeys));
	}

	return useful_pathkeys_list;
}

static void
add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, Path *epq_path,
								CreatePathFunc create_scan_path,
								CreateUpperPathFunc create_upper_path)
{
	List *useful_pathkeys_list = NIL; /* List of all pathkeys */
	ListCell *lc;

	Assert((create_scan_path || create_upper_path) && !(create_scan_path && create_upper_path));

	useful_pathkeys_list = get_useful_pathkeys_for_relation(root, rel);

	/* Create one path for each set of pathkeys we found above. */
	foreach (lc, useful_pathkeys_list)
	{
		double rows;
		int width;
		Cost startup_cost;
		Cost total_cost;
		List *useful_pathkeys = lfirst(lc);
		Path *sorted_epq_path;

		fdw_estimate_path_cost_size(root,
									rel,
									useful_pathkeys,
									&rows,
									&width,
									&startup_cost,
									&total_cost);

		/*
		 * The EPQ path must be at least as well sorted as the path itself, in
		 * case it gets used as input to a mergejoin.
		 */
		sorted_epq_path = epq_path;
		if (sorted_epq_path != NULL &&
			!pathkeys_contained_in(useful_pathkeys, sorted_epq_path->pathkeys))
			sorted_epq_path =
				(Path *) create_sort_path(root, rel, sorted_epq_path, useful_pathkeys, -1.0);

		if (create_scan_path)
		{
			Assert(IS_SIMPLE_REL(rel));
			add_path(rel,
					 create_scan_path(root,
									  rel,
									  NULL,
									  rows,
									  startup_cost,
									  total_cost,
									  useful_pathkeys,
									  NULL,
									  sorted_epq_path,
									  NIL));
		}
		else
		{
			Assert(IS_UPPER_REL(rel));
			add_path(rel,
					 create_upper_path(root,
									   rel,
									   NULL,
									   rows,
									   startup_cost,
									   total_cost,
									   useful_pathkeys,
									   sorted_epq_path,
									   NIL));
		}
	}
}

void
fdw_add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, Path *epq_path,
									CreatePathFunc create_scan_path)
{
	add_paths_with_pathkeys_for_rel(root, rel, epq_path, create_scan_path, NULL);
}

void
fdw_add_upper_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, Path *epq_path,
										  CreateUpperPathFunc create_upper_path)
{
	add_paths_with_pathkeys_for_rel(root, rel, epq_path, NULL, create_upper_path);
}

void
fdw_scan_info_init(ScanInfo *scaninfo, PlannerInfo *root, RelOptInfo *rel, Path *best_path,
				   List *scan_clauses)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);
	List *remote_exprs = NIL;
	List *local_exprs = NIL;
	List *params_list = NIL;
	List *current_time_idx = NIL;
	List *fdw_scan_tlist = NIL;
	List *fdw_recheck_quals = NIL;
	List *retrieved_attrs;
	List *fdw_private;
	Index scan_relid;
	StringInfoData sql;
	ListCell *lc;

	if (IS_SIMPLE_REL(rel))
	{
		/*
		 * For base relations, set scan_relid as the relid of the relation.
		 */
		scan_relid = rel->relid;

		/*
		 * In a base-relation scan, we must apply the given scan_clauses.
		 *
		 * Separate the scan_clauses into those that can be executed remotely
		 * and those that can't.  baserestrictinfo clauses that were
		 * previously determined to be safe or unsafe by classifyConditions
		 * are found in fpinfo->remote_conds and fpinfo->local_conds. Anything
		 * else in the scan_clauses list will be a join clause, which we have
		 * to check for remote-safety.
		 *
		 * Note: the join clauses we see here should be the exact same ones
		 * previously examined by GetForeignPaths.  Possibly it'd be worth
		 * passing forward the classification work done then, rather than
		 * repeating it here.
		 *
		 * This code must match "extract_actual_clauses(scan_clauses, false)"
		 * except for the additional decision about remote versus local
		 * execution.
		 */
		foreach (lc, scan_clauses)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			/* Ignore any pseudoconstants, they're dealt with elsewhere */
			if (rinfo->pseudoconstant)
				continue;

			if (list_member_ptr(fpinfo->remote_conds, rinfo))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else if (list_member_ptr(fpinfo->local_conds, rinfo))
				local_exprs = lappend(local_exprs, rinfo->clause);
			else if (is_foreign_expr(root, rel, rinfo->clause))
				remote_exprs = lappend(remote_exprs, rinfo->clause);
			else
				local_exprs = lappend(local_exprs, rinfo->clause);
		}

		/*
		 * For a base-relation scan, we have to support EPQ recheck, which
		 * should recheck all the remote quals.
		 */
		fdw_recheck_quals = remote_exprs;
	}
	else if (IS_JOIN_REL(rel))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign joins are not supported")));
	}
	else
	{
		/*
		 * Upper relation - set scan_relid to 0.
		 */
		scan_relid = 0;

		/*
		 * For a join rel, baserestrictinfo is NIL and we are not considering
		 * parameterization right now, so there should be no scan_clauses for
		 * a joinrel or an upper rel either.
		 */
		Assert(!scan_clauses);

		/*
		 * Instead we get the conditions to apply from the fdw_private
		 * structure.
		 */
		remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
		local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

		/*
		 * We leave fdw_recheck_quals empty in this case, since we never need
		 * to apply EPQ recheck clauses.  In the case of a joinrel, EPQ
		 * recheck is handled elsewhere --- see GetForeignJoinPaths().  If
		 * we're planning an upperrel (ie, remote grouping or aggregation)
		 * then there's no EPQ to do because SELECT FOR UPDATE wouldn't be
		 * allowed, and indeed we *can't* put the remote clauses into
		 * fdw_recheck_quals because the unaggregated Vars won't be available
		 * locally.
		 */

		/* Build the list of columns to be fetched from the data node. */
		fdw_scan_tlist = build_tlist_to_deparse(rel);
	}

	/*
	 * Build the query string to be sent for execution, and identify
	 * expressions to be sent as parameters.
	 */
	initStringInfo(&sql);
	deparseSelectStmtForRel(&sql,
							root,
							rel,
							fdw_scan_tlist,
							remote_exprs,
							best_path->pathkeys,
							false,
							&retrieved_attrs,
							&params_list,
							fpinfo->sca,
							&current_time_idx);

	/* Remember remote_exprs for possible use by PlanDirectModify */
	fpinfo->final_remote_exprs = remote_exprs;

	/*
	 * Build the fdw_private list that will be available to the executor.
	 * Items in the list must match order in enum FdwScanPrivateIndex.
	 */
	fdw_private = list_make5(makeString(sql.data),
							 retrieved_attrs,
							 makeInteger(fpinfo->fetch_size),
							 makeInteger(fpinfo->server->serverid),
							 (fpinfo->sca != NULL ? list_copy(fpinfo->sca->chunk_oids) : NIL));
	fdw_private = lappend(fdw_private, current_time_idx);
	Assert(!IS_JOIN_REL(rel));

	if (IS_UPPER_REL(rel))
		fdw_private = lappend(fdw_private, makeString(fpinfo->relation_name->data));

	scaninfo->fdw_private = fdw_private;
	scaninfo->fdw_scan_tlist = fdw_scan_tlist;
	scaninfo->fdw_recheck_quals = fdw_recheck_quals;
	scaninfo->local_exprs = local_exprs;
	scaninfo->params_list = params_list;
	scaninfo->scan_relid = scan_relid;
	scaninfo->data_node_serverid = rel->serverid;
}

/*
 * Merge FDW options from input relations into a new set of options for a join
 * or an upper rel.
 *
 * For a join relation, FDW-specific information about the inner and outer
 * relations is provided using fpinfo_i and fpinfo_o.  For an upper relation,
 * fpinfo_o provides the information for the input relation; fpinfo_i is
 * expected to NULL.
 */
static void
merge_fdw_options(TsFdwRelInfo *fpinfo, const TsFdwRelInfo *fpinfo_o, const TsFdwRelInfo *fpinfo_i)
{
	/* We must always have fpinfo_o. */
	Assert(fpinfo_o);

	/* fpinfo_i may be NULL, but if present the servers must both match. */
	Assert(!fpinfo_i || fpinfo_i->server->serverid == fpinfo_o->server->serverid);

	/* Currently, we don't support JOINs, so Asserting fpinfo_i is NULL here
	 * in the meantime. */
	Assert(fpinfo_i == NULL);

	/*
	 * Copy the server specific FDW options.  (For a join, both relations come
	 * from the same server, so the server options should have the same value
	 * for both relations.)
	 */
	fpinfo->fdw_startup_cost = fpinfo_o->fdw_startup_cost;
	fpinfo->fdw_tuple_cost = fpinfo_o->fdw_tuple_cost;
	fpinfo->shippable_extensions = fpinfo_o->shippable_extensions;
	fpinfo->fetch_size = fpinfo_o->fetch_size;
}

/*
 * Assess whether the aggregation, grouping and having operations can be pushed
 * down to the data node.  As a side effect, save information we obtain in
 * this function to TsFdwRelInfo of the input relation.
 */
static bool
foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel, Node *havingQual)
{
	Query *query = root->parse;
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(grouped_rel);
	PathTarget *grouping_target = grouped_rel->reltarget;
	TsFdwRelInfo *ofpinfo;
	List *aggvars;
	ListCell *lc;
	int i;
	List *tlist = NIL;

	/* Cannot have grouping sets since that wouldn't be a distinct coverage of
	 * all partition keys */
	Assert(query->groupingSets == NIL);

	/* Get the fpinfo of the underlying scan relation. */
	ofpinfo = (TsFdwRelInfo *) fdw_relinfo_get(fpinfo->outerrel);

	/*
	 * If underlying scan relation has any local conditions, those conditions
	 * are required to be applied before performing aggregation.  Hence the
	 * aggregate cannot be pushed down.
	 */
	if (ofpinfo->local_conds)
		return false;

	/*
	 * Examine grouping expressions, as well as other expressions we'd need to
	 * compute, and check whether they are safe to push down to the data
	 * node.  All GROUP BY expressions will be part of the grouping target
	 * and thus there is no need to search for them separately.  Add grouping
	 * expressions into target list which will be passed to data node.
	 */
	i = 0;
	foreach (lc, grouping_target->exprs)
	{
		Expr *expr = (Expr *) lfirst(lc);
		Index sgref = get_pathtarget_sortgroupref(grouping_target, i);
		ListCell *l;

		/* Check whether this expression is part of GROUP BY clause */
		if (sgref && get_sortgroupref_clause_noerr(sgref, query->groupClause))
		{
			TargetEntry *tle;

			/*
			 * If any GROUP BY expression is not shippable, then we cannot
			 * push down aggregation to the data node.
			 */
			if (!is_foreign_expr(root, grouped_rel, expr))
				return false;

			/*
			 * Pushable, so add to tlist.  We need to create a TLE for this
			 * expression and apply the sortgroupref to it.  We cannot use
			 * add_to_flat_tlist() here because that avoids making duplicate
			 * entries in the tlist.  If there are duplicate entries with
			 * distinct sortgrouprefs, we have to duplicate that situation in
			 * the output tlist.
			 */
			tle = makeTargetEntry(expr, list_length(tlist) + 1, NULL, false);
			tle->ressortgroupref = sgref;
			tlist = lappend(tlist, tle);
		}
		else
		{
			/*
			 * Non-grouping expression we need to compute.  Is it shippable?
			 */
			if (is_foreign_expr(root, grouped_rel, expr))
			{
				/* Yes, so add to tlist as-is; OK to suppress duplicates */
				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
			else
			{
				/* Not pushable as a whole; extract its Vars and aggregates */
				aggvars = pull_var_clause((Node *) expr, PVC_INCLUDE_AGGREGATES);

				/*
				 * If any aggregate expression is not shippable, then we
				 * cannot push down aggregation to the data node.
				 */
				if (!is_foreign_expr(root, grouped_rel, (Expr *) aggvars))
					return false;

				/*
				 * Add aggregates, if any, into the targetlist.  Plain Vars
				 * outside an aggregate can be ignored, because they should be
				 * either same as some GROUP BY column or part of some GROUP
				 * BY expression.  In either case, they are already part of
				 * the targetlist and thus no need to add them again.  In fact
				 * including plain Vars in the tlist when they do not match a
				 * GROUP BY column would cause the data node to complain
				 * that the shipped query is invalid.
				 */
				foreach (l, aggvars)
				{
					Expr *expr = (Expr *) lfirst(l);

					if (IsA(expr, Aggref))
						tlist = add_to_flat_tlist(tlist, list_make1(expr));
				}
			}
		}

		i++;
	}

	/*
	 * Classify the pushable and non-pushable HAVING clauses and save them in
	 * remote_conds and local_conds of the grouped rel's fpinfo.
	 */
	if (havingQual)
	{
		ListCell *lc;

		foreach (lc, (List *) havingQual)
		{
			Expr *expr = (Expr *) lfirst(lc);
			RestrictInfo *rinfo;

			/*
			 * Currently, the core code doesn't wrap havingQuals in
			 * RestrictInfos, so we must make our own.
			 */
			Assert(!IsA(expr, RestrictInfo));
			rinfo = make_restrictinfo(expr,
									  true,
									  false,
									  false,
									  root->qual_security_level,
									  grouped_rel->relids,
									  NULL,
									  NULL);
			if (is_foreign_expr(root, grouped_rel, expr))
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	/*
	 * If there are any local conditions, pull Vars and aggregates from it and
	 * check whether they are safe to pushdown or not.
	 */
	if (fpinfo->local_conds)
	{
		List *aggvars = NIL;
		ListCell *lc;

		foreach (lc, fpinfo->local_conds)
		{
			RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

			aggvars = list_concat(aggvars,
								  pull_var_clause((Node *) rinfo->clause, PVC_INCLUDE_AGGREGATES));
		}

		foreach (lc, aggvars)
		{
			Expr *expr = (Expr *) lfirst(lc);

			/*
			 * If aggregates within local conditions are not safe to push
			 * down, then we cannot push down the query.  Vars are already
			 * part of GROUP BY clause which are checked above, so no need to
			 * access them again here.
			 */
			if (IsA(expr, Aggref))
			{
				if (!is_foreign_expr(root, grouped_rel, expr))
					return false;

				tlist = add_to_flat_tlist(tlist, list_make1(expr));
			}
		}
	}

	/* Store generated targetlist */
	fpinfo->grouped_tlist = tlist;

	/* Safe to pushdown */
	fpinfo->pushdown_safe = true;

	/*
	 * Set cached relation costs to some negative value, so that we can detect
	 * when they are set to some sensible costs, during one (usually the
	 * first) of the calls to fdw_estimate_path_cost_size().
	 */
	fpinfo->rel_startup_cost = -1;
	fpinfo->rel_total_cost = -1;

	/*
	 * Set the string describing this grouped relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name, "Aggregate on (%s)", ofpinfo->relation_name->data);

	return true;
}

/*
 * add_foreign_grouping_paths
 *		Add foreign path for grouping and/or aggregation.
 *
 * Given input_rel represents the underlying scan.  The paths are added to the
 * given grouped_rel.
 */
static void
add_foreign_grouping_paths(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *grouped_rel,
						   GroupPathExtraData *extra, CreateUpperPathFunc create_path)
{
	Query *parse = root->parse;
	TsFdwRelInfo *ifpinfo = fdw_relinfo_get(input_rel);
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(grouped_rel);
	Path *grouppath;
	double rows;
	int width;
	Cost startup_cost;
	Cost total_cost;

	/* Nothing to be done, if there is no grouping or aggregation required. */
	if (!parse->groupClause && !parse->groupingSets && !parse->hasAggs && !root->hasHavingQual)
		return;

	/* save the input_rel as outerrel in fpinfo */
	fpinfo->outerrel = input_rel;

	/*
	 * Copy foreign table, data node, user mapping, FDW options etc.
	 * details from the input relation's fpinfo.
	 */
	fpinfo->table = ifpinfo->table;
	fpinfo->server = ifpinfo->server;
	fpinfo->sca = ifpinfo->sca;
	merge_fdw_options(fpinfo, ifpinfo, NULL);

	/*
	 * Assess if it is safe to push down aggregation and grouping.
	 *
	 * Use HAVING qual from extra. In case of child partition, it will have
	 * translated Vars.
	 */
	if (!foreign_grouping_ok(root, grouped_rel, extra->havingQual))
		return;

	/* Estimate the cost of push down */
	fdw_estimate_path_cost_size(root, grouped_rel, NIL, &rows, &width, &startup_cost, &total_cost);

	/* Now update this information in the fpinfo */
	fpinfo->rows = rows;
	fpinfo->width = width;
	fpinfo->startup_cost = startup_cost;
	fpinfo->total_cost = total_cost;

	/* Create and add path to the grouping relation. */
	grouppath = (Path *) create_path(root,
									 grouped_rel,
									 grouped_rel->reltarget,
									 rows,
									 startup_cost,
									 total_cost,
									 NIL, /* no pathkeys */
									 NULL,
									 NIL); /* no fdw_private */

	/* Add generated path into grouped_rel by add_path(). */
	add_path(grouped_rel, grouppath);

	/* Add paths with pathkeys if there's an order by clause */
	if (root->sort_pathkeys != NIL)
		fdw_add_upper_paths_with_pathkeys_for_rel(root, grouped_rel, NULL, create_path);
}

void
fdw_create_upper_paths(TsFdwRelInfo *input_fpinfo, PlannerInfo *root, UpperRelationKind stage,
					   RelOptInfo *input_rel, RelOptInfo *output_rel, void *extra,
					   CreateUpperPathFunc create_path)
{
	Assert(input_fpinfo != NULL);

	/*
	 * If input rel is not safe to pushdown, then simply return as we cannot
	 * perform any post-join operations on the data node.
	 */
	if (!input_fpinfo->pushdown_safe)
		return;

	/* Ignore stages we don't support; and skip any duplicate calls (i.e.,
	 * output_rel->fdw_private has already been set by a previous call to this
	 * function). */
	if ((stage != UPPERREL_GROUP_AGG && stage != UPPERREL_PARTIAL_GROUP_AGG) ||
		output_rel->fdw_private)
		return;

	input_fpinfo = fdw_relinfo_alloc(output_rel, input_fpinfo->type);
	input_fpinfo->pushdown_safe = false;

	add_foreign_grouping_paths(root,
							   input_rel,
							   output_rel,
							   (GroupPathExtraData *) extra,
							   create_path);
}
