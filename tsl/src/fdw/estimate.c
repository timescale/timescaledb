/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/nodes.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/cost.h>
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>
#include <optimizer/paths.h>
#include <utils/selfuncs.h>
#include <utils/rel.h>
#include <lib/stringinfo.h>
#include <miscadmin.h>

#include <remote/connection.h>
#include <remote/async.h>
#include <remote/dist_txn.h>

#include <compat/compat.h>
#include "relinfo.h"
#include "estimate.h"
#include "deparse.h"

/* If no remote estimates, assume a sort costs 5% extra.  */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.05

typedef struct CostEstimate
{
	double rows;
	double retrieved_rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	Cost cpu_per_tuple;
	Cost run_cost;
} CostEstimate;

static bool
find_first_aggref_walker(Node *node, Aggref **aggref)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref))
	{
		*aggref = castNode(Aggref, node);
		return true;
	}

	return expression_tree_walker(node, find_first_aggref_walker, aggref);
}

/*
 * Get the AggsSplit mode of a relation.
 *
 * The AggSplit (partial or full aggregation) affects costing.
 * All aggregates to compute this relation must have the same
 * mode, so we only check mode on first match.
 */
static AggSplit
get_aggsplit(PlannerInfo *root, RelOptInfo *rel)
{
	Aggref *agg;
	Assert(root->parse->hasAggs);

	if (find_first_aggref_walker((Node *) rel->reltarget->exprs, &agg))
		return agg->aggsplit;

	/* If the aggregate is only referenced in the HAVING clause it will
	 * not be present in the targetlist so we have to check HAVING clause too. */
	if (root->hasHavingQual && find_first_aggref_walker((Node *) root->parse->havingQual, &agg))
		return agg->aggsplit;

	/* Since PlannerInfo has hasAggs true (checked in caller) we should
	 * never get here and always find an Aggref. */
	elog(ERROR, "no aggref found in targetlist or HAVING clause");
	pg_unreachable();
}

#define REL_HAS_CACHED_COSTS(fpinfo)                                                               \
	((fpinfo)->rel_startup_cost >= 0 && (fpinfo)->rel_total_cost >= 0 &&                           \
	 (fpinfo)->rel_retrieved_rows >= 0)

/*
 * Adjust the cost estimates of a foreign grouping path to include the cost of
 * generating properly-sorted output.
 */
static void
adjust_foreign_grouping_path_cost(PlannerInfo *root, List *pathkeys, double retrieved_rows,
								  double width, double limit_tuples, Cost *p_startup_cost,
								  Cost *p_run_cost)
{
	/*
	 * If the GROUP BY clause isn't sort-able, the plan chosen by the remote
	 * side is unlikely to generate properly-sorted output, so it would need
	 * an explicit sort; adjust the given costs with cost_sort().  Likewise,
	 * if the GROUP BY clause is sort-able but isn't a superset of the given
	 * pathkeys, adjust the costs with that function.  Otherwise, adjust the
	 * costs by applying the same heuristic as for the scan or join case.
	 */
	if (!grouping_is_sortable(root->parse->groupClause) ||
		!pathkeys_contained_in(pathkeys, root->group_pathkeys))
	{
		Path sort_path; /* dummy for result of cost_sort */

		cost_sort(&sort_path,
				  root,
				  pathkeys,
				  *p_startup_cost + *p_run_cost,
				  retrieved_rows,
				  width,
				  0.0,
				  work_mem,
				  limit_tuples);

		*p_startup_cost = sort_path.startup_cost;
		*p_run_cost = sort_path.total_cost - sort_path.startup_cost;
	}
	else
	{
		/*
		 * The default extra cost seems too large for foreign-grouping cases;
		 * add 1/4th of that default.
		 */
		double sort_multiplier = 1.0 + (DEFAULT_FDW_SORT_MULTIPLIER - 1.0) * 0.25;

		*p_startup_cost *= sort_multiplier;
		*p_run_cost *= sort_multiplier;
	}
}

/*
 * fdw_estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign relation
 *		either a base relation or a join between foreign relations or an upper
 *		relation containing foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 * fpextra specifies additional post-scan/join-processing steps such as the
 * final sort and the LIMIT restriction.
 *
 * The function returns the cost and size estimates in p_rows, p_width,
 * p_startup_cost and p_total_cost variables.
 */
void
fdw_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *foreignrel, List *param_join_conds,
							List *pathkeys, TsFdwPathExtraData *fpextra, double *p_rows,
							int *p_width, Cost *p_startup_cost, Cost *p_total_cost)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(foreignrel);
	double rows;
	double retrieved_rows;
	int width;
	Cost startup_cost;
	Cost total_cost;
	Cost run_cost = 0;

	/* Make sure the core code has set up the relation's reltarget */
	Assert(foreignrel->reltarget);

	if (IS_JOIN_REL(foreignrel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign joins are not supported")));

	/*
	 * We don't support join conditions in this mode (hence, no
	 * parameterized paths can be made).
	 */
	Assert(param_join_conds == NIL);

	/*
	 * In Timescale, we should be relying on local ANALYZE statistics because
	 * they get updated with remote stats appropriately
	 *
	 * We will come here again and again with different set of pathkeys or
	 * additional post-scan/join-processing steps that caller wants to
	 * cost.  We don't need to calculate the cost/size estimates for the
	 * underlying scan, join, or grouping each time.  Instead, use those
	 * estimates if we have cached them already.
	 */
	if (REL_HAS_CACHED_COSTS(fpinfo))
	{
		Assert(fpinfo->rel_retrieved_rows >= 0);

		rows = fpinfo->rows;
		retrieved_rows = fpinfo->rel_retrieved_rows;
		width = fpinfo->width;
		startup_cost = fpinfo->rel_startup_cost;
		run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;

		/*
		 * If we estimate the costs of a foreign scan or a foreign join
		 * with additional post-scan/join-processing steps, the scan or
		 * join costs obtained from the cache wouldn't yet contain the
		 * eval costs for the final scan/join target, which would've been
		 * updated by apply_scanjoin_target_to_paths(); add the eval costs
		 * now.
		 */
		if (fpextra && !IS_UPPER_REL(foreignrel))
		{
			/* Shouldn't get here unless we have LIMIT */
			Assert(fpextra->has_limit);
			Assert(foreignrel->reloptkind == RELOPT_BASEREL ||
				   foreignrel->reloptkind == RELOPT_JOINREL);
			startup_cost += foreignrel->reltarget->cost.startup;
			run_cost += foreignrel->reltarget->cost.per_tuple * rows;
		}
	}
	else if (IS_JOIN_REL(foreignrel))
	{
		TsFdwRelInfo *fpinfo_i;
		TsFdwRelInfo *fpinfo_o;
		QualCost join_cost;
		QualCost remote_conds_cost;
		double nrows;

		/* Use rows/width estimates made by the core code. */
		rows = foreignrel->rows;
		width = foreignrel->reltarget->width;

		/* For join we expect inner and outer relations set */
		Assert(fpinfo->innerrel && fpinfo->outerrel);

		fpinfo_i = fdw_relinfo_get(fpinfo->innerrel);
		fpinfo_o = fdw_relinfo_get(fpinfo->outerrel);

		/* Estimate of number of rows in cross product */
		nrows = fpinfo_i->rows * fpinfo_o->rows;

		/*
		 * Back into an estimate of the number of retrieved rows.  Just in
		 * case this is nuts, clamp to at most nrows.
		 */
		retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
		retrieved_rows = Min(retrieved_rows, nrows);

		/*
		 * The cost of foreign join is estimated as cost of generating
		 * rows for the joining relations + cost for applying quals on the
		 * rows.
		 */

		/*
		 * Calculate the cost of clauses pushed down to the foreign server
		 */
		cost_qual_eval(&remote_conds_cost, fpinfo->remote_conds, root);
		/* Calculate the cost of applying join clauses */
		cost_qual_eval(&join_cost, fpinfo->joinclauses, root);

		/*
		 * Startup cost includes startup cost of joining relations and the
		 * startup cost for join and other clauses. We do not include the
		 * startup cost specific to join strategy (e.g. setting up hash
		 * tables) since we do not know what strategy the foreign server
		 * is going to use.
		 */
		startup_cost = fpinfo_i->rel_startup_cost + fpinfo_o->rel_startup_cost;
		startup_cost += join_cost.startup;
		startup_cost += remote_conds_cost.startup;
		startup_cost += fpinfo->local_conds_cost.startup;

		/*
		 * Run time cost includes:
		 *
		 * 1. Run time cost (total_cost - startup_cost) of relations being
		 * joined
		 *
		 * 2. Run time cost of applying join clauses on the cross product
		 * of the joining relations.
		 *
		 * 3. Run time cost of applying pushed down other clauses on the
		 * result of join
		 *
		 * 4. Run time cost of applying nonpushable other clauses locally
		 * on the result fetched from the foreign server.
		 */
		run_cost = fpinfo_i->rel_total_cost - fpinfo_i->rel_startup_cost;
		run_cost += fpinfo_o->rel_total_cost - fpinfo_o->rel_startup_cost;
		run_cost += nrows * join_cost.per_tuple;
		nrows = clamp_row_est(nrows * fpinfo->joinclause_sel);
		run_cost += nrows * remote_conds_cost.per_tuple;
		run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;

		/* Add in tlist eval cost for each output row */
		startup_cost += foreignrel->reltarget->cost.startup;
		run_cost += foreignrel->reltarget->cost.per_tuple * rows;
	}
	else if (IS_UPPER_REL(foreignrel))
	{
		RelOptInfo *outerrel = fpinfo->outerrel;
		TsFdwRelInfo *ofpinfo;
		AggClauseCosts aggcosts;
		double input_rows;
		int numGroupCols;
		double numGroups = 1;

		/* The upper relation should have its outer relation set */
		Assert(outerrel);
		/* and that outer relation should have its reltarget set */
		Assert(outerrel->reltarget);

		/*
		 * This cost model is mixture of costing done for sorted and
		 * hashed aggregates in cost_agg().  We are not sure which
		 * strategy will be considered at remote side, thus for
		 * simplicity, we put all startup related costs in startup_cost
		 * and all finalization and run cost are added in total_cost.
		 */

		ofpinfo = fdw_relinfo_get(outerrel);

		/* Get rows from input rel */
		input_rows = ofpinfo->rows;

		/* Collect statistics about aggregates for estimating costs. */
		MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
		if (root->parse->hasAggs)
		{
			/*
			 * Get the aggsplit to use in order to support push-down of partial
			 * aggregation
			 */
			AggSplit aggsplit = get_aggsplit(root, foreignrel);

			get_agg_clause_costs_compat(root, (Node *) fpinfo->grouped_tlist, aggsplit, &aggcosts);
		}

		/* Get number of grouping columns and possible number of groups */
		numGroupCols = list_length(root->parse->groupClause);
		numGroups = estimate_num_groups_compat(root,
											   get_sortgrouplist_exprs(root->parse->groupClause,
																	   fpinfo->grouped_tlist),
											   input_rows,
											   NULL,
											   NULL);

		/*
		 * Get the retrieved_rows and rows estimates.  If there are HAVING
		 * quals, account for their selectivity.
		 */
		if (root->parse->havingQual)
		{
			/* Factor in the selectivity of the remotely-checked quals */
			retrieved_rows = clamp_row_est(
				numGroups *
				clauselist_selectivity(root, fpinfo->remote_conds, 0, JOIN_INNER, NULL));
			/* Factor in the selectivity of the locally-checked quals */
			rows = clamp_row_est(retrieved_rows * fpinfo->local_conds_sel);
		}
		else
		{
			/*
			 * Number of rows expected from data node will be same as
			 * that of number of groups.
			 */
			rows = retrieved_rows = numGroups;
		}

		/* Use width estimate made by the core code. */
		width = foreignrel->reltarget->width;

		/*-----
		 * Startup cost includes:
		 *	  1. Startup cost for underneath input relation, adjusted for
		 *	     tlist replacement by apply_scanjoin_target_to_paths()
		 *	  2. Cost of performing aggregation, per cost_agg()
		 *-----
		 */
		startup_cost = ofpinfo->rel_startup_cost;
		startup_cost += outerrel->reltarget->cost.startup;
		startup_cost += aggcosts.transCost.startup;
		startup_cost += aggcosts.transCost.per_tuple * input_rows;
		startup_cost += aggcosts.finalCost.startup;
		startup_cost += (cpu_operator_cost * numGroupCols) * input_rows;

		/*-----
		 * Run time cost includes:
		 *	  1. Run time cost of underneath input relation, adjusted for
		 *	     tlist replacement by apply_scanjoin_target_to_paths()
		 *	  2. Run time cost of performing aggregation, per cost_agg()
		 *-----
		 */
		run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
		run_cost += outerrel->reltarget->cost.per_tuple * input_rows;
		run_cost += aggcosts.finalCost.per_tuple * numGroups;
		run_cost += cpu_tuple_cost * numGroups;

		/* Account for the eval cost of HAVING quals, if any */
		if (root->parse->havingQual)
		{
			QualCost remote_cost;

			/* Add in the eval cost of the remotely-checked quals */
			cost_qual_eval(&remote_cost, fpinfo->remote_conds, root);
			startup_cost += remote_cost.startup;
			run_cost += remote_cost.per_tuple * numGroups;
			/* Add in the eval cost of the locally-checked quals */
			startup_cost += fpinfo->local_conds_cost.startup;
			run_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		}

		/* Add in tlist eval cost for each output row */
		startup_cost += foreignrel->reltarget->cost.startup;
		run_cost += foreignrel->reltarget->cost.per_tuple * rows;
	}
	else
	{
		Cost cpu_per_tuple;

		/* Use rows/width estimates made by set_baserel_size_estimates. */
		rows = foreignrel->rows;
		width = foreignrel->reltarget->width;

		/*
		 * Back into an estimate of the number of retrieved rows.  Just in
		 * case this is nuts, clamp to at most foreignrel->tuples.
		 */
		retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);
		retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

		/*
		 * Cost as though this were a seqscan, which is pessimistic.  We
		 * effectively imagine the local_conds are being evaluated
		 * remotely, too.
		 */
		startup_cost = 0;
		run_cost = 0;
		run_cost += seq_page_cost * foreignrel->pages;

		startup_cost += foreignrel->baserestrictcost.startup;
		cpu_per_tuple = cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
		run_cost += cpu_per_tuple * foreignrel->tuples;

		/* Add in tlist eval cost for each output row */
		startup_cost += foreignrel->reltarget->cost.startup;
		run_cost += foreignrel->reltarget->cost.per_tuple * rows;
	}

	/*
	 * Without remote estimates, we have no real way to estimate the cost
	 * of generating sorted output.  It could be free if the query plan
	 * the remote side would have chosen generates properly-sorted output
	 * anyway, but in most cases it will cost something.  Estimate a value
	 * high enough that we won't pick the sorted path when the ordering
	 * isn't locally useful, but low enough that we'll err on the side of
	 * pushing down the ORDER BY clause when it's useful to do so.
	 */
	if (pathkeys != NIL)
	{
		if (IS_UPPER_REL(foreignrel))
		{
			Assert(fpinfo->stage == UPPERREL_GROUP_AGG ||
				   fpinfo->stage == UPPERREL_PARTIAL_GROUP_AGG);
			adjust_foreign_grouping_path_cost(root,
											  pathkeys,
											  retrieved_rows,
											  width,
											  fpextra ? fpextra->limit_tuples : -1,
											  &startup_cost,
											  &run_cost);
		}
		else
		{
			startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		}
	}

	total_cost = startup_cost + run_cost;

	/* Adjust the cost estimates if we have LIMIT */
	if (fpextra && fpextra->has_limit)
	{
		adjust_limit_rows_costs(&rows,
								&startup_cost,
								&total_cost,
								fpextra->offset_est,
								fpextra->count_est);
		retrieved_rows = rows;
	}

	/*
	 * If this includes the final sort step, the given target, which will be
	 * applied to the resulting path, might have different expressions from
	 * the foreignrel's reltarget (see make_sort_input_target()); adjust tlist
	 * eval costs.
	 */
	if (fpextra && fpextra->has_final_sort && fpextra->target != foreignrel->reltarget)
	{
		QualCost oldcost = foreignrel->reltarget->cost;
		QualCost newcost = fpextra->target->cost;

		startup_cost += newcost.startup - oldcost.startup;
		total_cost += newcost.startup - oldcost.startup;
		total_cost += (newcost.per_tuple - oldcost.per_tuple) * rows;
	}

	/*
	 * Cache the retrieved rows and cost estimates for scans, joins, or
	 * groupings without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps, before adding the costs for
	 * transferring data from the foreign server.  These estimates are useful
	 * for costing remote joins involving this relation or costing other
	 * remote operations on this relation such as remote sorts and remote
	 * LIMIT restrictions, when the costs can not be obtained from the foreign
	 * server.  This function will be called at least once for every foreign
	 * relation without any parameterization, pathkeys, or additional
	 * post-scan/join-processing steps.
	 */
	if (pathkeys == NIL && param_join_conds == NIL && fpextra == NULL)
	{
		fpinfo->rel_retrieved_rows = retrieved_rows;
		fpinfo->rel_startup_cost = startup_cost;
		fpinfo->rel_total_cost = total_cost;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	startup_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_startup_cost;
	total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
	total_cost += cpu_tuple_cost * retrieved_rows;

	/*
	 * If we have LIMIT, we should prefer performing the restriction remotely
	 * rather than locally, as the former avoids extra row fetches from the
	 * remote that the latter might cause.  But since the core code doesn't
	 * account for such fetches when estimating the costs of the local
	 * restriction (see create_limit_path()), there would be no difference
	 * between the costs of the local restriction and the costs of the remote
	 * restriction estimated above if we don't use remote estimates (except
	 * for the case where the foreignrel is a grouping relation, the given
	 * pathkeys is not NIL, and the effects of a bounded sort for that rel is
	 * accounted for in costing the remote restriction).  Tweak the costs of
	 * the remote restriction to ensure we'll prefer it if LIMIT is a useful
	 * one.
	 */
	if (fpextra && fpextra->has_limit && fpextra->limit_tuples > 0 &&
		fpextra->limit_tuples < fpinfo->rows)
	{
		Assert(fpinfo->rows > 0);
		total_cost -= (total_cost - startup_cost) * 0.05 * (fpinfo->rows - fpextra->limit_tuples) /
					  fpinfo->rows;
	}

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}
