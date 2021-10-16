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

static void
get_upper_rel_estimate(PlannerInfo *root, RelOptInfo *rel, CostEstimate *ce)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);
	TsFdwRelInfo *ofpinfo = fdw_relinfo_get(fpinfo->outerrel);
	AggClauseCosts aggcosts;
	double input_rows;
	int num_group_cols;
	double num_groups = 1;

	/* Make sure the core code set the pathtarget. */
	Assert(rel->reltarget != NULL);

	/*
	 * This cost model is mixture of costing done for sorted and
	 * hashed aggregates in cost_agg().  We are not sure which
	 * strategy will be considered at remote side, thus for
	 * simplicity, we put all startup related costs in startup_cost
	 * and all finalization and run cost are added in total_cost.
	 *
	 * Also, core does not care about costing HAVING expressions and
	 * adding that to the costs.  So similarly, here too we are not
	 * considering remote and local conditions for costing.
	 */

	/* Get rows from input rel */
	input_rows = ofpinfo->rows;

	/* Collect statistics about aggregates for estimating costs. */
	MemSet(&aggcosts, 0, sizeof(AggClauseCosts));

	if (root->parse->hasAggs)
	{
		/* Get the aggsplit to use in order to support push-down of partial
		 * aggregation */
		AggSplit aggsplit = get_aggsplit(root, rel);

		get_agg_clause_costs_compat(root, (Node *) fpinfo->grouped_tlist, aggsplit, &aggcosts);
	}

	/* Get number of grouping columns and possible number of groups */
	num_group_cols = list_length(root->parse->groupClause);
	num_groups = estimate_num_groups_compat(root,
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
		ce->retrieved_rows = clamp_row_est(
			num_groups * clauselist_selectivity(root, fpinfo->remote_conds, 0, JOIN_INNER, NULL));
		/* Factor in the selectivity of the locally-checked quals */
		ce->rows = clamp_row_est(ce->retrieved_rows * fpinfo->local_conds_sel);
	}
	else
	{
		/*
		 * Number of rows expected from data node will be same as
		 * that of number of groups.
		 */
		ce->rows = ce->retrieved_rows = num_groups;
	}

	/* Use width estimate made by the core code. */
	ce->width = rel->reltarget->width;

	/*-----
	 * Startup cost includes:
	 *	  1. Startup cost for underneath input * relation
	 *	  2. Cost of performing aggregation, per cost_agg()
	 *	  3. Startup cost for PathTarget eval
	 *-----
	 */
	ce->startup_cost = ofpinfo->rel_startup_cost;
	ce->startup_cost += rel->reltarget->cost.startup;
	ce->startup_cost += aggcosts.transCost.startup;
	ce->startup_cost += aggcosts.transCost.per_tuple * input_rows;
	ce->startup_cost += aggcosts.finalCost.startup;
	ce->startup_cost += (cpu_operator_cost * num_group_cols) * input_rows;

	/*-----
	 * Run time cost includes:
	 *	  1. Run time cost of underneath input relation, adjusted for
	 *	     tlist replacement by apply_scanjoin_target_to_paths()
	 *	  2. Run time cost of performing aggregation, per cost_agg()
	 *-----
	 */
	ce->run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
	ce->run_cost += rel->reltarget->cost.per_tuple * input_rows;
	ce->run_cost += aggcosts.finalCost.per_tuple * num_groups;
	ce->run_cost += cpu_tuple_cost * num_groups;

	/* Account for the eval cost of HAVING quals, if any */
	if (root->parse->havingQual)
	{
		QualCost remote_cost;

		/* Add in the eval cost of the remotely-checked quals */
		cost_qual_eval(&remote_cost, fpinfo->remote_conds, root);
		ce->startup_cost += remote_cost.startup;
		ce->run_cost += remote_cost.per_tuple * num_groups;
		/* Add in the eval cost of the locally-checked quals */
		ce->startup_cost += fpinfo->local_conds_cost.startup;
		ce->run_cost += fpinfo->local_conds_cost.per_tuple * ce->retrieved_rows;
	}

	/* Add in tlist eval cost for each output row */
	ce->startup_cost += rel->reltarget->cost.startup;
	ce->run_cost += rel->reltarget->cost.per_tuple * ce->rows;
}

static void
get_base_rel_estimate(PlannerInfo *root, RelOptInfo *rel, CostEstimate *ce)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);

	ce->rows = rel->rows;
	ce->width = rel->reltarget->width;

	/* Back into an estimate of the number of retrieved rows. */
	ce->retrieved_rows = clamp_row_est(ce->rows / fpinfo->local_conds_sel);

	/* Clamp retrieved rows estimates to at most rel->tuples. */
	ce->retrieved_rows = Min(ce->retrieved_rows, rel->tuples);

	/*
	 * Cost as though this were a seqscan, which is pessimistic.  We
	 * effectively imagine the local_conds are being evaluated
	 * remotely, too.
	 */
	ce->startup_cost = 0;
	ce->run_cost = 0;
	ce->run_cost += seq_page_cost * rel->pages;

	ce->startup_cost += rel->baserestrictcost.startup;
	ce->cpu_per_tuple = cpu_tuple_cost + rel->baserestrictcost.per_tuple;
	ce->run_cost += ce->cpu_per_tuple * rel->tuples;

	/* Add in tlist eval cost for each output row */
	ce->startup_cost += rel->reltarget->cost.startup;
	ce->run_cost += rel->reltarget->cost.per_tuple * ce->rows;
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
 *		Get cost and size estimates for a foreign scan on given foreign
 *		relation either a base relation or an upper relation containing
 *		foreign relations. Estimate rows using whatever statistics we have
 *      locally, in a way similar to ordinary tables.
 *
 * pathkeys specify the expected sort order if any for given path being costed.
 *
 * The function returns the cost and size estimates in p_row, p_width,
 * p_startup_cost and p_total_cost variables.
 */
void
fdw_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *rel, List *pathkeys, double *p_rows,
							int *p_width, Cost *p_startup_cost, Cost *p_total_cost)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);
	CostEstimate ce = {
		/*
		 * Use rows/width estimates made by set_baserel_size_estimates() for
		 * base foreign relations.
		 */
		.rows = rel->rows,
		.width = rel->reltarget->width,
	};

	if (IS_JOIN_REL(rel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign joins are not supported")));

	/*
	 * We will come here again and again with different set of pathkeys
	 * that caller wants to cost. We don't need to calculate the cost of
	 * bare scan each time. Instead, use the costs if we have cached them
	 * already.
	 */
	if (REL_HAS_CACHED_COSTS(fpinfo))
	{
		ce.rows = fpinfo->rows;
		ce.width = fpinfo->width;
		ce.startup_cost = fpinfo->rel_startup_cost;
		ce.run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
		ce.retrieved_rows = fpinfo->rel_retrieved_rows;
	}
	else if (IS_UPPER_REL(rel))
		get_upper_rel_estimate(root, rel, &ce);
	else
		get_base_rel_estimate(root, rel, &ce);

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
		if (IS_UPPER_REL(rel))
		{
			Assert(rel->reloptkind == RELOPT_UPPER_REL ||
				   rel->reloptkind == RELOPT_OTHER_UPPER_REL);

			/* FIXME: Currently don't have a way to pass on limit here */
			const double limit_tuples = -1;

			adjust_foreign_grouping_path_cost(root,
											  pathkeys,
											  ce.retrieved_rows,
											  ce.width,
											  limit_tuples,
											  &ce.startup_cost,
											  &ce.run_cost);
		}
		else
		{
			ce.startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			ce.run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		}
	}

	ce.total_cost = ce.startup_cost + ce.run_cost;

	/*
	 * Cache the costs for scans without any pathkeys
	 * before adding the costs for transferring data from the data node.
	 * These costs are useful for costing the join between this relation and
	 * another foreign relation or to calculate the costs of paths with
	 * pathkeys for this relation, when the costs can not be obtained from the
	 * data node. This function will be called at least once for every
	 * foreign relation without pathkeys.
	 */
	if (!REL_HAS_CACHED_COSTS(fpinfo) && pathkeys == NIL)
	{
		fpinfo->rel_startup_cost = ce.startup_cost;
		fpinfo->rel_total_cost = ce.total_cost;
		fpinfo->rel_retrieved_rows = ce.retrieved_rows;
	}

	/*
	 * Add some additional cost factors to account for connection overhead
	 * (fdw_startup_cost), transferring data across the network
	 * (fdw_tuple_cost per retrieved row), and local manipulation of the data
	 * (cpu_tuple_cost per retrieved row).
	 */
	ce.startup_cost += fpinfo->fdw_startup_cost;
	ce.total_cost += fpinfo->fdw_startup_cost;
	ce.total_cost += fpinfo->fdw_tuple_cost * ce.retrieved_rows;
	ce.total_cost += cpu_tuple_cost * ce.retrieved_rows;

	/* Return results. */
	*p_rows = ce.rows;
	*p_width = ce.width;
	*p_startup_cost = ce.startup_cost;
	*p_total_cost = ce.total_cost;
}
