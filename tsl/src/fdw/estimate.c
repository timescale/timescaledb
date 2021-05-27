/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/nodes.h>
#include <optimizer/cost.h>
#include <optimizer/clauses.h>
#include <optimizer/tlist.h>
#include <utils/selfuncs.h>
#include <utils/rel.h>
#include <lib/stringinfo.h>

#include <remote/connection.h>
#include <remote/async.h>
#include <remote/dist_txn.h>

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

/*
 * Get the AggsSplit mode of a relation.
 *
 * The AggSplit (partial or full aggregation) affects costing.
 */
static AggSplit
get_aggsplit(RelOptInfo *rel)
{
	ListCell *lc;

	foreach (lc, rel->reltarget->exprs)
	{
		Node *expr = lfirst(lc);

		if (IsA(expr, Aggref))
		{
			/* All aggregates to compute this relation must have the same
			 * mode, so we can return here. */
			return castNode(Aggref, expr)->aggsplit;
		}
	}
	/* Assume AGGSPLIT_SIMPLE (non-partial) aggregate. We really shouldn't
	 * get there though if this function is called on upper rel with an
	 * aggregate. */
	pg_unreachable();

	return AGGSPLIT_SIMPLE;
}

static void
get_upper_rel_estimate(PlannerInfo *root, RelOptInfo *rel, CostEstimate *ce)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);
	TsFdwRelInfo *ofpinfo = fdw_relinfo_get(fpinfo->outerrel);
	PathTarget *ptarget = rel->reltarget;
	AggClauseCosts aggcosts;
	double input_rows;
	int num_group_cols;
	double num_groups = 1;

	/* Make sure the core code set the pathtarget. */
	Assert(ptarget != NULL);

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

	/* Get rows and width from input rel */
	input_rows = ofpinfo->rows;
	ce->width = ofpinfo->width;

	/* Collect statistics about aggregates for estimating costs. */
	MemSet(&aggcosts, 0, sizeof(AggClauseCosts));

	if (root->parse->hasAggs)
	{
		AggSplit aggsplit = get_aggsplit(rel);

		get_agg_clause_costs(root, (Node *) fpinfo->grouped_tlist, aggsplit, &aggcosts);

		/*
		 * The cost of aggregates in the HAVING qual will be the same
		 * for each child as it is for the parent, so there's no need
		 * to use a translated version of havingQual.
		 */
		get_agg_clause_costs(root, (Node *) root->parse->havingQual, aggsplit, &aggcosts);
	}

	/* Get number of grouping columns and possible number of groups */
	num_group_cols = list_length(root->parse->groupClause);
	num_groups = estimate_num_groups(root,
									 get_sortgrouplist_exprs(root->parse->groupClause,
															 fpinfo->grouped_tlist),
									 input_rows,
									 NULL);

	/*
	 * Number of rows expected from data node will be same as
	 * that of number of groups.
	 */
	ce->rows = ce->retrieved_rows = num_groups;

	/*-----
	 * Startup cost includes:
	 *	  1. Startup cost for underneath input * relation
	 *	  2. Cost of performing aggregation, per cost_agg()
	 *	  3. Startup cost for PathTarget eval
	 *-----
	 */
	ce->startup_cost = ofpinfo->rel_startup_cost;
	ce->startup_cost += aggcosts.transCost.startup;
	ce->startup_cost += aggcosts.transCost.per_tuple * input_rows;
	ce->startup_cost += cpu_operator_cost * num_group_cols * input_rows;
	ce->startup_cost += ptarget->cost.startup;

	/*-----
	 * Run time cost includes:
	 *	  1. Run time cost of underneath input relation
	 *	  2. Run time cost of performing aggregation, per cost_agg()
	 *	  3. PathTarget eval cost for each output row
	 *-----
	 */
	ce->run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
	ce->run_cost += aggcosts.finalCost.per_tuple * num_groups;
	ce->run_cost += cpu_tuple_cost * num_groups;
	ce->run_cost += ptarget->cost.per_tuple * num_groups;

	/* Update the relation's number of output rows. Needed on UPPER rels as
	 * "cached" value when we compute costs for different pathkeys */
	rel->rows = ce->rows;
}

static void
get_base_rel_estimate(PlannerInfo *root, RelOptInfo *rel, CostEstimate *ce)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);

	/* Back into an estimate of the number of retrieved rows. */
	ce->retrieved_rows = clamp_row_est(rel->rows / fpinfo->local_conds_sel);

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
}

#define REL_HAS_CACHED_COSTS(fpinfo)                                                               \
	((fpinfo)->rel_startup_cost >= 0 && (fpinfo)->rel_total_cost >= 0 &&                           \
	 (fpinfo)->rel_retrieved_rows >= 0)

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
		/* TODO: check if sort covered by local index and use other sort multiplier */
		ce.startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		ce.run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
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
