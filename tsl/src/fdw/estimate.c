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

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/*
 * Estimate costs of executing a SQL statement remotely.
 * The given "sql" must be an EXPLAIN command.
 */
static void
get_remote_estimate(const char *sql, TSConnection *conn, double *rows, int *width,
					Cost *startup_cost, Cost *total_cost)
{
	AsyncResponseResult *volatile rsp = NULL;

	/* PGresult must be released before leaving this function. */
	PG_TRY();
	{
		AsyncRequest *req;
		PGresult *res;
		char *line;
		char *p;
		int n;

		/*
		 * Execute EXPLAIN remotely.
		 */
		req = async_request_send(conn, sql);
		rsp = async_request_wait_any_result(req);
		res = async_response_result_get_pg_result(rsp);

		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			async_response_report_error((AsyncResponse *) rsp, ERROR);

		/*
		 * Extract cost numbers for topmost plan node.  Note we search for a
		 * left paren from the end of the line to avoid being confused by
		 * other uses of parentheses.
		 */
		line = PQgetvalue(res, 0, 0);
		p = strrchr(line, '(');
		if (p == NULL)
			elog(ERROR, "could not interpret EXPLAIN output: \"%s\"", line);
		n = sscanf(p, "(cost=%lf..%lf rows=%lf width=%d)", startup_cost, total_cost, rows, width);
		if (n != 4)
			elog(ERROR, "could not interpret EXPLAIN output: \"%s\"", line);

		async_response_result_close(rsp);
	}
	PG_CATCH();
	{
		if (NULL != rsp)
			async_response_result_close(rsp);

		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * fdw_estimate_path_cost_size
 *		Get cost and size estimates for a foreign scan on given foreign
 *		relation either a base relation or an upper relation containing
 *		foreign relations.
 *
 * param_join_conds are the parameterization clauses with outer relations.
 * pathkeys specify the expected sort order if any for given path being costed.
 *
 * The function returns the cost and size estimates in p_row, p_width,
 * p_startup_cost and p_total_cost variables.
 */
void
fdw_estimate_path_cost_size(PlannerInfo *root, RelOptInfo *rel, List *param_join_conds,
							List *pathkeys, double *p_rows, int *p_width, Cost *p_startup_cost,
							Cost *p_total_cost)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(rel);
	double rows = 0;
	double retrieved_rows;
	int width = 0;
	Cost startup_cost = 0;
	Cost total_cost = 0;
	Cost cpu_per_tuple;

	/*
	 * If the table or the server is configured to use remote estimates,
	 * connect to the foreign server and execute EXPLAIN to estimate the
	 * number of rows selected by the restriction+join clauses.  Otherwise,
	 * estimate rows using whatever statistics we have locally, in a way
	 * similar to ordinary tables.
	 */
	if (fpinfo->use_remote_estimate)
	{
		List *remote_param_join_conds;
		List *local_param_join_conds;
		StringInfoData sql;
		TSConnection *conn;
		Selectivity local_sel;
		QualCost local_cost;
		List *fdw_scan_tlist = NIL;
		List *remote_conds;

		/* Required only to be passed to deparseSelectStmtForRel */
		List *retrieved_attrs;

		/*
		 * param_join_conds might contain both clauses that are safe to send
		 * across, and clauses that aren't.
		 */
		classify_conditions(root,
							rel,
							param_join_conds,
							&remote_param_join_conds,
							&local_param_join_conds);

		if (IS_JOIN_REL(rel))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("foreign joins are not supported")));

		/* Build the list of columns to be fetched from the foreign server. */
		if (IS_UPPER_REL(rel))
			fdw_scan_tlist = build_tlist_to_deparse(rel);
		else
			fdw_scan_tlist = NIL;

		/*
		 * The complete list of remote conditions includes everything from
		 * baserestrictinfo plus any extra join_conds relevant to this
		 * particular path.
		 */
		remote_conds = list_concat(list_copy(remote_param_join_conds), fpinfo->remote_conds);

		/*
		 * Construct EXPLAIN query including the desired SELECT, FROM, and
		 * WHERE clauses. Params and other-relation Vars are replaced by dummy
		 * values, so don't request params_list.
		 */
		initStringInfo(&sql);
		appendStringInfoString(&sql, "EXPLAIN ");
		deparseSelectStmtForRel(&sql,
								root,
								rel,
								fdw_scan_tlist,
								remote_conds,
								pathkeys,
								false,
								&retrieved_attrs,
								NULL,
								fpinfo->sca);

		/* Get the remote estimate */
		conn = remote_dist_txn_get_connection(fpinfo->user, REMOTE_TXN_NO_PREP_STMT);
		get_remote_estimate(sql.data, conn, &rows, &width, &startup_cost, &total_cost);

		retrieved_rows = rows;

		/* Factor in the selectivity of the locally-checked quals */
		local_sel =
			clauselist_selectivity(root, local_param_join_conds, rel->relid, JOIN_INNER, NULL);
		local_sel *= fpinfo->local_conds_sel;

		rows = clamp_row_est(rows * local_sel);

		/* Add in the eval cost of the locally-checked quals */
		startup_cost += fpinfo->local_conds_cost.startup;
		total_cost += fpinfo->local_conds_cost.per_tuple * retrieved_rows;
		cost_qual_eval(&local_cost, local_param_join_conds, root);
		startup_cost += local_cost.startup;
		total_cost += local_cost.per_tuple * retrieved_rows;
	}
	else
	{
		Cost run_cost = 0;

		/*
		 * We don't support join conditions in this mode (hence, no
		 * parameterized paths can be made).
		 */
		Assert(param_join_conds == NIL);

		/*
		 * Use rows/width estimates made by set_baserel_size_estimates() for
		 * base foreign relations.
		 */
		rows = rel->rows;
		width = rel->reltarget->width;

		/* Back into an estimate of the number of retrieved rows. */
		retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

		/*
		 * We will come here again and again with different set of pathkeys
		 * that caller wants to cost. We don't need to calculate the cost of
		 * bare scan each time. Instead, use the costs if we have cached them
		 * already.
		 */
		if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0)
		{
			startup_cost = fpinfo->rel_startup_cost;
			run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
		}
		else if (IS_JOIN_REL(rel))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("foreign joins are not supported")));
		}
		else if (IS_UPPER_REL(rel))
		{
			TsFdwRelInfo *ofpinfo;
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

			ofpinfo = fdw_relinfo_get(fpinfo->outerrel);

			/* Get rows and width from input rel */
			input_rows = ofpinfo->rows;
			width = ofpinfo->width;

			/* Collect statistics about aggregates for estimating costs. */
			MemSet(&aggcosts, 0, sizeof(AggClauseCosts));
			if (root->parse->hasAggs)
			{
				get_agg_clause_costs(root,
									 (Node *) fpinfo->grouped_tlist,
									 AGGSPLIT_SIMPLE,
									 &aggcosts);

				/*
				 * The cost of aggregates in the HAVING qual will be the same
				 * for each child as it is for the parent, so there's no need
				 * to use a translated version of havingQual.
				 */
				get_agg_clause_costs(root,
									 (Node *) root->parse->havingQual,
									 AGGSPLIT_SIMPLE,
									 &aggcosts);
			}

			/* Get number of grouping columns and possible number of groups */
			num_group_cols = list_length(root->parse->groupClause);
			num_groups = estimate_num_groups(root,
											 get_sortgrouplist_exprs(root->parse->groupClause,
																	 fpinfo->grouped_tlist),
											 input_rows,
											 NULL);

			/*
			 * Number of rows expected from foreign server will be same as
			 * that of number of groups.
			 */
			rows = retrieved_rows = num_groups;

			/*-----
			 * Startup cost includes:
			 *	  1. Startup cost for underneath input * relation
			 *	  2. Cost of performing aggregation, per cost_agg()
			 *	  3. Startup cost for PathTarget eval
			 *-----
			 */
			startup_cost = ofpinfo->rel_startup_cost;
			startup_cost += aggcosts.transCost.startup;
			startup_cost += aggcosts.transCost.per_tuple * input_rows;
			startup_cost += (cpu_operator_cost * num_group_cols) * input_rows;
			startup_cost += ptarget->cost.startup;

			/*-----
			 * Run time cost includes:
			 *	  1. Run time cost of underneath input relation
			 *	  2. Run time cost of performing aggregation, per cost_agg()
			 *	  3. PathTarget eval cost for each output row
			 *-----
			 */
			run_cost = ofpinfo->rel_total_cost - ofpinfo->rel_startup_cost;
			run_cost += aggcosts.finalCost * num_groups;
			run_cost += cpu_tuple_cost * num_groups;
			run_cost += ptarget->cost.per_tuple * num_groups;
		}
		else
		{
			/* Clamp retrieved rows estimates to at most rel->tuples. */
			retrieved_rows = Min(retrieved_rows, rel->tuples);

			/*
			 * Cost as though this were a seqscan, which is pessimistic.  We
			 * effectively imagine the local_conds are being evaluated
			 * remotely, too.
			 */
			startup_cost = 0;
			run_cost = 0;
			run_cost += seq_page_cost * rel->pages;

			startup_cost += rel->baserestrictcost.startup;
			cpu_per_tuple = cpu_tuple_cost + rel->baserestrictcost.per_tuple;
			run_cost += cpu_per_tuple * rel->tuples;
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
			startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
			run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
		}

		total_cost = startup_cost + run_cost;
	}

	/*
	 * Cache the costs for scans without any pathkeys or parameterization
	 * before adding the costs for transferring data from the foreign server.
	 * These costs are useful for costing the join between this relation and
	 * another foreign relation or to calculate the costs of paths with
	 * pathkeys for this relation, when the costs can not be obtained from the
	 * foreign server. This function will be called at least once for every
	 * foreign relation without pathkeys and parameterization.
	 */
	if (pathkeys == NIL && param_join_conds == NIL)
	{
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

	/* Return results. */
	*p_rows = rows;
	*p_width = width;
	*p_startup_cost = startup_cost;
	*p_total_cost = total_cost;
}
