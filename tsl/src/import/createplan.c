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
 *
 * backend/optimizer/plan/createplan.c
 */


#include <postgres.h>

#include "compat/compat.h"

#include "createplan.h"

#include <miscadmin.h>
#include <nodes/pathnodes.h>
#include <nodes/plannodes.h>
#include <optimizer/cost.h>

/*
 * Copy of the Postgres' static function from createplan.c.
 *
 * Some places in Columnar Scan plannign build Sort nodes that don't have a directly
 * corresponding Path node.  The cost of the sort is, or should have been,
 * included in the cost of the Path node we're working from, but since it's
 * not split out, we have to re-figure it using cost_sort().  This is just
 * to label the Sort node nicely for EXPLAIN.
 *
 * limit_tuples is as for cost_sort (in particular, pass -1 if no limit)
 */
void
ts_label_sort_with_costsize(PlannerInfo *root, Sort *plan, double limit_tuples)
{
	Plan *lefttree = plan->plan.lefttree;
	Path sort_path; /* dummy for result of cost_sort */

	/*
	 * This function shouldn't have to deal with IncrementalSort plans because
	 * they are only created from corresponding Path nodes.
	 */
	Assert(IsA(plan, Sort));

	cost_sort(&sort_path,
			  root,
			  NIL,
#if PG18_GE
			  lefttree->disabled_nodes,
#endif
			  lefttree->total_cost,
			  lefttree->plan_rows,
			  lefttree->plan_width,
			  0.0,
			  work_mem,
			  limit_tuples);
	plan->plan.startup_cost = sort_path.startup_cost;
	plan->plan.total_cost = sort_path.total_cost;
	plan->plan.plan_rows = lefttree->plan_rows;
	plan->plan.plan_width = lefttree->plan_width;
	plan->plan.parallel_aware = false;
	plan->plan.parallel_safe = lefttree->parallel_safe;
}
