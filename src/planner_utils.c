#include <postgres.h>
#include <nodes/plannodes.h>
#include <miscadmin.h>

#include "planner_utils.h"

static void plantree_walker(Plan **plan, void (*walker) (Plan **, void *), void *ctx);

static inline void
			plantree_walk_subplans(List *plans, void (*walker) (Plan **, void *), void *ctx)
{
	ListCell   *lc;

	if (plans == NIL)
		return;

	foreach(lc, plans)
		plantree_walker((Plan **) &lfirst(lc), walker, ctx);
}

/* A plan tree walker. Similar to planstate_tree_walker in PostgreSQL's
 * nodeFuncs.c, but this walks a Plan tree as opposed to a PlanState tree. */
static void
			plantree_walker(Plan **planptr, void (*walker) (Plan **, void *), void *context)
{
	Plan	   *plan = *planptr;

	if (plan == NULL)
		return;

	check_stack_depth();

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			plantree_walk_subplans(((ModifyTable *) plan)->plans, walker, context);
			break;
		case T_Append:
			plantree_walk_subplans(((Append *) plan)->appendplans, walker, context);
			break;
		case T_MergeAppend:
			plantree_walk_subplans(((MergeAppend *) plan)->mergeplans, walker, context);
			break;
		case T_BitmapAnd:
			plantree_walk_subplans(((BitmapAnd *) plan)->bitmapplans, walker, context);
			break;
		case T_BitmapOr:
			plantree_walk_subplans(((BitmapOr *) plan)->bitmapplans, walker, context);
			break;
		case T_SubqueryScan:
			walker(&((SubqueryScan *) plan)->subplan, context);
			break;
		case T_CustomScan:
			plantree_walk_subplans(((CustomScan *) plan)->custom_plans, walker, context);
			break;
		default:
			break;
	}

	plantree_walker(&outerPlan(plan), walker, context);
	plantree_walker(&innerPlan(plan), walker, context);
	walker(planptr, context);
}

void
			planned_stmt_walker(PlannedStmt *stmt, void (*walker) (Plan **, void *), void *context)
{
	ListCell   *lc;

	plantree_walker(&stmt->planTree, walker, context);

	foreach(lc, stmt->subplans)
		plantree_walker((Plan **) &lfirst(lc), walker, context);
}
