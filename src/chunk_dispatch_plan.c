#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <utils/rel.h>
#include <catalog/pg_type.h>

#include "chunk_dispatch_plan.h"
#include "chunk_dispatch_state.h"

/*
 * Create a ChunkDispatchState node from this plan. This is the full execution
 * state that replaces the plan node as the plan moves from planning to
 * execution.
 */
static Node *
create_chunk_dispatch_state(CustomScan *cscan)
{
	ChunkDispatchInfo *info = linitial(cscan->custom_private);

	return (Node *) chunk_dispatch_state_create(info->hypertable_relid,
											  linitial(cscan->custom_plans));
}

static CustomScanMethods chunk_dispatch_plan_methods = {
	.CustomName = "ChunkDispatch",
	.CreateCustomScanState = create_chunk_dispatch_state,
};

/* Create a chunk dispatch plan node in the form of a CustomScan node. The
 * purpose of this plan node is to dispatch (route) tuples to the correct chunk
 * in a hypertable.
 *
 * Note that CustomScan nodes cannot be extended (by struct embedding) because
 * they might be copied, therefore we pass any extra info as a ChunkDispatchInfo
 * in the custom_private field.
 *
 * The chunk dispatch plan takes the original tuple-producing subplan, which was
 * part of a ModifyTable node, and uses this subplan to produce new tuples to
 * dispatch.
 */
CustomScan *
chunk_dispatch_plan_create(Plan *subplan, Oid hypertable_relid)
{
	CustomScan *cscan = makeNode(CustomScan);
	ChunkDispatchInfo *info = palloc(sizeof(ChunkDispatchInfo));

	info->hypertable_relid = hypertable_relid;
	cscan->custom_private = list_make1(info);
	cscan->methods = &chunk_dispatch_plan_methods;
	cscan->custom_plans = list_make1(subplan);
	cscan->scan.scanrelid = 0;	/* Indicate this is not a real relation we are
								 * scanning */

	/* Copy costs from the original plan */
	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	/*
	 * Copy target list from parent table. This should work since hypertables
	 * mandate that chunks have identical column definitions
	 */
	cscan->scan.plan.targetlist = subplan->targetlist;
	cscan->custom_scan_tlist = NIL;

	return cscan;
}
