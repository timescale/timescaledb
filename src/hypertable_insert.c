#include <postgres.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <executor/nodeModifyTable.h>
#include <utils/rel.h>
#include <catalog/pg_type.h>

#include "hypertable_insert.h"
#include "chunk_dispatch_info.h"
#include "chunk_dispatch_state.h"

/*
 * HypertableInsert (with corresponding executor node) is a plan node that
 * implements INSERTs for hypertables. It is mostly a wrapper around the
 * ModifyTable plan node that simply calls the wrapped ModifyTable plan without
 * doing much else, apart from some initial state setup.
 *
 * The wrapping is needed to setup state in the execution phase, and give access
 * to the ModifyTableState node to sub-plan states in the PlanState tree. For
 * instance, the ChunkDispatchState node needs to set the arbiter index list in
 * the ModifyTableState node whenever it inserts into a new chunk.
 */


static void
hypertable_insert_begin(CustomScanState *node, EState *estate, int eflags)
{
	HypertableInsertState *state = (HypertableInsertState *) node;
	PlanState  *ps = ExecInitNode(&state->mt->plan, estate, eflags);

	node->custom_ps = list_make1(ps);

	if (IsA(ps, ModifyTableState))
	{
		ModifyTableState *mtstate = (ModifyTableState *) ps;
		int			i;

		/*
		 * Find all ChunkDispatchState subnodes and set their parent
		 * ModifyTableState node
		 */
		for (i = 0; i < mtstate->mt_nplans; i++)
		{
			if (IsA(mtstate->mt_plans[i], CustomScanState))
			{
				CustomScanState *csstate = (CustomScanState *) mtstate->mt_plans[i];

				if (strcmp(csstate->methods->CustomName, CHUNK_DISPATCH_STATE_NAME) == 0)
				{
					ChunkDispatchState *cdstate = (ChunkDispatchState *) mtstate->mt_plans[i];

					chunk_dispatch_state_set_parent(cdstate, mtstate);
				}
			}
		}
	}
}

static TupleTableSlot *
hypertable_insert_exec(CustomScanState *node)
{
	return ExecProcNode(linitial(node->custom_ps));
}

static void
hypertable_insert_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
}

static void
hypertable_insert_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
}

static CustomExecMethods hypertable_insert_state_methods = {
	.CustomName = "HypertableInsertState",
	.BeginCustomScan = hypertable_insert_begin,
	.EndCustomScan = hypertable_insert_end,
	.ExecCustomScan = hypertable_insert_exec,
	.ReScanCustomScan = hypertable_insert_rescan,
};

static Node *
hypertable_insert_state_create(CustomScan *cscan)
{
	HypertableInsertState *state;

	state = (HypertableInsertState *) newNode(sizeof(HypertableInsertState), T_CustomScanState);
	state->cscan_state.methods = &hypertable_insert_state_methods;
	state->mt = (ModifyTable *) cscan->scan.plan.lefttree;

	return (Node *) state;
}

static CustomScanMethods hypertable_insert_plan_methods = {
	.CustomName = "HypertableInsert",
	.CreateCustomScanState = hypertable_insert_state_create,
};

Plan *
hypertable_insert_plan_create(ModifyTable *mt)
{
	CustomScan *cscan = makeNode(CustomScan);

	cscan->methods = &hypertable_insert_plan_methods;
	cscan->custom_plans = list_make1(mt);
	cscan->scan.plan.lefttree = &mt->plan;
	cscan->scan.scanrelid = 0;	/* This is not a real relation */

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;
	cscan->scan.plan.targetlist = mt->plan.targetlist;
	cscan->custom_scan_tlist = NIL;

	return &cscan->scan.plan;
}
