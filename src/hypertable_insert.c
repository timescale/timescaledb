/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <executor/nodeModifyTable.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <foreign/foreign.h>
#include <catalog/pg_type.h>

#include "hypertable_insert.h"
#include "chunk_dispatch_state.h"
#include "chunk_dispatch_plan.h"
#include "hypertable_cache.h"

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
	PlanState *ps;

	ps = ExecInitNode(&state->mt->plan, estate, eflags);

	node->custom_ps = list_make1(ps);

	if (IsA(ps, ModifyTableState))
	{
		ModifyTableState *mtstate = (ModifyTableState *) ps;
		int i;

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

					ts_chunk_dispatch_state_set_parent(cdstate, mtstate);
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
	state->mt = (ModifyTable *) linitial(cscan->custom_plans);

	/*
	 * Restore ModifyTable arbiterIndexes to the original value
	 * this is necessary in case this plan gets executed multiple
	 * times in a prepared statement.
	 */
	state->mt->arbiterIndexes = linitial(cscan->custom_private);

	return (Node *) state;
}

static CustomScanMethods hypertable_insert_plan_methods = {
	.CustomName = "HypertableInsert",
	.CreateCustomScanState = hypertable_insert_state_create,
};

/*
 * This copies the target list on the ModifyTable plan node to our wrapping
 * HypertableInsert plan node after set_plan_references() has run. This ensures
 * that the top-level target list reflects the projection done in a RETURNING
 * statement.
 */
void
ts_hypertable_insert_fixup_tlist(Plan *plan)
{
	if (IsA(plan, CustomScan))
	{
		CustomScan *cscan = (CustomScan *) plan;

		if (cscan->methods == &hypertable_insert_plan_methods)
		{
			ModifyTable *mt = linitial(cscan->custom_plans);

			Assert(IsA(mt, ModifyTable));

			cscan->scan.plan.targetlist = copyObject(mt->plan.targetlist);
			cscan->custom_scan_tlist = NIL;
		}
	}

	return;
}

static Plan *
hypertable_insert_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
							  List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	ModifyTable *mt = linitial(custom_plans);

	Assert(IsA(mt, ModifyTable));

	cscan->methods = &hypertable_insert_plan_methods;
	cscan->custom_plans = list_make1(mt);
	cscan->scan.scanrelid = 0;

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;

	/*
	 * Since this is the top-level plan (above ModifyTable) we need to use the
	 * same targetlist as ModifyTable. However, that targetlist is not set at
	 * this point as it is created by setrefs.c at the end of the planning. It
	 * accounts for things like returning lists that might order attributes in
	 * a way that does not match the order in the base relation. To get around
	 * this we use a temporary target list here and later fix it up after the
	 * standard planner has run.
	 */
	cscan->scan.plan.targetlist = copyObject(root->processed_tlist);

	/* Set the custom scan target list for, e.g., explains */
	cscan->custom_scan_tlist = copyObject(cscan->scan.plan.targetlist);

	/*
	 * we save the original list of arbiter indexes here
	 * because we modify that list during execution and
	 * we still need the original list in case that plan
	 * gets reused
	 */
	cscan->custom_private = list_make1(mt->arbiterIndexes);

	return &cscan->scan.plan;
}

static CustomPathMethods hypertable_insert_path_methods = {
	.CustomName = "HypertableInsertPath",
	.PlanCustomPath = hypertable_insert_plan_create,
};

Path *
ts_hypertable_insert_path_create(PlannerInfo *root, ModifyTablePath *mtpath)
{
	Path *path = &mtpath->path;
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *lc_path, *lc_rel;
	List *subpaths = NIL;
	Hypertable *ht = NULL;
	HypertableInsertPath *hipath;

	Assert(list_length(mtpath->subpaths) == list_length(mtpath->resultRelations));

	forboth (lc_path, mtpath->subpaths, lc_rel, mtpath->resultRelations)
	{
		Path *subpath = lfirst(lc_path);
		Index rti = lfirst_int(lc_rel);
		RangeTblEntry *rte = planner_rt_fetch(rti, root);

		ht = ts_hypertable_cache_get_entry(hcache, rte->relid, CACHE_FLAG_MISSING_OK);

		if (ht != NULL)
		{
			if (root->parse->onConflict != NULL &&
				root->parse->onConflict->constraint != InvalidOid)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypertables do not support ON CONFLICT statements that reference "
								"constraints"),
						 errhint("Use column names to infer indexes instead.")));

			/*
			 * We replace the plan with our custom chunk dispatch plan.
			 */
			subpath = ts_chunk_dispatch_path_create(mtpath, subpath, rti, rte->relid);
		}

		subpaths = lappend(subpaths, subpath);
	}

	Assert(NULL != ht);

	ts_cache_release(hcache);

	hipath = palloc0(sizeof(HypertableInsertPath));

	/* Copy costs, etc. */
	memcpy(&hipath->cpath.path, path, sizeof(Path));
	hipath->cpath.path.type = T_CustomPath;
	hipath->cpath.path.pathtype = T_CustomScan;
	hipath->cpath.custom_paths = list_make1(mtpath);
	hipath->cpath.methods = &hypertable_insert_path_methods;
	path = &hipath->cpath.path;
	mtpath->subpaths = subpaths;

	return path;
}
