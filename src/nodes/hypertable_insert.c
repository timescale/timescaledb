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
#include <optimizer/plancat.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <foreign/foreign.h>
#include <catalog/pg_type.h>

#include "hypertable_insert.h"
#include "chunk_dispatch_state.h"
#include "chunk_dispatch_plan.h"
#include "hypertable_cache.h"
#include "hypertable_data_node.h"
#include "cross_module_fn.h"
#include "guc.h"

static List *
get_chunk_dispatch_states(PlanState *substate)
{
	switch (nodeTag(substate))
	{
		case T_CustomScanState:
		{
			CustomScanState *csstate = castNode(CustomScanState, substate);
			ListCell *lc;
			List *result = NIL;

			if (ts_chunk_dispatch_is_state(substate))
				return list_make1(substate);

			/*
			 * In case of remote insert, the ChunkDispatchState could be a
			 * child of a ServerDispatchState subnode.
			 */
			foreach (lc, csstate->custom_ps)
			{
				PlanState *ps = lfirst(lc);

				result = list_concat(result, get_chunk_dispatch_states(ps));
			}
			return result;
		}
		case T_ResultState:
			return get_chunk_dispatch_states(castNode(ResultState, substate)->ps.lefttree);
		default:
			break;
	}

	return NIL;
}

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
	ModifyTableState *mtstate;
	PlanState *ps;
	List *chunk_dispatch_states = NIL;
	ListCell *lc;
	int i;

	ps = ExecInitNode(&state->mt->plan, estate, eflags);
	node->custom_ps = list_make1(ps);
	mtstate = castNode(ModifyTableState, ps);

	/*
	 * Find all ChunkDispatchState subnodes and set their parent
	 * ModifyTableState node
	 */
	for (i = 0; i < mtstate->mt_nplans; i++)
	{
		List *substates = get_chunk_dispatch_states(mtstate->mt_plans[i]);

		chunk_dispatch_states = list_concat(chunk_dispatch_states, substates);
	}

	/* Ensure that we found at least one ChunkDispatchState node */
	Assert(list_length(chunk_dispatch_states) > 0);

	foreach (lc, chunk_dispatch_states)
		ts_chunk_dispatch_state_set_parent((ChunkDispatchState *) lfirst(lc), mtstate);
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

static void
hypertable_insert_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	HypertableInsertState *state = (HypertableInsertState *) node;
	List *fdw_private = linitial_node(List, state->mt->fdwPrivLists);
	ModifyTableState *mtstate = linitial(node->custom_ps);
	Index rti = state->mt->nominalRelation;
	RangeTblEntry *rte = rt_fetch(rti, es->rtable);
	const char *relname = get_rel_name(rte->relid);
	const char *namespace = get_namespace_name(get_rel_namespace(rte->relid));

	Assert(IsA(mtstate, ModifyTableState));

	if (NULL != state->fdwroutine)
	{
		appendStringInfo(es->str, "Insert on distributed hypertable");

		if (es->verbose)
		{
			List *node_names = NIL;
			ListCell *lc;

			appendStringInfo(es->str,
							 " %s.%s\n",
							 quote_identifier(namespace),
							 quote_identifier(relname));

			foreach (lc, state->serveroids)
			{
				ForeignServer *server = GetForeignServer(lfirst_oid(lc));

				node_names = lappend(node_names, server->servername);
			}

			ExplainPropertyList("Data nodes", node_names, es);
		}
		else
			appendStringInfo(es->str, " %s\n", quote_identifier(relname));

		/* Let the foreign data wrapper add its part of the explain, but only
		 * if this was using the non-direct API. */
		if (NIL != fdw_private && state->fdwroutine->ExplainForeignModify)
			state->fdwroutine->ExplainForeignModify(mtstate,
													mtstate->resultRelInfo,
													fdw_private,
													0,
													es);
	}
}

static CustomExecMethods hypertable_insert_state_methods = {
	.CustomName = "HypertableInsertState",
	.BeginCustomScan = hypertable_insert_begin,
	.EndCustomScan = hypertable_insert_end,
	.ExecCustomScan = hypertable_insert_exec,
	.ReScanCustomScan = hypertable_insert_rescan,
	.ExplainCustomScan = hypertable_insert_explain,
};

static Node *
hypertable_insert_state_create(CustomScan *cscan)
{
	HypertableInsertState *state;
	ModifyTable *mt = castNode(ModifyTable, linitial(cscan->custom_plans));
	Oid serverid;

	state = (HypertableInsertState *) newNode(sizeof(HypertableInsertState), T_CustomScanState);
	state->cscan_state.methods = &hypertable_insert_state_methods;
	state->mt = mt;
	state->mt->arbiterIndexes = linitial(cscan->custom_private);

	/*
	 * Get the list of data nodes to insert on.
	 */
	state->serveroids = lsecond(cscan->custom_private);

	/*
	 * Get the FDW routine for the first data node. It should be the same for
	 * all of them
	 */
	if (NIL != state->serveroids)
	{
		serverid = linitial_oid(state->serveroids);
		state->fdwroutine = GetFdwRoutineByServerId(serverid);
		Assert(state->fdwroutine != NULL);
	}
	else
		state->fdwroutine = NULL;

	return (Node *) state;
}

static CustomScanMethods hypertable_insert_plan_methods = {
	.CustomName = "HypertableInsert",
	.CreateCustomScanState = hypertable_insert_state_create,
};

/*
 * Make a targetlist to meet CustomScan expectations.
 *
 * When a CustomScan isn't scanning a real relation (scanrelid=0), it will build
 * a virtual TupleDesc for the scan "input" based on custom_scan_tlist. The
 * "output" targetlist is then expected to reference the attributes of the
 * input's TupleDesc. Without projection, the targetlist will be only Vars with
 * varno set to INDEX_VAR (to indicate reference to the TupleDesc instead of a
 * real relation) and matching the order of the attributes in the TupleDesc.
 *
 * Any other order, or non-Vars, will lead to the CustomScan performing
 * projection.
 *
 * Since the CustomScan for hypertable insert just wraps ModifyTable, no
 * projection is needed, so we'll build a targetlist to avoid this.
 */
static List *
make_var_targetlist(const List *tlist)
{
	List *new_tlist = NIL;
	ListCell *lc;
	int resno = 1;

	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var *var = makeVarFromTargetEntry(INDEX_VAR, tle);

		var->varattno = resno;
		new_tlist = lappend(new_tlist, makeTargetEntry(&var->xpr, resno, tle->resname, false));
		resno++;
	}

	return new_tlist;
}

/*
 * Plan the private FDW data for a remote hypertable. Note that the private data
 * for a result relation is a list, so we return a list of lists, one for each
 * result relation.  In case of no remote modify, we still need to return a list
 * of empty lists.
 */
static void
plan_remote_modify(PlannerInfo *root, HypertableInsertPath *hipath, ModifyTable *mt,
				   FdwRoutine *fdwroutine)
{
	List *fdw_private_list = NIL;
	/* Keep any existing direct modify plans */
	Bitmapset *direct_modify_plans = mt->fdwDirectModifyPlans;
	ListCell *lc;
	int i = 0;

	/* Iterate all subplans / result relations to check which ones are inserts
	 * into hypertables. In case we find a remote hypertable insert, we either
	 * have to plan it using the FDW or, in case of data node dispatching, we
	 * just need to mark the plan as "direct" to let ModifyTable know it
	 * should not invoke the regular FDW modify API. */
	foreach (lc, mt->resultRelations)
	{
		Index rti = lfirst_int(lc);
		RangeTblEntry *rte = planner_rt_fetch(rti, root);
		List *fdwprivate = NIL;
		bool is_data_node_dispatch = bms_is_member(i, hipath->data_node_dispatch_plans);

		/* If data node batching is supported, we won't actually use the FDW
		 * direct modify API (everything is done in DataNodeDispatch), but we
		 * need to trick ModifyTable to believe we're doing direct modify so
		 * that it doesn't invoke the non-direct FDW API for inserts. Instead,
		 * it should handle only returning projections as if it was a direct
		 * modify. We do this by adding the result relation's plan to
		 * fdwDirectModifyPlans. See ExecModifyTable for more details. */
		if (is_data_node_dispatch)
			direct_modify_plans = bms_add_member(direct_modify_plans, i);

		if (!is_data_node_dispatch && NULL != fdwroutine && fdwroutine->PlanForeignModify != NULL &&
			ts_is_hypertable(rte->relid))
			fdwprivate = fdwroutine->PlanForeignModify(root, mt, rti, i);
		else
			fdwprivate = NIL;

		i++;
		fdw_private_list = lappend(fdw_private_list, fdwprivate);
	}

	mt->fdwDirectModifyPlans = direct_modify_plans;
	mt->fdwPrivLists = fdw_private_list;
}

/*
 * Construct the HypertableInsert's target list based on the ModifyTable's
 * target list, which now exists after having been created by
 * set_plan_references().
 */
void
ts_hypertable_insert_fixup_tlist(Plan *plan)
{
	if (IsA(plan, CustomScan))
	{
		CustomScan *cscan = (CustomScan *) plan;

		if (cscan->methods == &hypertable_insert_plan_methods)
		{
			ModifyTable *mt = linitial_node(ModifyTable, cscan->custom_plans);

			if (mt->plan.targetlist == NIL)
			{
				cscan->custom_scan_tlist = NIL;
				cscan->scan.plan.targetlist = NIL;
			}
			else
			{
				/* The input is the output of the child ModifyTable node */
				cscan->custom_scan_tlist = mt->plan.targetlist;

				/* The output is a direct mapping of the input */
				cscan->scan.plan.targetlist = make_var_targetlist(mt->plan.targetlist);
			}
		}
	}
}

static Plan *
hypertable_insert_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
							  List *tlist, List *clauses, List *custom_plans)
{
	HypertableInsertPath *hipath = (HypertableInsertPath *) best_path;
	CustomScan *cscan = makeNode(CustomScan);
	ModifyTable *mt = linitial(custom_plans);
	FdwRoutine *fdwroutine = NULL;

	Assert(IsA(mt, ModifyTable));

	cscan->methods = &hypertable_insert_plan_methods;
	cscan->custom_plans = list_make1(mt);
	cscan->scan.scanrelid = 0;

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;

	if (NIL != hipath->serveroids)
	{
		/* Get the foreign data wrapper routines for the first data node. Should be
		 * the same for all data nodes. */
		Oid serverid = linitial_oid(hipath->serveroids);

		fdwroutine = GetFdwRoutineByServerId(serverid);
	}

	/*
	 * A remote hypertable is not a foreign table since it cannot have indexes
	 * in that case. But we run the FDW planning for the hypertable here as if
	 * it was a foreign table. This is because when we do an FDW insert of a
	 * foreign table chunk, we actually would like to do that as if the INSERT
	 * happened on the root table. Thus we need the plan state from the root
	 * table, which we can reuse on every chunk. This plan state includes,
	 * e.g., a deparsed INSERT statement that references the hypertable
	 * instead of a chunk.
	 */
	plan_remote_modify(root, hipath, mt, fdwroutine);

	/* The tlist is always NIL since the ModifyTable subplan doesn't have its
	 * targetlist set until set_plan_references (setrefs.c) is run */
	Assert(tlist == NIL);

	/* Target list handling here needs special attention. Intuitively, we'd like
	 * to adopt the target list of the ModifyTable subplan we wrap without
	 * further projection. For a CustomScan this means setting the "input"
	 * custom_scan_tlist to the ModifyTable's target list and having an "output"
	 * targetlist that references the TupleDesc that is created from the
	 * custom_scan_tlist at execution time. Now, while this seems
	 * straight-forward, there are several things with how ModifyTable nodes are
	 * handled in the planner that complicates this:
	 *
	 * - First, ModifyTable doesn't have a targetlist set at this point, and
	 *   it is only set later in set_plan_references (setrefs.c) if there's a
	 *	 RETURNING clause.
	 *
	 * - Second, top-level plan nodes, except for ModifyTable nodes, need to
	 *	 have a targetlist matching root->processed_tlist. This is asserted in
	 *	 apply_tlist_labeling, which is called in create_plan (createplan.c)
	 *	 immediately after this function returns. ModifyTable is exempted
	 *	 because it doesn't always have a targetlist that matches
	 *	 processed_tlist. So, even if we had access to ModifyTable's
	 *	 targetlist here we wouldn't be able to use it since we're a
	 *	 CustomScan and thus not exempted.
	 *
	 * - Third, a CustomScan's targetlist should reference the attributes of the
	 *   TupleDesc that gets created from the custom_scan_tlist at the start of
	 *   execution. This means we need to make the targetlist into all Vars with
	 *   attribute numbers that correspond to the TupleDesc instead of result
	 *   relation in the ModifyTable.
	 *
	 * To get around these issues, we set the targetlist here to
	 * root->processed_tlist, and at the end of planning when the ModifyTable's
	 * targetlist is set, we go back and fix up the CustomScan's targetlist.
	 */
	cscan->scan.plan.targetlist = copyObject(root->processed_tlist);
	cscan->custom_scan_tlist = cscan->scan.plan.targetlist;

	/*
	 * we save the original list of arbiter indexes here
	 * because we modify that list during execution and
	 * we still need the original list in case that plan
	 * gets reused.
	 *
	 * We also pass on the data nodes to insert on.
	 */
	cscan->custom_private = list_make2(mt->arbiterIndexes, hipath->serveroids);

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
	Bitmapset *data_node_dispatch_plans = NULL;
	Hypertable *ht = NULL;
	HypertableInsertPath *hipath;
	int i = 0;

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

			if (hypertable_is_distributed(ht) && ts_guc_max_insert_batch_size > 0)
			{
				/* Remember that this will become a data node dispatch plan. We
				 * need to know later whether or not to plan this using the
				 * FDW API. */
				data_node_dispatch_plans = bms_add_member(data_node_dispatch_plans, i);
				subpath = ts_cm_functions->data_node_dispatch_path_create(root, mtpath, rti, i);
			}
			else
				subpath = ts_chunk_dispatch_path_create(root, mtpath, rti, i);
		}

		i++;
		subpaths = lappend(subpaths, subpath);
	}

	if (NULL == ht)
		elog(ERROR, "no hypertable found in INSERT plan");

	hipath = palloc0(sizeof(HypertableInsertPath));

	/* Copy costs, etc. */
	memcpy(&hipath->cpath.path, path, sizeof(Path));
	hipath->cpath.path.type = T_CustomPath;
	hipath->cpath.path.pathtype = T_CustomScan;
	hipath->cpath.custom_paths = list_make1(mtpath);
	hipath->cpath.methods = &hypertable_insert_path_methods;
	hipath->data_node_dispatch_plans = data_node_dispatch_plans;
	hipath->serveroids = ts_hypertable_get_available_data_node_server_oids(ht);
	path = &hipath->cpath.path;
	mtpath->subpaths = subpaths;

	ts_cache_release(hcache);

	return path;
}
