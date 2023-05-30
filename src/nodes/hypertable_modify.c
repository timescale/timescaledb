/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <executor/execPartition.h>
#include <executor/nodeModifyTable.h>
#include <foreign/foreign.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/plannodes.h>
#include <optimizer/optimizer.h>
#include <optimizer/plancat.h>
#include <parser/parsetree.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>

#include "nodes/chunk_dispatch/chunk_dispatch.h"
#include "cross_module_fn.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "hypertable_modify.h"
#include "nodes/chunk_append/chunk_append.h"
#include "ts_catalog/hypertable_data_node.h"

#if PG14_GE
static void fireASTriggers(ModifyTableState *node);
static void fireBSTriggers(ModifyTableState *node);
static TupleTableSlot *ExecModifyTable(CustomScanState *cs_node, PlanState *pstate);
static TupleTableSlot *ExecProcessReturning(ResultRelInfo *resultRelInfo, TupleTableSlot *tupleSlot,
											TupleTableSlot *planSlot);
static void ExecInitInsertProjection(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo);
static void ExecInitUpdateProjection(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo);
static void ExecCheckPlanOutput(Relation resultRel, List *targetList);
static TupleTableSlot *ExecGetInsertNewTuple(ResultRelInfo *relinfo, TupleTableSlot *planSlot);
static void ExecBatchInsert(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo,
							TupleTableSlot **slots, TupleTableSlot **planSlots, int numSlots,
							EState *estate, bool canSetTag);
static TupleTableSlot *ExecDelete(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
								  ItemPointer tupleid, HeapTuple oldtuple, bool processReturning,
								  bool canSetTag, bool changingPart, bool *tupleDeleted,
								  TupleTableSlot **epqreturnslot);
static TupleTableSlot *ExecUpdate(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
								  ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
								  bool canSetTag);
static bool ExecOnConflictUpdate(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
								 ItemPointer conflictTid, TupleTableSlot *excludedSlot,
								 bool canSetTag, TupleTableSlot **returning);
static void ExecCheckTupleVisible(EState *estate, Relation rel, TupleTableSlot *slot);
static void ExecCheckTIDVisible(EState *estate, ResultRelInfo *relinfo, ItemPointer tid,
								TupleTableSlot *tempSlot);
#endif

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

			if (ts_is_chunk_dispatch_state(substate))
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
hypertable_modify_begin(CustomScanState *node, EState *estate, int eflags)
{
	HypertableModifyState *state = (HypertableModifyState *) node;
	ModifyTableState *mtstate;
	PlanState *ps;
	List *chunk_dispatch_states = NIL;
	ListCell *lc;

	ModifyTable *mt = castNode(ModifyTable, &state->mt->plan);
	/*
	 * To make statement trigger defined on the hypertable work
	 * we need to set the hypertable as the rootRelation otherwise
	 * statement trigger defined only on the hypertable will not fire.
	 */
	if (mt->operation == CMD_DELETE || mt->operation == CMD_UPDATE)
		mt->rootRelation = mt->nominalRelation;
#if PG15_GE
	if (mt->operation == CMD_MERGE)
	{
		mt->rootRelation = mt->nominalRelation;
	}
#endif
	ps = ExecInitNode(&mt->plan, estate, eflags);
	node->custom_ps = list_make1(ps);
	mtstate = castNode(ModifyTableState, ps);

	/*
	 * If this is not the primary ModifyTable node, postgres added it to the
	 * beginning of es_auxmodifytables, to be executed by ExecPostprocessPlan.
	 * Unfortunately that strips off the HypertableInsert node leading to
	 * tuple routing not working in INSERTs inside CTEs. To make INSERTs
	 * inside CTEs work we have to fix es_auxmodifytables and add back the
	 * HypertableModifyState.
	 */
	if (estate->es_auxmodifytables && linitial(estate->es_auxmodifytables) == mtstate)
		linitial(estate->es_auxmodifytables) = node;

		/*
		 * Find all ChunkDispatchState subnodes and set their parent
		 * ModifyTableState node
		 * We assert we only have 1 ModifyTable subpath when we create
		 * the HypertableInsert path so this should not have changed here.
		 */
#if PG14_LT
	Assert(mtstate->mt_nplans == 1);
	PlanState *subplan = mtstate->mt_plans[0];
#else
	PlanState *subplan = outerPlanState(mtstate);
#endif

	if (mtstate->operation == CMD_INSERT
#if PG15_GE
		|| mtstate->operation == CMD_MERGE
#endif
	)
	{
		/* setup chunk dispatch state only for INSERTs */
		chunk_dispatch_states = get_chunk_dispatch_states(subplan);

		/* Ensure that we found at least one ChunkDispatchState node */
		Assert(list_length(chunk_dispatch_states) > 0);

		foreach (lc, chunk_dispatch_states)
			ts_chunk_dispatch_state_set_parent((ChunkDispatchState *) lfirst(lc), mtstate);
	}
}

static TupleTableSlot *
hypertable_modify_exec(CustomScanState *node)
{
#if PG14_LT
	return ExecProcNode(linitial(node->custom_ps));
#else
	ModifyTableState *mtstate = linitial_node(ModifyTableState, node->custom_ps);
	return ExecModifyTable(node, &mtstate->ps);
#endif
}

static void
hypertable_modify_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
}

static void
hypertable_modify_rescan(CustomScanState *node)
{
	ExecReScan(linitial(node->custom_ps));
}

static void
hypertable_modify_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	HypertableModifyState *state = (HypertableModifyState *) node;
	List *fdw_private = linitial_node(List, state->mt->fdwPrivLists);
	ModifyTableState *mtstate = linitial_node(ModifyTableState, node->custom_ps);
	Index rti = state->mt->nominalRelation;
	RangeTblEntry *rte = rt_fetch(rti, es->rtable);
	const char *relname = get_rel_name(rte->relid);
	const char *namespace = get_namespace_name(get_rel_namespace(rte->relid));

#if PG14_GE
	/*
	 * The targetlist for this node will have references that cannot be resolved by
	 * EXPLAIN. So for EXPLAIN VERBOSE we clear the targetlist so that EXPLAIN does not
	 * complain. PostgreSQL does something equivalent and does not print the targetlist
	 * for ModifyTable for EXPLAIN VERBOSE.
	 */
	if (((ModifyTable *) mtstate->ps.plan)->operation == CMD_DELETE && es->verbose &&
		ts_is_chunk_append_plan(mtstate->ps.plan->lefttree))
	{
		mtstate->ps.plan->lefttree->targetlist = NULL;
		((CustomScan *) mtstate->ps.plan->lefttree)->custom_scan_tlist = NULL;
	}
#if PG15_GE
	if (((ModifyTable *) mtstate->ps.plan)->operation == CMD_MERGE && es->verbose)
	{
		mtstate->ps.plan->lefttree->targetlist = NULL;
		((CustomScan *) mtstate->ps.plan->lefttree)->custom_scan_tlist = NULL;
	}
#endif
	/*
	 * Since we hijack the ModifyTable node instrumentation on ModifyTable will
	 * be missing so we set it to instrumentation of HypertableModify node.
	 */
	mtstate->ps.instrument = node->ss.ps.instrument;
#endif

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

static CustomExecMethods hypertable_modify_state_methods = {
	.CustomName = "HypertableModifyState",
	.BeginCustomScan = hypertable_modify_begin,
	.EndCustomScan = hypertable_modify_end,
	.ExecCustomScan = hypertable_modify_exec,
	.ReScanCustomScan = hypertable_modify_rescan,
	.ExplainCustomScan = hypertable_modify_explain,
};

static Node *
hypertable_modify_state_create(CustomScan *cscan)
{
	HypertableModifyState *state;
	ModifyTable *mt = castNode(ModifyTable, linitial(cscan->custom_plans));
	Oid serverid;

	state = (HypertableModifyState *) newNode(sizeof(HypertableModifyState), T_CustomScanState);
	state->cscan_state.methods = &hypertable_modify_state_methods;
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

static CustomScanMethods hypertable_modify_plan_methods = {
	.CustomName = "HypertableModify",
	.CreateCustomScanState = hypertable_modify_state_create,
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
plan_remote_modify(PlannerInfo *root, HypertableModifyPath *hmpath, ModifyTable *mt,
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
		bool is_distributed_insert = bms_is_member(i, hmpath->distributed_insert_plans);

		/* If data node batching is supported, we won't actually use the FDW
		 * direct modify API (everything is done in DataNodeDispatch), but we
		 * need to trick ModifyTable to believe we're doing direct modify so
		 * that it doesn't invoke the non-direct FDW API for inserts. Instead,
		 * it should handle only returning projections as if it was a direct
		 * modify. We do this by adding the result relation's plan to
		 * fdwDirectModifyPlans. See ExecModifyTable for more details. */
		if (is_distributed_insert)
			direct_modify_plans = bms_add_member(direct_modify_plans, i);

		if (!is_distributed_insert && NULL != fdwroutine && fdwroutine->PlanForeignModify != NULL &&
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
ts_hypertable_modify_fixup_tlist(Plan *plan)
{
	if (IsA(plan, CustomScan))
	{
		CustomScan *cscan = (CustomScan *) plan;

		if (cscan->methods == &hypertable_modify_plan_methods)
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

/* ROWID_VAR only exists in PG14+ */
#if PG14_GE
List *
ts_replace_rowid_vars(PlannerInfo *root, List *tlist, int varno)
{
	ListCell *lc;
	tlist = list_copy(tlist);
	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (IsA(tle->expr, Var) && castNode(Var, tle->expr)->varno == ROWID_VAR)
		{
			tle = copyObject(tle);
			Var *var = castNode(Var, copyObject(tle->expr));
			RowIdentityVarInfo *ridinfo =
				(RowIdentityVarInfo *) list_nth(root->row_identity_vars, var->varattno - 1);
			var = copyObject(ridinfo->rowidvar);
			var->varno = varno;
			var->varnosyn = 0;
			var->varattnosyn = 0;
			tle->expr = (Expr *) var;
			lfirst(lc) = tle;
		}
	}
	return tlist;
}
#endif

static Plan *
hypertable_modify_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
							  List *tlist, List *clauses, List *custom_plans)
{
	HypertableModifyPath *hmpath = (HypertableModifyPath *) best_path;
	CustomScan *cscan = makeNode(CustomScan);
	ModifyTable *mt = linitial_node(ModifyTable, custom_plans);
	FdwRoutine *fdwroutine = NULL;

	cscan->methods = &hypertable_modify_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.scanrelid = 0;

	/* Copy costs, etc., from the original plan */
	cscan->scan.plan.startup_cost = mt->plan.startup_cost;
	cscan->scan.plan.total_cost = mt->plan.total_cost;
	cscan->scan.plan.plan_rows = mt->plan.plan_rows;
	cscan->scan.plan.plan_width = mt->plan.plan_width;

	if (NIL != hmpath->serveroids)
	{
		/* Get the foreign data wrapper routines for the first data node. Should be
		 * the same for all data nodes. */
		Oid serverid = linitial_oid(hmpath->serveroids);

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
	plan_remote_modify(root, hmpath, mt, fdwroutine);

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

	/*
	 * For UPDATE/DELETE/MERGE processed_tlist will have ROWID_VAR. We
	 * need to remove those because set_customscan_references will bail
	 * if it sees ROWID_VAR entries in the targetlist.
	 */
#if PG14_GE
	if (mt->operation == CMD_UPDATE || mt->operation == CMD_DELETE
#if PG15_GE
		|| mt->operation == CMD_MERGE
#endif
	)
	{
		cscan->scan.plan.targetlist =
			ts_replace_rowid_vars(root, cscan->scan.plan.targetlist, mt->nominalRelation);

		if (mt->operation == CMD_UPDATE && ts_is_chunk_append_plan(mt->plan.lefttree))
		{
			mt->plan.lefttree->targetlist =
				ts_replace_rowid_vars(root, mt->plan.lefttree->targetlist, mt->nominalRelation);
		}
	}
#else
	/*
	 * For postgres versions < PG14 we only route INSERT through our custom node.
	 */
	Assert(mt->operation == CMD_INSERT);
#endif
	cscan->custom_scan_tlist = cscan->scan.plan.targetlist;

	/*
	 * we save the original list of arbiter indexes here
	 * because we modify that list during execution and
	 * we still need the original list in case that plan
	 * gets reused.
	 *
	 * We also pass on the data nodes to insert on.
	 */
	cscan->custom_private = list_make2(mt->arbiterIndexes, hmpath->serveroids);

	return &cscan->scan.plan;
}

static CustomPathMethods hypertable_modify_path_methods = {
	.CustomName = "HypertableModifyPath",
	.PlanCustomPath = hypertable_modify_plan_create,
};

Path *
ts_hypertable_modify_path_create(PlannerInfo *root, ModifyTablePath *mtpath, Hypertable *ht,
								 RelOptInfo *rel)
{
	Path *path = &mtpath->path;
	Path *subpath = NULL;
	Cache *hcache = ts_hypertable_cache_pin();
	Bitmapset *distributed_insert_plans = NULL;
	HypertableModifyPath *hmpath;
	int i = 0;

#if PG14_LT
	/* Since it's theoretically possible for ModifyTablePath to have multiple subpaths
	 * in PG < 14 we assert that we only get 1 subpath here. */
	Assert(list_length(mtpath->subpaths) == list_length(mtpath->resultRelations));
	if (list_length(mtpath->subpaths) > 1)
		/* This should never happen but if it ever does it's safer to
		 * error here as the rest of the code assumes there is only 1 subpath.
		 */
		elog(ERROR, "multiple top-level subpaths found during INSERT");
#endif

#if PG14_GE
	/* PG14 only copies child rows and width if returningLists is not
	 * empty. Since we do not know target chunks during planning we
	 * do not have that information when postgres creates the path.
	 */
	if (mtpath->returningLists == NIL)
	{
		mtpath->path.rows = mtpath->subpath->rows;
		mtpath->path.pathtarget->width = mtpath->subpath->pathtarget->width;
	}
#endif

	Index rti = mtpath->nominalRelation;

	if (mtpath->operation == CMD_INSERT
#if PG15_GE
		|| mtpath->operation == CMD_MERGE
#endif
	)
	{
		if (hypertable_is_distributed(ht) && ts_guc_max_insert_batch_size > 0)
		{
			/* Remember that this will become a data node dispatch/copy
			 * plan. We need to know later whether or not to plan this
			 * using the FDW API. */
			distributed_insert_plans = bms_add_member(distributed_insert_plans, i);
			subpath = ts_cm_functions->distributed_insert_path_create(root, mtpath, rti, i);
		}
		else
			subpath = ts_chunk_dispatch_path_create(root, mtpath, rti, i);
	}

	hmpath = palloc0(sizeof(HypertableModifyPath));

	/* Copy costs, etc. */
	memcpy(&hmpath->cpath.path, path, sizeof(Path));
	hmpath->cpath.path.type = T_CustomPath;
	hmpath->cpath.path.pathtype = T_CustomScan;
	hmpath->cpath.custom_paths = list_make1(mtpath);
	hmpath->cpath.methods = &hypertable_modify_path_methods;
	hmpath->distributed_insert_plans = distributed_insert_plans;
	hmpath->serveroids = ts_hypertable_get_available_data_node_server_oids(ht);
	path = &hmpath->cpath.path;
#if PG14_LT
	mtpath->subpaths = list_make1(subpath);
#else
	if (subpath)
		mtpath->subpath = subpath;
#endif

	ts_cache_release(hcache);

	return path;
}

/*
 * Callback for ModifyTableState->GetUpdateNewTuple for use by regular UPDATE.
 */
#if PG14_GE
static TupleTableSlot *
internalGetUpdateNewTuple(ResultRelInfo *relinfo, TupleTableSlot *planSlot, TupleTableSlot *oldSlot,
#if PG15_GE
						  MergeActionState *relaction
#else
						  void *temp
#endif
)
{
	ProjectionInfo *newProj = relinfo->ri_projectNew;
	ExprContext *econtext;

	econtext = newProj->pi_exprContext;
	econtext->ecxt_outertuple = planSlot;
	econtext->ecxt_scantuple = oldSlot;
	return ExecProject(newProj);
}

/* ----------------------------------------------------------------
 *	   ExecModifyTable
 *
 *		Perform table modifications as required, and return RETURNING results
 *		if needed.
 * ----------------------------------------------------------------
 *
 * modified version of ExecModifyTable from executor/nodeModifyTable.c
 */
static TupleTableSlot *
ExecModifyTable(CustomScanState *cs_node, PlanState *pstate)
{
	HypertableModifyState *ht_state = (HypertableModifyState *) cs_node;
	ModifyTableState *node = castNode(ModifyTableState, pstate);
	ModifyTableContext context;
	EState *estate = node->ps.state;
	CmdType operation = node->operation;
	ResultRelInfo *resultRelInfo;
	PlanState *subplanstate;
	TupleTableSlot *slot;
	TupleTableSlot *oldSlot;
	ItemPointer tupleid;
	ItemPointerData tuple_ctid;
	HeapTupleData oldtupdata;
	HeapTuple oldtuple;
	List *relinfos = NIL;
	ListCell *lc;
	ChunkDispatchState *cds = NULL;

	CHECK_FOR_INTERRUPTS();

	/*
	 * This should NOT get called during EvalPlanQual; we should have passed a
	 * subplan tree to EvalPlanQual, instead.  Use a runtime test not just
	 * Assert because this condition is easy to miss in testing.  (Note:
	 * although ModifyTable should not get executed within an EvalPlanQual
	 * operation, we do have to allow it to be initialized and shut down in
	 * case it is within a CTE subplan.  Hence this test must be here, not in
	 * ExecInitModifyTable.)
	 */
	if (estate->es_epq_active != NULL)
		elog(ERROR, "ModifyTable should not be called during EvalPlanQual");

	/*
	 * If we've already completed processing, don't try to do more.  We need
	 * this test because ExecPostprocessPlan might call us an extra time, and
	 * our subplan's nodes aren't necessarily robust against being called
	 * extra times.
	 */
	if (node->mt_done)
		return NULL;

	/*
	 * On first call, fire BEFORE STATEMENT triggers before proceeding.
	 */
	if (node->fireBSTriggers)
	{
		fireBSTriggers(node);
		node->fireBSTriggers = false;
	}

	/* Preload local variables */
	resultRelInfo = node->resultRelInfo + node->mt_lastResultIndex;
	subplanstate = outerPlanState(node);

	if (operation == CMD_INSERT
#if PG15_GE
		|| operation == CMD_MERGE
#endif
	)
	{
		if (ts_is_chunk_dispatch_state(subplanstate))
		{
			cds = (ChunkDispatchState *) subplanstate;
		}
		else
		{
			Assert(list_length(get_chunk_dispatch_states(subplanstate)) == 1);
			cds = linitial(get_chunk_dispatch_states(subplanstate));
		}
	}
	/* Set global context */
	context.mtstate = node;
	context.epqstate = &node->mt_epqstate;
	context.estate = estate;
	/*
	 * For UPDATE/DELETE on compressed hypertable, decompress chunks and
	 * move rows to uncompressed chunks.
	 */
	if ((operation == CMD_DELETE || operation == CMD_UPDATE) && !ht_state->comp_chunks_processed)
	{
		if (ts_cm_functions->decompress_target_segments)
		{
			ts_cm_functions->decompress_target_segments(node);
			ht_state->comp_chunks_processed = true;
			/*
			 * save snapshot set during ExecutorStart(), since this is the same
			 * snapshot used to SeqScan of uncompressed chunks
			 */
			ht_state->snapshot = estate->es_snapshot;
			/* use current transaction snapshot */
			estate->es_snapshot = GetTransactionSnapshot();
			CommandCounterIncrement();
			/* mark rows visible */
			estate->es_output_cid = GetCurrentCommandId(true);
		}
	}
	/*
	 * Fetch rows from subplan, and execute the required table modification
	 * for each row.
	 */
	for (;;)
	{
		Oid resultoid = InvalidOid;
		/*
		 * Reset the per-output-tuple exprcontext.  This is needed because
		 * triggers expect to use that context as workspace.  It's a bit ugly
		 * to do this below the top level of the plan, however.  We might need
		 * to rethink this later.
		 */
		ResetPerTupleExprContext(estate);

		/*
		 * Reset per-tuple memory context used for processing on conflict and
		 * returning clauses, to free any expression evaluation storage
		 * allocated in the previous cycle.
		 */
		if (pstate->ps_ExprContext)
			ResetExprContext(pstate->ps_ExprContext);

		context.planSlot = ExecProcNode(subplanstate);

		/* No more tuples to process? */
		if (TupIsNull(context.planSlot))
			break;

#if PG15_GE
		/* copy INSERT merge action list to result relation info of corresponding chunk */
		if (cds && cds->rri && operation == CMD_MERGE)
			cds->rri->ri_notMatchedMergeAction = resultRelInfo->ri_notMatchedMergeAction;
#endif
		/*
		 * When there are multiple result relations, each tuple contains a
		 * junk column that gives the OID of the rel from which it came.
		 * Extract it and select the correct result relation.
		 */
		if (AttributeNumberIsValid(node->mt_resultOidAttno))
		{
			Datum datum;
			bool isNull;

			datum = ExecGetJunkAttribute(context.planSlot, node->mt_resultOidAttno, &isNull);
			if (isNull)
			{
#if PG15_GE
				if (operation == CMD_MERGE)
				{
					EvalPlanQualSetSlot(&node->mt_epqstate, context.planSlot);
					ht_ExecMerge(&context, node->resultRelInfo, cds, NULL, node->canSetTag);
					continue; /* no RETURNING support yet */
				}
#endif
				elog(ERROR, "tableoid is NULL");
			}
			resultoid = DatumGetObjectId(datum);

			/* If it's not the same as last time, we need to locate the rel */
			if (resultoid != node->mt_lastResultOid)
				resultRelInfo = ExecLookupResultRelByOid(node, resultoid, false, true);
		}

		/*
		 * If resultRelInfo->ri_usesFdwDirectModify is true, all we need to do
		 * here is compute the RETURNING expressions.
		 */
		if (resultRelInfo->ri_usesFdwDirectModify)
		{
			Assert(resultRelInfo->ri_projectReturning);

			/*
			 * A scan slot containing the data that was actually inserted,
			 * updated or deleted has already been made available to
			 * ExecProcessReturning by IterateDirectModify, so no need to
			 * provide it here.
			 */
			slot = ExecProcessReturning(resultRelInfo, NULL, context.planSlot);

			return slot;
		}

		EvalPlanQualSetSlot(&node->mt_epqstate, context.planSlot);
		slot = context.planSlot;

		tupleid = NULL;
		oldtuple = NULL;

		/*
		 * For UPDATE/DELETE, fetch the row identity info for the tuple to be
		 * updated/deleted.  For a heap relation, that's a TID; otherwise we
		 * may have a wholerow junk attr that carries the old tuple in toto.
		 * Keep this in step with the part of ExecInitModifyTable that sets up
		 * ri_RowIdAttNo.
		 */
		if (operation == CMD_UPDATE || operation == CMD_DELETE
#if PG15_GE
			|| operation == CMD_MERGE
#endif
		)
		{
			char relkind;
			Datum datum;
			bool isNull;

			relkind = resultRelInfo->ri_RelationDesc->rd_rel->relkind;
			/* Since this is a hypertable relkind should be RELKIND_RELATION for a local
			 * chunk or  RELKIND_FOREIGN_TABLE for a chunk that is a foreign table
			 * (OSM chunks)
			 */
			Assert(relkind == RELKIND_RELATION || relkind == RELKIND_FOREIGN_TABLE);

			if (relkind == RELKIND_RELATION || relkind == RELKIND_MATVIEW ||
				relkind == RELKIND_PARTITIONED_TABLE)
			{
				/* ri_RowIdAttNo refers to a ctid attribute */
				Assert(AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo));
				datum = ExecGetJunkAttribute(slot, resultRelInfo->ri_RowIdAttNo, &isNull);
				/* shouldn't ever get a null result... */
				if (isNull)
				{
#if PG15_GE
					if (operation == CMD_MERGE)
					{
						EvalPlanQualSetSlot(&node->mt_epqstate, context.planSlot);
						ht_ExecMerge(&context, node->resultRelInfo, cds, NULL, node->canSetTag);
						continue; /* no RETURNING support yet */
					}
#endif
					elog(ERROR, "ctid is NULL");
				}

				tupleid = (ItemPointer) DatumGetPointer(datum);
				tuple_ctid = *tupleid; /* be sure we don't free ctid!! */
				tupleid = &tuple_ctid;
			}

			/*
			 * Use the wholerow attribute, when available, to reconstruct the
			 * old relation tuple.  The old tuple serves one or both of two
			 * purposes: 1) it serves as the OLD tuple for row triggers, 2) it
			 * provides values for any unchanged columns for the NEW tuple of
			 * an UPDATE, because the subplan does not produce all the columns
			 * of the target table.
			 *
			 * Note that the wholerow attribute does not carry system columns,
			 * so foreign table triggers miss seeing those, except that we
			 * know enough here to set t_tableOid.  Quite separately from
			 * this, the FDW may fetch its own junk attrs to identify the row.
			 *
			 * Other relevant relkinds, currently limited to views, always
			 * have a wholerow attribute.
			 */
			else if (AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
			{
				datum = ExecGetJunkAttribute(slot, resultRelInfo->ri_RowIdAttNo, &isNull);
				/* shouldn't ever get a null result... */
				if (isNull)
					elog(ERROR, "wholerow is NULL");

				oldtupdata.t_data = DatumGetHeapTupleHeader(datum);
				oldtupdata.t_len = HeapTupleHeaderGetDatumLength(oldtupdata.t_data);
				ItemPointerSetInvalid(&(oldtupdata.t_self));
				/* Historically, view triggers see invalid t_tableOid. */
				oldtupdata.t_tableOid = (relkind == RELKIND_VIEW) ?
											InvalidOid :
											RelationGetRelid(resultRelInfo->ri_RelationDesc);

				oldtuple = &oldtupdata;
			}
			else
			{
				/* Only foreign tables are allowed to omit a row-ID attr */
				Assert(relkind == RELKIND_FOREIGN_TABLE);
			}
		}

		switch (operation)
		{
			case CMD_INSERT:
				/* Initialize projection info if first time for this table */
				if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
					ExecInitInsertProjection(node, resultRelInfo);
				slot = ExecGetInsertNewTuple(resultRelInfo, context.planSlot);
				slot = ExecInsert(&context, cds->rri, slot, node->canSetTag);
				break;
			case CMD_UPDATE:
				/* Initialize projection info if first time for this table */
				if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
					ExecInitUpdateProjection(node, resultRelInfo);

				/*
				 * Make the new tuple by combining plan's output tuple with
				 * the old tuple being updated.
				 */
				oldSlot = resultRelInfo->ri_oldTupleSlot;
				if (oldtuple != NULL)
				{
					/* Use the wholerow junk attr as the old tuple. */
					ExecForceStoreHeapTuple(oldtuple, oldSlot, false);
				}
				else
				{
					/* Fetch the most recent version of old tuple. */
					Relation relation = resultRelInfo->ri_RelationDesc;

					Assert(tupleid != NULL);
					if (!table_tuple_fetch_row_version(relation, tupleid, SnapshotAny, oldSlot))
						elog(ERROR, "failed to fetch tuple being updated");
				}
#if PG14_LT
				slot = ExecGetUpdateNewTuple(resultRelInfo, planSlot, oldSlot);
#else
				slot = internalGetUpdateNewTuple(resultRelInfo, context.planSlot, oldSlot, NULL);
#endif
#if PG15_GE
				context.GetUpdateNewTuple = internalGetUpdateNewTuple;
				context.relaction = NULL;
#endif
				/* Now apply the update. */
				slot =
					ExecUpdate(&context, resultRelInfo, tupleid, oldtuple, slot, node->canSetTag);
				break;
			case CMD_DELETE:
				slot = ExecDelete(&context,
								  resultRelInfo,
								  tupleid,
								  oldtuple,
								  true,
								  node->canSetTag,
								  false,
								  NULL,
								  NULL);
				break;
#if PG15_GE
			case CMD_MERGE:
				slot = ht_ExecMerge(&context, resultRelInfo, cds, tupleid, node->canSetTag);
				break;
#endif
			default:
				elog(ERROR, "unknown operation");
				break;
		}

		/*
		 * If we got a RETURNING result, return it to caller.  We'll continue
		 * the work on next call.
		 */
		if (slot)
			return slot;
	}

	/*
	 * Insert remaining tuples for batch insert.
	 */
	relinfos = estate->es_opened_result_relations;

	if (ht_state->comp_chunks_processed)
	{
		estate->es_snapshot = ht_state->snapshot;
		ht_state->comp_chunks_processed = false;
	}

	foreach (lc, relinfos)
	{
		resultRelInfo = lfirst(lc);
		if (resultRelInfo->ri_NumSlots > 0)
			ExecBatchInsert(node,
							resultRelInfo,
							resultRelInfo->ri_Slots,
							resultRelInfo->ri_PlanSlots,
							resultRelInfo->ri_NumSlots,
							estate,
							node->canSetTag);
	}

	/*
	 * We're done, but fire AFTER STATEMENT triggers before exiting.
	 */
	fireASTriggers(node);

	node->mt_done = true;

	return NULL;
}

/*
 * Process BEFORE EACH STATEMENT triggers
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static void
fireBSTriggers(ModifyTableState *node)
{
	ModifyTable *plan = (ModifyTable *) node->ps.plan;
	ResultRelInfo *resultRelInfo = node->rootResultRelInfo;

	switch (node->operation)
	{
		case CMD_INSERT:
			ExecBSInsertTriggers(node->ps.state, resultRelInfo);
			if (plan->onConflictAction == ONCONFLICT_UPDATE)
				ExecBSUpdateTriggers(node->ps.state, resultRelInfo);
			break;
		case CMD_UPDATE:
			ExecBSUpdateTriggers(node->ps.state, resultRelInfo);
			break;
		case CMD_DELETE:
			ExecBSDeleteTriggers(node->ps.state, resultRelInfo);
			break;
#if PG15_GE
		case CMD_MERGE:
			if (node->mt_merge_subcommands & MERGE_INSERT)
				ExecBSInsertTriggers(node->ps.state, resultRelInfo);
			if (node->mt_merge_subcommands & MERGE_UPDATE)
				ExecBSUpdateTriggers(node->ps.state, resultRelInfo);
			if (node->mt_merge_subcommands & MERGE_DELETE)
				ExecBSDeleteTriggers(node->ps.state, resultRelInfo);
			break;
#endif
		default:
			elog(ERROR, "unknown operation");
			break;
	}
}

/*
 * Process AFTER EACH STATEMENT triggers
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static void
fireASTriggers(ModifyTableState *node)
{
	ModifyTable *plan = (ModifyTable *) node->ps.plan;
	ResultRelInfo *resultRelInfo = node->rootResultRelInfo;

	switch (node->operation)
	{
		case CMD_INSERT:
			if (plan->onConflictAction == ONCONFLICT_UPDATE)
				ExecASUpdateTriggers(node->ps.state, resultRelInfo, node->mt_oc_transition_capture);
			ExecASInsertTriggers(node->ps.state, resultRelInfo, node->mt_transition_capture);
			break;
		case CMD_UPDATE:
			ExecASUpdateTriggers(node->ps.state, resultRelInfo, node->mt_transition_capture);
			break;
		case CMD_DELETE:
			ExecASDeleteTriggers(node->ps.state, resultRelInfo, node->mt_transition_capture);
			break;
#if PG15_GE
		case CMD_MERGE:
			if (node->mt_merge_subcommands & MERGE_INSERT)
				ExecASInsertTriggers(node->ps.state, resultRelInfo, node->mt_transition_capture);
			if (node->mt_merge_subcommands & MERGE_UPDATE)
				ExecASUpdateTriggers(node->ps.state, resultRelInfo, node->mt_transition_capture);
			if (node->mt_merge_subcommands & MERGE_DELETE)
				ExecASDeleteTriggers(node->ps.state, resultRelInfo, node->mt_transition_capture);
			break;
#endif
		default:
			elog(ERROR, "unknown operation");
			break;
	}
}

/*
 * ExecProcessReturning --- evaluate a RETURNING list
 *
 * resultRelInfo: current result rel
 * tupleSlot: slot holding tuple actually inserted/updated/deleted
 * planSlot: slot holding tuple returned by top subplan node
 *
 * Note: If tupleSlot is NULL, the FDW should have already provided econtext's
 * scan tuple.
 *
 * Returns a slot holding the result tuple
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static TupleTableSlot *
ExecProcessReturning(ResultRelInfo *resultRelInfo, TupleTableSlot *tupleSlot,
					 TupleTableSlot *planSlot)
{
	ProjectionInfo *projectReturning = resultRelInfo->ri_projectReturning;
	ExprContext *econtext = projectReturning->pi_exprContext;

	/* Make tuple and any needed join variables available to ExecProject */
	if (tupleSlot)
		econtext->ecxt_scantuple = tupleSlot;
	econtext->ecxt_outertuple = planSlot;

	/*
	 * RETURNING expressions might reference the tableoid column, so
	 * reinitialize tts_tableOid before evaluating them.
	 */
	econtext->ecxt_scantuple->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/* Compute the RETURNING expressions */
	return ExecProject(projectReturning);
}

/*
 * ExecInitInsertProjection
 *		Do one-time initialization of projection data for INSERT tuples.
 *
 * INSERT queries may need a projection to filter out junk attrs in the tlist.
 *
 * This is also a convenient place to verify that the
 * output of an INSERT matches the target table.
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static void
ExecInitInsertProjection(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	Plan *subplan = outerPlan(node);
	EState *estate = mtstate->ps.state;
	List *insertTargetList = NIL;
	bool need_projection = false;
	ListCell *l;

	/* Extract non-junk columns of the subplan's result tlist. */
	foreach (l, subplan->targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(l);

		if (!tle->resjunk)
			insertTargetList = lappend(insertTargetList, tle);
		else
			need_projection = true;
	}

	/*
	 * The junk-free list must produce a tuple suitable for the result
	 * relation.
	 */
	ExecCheckPlanOutput(resultRelInfo->ri_RelationDesc, insertTargetList);

	/* We'll need a slot matching the table's format. */
	resultRelInfo->ri_newTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);

	/* Build ProjectionInfo if needed (it probably isn't). */
	if (need_projection)
	{
		TupleDesc relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

		/* need an expression context to do the projection */
		if (mtstate->ps.ps_ExprContext == NULL)
			ExecAssignExprContext(estate, &mtstate->ps);

		resultRelInfo->ri_projectNew = ExecBuildProjectionInfo(insertTargetList,
															   mtstate->ps.ps_ExprContext,
															   resultRelInfo->ri_newTupleSlot,
															   &mtstate->ps,
															   relDesc);
	}

	resultRelInfo->ri_projectNewInfoValid = true;
}

/*
 * ExecInitUpdateProjection
 *		Do one-time initialization of projection data for UPDATE tuples.
 *
 * UPDATE always needs a projection, because (1) there's always some junk
 * attrs, and (2) we may need to merge values of not-updated columns from
 * the old tuple into the final tuple.  In UPDATE, the tuple arriving from
 * the subplan contains only new values for the changed columns, plus row
 * identity info in the junk attrs.
 *
 * This is "one-time" for any given result rel, but we might touch more than
 * one result rel in the course of an inherited UPDATE, and each one needs
 * its own projection due to possible column order variation.
 *
 * This is also a convenient place to verify that the output of an UPDATE
 * matches the target table (ExecBuildUpdateProjection does that).
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static void
ExecInitUpdateProjection(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	Plan *subplan = outerPlan(node);
	EState *estate = mtstate->ps.state;
	TupleDesc relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
	int whichrel;
	List *updateColnos;

	/*
	 * Usually, mt_lastResultIndex matches the target rel.  If it happens not
	 * to, we can get the index the hard way with an integer division.
	 */
	whichrel = mtstate->mt_lastResultIndex;
	if (resultRelInfo != mtstate->resultRelInfo + whichrel)
	{
		whichrel = resultRelInfo - mtstate->resultRelInfo;
		Assert(whichrel >= 0 && whichrel < mtstate->mt_nrels);
	}

	updateColnos = (List *) list_nth(node->updateColnosLists, whichrel);

	/*
	 * For UPDATE, we use the old tuple to fill up missing values in the tuple
	 * produced by the subplan to get the new tuple.  We need two slots, both
	 * matching the table's desired format.
	 */
	resultRelInfo->ri_oldTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);
	resultRelInfo->ri_newTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);

	/* need an expression context to do the projection */
	if (mtstate->ps.ps_ExprContext == NULL)
		ExecAssignExprContext(estate, &mtstate->ps);

	resultRelInfo->ri_projectNew = ExecBuildUpdateProjection(subplan->targetlist,
															 false, /* subplan did the evaluation */
															 updateColnos,
															 relDesc,
															 mtstate->ps.ps_ExprContext,
															 resultRelInfo->ri_newTupleSlot,
															 &mtstate->ps);

	resultRelInfo->ri_projectNewInfoValid = true;
}

/*
 * Verify that the tuples to be produced by INSERT match the
 * target relation's rowtype
 *
 * We do this to guard against stale plans.  If plan invalidation is
 * functioning properly then we should never get a failure here, but better
 * safe than sorry.  Note that this is called after we have obtained lock
 * on the target rel, so the rowtype can't change underneath us.
 *
 * The plan output is represented by its targetlist, because that makes
 * handling the dropped-column case easier.
 *
 * We used to use this for UPDATE as well, but now the equivalent checks
 * are done in ExecBuildUpdateProjection.
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static void
ExecCheckPlanOutput(Relation resultRel, List *targetList)
{
	TupleDesc resultDesc = RelationGetDescr(resultRel);
	int attno = 0;
	ListCell *lc;

	foreach (lc, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr;

		Assert(!tle->resjunk); /* caller removed junk items already */

		if (attno >= resultDesc->natts)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("table row type and query-specified row type do not match"),
					 errdetail("Query has too many columns.")));
		attr = TupleDescAttr(resultDesc, attno);
		attno++;

		if (!attr->attisdropped)
		{
			/* Normal case: demand type match */
			if (exprType((Node *) tle->expr) != attr->atttypid)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Table has type %s at ordinal position %d, but query expects "
								   "%s.",
								   format_type_be(attr->atttypid),
								   attno,
								   format_type_be(exprType((Node *) tle->expr)))));
		}
		else
		{
			/*
			 * For a dropped column, we can't check atttypid (it's likely 0).
			 * In any case the planner has most likely inserted an INT4 null.
			 * What we insist on is just *some* NULL constant.
			 */
			if (!IsA(tle->expr, Const) || !((Const *) tle->expr)->constisnull)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Query provides a value for a dropped column at ordinal "
								   "position %d.",
								   attno)));
		}
	}
	if (attno != resultDesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("table row type and query-specified row type do not match"),
				 errdetail("Query has too few columns.")));
}

/*
 * ExecGetInsertNewTuple
 *		This prepares a "new" tuple ready to be inserted into given result
 *		relation, by removing any junk columns of the plan's output tuple
 *		and (if necessary) coercing the tuple to the right tuple format.
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static TupleTableSlot *
ExecGetInsertNewTuple(ResultRelInfo *relinfo, TupleTableSlot *planSlot)
{
	ProjectionInfo *newProj = relinfo->ri_projectNew;
	ExprContext *econtext;

	/*
	 * If there's no projection to be done, just make sure the slot is of the
	 * right type for the target rel.  If the planSlot is the right type we
	 * can use it as-is, else copy the data into ri_newTupleSlot.
	 */
	if (newProj == NULL)
	{
		if (relinfo->ri_newTupleSlot->tts_ops != planSlot->tts_ops)
		{
			ExecCopySlot(relinfo->ri_newTupleSlot, planSlot);
			return relinfo->ri_newTupleSlot;
		}
		else
			return planSlot;
	}

	/*
	 * Else project; since the projection output slot is ri_newTupleSlot, this
	 * will also fix any slot-type problem.
	 *
	 * Note: currently, this is dead code, because INSERT cases don't receive
	 * any junk columns so there's never a projection to be done.
	 */
	econtext = newProj->pi_exprContext;
	econtext->ecxt_outertuple = planSlot;
	return ExecProject(newProj);
}

/*
 * ExecGetUpdateNewTuple
 *		This prepares a "new" tuple by combining an UPDATE subplan's output
 *		tuple (which contains values of changed columns) with unchanged
 *		columns taken from the old tuple.
 *
 * The subplan tuple might also contain junk columns, which are ignored.
 * Note that the projection also ensures we have a slot of the right type.
 */
TupleTableSlot *
ExecGetUpdateNewTuple(ResultRelInfo *relinfo, TupleTableSlot *planSlot, TupleTableSlot *oldSlot)
{
	/* Use a few extra Asserts to protect against outside callers */
	Assert(relinfo->ri_projectNewInfoValid);
	Assert(planSlot != NULL && !TTS_EMPTY(planSlot));
	Assert(oldSlot != NULL && !TTS_EMPTY(oldSlot));

	return internalGetUpdateNewTuple(relinfo, planSlot, oldSlot, NULL);
}

/* ----------------------------------------------------------------
 *		ExecInsert
 *
 *		For INSERT, we have to insert the tuple into the target relation
 *		(or partition thereof) and insert appropriate tuples into the index
 *		relations.
 *
 *		slot contains the new tuple value to be stored.
 *		planSlot is the output of the ModifyTable's subplan; we use it
 *		to access "junk" columns that are not going to be stored.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 *
 *		This may change the currently active tuple conversion map in
 *		mtstate->mt_transition_capture, so the callers must take care to
 *		save the previous value to avoid losing track of it.
 * ----------------------------------------------------------------
 *
 * copied and modified version of ExecInsert from executor/nodeModifyTable.c
 */
TupleTableSlot *
ExecInsert(ModifyTableContext *context, ResultRelInfo *resultRelInfo, TupleTableSlot *slot,
		   bool canSetTag)
{
	ModifyTableState *mtstate = context->mtstate;
	EState *estate = context->estate;
	Relation resultRelationDesc;
	List *recheckIndexes = NIL;
	TupleTableSlot *planSlot = context->planSlot;
	TupleTableSlot *result = NULL;
	TransitionCaptureState *ar_insert_trig_tcs;
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	OnConflictAction onconflict = node->onConflictAction;
	MemoryContext oldContext;

	Assert(!mtstate->mt_partition_tuple_routing);

	ExecMaterializeSlot(slot);

	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/*
	 * Open the table's indexes, if we have not done so already, so that we
	 * can add new index entries for the inserted tuple.
	 */
	if (resultRelationDesc->rd_rel->relhasindex && resultRelInfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(resultRelInfo, onconflict != ONCONFLICT_NONE);

	/*
	 * BEFORE ROW INSERT Triggers.
	 *
	 * Note: We fire BEFORE ROW TRIGGERS for every attempted insertion in an
	 * INSERT ... ON CONFLICT statement.  We cannot check for constraint
	 * violations before firing these triggers, because they can change the
	 * values to insert.  Also, they can run arbitrary user-defined code with
	 * side-effects that we can't cancel by just not inserting the tuple.
	 */
	if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_insert_before_row)
	{
		if (!ExecBRInsertTriggers(estate, resultRelInfo, slot))
			return NULL; /* "do nothing" */
	}

	/* INSTEAD OF ROW INSERT Triggers */
	if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_insert_instead_row)
	{
		if (!ExecIRInsertTriggers(estate, resultRelInfo, slot))
			return NULL; /* "do nothing" */
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		/*
		 * GENERATED expressions might reference the tableoid column, so
		 * (re-)initialize tts_tableOid before evaluating them.
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

		/*
		 * Compute stored generated columns
		 */
		if (resultRelationDesc->rd_att->constr &&
			resultRelationDesc->rd_att->constr->has_generated_stored)
			ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_INSERT);

		/*
		 * If the FDW supports batching, and batching is requested, accumulate
		 * rows and insert them in batches. Otherwise use the per-row inserts.
		 */
		if (resultRelInfo->ri_BatchSize > 1)
		{
			/*
			 * If a certain number of tuples have already been accumulated, or
			 * a tuple has come for a different relation than that for the
			 * accumulated tuples, perform the batch insert
			 */
			if (resultRelInfo->ri_NumSlots == resultRelInfo->ri_BatchSize)
			{
				ExecBatchInsert(mtstate,
								resultRelInfo,
								resultRelInfo->ri_Slots,
								resultRelInfo->ri_PlanSlots,
								resultRelInfo->ri_NumSlots,
								estate,
								canSetTag);
				resultRelInfo->ri_NumSlots = 0;
			}

			oldContext = MemoryContextSwitchTo(estate->es_query_cxt);

			if (resultRelInfo->ri_Slots == NULL)
			{
				resultRelInfo->ri_Slots =
					palloc(sizeof(TupleTableSlot *) * resultRelInfo->ri_BatchSize);
				resultRelInfo->ri_PlanSlots =
					palloc(sizeof(TupleTableSlot *) * resultRelInfo->ri_BatchSize);
			}

			/*
			 * Initialize the batch slots. We don't know how many slots will
			 * be needed, so we initialize them as the batch grows, and we
			 * keep them across batches. To mitigate an inefficiency in how
			 * resource owner handles objects with many references (as with
			 * many slots all referencing the same tuple descriptor) we copy
			 * the appropriate tuple descriptor for each slot.
			 */
			if (resultRelInfo->ri_NumSlots >= resultRelInfo->ri_NumSlotsInitialized)
			{
				TupleDesc tdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
				TupleDesc plan_tdesc = CreateTupleDescCopy(planSlot->tts_tupleDescriptor);

				resultRelInfo->ri_Slots[resultRelInfo->ri_NumSlots] =
					MakeSingleTupleTableSlot(tdesc, slot->tts_ops);

				resultRelInfo->ri_PlanSlots[resultRelInfo->ri_NumSlots] =
					MakeSingleTupleTableSlot(plan_tdesc, planSlot->tts_ops);

				/* remember how many batch slots we initialized */
				resultRelInfo->ri_NumSlotsInitialized++;
			}

			ExecCopySlot(resultRelInfo->ri_Slots[resultRelInfo->ri_NumSlots], slot);

			ExecCopySlot(resultRelInfo->ri_PlanSlots[resultRelInfo->ri_NumSlots], planSlot);

			resultRelInfo->ri_NumSlots++;

			MemoryContextSwitchTo(oldContext);

			return NULL;
		}

		/*
		 * insert into foreign table: let the FDW do it
		 */
		slot =
			resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate, resultRelInfo, slot, planSlot);

		if (slot == NULL) /* "do nothing" */
			return NULL;

		/*
		 * AFTER ROW Triggers or RETURNING expressions might reference the
		 * tableoid column, so (re-)initialize tts_tableOid before evaluating
		 * them.  (This covers the case where the FDW replaced the slot.)
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);
	}
	else
	{
		WCOKind wco_kind;

		/*
		 * Constraints and GENERATED expressions might reference the tableoid
		 * column, so (re-)initialize tts_tableOid before evaluating them.
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelationDesc);

		/*
		 * Compute stored generated columns
		 */
		if (resultRelationDesc->rd_att->constr &&
			resultRelationDesc->rd_att->constr->has_generated_stored)
			ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_INSERT);

		/*
		 * Check any RLS WITH CHECK policies.
		 *
		 * Normally we should check INSERT policies. But if the insert is the
		 * result of a partition key update that moved the tuple to a new
		 * partition, we should instead check UPDATE policies, because we are
		 * executing policies defined on the target table, and not those
		 * defined on the child partitions.
		 *
		 * If we're running MERGE, we refer to the action that we're executing
		 * to know if we're doing an INSERT or UPDATE to a partition table.
		 */
		if (mtstate->operation == CMD_UPDATE)
			wco_kind = WCO_RLS_UPDATE_CHECK;
#if PG15_GE
		else if (mtstate->operation == CMD_MERGE)
			wco_kind = (context->relaction->mas_action->commandType == CMD_UPDATE) ?
						   WCO_RLS_UPDATE_CHECK :
						   WCO_RLS_INSERT_CHECK;
#endif
		else
			wco_kind = WCO_RLS_INSERT_CHECK;

		/*
		 * ExecWithCheckOptions() will skip any WCOs which are not of the kind
		 * we are looking for at this point.
		 */
		if (resultRelInfo->ri_WithCheckOptions != NIL)
			ExecWithCheckOptions(wco_kind, resultRelInfo, slot, estate);

		/*
		 * Check the constraints of the tuple.
		 */
		if (resultRelationDesc->rd_att->constr)
			ExecConstraints(resultRelInfo, slot, estate);

		/*
		 * Also check the tuple against the partition constraint, if there is
		 * one; except that if we got here via tuple-routing, we don't need to
		 * if there's no BR trigger defined on the partition.
		 */
		if (resultRelationDesc->rd_rel->relispartition &&
			(resultRelInfo->ri_RootResultRelInfo == NULL ||
			 (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_insert_before_row)))
			ExecPartitionCheck(resultRelInfo, slot, estate, true);

		if (onconflict != ONCONFLICT_NONE && resultRelInfo->ri_NumIndices > 0)
		{
			/* Perform a speculative insertion. */
			uint32 specToken;
			ItemPointerData conflictTid;
			bool specConflict;
			List *arbiterIndexes;

			arbiterIndexes = resultRelInfo->ri_onConflictArbiterIndexes;

			/*
			 * Do a non-conclusive check for conflicts first.
			 *
			 * We're not holding any locks yet, so this doesn't guarantee that
			 * the later insert won't conflict.  But it avoids leaving behind
			 * a lot of canceled speculative insertions, if you run a lot of
			 * INSERT ON CONFLICT statements that do conflict.
			 *
			 * We loop back here if we find a conflict below, either during
			 * the pre-check, or when we re-check after inserting the tuple
			 * speculatively.
			 */
		vlock:
			specConflict = false;
			if (!ExecCheckIndexConstraints(resultRelInfo,
										   slot,
										   estate,
										   &conflictTid,
										   arbiterIndexes))
			{
				/* committed conflict tuple found */
				if (onconflict == ONCONFLICT_UPDATE)
				{
					/*
					 * In case of ON CONFLICT DO UPDATE, execute the UPDATE
					 * part.  Be prepared to retry if the UPDATE fails because
					 * of another concurrent UPDATE/DELETE to the conflict
					 * tuple.
					 */
					TupleTableSlot *returning = NULL;

					if (ExecOnConflictUpdate(context,
											 resultRelInfo,
											 &conflictTid,
											 slot,
											 canSetTag,
											 &returning))
					{
						InstrCountTuples2(&mtstate->ps, 1);
						return returning;
					}
					else
						goto vlock;
				}
				else
				{
					/*
					 * In case of ON CONFLICT DO NOTHING, do nothing. However,
					 * verify that the tuple is visible to the executor's MVCC
					 * snapshot at higher isolation levels.
					 *
					 * Using ExecGetReturningSlot() to store the tuple for the
					 * recheck isn't that pretty, but we can't trivially use
					 * the input slot, because it might not be of a compatible
					 * type. As there's no conflicting usage of
					 * ExecGetReturningSlot() in the DO NOTHING case...
					 */
					Assert(onconflict == ONCONFLICT_NOTHING);
					ExecCheckTIDVisible(estate,
										resultRelInfo,
										&conflictTid,
										ExecGetReturningSlot(estate, resultRelInfo));
					InstrCountTuples2(&mtstate->ps, 1);
					return NULL;
				}
			}

			/*
			 * Before we start insertion proper, acquire our "speculative
			 * insertion lock".  Others can use that to wait for us to decide
			 * if we're going to go ahead with the insertion, instead of
			 * waiting for the whole transaction to complete.
			 */
			specToken = SpeculativeInsertionLockAcquire(GetCurrentTransactionId());

			/* insert the tuple, with the speculative token */
			table_tuple_insert_speculative(resultRelationDesc,
										   slot,
										   estate->es_output_cid,
										   0,
										   NULL,
										   specToken);

			/* insert index entries for tuple */
			recheckIndexes = ExecInsertIndexTuplesCompat(resultRelInfo,
														 slot,
														 estate,
														 false,
														 true,
														 &specConflict,
														 arbiterIndexes,
														 false);

			/* adjust the tuple's state accordingly */
			table_tuple_complete_speculative(resultRelationDesc, slot, specToken, !specConflict);

			/*
			 * Wake up anyone waiting for our decision.  They will re-check
			 * the tuple, see that it's no longer speculative, and wait on our
			 * XID as if this was a regularly inserted tuple all along.  Or if
			 * we killed the tuple, they will see it's dead, and proceed as if
			 * the tuple never existed.
			 */
			SpeculativeInsertionLockRelease(GetCurrentTransactionId());

			/*
			 * If there was a conflict, start from the beginning.  We'll do
			 * the pre-check again, which will now find the conflicting tuple
			 * (unless it aborts before we get there).
			 */
			if (specConflict)
			{
				list_free(recheckIndexes);
				goto vlock;
			}

			/* Since there was no insertion conflict, we're done */
		}
		else
		{
			/* insert the tuple normally */
			table_tuple_insert(resultRelationDesc, slot, estate->es_output_cid, 0, NULL);

			/* insert index entries for tuple */
			if (resultRelInfo->ri_NumIndices > 0)
				recheckIndexes = ExecInsertIndexTuplesCompat(resultRelInfo,
															 slot,
															 estate,
															 false,
															 false,
															 NULL,
															 NIL,
															 false);
		}
	}

	if (canSetTag)
		(estate->es_processed)++;

	/*
	 * If this insert is the result of a partition key update that moved the
	 * tuple to a new partition, put this row into the transition NEW TABLE,
	 * if there is one. We need to do this separately for DELETE and INSERT
	 * because they happen on different tables.
	 */
	ar_insert_trig_tcs = mtstate->mt_transition_capture;
	if (mtstate->operation == CMD_UPDATE && mtstate->mt_transition_capture &&
		mtstate->mt_transition_capture->tcs_update_new_table)
	{
		ExecARUpdateTriggersCompat(estate,
								   resultRelInfo,
								   NULL, /* src_partinfo */
								   NULL, /* dst_partinfo */
								   NULL,
								   NULL,
								   slot,
								   NULL,
								   mtstate->mt_transition_capture,
								   false /* is_crosspart_update */
		);

		/*
		 * We've already captured the NEW TABLE row, so make sure any AR
		 * INSERT trigger fired below doesn't capture it again.
		 */
		ar_insert_trig_tcs = NULL;
	}

	/* AFTER ROW INSERT Triggers */
	ExecARInsertTriggers(estate, resultRelInfo, slot, recheckIndexes, ar_insert_trig_tcs);

	list_free(recheckIndexes);

	/*
	 * Check any WITH CHECK OPTION constraints from parent views.  We are
	 * required to do this after testing all constraints and uniqueness
	 * violations per the SQL spec, so we do it after actually inserting the
	 * record into the heap and all indexes.
	 *
	 * ExecWithCheckOptions will elog(ERROR) if a violation is found, so the
	 * tuple will never be seen, if it violates the WITH CHECK OPTION.
	 *
	 * ExecWithCheckOptions() will skip any WCOs which are not of the kind we
	 * are looking for at this point.
	 */
	if (resultRelInfo->ri_WithCheckOptions != NIL)
		ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);

	/* Process RETURNING if present */
	if (resultRelInfo->ri_projectReturning)
		result = ExecProcessReturning(resultRelInfo, slot, planSlot);

	return result;
}

/* ----------------------------------------------------------------
 *		ExecBatchInsert
 *
 *		Insert multiple tuples in an efficient way.
 *		Currently, this handles inserting into a foreign table without
 *		RETURNING clause.
 * ----------------------------------------------------------------
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static void
ExecBatchInsert(ModifyTableState *mtstate, ResultRelInfo *resultRelInfo, TupleTableSlot **slots,
				TupleTableSlot **planSlots, int numSlots, EState *estate, bool canSetTag)
{
	int i;
	int numInserted = numSlots;
	TupleTableSlot *slot = NULL;
	TupleTableSlot **rslots;

	/*
	 * insert into foreign table: let the FDW do it
	 */
	rslots = resultRelInfo->ri_FdwRoutine->ExecForeignBatchInsert(estate,
																  resultRelInfo,
																  slots,
																  planSlots,
																  &numInserted);

	for (i = 0; i < numInserted; i++)
	{
		slot = rslots[i];

		/*
		 * AFTER ROW Triggers or RETURNING expressions might reference the
		 * tableoid column, so (re-)initialize tts_tableOid before evaluating
		 * them.
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

		/* AFTER ROW INSERT Triggers */
		ExecARInsertTriggers(estate, resultRelInfo, slot, NIL, mtstate->mt_transition_capture);

		/*
		 * Check any WITH CHECK OPTION constraints from parent views.  See the
		 * comment in ExecInsert.
		 */
		if (resultRelInfo->ri_WithCheckOptions != NIL)
			ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);
	}

	if (canSetTag && numInserted > 0)
		estate->es_processed += numInserted;
}

/* ----------------------------------------------------------------
 *		ExecUpdate
 *
 *		note: we can't run UPDATE queries with transactions
 *		off because UPDATEs are actually INSERTs and our
 *		scan will mistakenly loop forever, updating the tuple
 *		it just inserted..  This should be fixed but until it
 *		is, we don't want to get stuck in an infinite loop
 *		which corrupts your database..
 *
 *		When updating a table, tupleid identifies the tuple to
 *		update and oldtuple is NULL.  When updating a view, oldtuple
 *		is passed to the INSTEAD OF triggers and identifies what to
 *		update, and tupleid is invalid.  When updating a foreign table,
 *		tupleid is invalid; the FDW has to figure out which row to
 *		update using data from the planSlot.  oldtuple is passed to
 *		foreign table triggers; it is NULL when the foreign table has
 *		no relevant triggers.
 *
 *		slot contains the new tuple value to be stored.
 *		planSlot is the output of the ModifyTable's subplan; we use it
 *		to access values from other input tables (for RETURNING),
 *		row-ID junk columns, etc.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 *
 * copied and modified version of ExecUpdate from executor/nodeModifyTable.c
 */
static TupleTableSlot *
ExecUpdate(ModifyTableContext *context, ResultRelInfo *resultRelInfo, ItemPointer tupleid,
		   HeapTuple oldtuple, TupleTableSlot *slot, bool canSetTag)
{
	EState *estate = context->estate;
	Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;
	TM_Result result;
	List *recheckIndexes = NIL;
	UpdateContext updateCxt = { 0 };

	/*
	 * Prepare for the update. This includes BEFORE ROW triggers, so we're
	 * done if it says we are.
	 */
	if (!ht_ExecUpdatePrologue(context, resultRelInfo, tupleid, oldtuple, slot, NULL))
		return NULL;

	/* INSTEAD OF ROW UPDATE Triggers */
	if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_update_instead_row)
	{
		if (!ExecIRUpdateTriggers(estate, resultRelInfo, oldtuple, slot))
			return NULL; /* "do nothing" */
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		ht_ExecUpdatePrepareSlot(resultRelInfo, slot, estate);

		/*
		 * update in foreign table: let the FDW do it
		 */
		slot = resultRelInfo->ri_FdwRoutine->ExecForeignUpdate(estate,
															   resultRelInfo,
															   slot,
															   context->planSlot);

		if (slot == NULL) /* "do nothing" */
			return NULL;

		/*
		 * AFTER ROW Triggers or RETURNING expressions might reference the
		 * tableoid column, so (re-)initialize tts_tableOid before evaluating
		 * them.  (This covers the case where the FDW replaced the slot.)
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelationDesc);
	}
	else
	{
		/* Fill in the slot appropriately */
		ht_ExecUpdatePrepareSlot(resultRelInfo, slot, estate);

	redo_act:
		result = ht_ExecUpdateAct(context,
								  resultRelInfo,
								  tupleid,
								  oldtuple,
								  slot,
								  canSetTag,
								  &updateCxt);

		/*
		 * If ExecUpdateAct reports that a cross-partition update was done,
		 * then the returning tuple has been projected and there's nothing
		 * else for us to do.
		 */
		if (updateCxt.crossPartUpdate)
			return context->cpUpdateReturningSlot;

		switch (result)
		{
			case TM_SelfModified:

				/*
				 * The target tuple was already updated or deleted by the
				 * current command, or by a later command in the current
				 * transaction.  The former case is possible in a join UPDATE
				 * where multiple tuples join to the same target tuple. This
				 * is pretty questionable, but Postgres has always allowed it:
				 * we just execute the first update action and ignore
				 * additional update attempts.
				 *
				 * The latter case arises if the tuple is modified by a
				 * command in a BEFORE trigger, or perhaps by a command in a
				 * volatile function used in the query.  In such situations we
				 * should not ignore the update, but it is equally unsafe to
				 * proceed.  We don't want to discard the original UPDATE
				 * while keeping the triggered actions based on it; and we
				 * have no principled way to merge this update with the
				 * previous ones.  So throwing an error is the only safe
				 * course.
				 *
				 * If a trigger actually intends this type of interaction, it
				 * can re-execute the UPDATE (assuming it can figure out how)
				 * and then return NULL to cancel the outer update.
				 */
				if (context->tmfd.cmax != estate->es_output_cid)
					ereport(ERROR,
							(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
							 errmsg("tuple to be updated was already modified by an operation "
									"triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger "
									 "to propagate changes to other rows.")));

				/* Else, already updated by self; nothing to do */
				return NULL;

			case TM_Ok:
				break;

			case TM_Updated:
			{
				TupleTableSlot *inputslot;
				TupleTableSlot *epqslot;
				TupleTableSlot *oldSlot;

				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));

				/*
				 * Already know that we're going to need to do EPQ, so
				 * fetch tuple directly into the right slot.
				 */
				inputslot = EvalPlanQualSlot(context->epqstate,
											 resultRelationDesc,
											 resultRelInfo->ri_RangeTableIndex);

				result = table_tuple_lock(resultRelationDesc,
										  tupleid,
										  estate->es_snapshot,
										  inputslot,
										  estate->es_output_cid,
										  context->lockmode,
										  LockWaitBlock,
										  TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
										  &context->tmfd);

				switch (result)
				{
					case TM_Ok:
						Assert(context->tmfd.traversed);

						epqslot = EvalPlanQual(context->epqstate,
											   resultRelationDesc,
											   resultRelInfo->ri_RangeTableIndex,
											   inputslot);
						if (TupIsNull(epqslot))
							/* Tuple not passing quals anymore, exiting... */
							return NULL;

						/* Make sure ri_oldTupleSlot is initialized. */
						if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
							ExecInitUpdateProjection(context->mtstate, resultRelInfo);

						/* Fetch the most recent version of old tuple. */
						oldSlot = resultRelInfo->ri_oldTupleSlot;
						if (!table_tuple_fetch_row_version(resultRelationDesc,
														   tupleid,
														   SnapshotAny,
														   oldSlot))
							elog(ERROR, "failed to fetch tuple being updated");
						slot = ExecGetUpdateNewTuple(resultRelInfo, epqslot, oldSlot);
						goto redo_act;

					case TM_Deleted:
						/* tuple already deleted; nothing to do */
						return NULL;

					case TM_SelfModified:

						/*
						 * This can be reached when following an update
						 * chain from a tuple updated by another session,
						 * reaching a tuple that was already updated in
						 * this transaction. If previously modified by
						 * this command, ignore the redundant update,
						 * otherwise error out.
						 *
						 * See also TM_SelfModified response to
						 * table_tuple_update() above.
						 */
						if (context->tmfd.cmax != estate->es_output_cid)
							ereport(ERROR,
									(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
									 errmsg("tuple to be updated was already modified by an "
											"operation triggered by the current command"),
									 errhint("Consider using an AFTER trigger instead of a BEFORE "
											 "trigger to propagate changes to other rows.")));
						return NULL;

					default:
						/* see table_tuple_lock call in ExecDelete() */
						elog(ERROR, "unexpected table_tuple_lock status: %u", result);
						return NULL;
				}
			}

			break;

			case TM_Deleted:
				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent delete")));
				/* tuple already deleted; nothing to do */
				return NULL;

			default:
				elog(ERROR, "unrecognized table_tuple_update status: %u", result);
				return NULL;
		}
	}

	if (canSetTag)
		(estate->es_processed)++;

	/* AFTER ROW UPDATE Triggers */
	ht_ExecUpdateEpilogue(context,
						  &updateCxt,
						  resultRelInfo,
						  tupleid,
						  oldtuple,
						  slot,
						  recheckIndexes);

	list_free(recheckIndexes);

	/* Process RETURNING if present */
	if (resultRelInfo->ri_projectReturning)
		return ExecProcessReturning(resultRelInfo, slot, context->planSlot);

	return NULL;
}

/*
 * ExecOnConflictUpdate --- execute UPDATE of INSERT ON CONFLICT DO UPDATE
 *
 * Try to lock tuple for update as part of speculative insertion.  If
 * a qual originating from ON CONFLICT DO UPDATE is satisfied, update
 * (but still lock row, even though it may not satisfy estate's
 * snapshot).
 *
 * Returns true if we're done (with or without an update), or false if
 * the caller must retry the INSERT from scratch.
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static bool
ExecOnConflictUpdate(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
					 ItemPointer conflictTid, TupleTableSlot *excludedSlot, bool canSetTag,
					 TupleTableSlot **returning)
{
	ModifyTableState *mtstate = context->mtstate;
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	Relation relation = resultRelInfo->ri_RelationDesc;
	ExprState *onConflictSetWhere = resultRelInfo->ri_onConflict->oc_WhereClause;
	TupleTableSlot *existing = resultRelInfo->ri_onConflict->oc_Existing;
	TM_FailureData tmfd;
	LockTupleMode lockmode;
	TM_Result test;
	Datum xminDatum;
	TransactionId xmin;
	bool isnull;

	/* Determine lock mode to use */
	lockmode = ExecUpdateLockMode(context->estate, resultRelInfo);

	/*
	 * Lock tuple for update.  Don't follow updates when tuple cannot be
	 * locked without doing so.  A row locking conflict here means our
	 * previous conclusion that the tuple is conclusively committed is not
	 * true anymore.
	 */
	test = table_tuple_lock(relation,
							conflictTid,
							context->estate->es_snapshot,
							existing,
							context->estate->es_output_cid,
							lockmode,
							LockWaitBlock,
							0,
							&tmfd);
	switch (test)
	{
		case TM_Ok:
			/* success! */
			break;

		case TM_Invisible:

			/*
			 * This can occur when a just inserted tuple is updated again in
			 * the same command. E.g. because multiple rows with the same
			 * conflicting key values are inserted.
			 *
			 * This is somewhat similar to the ExecUpdate() TM_SelfModified
			 * case.  We do not want to proceed because it would lead to the
			 * same row being updated a second time in some unspecified order,
			 * and in contrast to plain UPDATEs there's no historical behavior
			 * to break.
			 *
			 * It is the user's responsibility to prevent this situation from
			 * occurring.  These problems are why SQL-2003 similarly specifies
			 * that for SQL MERGE, an exception must be raised in the event of
			 * an attempt to update the same row twice.
			 */
			xminDatum = slot_getsysattr(existing, MinTransactionIdAttributeNumber, &isnull);
			Assert(!isnull);
			xmin = DatumGetTransactionId(xminDatum);

			if (TransactionIdIsCurrentTransactionId(xmin))
				ereport(ERROR,
						(errcode(ERRCODE_CARDINALITY_VIOLATION),
						 errmsg("ON CONFLICT DO UPDATE command cannot affect row a second time"),
						 errhint("Ensure that no rows proposed for insertion within the same "
								 "command have duplicate constrained values.")));

			/* This shouldn't happen */
			elog(ERROR, "attempted to lock invisible tuple");
			break;

		case TM_SelfModified:

			/*
			 * This state should never be reached. As a dirty snapshot is used
			 * to find conflicting tuples, speculative insertion wouldn't have
			 * seen this row to conflict with.
			 */
			elog(ERROR, "unexpected self-updated tuple");
			break;

		case TM_Updated:
			if (IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent update")));

			/*
			 * As long as we don't support an UPDATE of INSERT ON CONFLICT for
			 * a partitioned table we shouldn't reach to a case where tuple to
			 * be lock is moved to another partition due to concurrent update
			 * of the partition key.
			 */
			Assert(!ItemPointerIndicatesMovedPartitions(&tmfd.ctid));

			/*
			 * Tell caller to try again from the very start.
			 *
			 * It does not make sense to use the usual EvalPlanQual() style
			 * loop here, as the new version of the row might not conflict
			 * anymore, or the conflicting tuple has actually been deleted.
			 */
			ExecClearTuple(existing);
			return false;

		case TM_Deleted:
			if (IsolationUsesXactSnapshot())
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("could not serialize access due to concurrent delete")));

			/* see TM_Updated case */
			Assert(!ItemPointerIndicatesMovedPartitions(&tmfd.ctid));
			ExecClearTuple(existing);
			return false;

		default:
			elog(ERROR, "unrecognized table_tuple_lock status: %u", test);
	}

	/* Success, the tuple is locked. */

	/*
	 * Verify that the tuple is visible to our MVCC snapshot if the current
	 * isolation level mandates that.
	 *
	 * It's not sufficient to rely on the check within ExecUpdate() as e.g.
	 * CONFLICT ... WHERE clause may prevent us from reaching that.
	 *
	 * This means we only ever continue when a new command in the current
	 * transaction could see the row, even though in READ COMMITTED mode the
	 * tuple will not be visible according to the current statement's
	 * snapshot.  This is in line with the way UPDATE deals with newer tuple
	 * versions.
	 */
	ExecCheckTupleVisible(context->estate, relation, existing);

	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject.  The EXCLUDED tuple is installed in ecxt_innertuple, while
	 * the target's existing tuple is installed in the scantuple.  EXCLUDED
	 * has been made to reference INNER_VAR in setrefs.c, but there is no
	 * other redirection.
	 */
	econtext->ecxt_scantuple = existing;
	econtext->ecxt_innertuple = excludedSlot;
	econtext->ecxt_outertuple = NULL;

	if (!ExecQual(onConflictSetWhere, econtext))
	{
		ExecClearTuple(existing); /* see return below */
		InstrCountFiltered1(&mtstate->ps, 1);
		return true; /* done with the tuple */
	}

	if (resultRelInfo->ri_WithCheckOptions != NIL)
	{
		/*
		 * Check target's existing tuple against UPDATE-applicable USING
		 * security barrier quals (if any), enforced here as RLS checks/WCOs.
		 *
		 * The rewriter creates UPDATE RLS checks/WCOs for UPDATE security
		 * quals, and stores them as WCOs of "kind" WCO_RLS_CONFLICT_CHECK,
		 * but that's almost the extent of its special handling for ON
		 * CONFLICT DO UPDATE.
		 *
		 * The rewriter will also have associated UPDATE applicable straight
		 * RLS checks/WCOs for the benefit of the ExecUpdate() call that
		 * follows.  INSERTs and UPDATEs naturally have mutually exclusive WCO
		 * kinds, so there is no danger of spurious over-enforcement in the
		 * INSERT or UPDATE path.
		 */
		ExecWithCheckOptions(WCO_RLS_CONFLICT_CHECK, resultRelInfo, existing, mtstate->ps.state);
	}

	/* Project the new tuple version */
	ExecProject(resultRelInfo->ri_onConflict->oc_ProjInfo);

	/*
	 * Note that it is possible that the target tuple has been modified in
	 * this session, after the above table_tuple_lock. We choose to not error
	 * out in that case, in line with ExecUpdate's treatment of similar cases.
	 * This can happen if an UPDATE is triggered from within ExecQual(),
	 * ExecWithCheckOptions() or ExecProject() above, e.g. by selecting from a
	 * wCTE in the ON CONFLICT's SET.
	 */

	/* Execute UPDATE with projection */
	*returning = ExecUpdate(context,
							resultRelInfo,
							conflictTid,
							NULL,
							resultRelInfo->ri_onConflict->oc_ProjSlot,
							canSetTag);

	/*
	 * Clear out existing tuple, as there might not be another conflict among
	 * the next input rows. Don't want to hold resources till the end of the
	 * query.
	 */
	ExecClearTuple(existing);
	return true;
}

/*
 * ExecCheckTupleVisible -- verify tuple is visible
 *
 * It would not be consistent with guarantees of the higher isolation levels to
 * proceed with avoiding insertion (taking speculative insertion's alternative
 * path) on the basis of another tuple that is not visible to MVCC snapshot.
 * Check for the need to raise a serialization failure, and do so as necessary.
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static void
ExecCheckTupleVisible(EState *estate, Relation rel, TupleTableSlot *slot)
{
	if (!IsolationUsesXactSnapshot())
		return;

	if (!table_tuple_satisfies_snapshot(rel, slot, estate->es_snapshot))
	{
		Datum xminDatum;
		TransactionId xmin;
		bool isnull;

		xminDatum = slot_getsysattr(slot, MinTransactionIdAttributeNumber, &isnull);
		Assert(!isnull);
		xmin = DatumGetTransactionId(xminDatum);

		/*
		 * We should not raise a serialization failure if the conflict is
		 * against a tuple inserted by our own transaction, even if it's not
		 * visible to our snapshot.  (This would happen, for example, if
		 * conflicting keys are proposed for insertion in a single command.)
		 */
		if (!TransactionIdIsCurrentTransactionId(xmin))
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("could not serialize access due to concurrent update")));
	}
}

/*
 * ExecCheckTIDVisible -- convenience variant of ExecCheckTupleVisible()
 *
 * copied verbatim from executor/nodeModifyTable.c
 */
static void
ExecCheckTIDVisible(EState *estate, ResultRelInfo *relinfo, ItemPointer tid,
					TupleTableSlot *tempSlot)
{
	Relation rel = relinfo->ri_RelationDesc;

	/* Redundantly check isolation level */
	if (!IsolationUsesXactSnapshot())
		return;

	if (!table_tuple_fetch_row_version(rel, tid, SnapshotAny, tempSlot))
		elog(ERROR, "failed to fetch conflicting tuple for ON CONFLICT");
	ExecCheckTupleVisible(estate, rel, tempSlot);
	ExecClearTuple(tempSlot);
}

/* ----------------------------------------------------------------
 *		ExecDelete
 *
 *		DELETE is like UPDATE, except that we delete the tuple and no
 *		index modifications are needed.
 *
 *		When deleting from a table, tupleid identifies the tuple to
 *		delete and oldtuple is NULL.  When deleting from a view,
 *		oldtuple is passed to the INSTEAD OF triggers and identifies
 *		what to delete, and tupleid is invalid.  When deleting from a
 *		foreign table, tupleid is invalid; the FDW has to figure out
 *		which row to delete using data from the planSlot.  oldtuple is
 *		passed to foreign table triggers; it is NULL when the foreign
 *		table has no relevant triggers.  We use tupleDeleted to indicate
 *		whether the tuple is actually deleted, callers can use it to
 *		decide whether to continue the operation.  When this DELETE is a
 *		part of an UPDATE of partition-key, then the slot returned by
 *		EvalPlanQual() is passed back using output parameter epqslot.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 *
 * copied from executor/nodeModifyTable.c
 */
static TupleTableSlot *
ExecDelete(ModifyTableContext *context, ResultRelInfo *resultRelInfo, ItemPointer tupleid,
		   HeapTuple oldtuple, bool processReturning, bool canSetTag, bool changingPart,
		   bool *tupleDeleted, TupleTableSlot **epqreturnslot)
{
	EState *estate = context->estate;
	Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;
	TM_Result result;
	TupleTableSlot *slot = NULL;

	if (tupleDeleted)
		*tupleDeleted = false;

	/*
	 * Prepare for the delete.  This includes BEFORE ROW triggers, so we're
	 * done if it says we are.
	 */
	if (!ht_ExecDeletePrologue(context, resultRelInfo, tupleid, oldtuple, epqreturnslot, NULL))
		return NULL;

	/* INSTEAD OF ROW DELETE Triggers */
	if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_delete_instead_row)
	{
		bool dodelete;

		Assert(oldtuple != NULL);
		dodelete = ExecIRDeleteTriggers(estate, resultRelInfo, oldtuple);

		if (!dodelete) /* "do nothing" */
			return NULL;
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		/*
		 * delete from foreign table: let the FDW do it
		 *
		 * We offer the returning slot as a place to store RETURNING data,
		 * although the FDW can return some other slot if it wants.
		 */
		slot = ExecGetReturningSlot(estate, resultRelInfo);
		slot = resultRelInfo->ri_FdwRoutine->ExecForeignDelete(estate,
															   resultRelInfo,
															   slot,
															   context->planSlot);

		if (slot == NULL) /* "do nothing" */
			return NULL;

		/*
		 * RETURNING expressions might reference the tableoid column, so
		 * (re)initialize tts_tableOid before evaluating them.
		 */
		if (TTS_EMPTY(slot))
			ExecStoreAllNullTuple(slot);

		slot->tts_tableOid = RelationGetRelid(resultRelationDesc);
	}
	else
	{
		/*
		 * delete the tuple
		 *
		 * Note: if context->estate->es_crosscheck_snapshot isn't
		 * InvalidSnapshot, we check that the row to be deleted is visible to
		 * that snapshot, and throw a can't-serialize error if not. This is a
		 * special-case behavior needed for referential integrity updates in
		 * transaction-snapshot mode transactions.
		 */
	ldelete:;
		if (!ItemPointerIsValid(tupleid))
		{
			elog(ERROR,
				 "cannot update/delete rows from chunk \"%s\" as it is compressed",
				 NameStr(resultRelationDesc->rd_rel->relname));
		}
		result = ht_ExecDeleteAct(context, resultRelInfo, tupleid, changingPart);

		switch (result)
		{
			case TM_SelfModified:

				/*
				 * The target tuple was already updated or deleted by the
				 * current command, or by a later command in the current
				 * transaction.  The former case is possible in a join DELETE
				 * where multiple tuples join to the same target tuple. This
				 * is somewhat questionable, but Postgres has always allowed
				 * it: we just ignore additional deletion attempts.
				 *
				 * The latter case arises if the tuple is modified by a
				 * command in a BEFORE trigger, or perhaps by a command in a
				 * volatile function used in the query.  In such situations we
				 * should not ignore the deletion, but it is equally unsafe to
				 * proceed.  We don't want to discard the original DELETE
				 * while keeping the triggered actions based on its deletion;
				 * and it would be no better to allow the original DELETE
				 * while discarding updates that it triggered.  The row update
				 * carries some information that might be important according
				 * to business rules; so throwing an error is the only safe
				 * course.
				 *
				 * If a trigger actually intends this type of interaction, it
				 * can re-execute the DELETE and then return NULL to cancel
				 * the outer delete.
				 */
				if (context->tmfd.cmax != estate->es_output_cid)
					ereport(ERROR,
							(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
							 errmsg("tuple to be deleted was already modified by an operation "
									"triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger "
									 "to propagate changes to other rows.")));

				/* Else, already deleted by self; nothing to do */
				return NULL;

			case TM_Ok:
				break;

			case TM_Updated:
			{
				TupleTableSlot *inputslot;
				TupleTableSlot *epqslot;

				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent update")));

				/*
				 * Already know that we're going to need to do EPQ, so
				 * fetch tuple directly into the right slot.
				 */
				EvalPlanQualBegin(context->epqstate);
				inputslot = EvalPlanQualSlot(context->epqstate,
											 resultRelationDesc,
											 resultRelInfo->ri_RangeTableIndex);

				result = table_tuple_lock(resultRelationDesc,
										  tupleid,
										  estate->es_snapshot,
										  inputslot,
										  estate->es_output_cid,
										  LockTupleExclusive,
										  LockWaitBlock,
										  TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
										  &context->tmfd);

				switch (result)
				{
					case TM_Ok:
						Assert(context->tmfd.traversed);
						epqslot = EvalPlanQual(context->epqstate,
											   resultRelationDesc,
											   resultRelInfo->ri_RangeTableIndex,
											   inputslot);
						if (TupIsNull(epqslot))
							/* Tuple not passing quals anymore, exiting... */
							return NULL;

						/*
						 * If requested, skip delete and pass back the
						 * updated row.
						 */
						if (epqreturnslot)
						{
							*epqreturnslot = epqslot;
							return NULL;
						}
						else
							goto ldelete;

					case TM_SelfModified:

						/*
						 * This can be reached when following an update
						 * chain from a tuple updated by another session,
						 * reaching a tuple that was already updated in
						 * this transaction. If previously updated by this
						 * command, ignore the delete, otherwise error
						 * out.
						 *
						 * See also TM_SelfModified response to
						 * table_tuple_delete() above.
						 */
						if (context->tmfd.cmax != estate->es_output_cid)
							ereport(ERROR,
									(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
									 errmsg("tuple to be deleted was already modified by an "
											"operation triggered by the current command"),
									 errhint("Consider using an AFTER trigger instead of a BEFORE "
											 "trigger to propagate changes to other rows.")));
						return NULL;

					case TM_Deleted:
						/* tuple already deleted; nothing to do */
						return NULL;

					default:

						/*
						 * TM_Invisible should be impossible because we're
						 * waiting for updated row versions, and would
						 * already have errored out if the first version
						 * is invisible.
						 *
						 * TM_Updated should be impossible, because we're
						 * locking the latest version via
						 * TUPLE_LOCK_FLAG_FIND_LAST_VERSION.
						 */
						elog(ERROR, "unexpected table_tuple_lock status: %u", result);
						return NULL;
				}

				Assert(false);
				break;
			}

			case TM_Deleted:
				if (IsolationUsesXactSnapshot())
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("could not serialize access due to concurrent delete")));
				/*
				 * tuple already deleted; nothing to do. But MERGE might want
				 * to handle it differently. We've already filled-in
				 * actionInfo with sufficient information for MERGE to look
				 * at.
				 */
				return NULL;

			default:
				elog(ERROR, "unrecognized table_tuple_delete status: %u", result);
				return NULL;
		}

		/*
		 * Note: Normally one would think that we have to delete index tuples
		 * associated with the heap tuple now...
		 *
		 * ... but in POSTGRES, we have no need to do this because VACUUM will
		 * take care of it later.  We can't delete index tuples immediately
		 * anyway, since the tuple is still visible to other transactions.
		 */
	}

	if (canSetTag)
		(estate->es_processed)++;

	/* Tell caller that the delete actually happened. */
	if (tupleDeleted)
		*tupleDeleted = true;

	ht_ExecDeleteEpilogue(context, resultRelInfo, tupleid, oldtuple);

	/* Process RETURNING if present and if requested */
	if (processReturning && resultRelInfo->ri_projectReturning)
	{
		/*
		 * We have to put the target tuple into a slot, which means first we
		 * gotta fetch it.  We can use the trigger tuple slot.
		 */
		TupleTableSlot *rslot;

		if (resultRelInfo->ri_FdwRoutine)
		{
			/* FDW must have provided a slot containing the deleted row */
			Assert(!TupIsNull(slot));
		}
		else
		{
			slot = ExecGetReturningSlot(estate, resultRelInfo);
			if (oldtuple != NULL)
			{
				ExecForceStoreHeapTuple(oldtuple, slot, false);
			}
			else
			{
				if (!table_tuple_fetch_row_version(resultRelationDesc, tupleid, SnapshotAny, slot))
					elog(ERROR, "failed to fetch deleted tuple for DELETE RETURNING");
			}
		}

		rslot = ExecProcessReturning(resultRelInfo, slot, context->planSlot);

		/*
		 * Before releasing the target tuple again, make sure rslot has a
		 * local copy of any pass-by-reference values.
		 */
		ExecMaterializeSlot(rslot);

		ExecClearTuple(slot);

		return rslot;
	}

	return NULL;
}

#endif
