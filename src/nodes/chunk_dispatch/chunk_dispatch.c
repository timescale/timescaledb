/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/xact.h>
#include <catalog/pg_type.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <parser/parsetree.h>
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "chunk_tuple_routing.h"
#include "dimension.h"
#include "errors.h"
#include "guc.h"
#include "hypercube.h"
#include "nodes/modify_hypertable.h"
#include "subspace_store.h"

static Node *chunk_dispatch_state_create(CustomScan *cscan);

ChunkDispatch *
ts_chunk_dispatch_create(Hypertable *ht, EState *estate)
{
	ChunkDispatch *cd = palloc0(sizeof(ChunkDispatch));

	cd->hypertable = ht;
	cd->estate = estate;
	cd->cache =
		ts_subspace_store_init(ht->space, estate->es_query_cxt, ts_guc_max_open_chunks_per_insert);

	return cd;
}

void
ts_chunk_dispatch_destroy(ChunkDispatch *chunk_dispatch)
{
	ts_subspace_store_free(chunk_dispatch->cache);
}

static CustomScanMethods chunk_dispatch_plan_methods = {
	.CustomName = "ChunkDispatch",
	.CreateCustomScanState = chunk_dispatch_state_create,
};

/* Create a chunk dispatch plan node in the form of a CustomScan node. The
 * purpose of this plan node is to dispatch (route) tuples to the correct chunk
 * in a hypertable.
 *
 * Note that CustomScan nodes cannot be extended (by struct embedding) because
 * they might be copied, therefore we pass hypertable_relid in the
 * custom_private field.
 *
 * The chunk dispatch plan takes the original tuple-producing subplan, which
 * was part of a ModifyTable node, and imposes itself between the
 * ModifyTable plan and the subplan. During execution, the subplan will
 * produce the new tuples that the chunk dispatch node routes before passing
 * them up to the ModifyTable node.
 */
static Plan *
chunk_dispatch_plan_create(PlannerInfo *root, RelOptInfo *relopt, CustomPath *best_path,
						   List *tlist, List *clauses, List *custom_plans)
{
	ChunkDispatchPath *cdpath = (ChunkDispatchPath *) best_path;
	CustomScan *cscan = makeNode(CustomScan);
	ListCell *lc;

	foreach (lc, custom_plans)
	{
		Plan *subplan = lfirst(lc);

		cscan->scan.plan.startup_cost += subplan->startup_cost;
		cscan->scan.plan.total_cost += subplan->total_cost;
		cscan->scan.plan.plan_rows += subplan->plan_rows;
		cscan->scan.plan.plan_width += subplan->plan_width;
	}

	cscan->custom_private = list_make1_oid(cdpath->hypertable_relid);
	cscan->methods = &chunk_dispatch_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.scanrelid = 0; /* Indicate this is not a real relation we are
								* scanning */
	/* The "input" and "output" target lists should be the same */
	cscan->custom_scan_tlist = tlist;
	cscan->scan.plan.targetlist = tlist;

	if (root->parse->commandType == CMD_MERGE)
	{
		/* replace expressions of ROWID_VAR */
		tlist = ts_replace_rowid_vars(root, tlist, relopt->relid);
		cscan->scan.plan.targetlist = tlist;
		cscan->custom_scan_tlist = tlist;
	}
	return &cscan->scan.plan;
}

static CustomPathMethods chunk_dispatch_path_methods = {
	.CustomName = "ChunkDispatchPath",
	.PlanCustomPath = chunk_dispatch_plan_create,
};

Path *
ts_chunk_dispatch_path_create(PlannerInfo *root, ModifyTablePath *mtpath)
{
	ChunkDispatchPath *path = (ChunkDispatchPath *) palloc0(sizeof(ChunkDispatchPath));
	Path *subpath = mtpath->subpath;
	RangeTblEntry *rte = planner_rt_fetch(mtpath->nominalRelation, root);

	memcpy(&path->cpath.path, subpath, sizeof(Path));
	path->cpath.path.type = T_CustomPath;
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.methods = &chunk_dispatch_path_methods;
	path->cpath.custom_paths = list_make1(subpath);
	path->mtpath = mtpath;
	path->hypertable_relid = rte->relid;

	return &path->cpath.path;
}

static void
chunk_dispatch_begin(CustomScanState *node, EState *estate, int eflags)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	Hypertable *ht;
	Cache *hypertable_cache;
	PlanState *ps;

	ht = ts_hypertable_cache_get_cache_and_entry(state->hypertable_relid,
												 CACHE_FLAG_NONE,
												 &hypertable_cache);
	ps = ExecInitNode(state->subplan, estate, eflags);
	state->hypertable_cache = hypertable_cache;
	state->dispatch = ts_chunk_dispatch_create(ht, estate);
	state->dispatch->dispatch_state = state;
	node->custom_ps = list_make1(ps);
}

static AttrNumber
rel_get_natts(Oid relid)
{
	HeapTuple tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tp))
		elog(ERROR, "cache lookup failed for relation %u", relid);
	AttrNumber natts = ((Form_pg_class) GETSTRUCT(tp))->relnatts;
	ReleaseSysCache(tp);
	return natts;
}

static bool
attr_is_dropped_or_missing(Oid relid, AttrNumber attno)
{
	HeapTuple tp = SearchSysCache2(ATTNUM, ObjectIdGetDatum(relid), Int16GetDatum(attno));
	if (!HeapTupleIsValid(tp))
		return false;
	Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);
	bool result = att_tup->attisdropped || att_tup->atthasmissing;
	ReleaseSysCache(tp);
	return result;
}

static TupleTableSlot *
chunk_dispatch_exec(CustomScanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	PlanState *substate = linitial(node->custom_ps);
	TupleTableSlot *slot;
	Point *point;
	ChunkInsertState *cis;
	ChunkTupleRouting *ctr = state->dispatch->ctr;
	Hypertable *ht = ctr->hypertable;
	EState *estate = node->ss.ps.state;
	MemoryContext old;

	/* Get the next tuple from the subplan state node */
	slot = ExecProcNode(substate);

	if (TupIsNull(slot))
		return NULL;

	/* Reset the per-tuple exprcontext */
	ResetPerTupleExprContext(estate);

	/* Switch to the executor's per-tuple memory context */
	old = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	TupleTableSlot *newslot = NULL;
	if (ctr->mht_state->mt->operation == CMD_MERGE)
	{
		const AttrNumber natts = rel_get_natts(ht->main_table_relid);
		for (AttrNumber attno = 1; attno <= natts; attno++)
		{
			if (attr_is_dropped_or_missing(ht->main_table_relid, attno))
			{
				state->is_dropped_attr_exists = true;
				break;
			}
		}
		for (int i = 0; i < ht->space->num_dimensions; i++)
		{
			/*
			 * XXX do we need an additional support of NOT MATCHED BY SOURCE
			 * for PG >= 17? See PostgreSQL commit 0294df2f1f84
			 */
#if PG17_GE
			List *actionStates =
				ctr->hypertable_rri->ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET];
#else
			List *actionStates = ctr->hypertable_rri->ri_notMatchedMergeAction;
#endif
			ListCell *l;
			foreach (l, actionStates)
			{
				MergeActionState *action = (MergeActionState *) lfirst(l);
				CmdType commandType = action->mas_action->commandType;
				if (commandType == CMD_INSERT)
				{
					/* fetch full projection list */
					action->mas_proj->pi_exprContext->ecxt_innertuple = slot;
					newslot = ExecProject(action->mas_proj);
					break;
				}
			}
			if (newslot)
				break;
		}
	}
	/* Calculate the tuple's point in the N-dimensional hyperspace */
	point = ts_hyperspace_calculate_point(ht->space, (newslot ? newslot : slot));

	/* Find or create the insert state matching the point */
	cis = ts_chunk_tuple_routing_find_chunk(ctr, point);

	bool update_counter = cis->onConflictAction == ONCONFLICT_UPDATE;

	ts_chunk_tuple_routing_decompress_for_insert(cis, slot, ctr->estate, update_counter);

	MemoryContextSwitchTo(old);

	/*
	 * Save away the insert state for using it in ExecInsert().
	 *
	 * We need to return the original slot from the subplan since otherwise
	 * the slot might not match what is expected. The expected slot should
	 * contain a tuple with the same definition as the parent hypertable while
	 * the slot in the chunk insert state is a slot matching the chunk
	 * definition.
	 *
	 * If columns have been dropped from the parent hypertable, new chunks
	 * will not have these dropped attributes, which will cause problems later
	 * in the insert execution (see ExecGetInsertNewTuple()).
	 *
	 * We can probably improve this code by removing ChunkDispatch. See
	 * hypertable_modify.c for more information.
	 */
	state->cis = cis;

	return slot;
}

static void
chunk_dispatch_end(CustomScanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	PlanState *substate = linitial(node->custom_ps);

	ExecEndNode(substate);
	ts_chunk_dispatch_destroy(state->dispatch);
	ts_cache_release(&state->hypertable_cache);
}

static void
chunk_dispatch_rescan(CustomScanState *node)
{
	PlanState *substate = linitial(node->custom_ps);

	ExecReScan(substate);
}

static CustomExecMethods chunk_dispatch_state_methods = {
	.CustomName = "ChunkDispatchState",
	.BeginCustomScan = chunk_dispatch_begin,
	.EndCustomScan = chunk_dispatch_end,
	.ExecCustomScan = chunk_dispatch_exec,
	.ReScanCustomScan = chunk_dispatch_rescan,
};

/*
 * Create a ChunkDispatchState node from this plan. This is the full execution
 * state that replaces the plan node as the plan moves from planning to
 * execution.
 */
static Node *
chunk_dispatch_state_create(CustomScan *cscan)
{
	ChunkDispatchState *state;
	Oid hypertable_relid = linitial_oid(cscan->custom_private);

	state = (ChunkDispatchState *) newNode(sizeof(ChunkDispatchState), T_CustomScanState);
	state->hypertable_relid = hypertable_relid;
	Assert(list_length(cscan->custom_plans) == 1);
	state->subplan = linitial(cscan->custom_plans);
	state->cscan_state.methods = &chunk_dispatch_state_methods;
	return (Node *) state;
}

/*
 * Check whether the PlanState is a ChunkDispatchState node.
 */
bool
ts_is_chunk_dispatch_state(PlanState *state)
{
	CustomScanState *csstate = (CustomScanState *) state;

	if (!IsA(state, CustomScanState))
		return false;

	return csstate->methods == &chunk_dispatch_state_methods;
}
