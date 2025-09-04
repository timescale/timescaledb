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
	cd->hypertable_result_rel_info = NULL;
	cd->cache =
		ts_subspace_store_init(ht->space, estate->es_query_cxt, ts_guc_max_open_chunks_per_insert);
	cd->prev_cis = NULL;
	cd->prev_cis_oid = InvalidOid;
	cd->counters = palloc0(sizeof(SharedCounters));

	return cd;
}

void
ts_chunk_dispatch_destroy(ChunkDispatch *chunk_dispatch)
{
	ts_subspace_store_free(chunk_dispatch->cache);
}

static void
destroy_chunk_insert_state(void *cis)
{
	ts_chunk_insert_state_destroy((ChunkInsertState *) cis);
}

/*
 * Get the chunk insert state for the chunk that matches the given point in the
 * partitioned hyperspace.
 */
extern ChunkInsertState *
ts_chunk_dispatch_get_chunk_insert_state(ChunkDispatch *dispatch, Point *point,
										 const on_chunk_changed_func on_chunk_changed, void *data)
{
	ChunkInsertState *cis;
	bool cis_changed = true;
	Chunk *chunk = NULL;

	cis = ts_subspace_store_get(dispatch->cache, point);

	/*
	 * The chunk search functions may leak memory, so switch to a temporary
	 * memory context.
	 */
	MemoryContext old_context = MemoryContextSwitchTo(GetPerTupleMemoryContext(dispatch->estate));

	if (!cis)
	{
		/*
		 * Normally, for every row of the chunk except the first one, we expect
		 * the chunk to exist already. The "create" function would take a lock
		 * on the hypertable to serialize the concurrent chunk creation. Here we
		 * first use the "find" function to try to find the chunk without
		 * locking the hypertable. This serves as a fast path for the usual case
		 * where the chunk already exists.
		 */
		chunk = ts_hypertable_find_chunk_for_point(dispatch->hypertable, point);

		/*
		 * Frozen chunks require at least PG14.
		 */
		if (chunk && ts_chunk_is_frozen(chunk))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot INSERT into frozen chunk \"%s\"",
							get_rel_name(chunk->table_id))));
		if (chunk && IS_OSM_CHUNK(chunk))
		{
			const Dimension *time_dim =
				hyperspace_get_open_dimension(dispatch->hypertable->space, 0);
			Assert(time_dim != NULL);

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Cannot insert into tiered chunk range of %s.%s - attempt to create "
							"new chunk "
							"with range  [%s %s] failed",
							NameStr(dispatch->hypertable->fd.schema_name),
							NameStr(dispatch->hypertable->fd.table_name),
							ts_internal_to_time_string(chunk->cube->slices[0]->fd.range_start,
													   time_dim->fd.column_type),
							ts_internal_to_time_string(chunk->cube->slices[0]->fd.range_end,
													   time_dim->fd.column_type)),
					 errhint(
						 "Hypertable has tiered data with time range that overlaps the insert")));
		}

		if (!chunk)
		{
			chunk = ts_hypertable_create_chunk_for_point(dispatch->hypertable, point);
		}

		if (!chunk)
			elog(ERROR, "no chunk found or created");

		if (dispatch->create_compressed_chunk && !chunk->fd.compressed_chunk_id)
		{
			/*
			 * When we try to create a compressed chunk, we need to grab a lock on the
			 * chunk to synchronize with other concurrent insert operations trying to
			 * create the same compressed chunk.
			 */
			LockRelationOid(chunk->table_id, ShareUpdateExclusiveLock);
			chunk = ts_chunk_get_by_id(chunk->fd.id, CACHE_FLAG_NONE);
			/* recheck whether compressed chunk exists after acquiring the lock */
			if (!chunk->fd.compressed_chunk_id)
			{
				Hypertable *compressed_ht =
					ts_hypertable_get_by_id(dispatch->hypertable->fd.compressed_hypertable_id);
				Chunk *compressed_chunk =
					ts_cm_functions->compression_chunk_create(compressed_ht, chunk);
				ts_chunk_set_compressed_chunk(chunk, compressed_chunk->fd.id);
			}
		}

		cis = ts_chunk_insert_state_create(chunk->table_id, dispatch);
		ts_subspace_store_add(dispatch->cache, chunk->cube, cis, destroy_chunk_insert_state);
	}
	else if (cis->rel->rd_id == dispatch->prev_cis_oid && cis == dispatch->prev_cis)
	{
		/* got the same item from cache as before */
		cis_changed = false;
	}

	MemoryContextSwitchTo(old_context);

	if (cis_changed && on_chunk_changed)
		on_chunk_changed(cis, data);

	Assert(cis != NULL);
	dispatch->prev_cis = cis;
	dispatch->prev_cis_oid = cis->rel->rd_id;
	return cis;
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

/*
 * Change to another chunk for inserts.
 *
 * Prepare the ModifyTableState executor node for inserting into another
 * chunk. Called every time we switch to another chunk for inserts.
 */
static void
on_chunk_insert_state_changed(ChunkInsertState *cis, void *data)
{
	ChunkDispatchState *state = data;
	state->rri = cis->result_relation_info;
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
	ChunkDispatch *dispatch = state->dispatch;
	Hypertable *ht = dispatch->hypertable;
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
	if (dispatch->dispatch_state->mtstate->operation == CMD_MERGE)
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
			List *actionStates = dispatch->dispatch_state->mtstate->resultRelInfo
									 ->ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET];
#else
			List *actionStates =
				dispatch->dispatch_state->mtstate->resultRelInfo->ri_notMatchedMergeAction;
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

	/* Save the main table's (hypertable's) ResultRelInfo */
	if (!dispatch->hypertable_result_rel_info)
	{
		dispatch->hypertable_result_rel_info = dispatch->dispatch_state->mtstate->resultRelInfo;
	}

	/* Find or create the insert state matching the point */
	cis = ts_chunk_dispatch_get_chunk_insert_state(dispatch,
												   point,
												   on_chunk_insert_state_changed,
												   state);

	bool update_counter = cis->onConflictAction == ONCONFLICT_UPDATE;

	ts_chunk_tuple_routing_decompress_for_insert(cis, slot, dispatch->estate, update_counter);

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

/*
 * This function is called during the init phase of the INSERT (ModifyTable)
 * plan, and gives the ChunkDispatchState node the access it needs to the
 * internals of the ModifyTableState node.
 *
 * Note that the function is called by the parent of the ModifyTableState node,
 * which guarantees that the ModifyTableState is fully initialized even though
 * ChunkDispatchState is a child of ModifyTableState.
 */
void
ts_chunk_dispatch_state_set_parent(ChunkDispatchState *state, ModifyTableState *mtstate)
{
	/* Inserts on hypertables should always have one subplan */
	state->mtstate = mtstate;
}
