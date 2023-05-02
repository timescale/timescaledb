/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <nodes/nodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <parser/parsetree.h>
#include <utils/rel.h>
#include <catalog/pg_type.h>

#include "compat/compat.h"
#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "errors.h"
#include "subspace_store.h"
#include "dimension.h"
#include "guc.h"
#include "ts_catalog/chunk_data_node.h"

static Node *chunk_dispatch_state_create(CustomScan *cscan);

ChunkDispatch *
ts_chunk_dispatch_create(Hypertable *ht, EState *estate, int eflags)
{
	ChunkDispatch *cd = palloc0(sizeof(ChunkDispatch));

	cd->hypertable = ht;
	cd->estate = estate;
	cd->eflags = eflags;
	cd->hypertable_result_rel_info = NULL;
	cd->cache =
		ts_subspace_store_init(ht->space, estate->es_query_cxt, ts_guc_max_open_chunks_per_insert);
	cd->prev_cis = NULL;
	cd->prev_cis_oid = InvalidOid;

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
										 TupleTableSlot *slot,
										 const on_chunk_changed_func on_chunk_changed, void *data)
{
	ChunkInsertState *cis;
	bool cis_changed = true;
	bool found = true;
	Chunk *chunk = NULL;

	/* Direct inserts into internal compressed hypertable is not supported.
	 * For compression chunks are created explicitly by compress_chunk and
	 * inserted into directly so we should never end up in this code path
	 * for a compressed hypertable.
	 */
	if (dispatch->hypertable->fd.compression_state == HypertableInternalCompressionTable)
		elog(ERROR, "direct insert into internal compressed hypertable is not supported");

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
		Assert(slot);
		chunk = ts_hypertable_find_chunk_for_point(dispatch->hypertable, point);

#if PG14_GE
		/*
		 * Frozen chunks require at least PG14.
		 */
		if (chunk && ts_chunk_is_frozen(chunk))
			elog(ERROR, "cannot INSERT into frozen chunk \"%s\"", get_rel_name(chunk->table_id));
#endif

		if (!chunk)
		{
			chunk = ts_hypertable_create_chunk_for_point(dispatch->hypertable, point, &found);
		}

		if (!chunk)
			elog(ERROR, "no chunk found or created");

		/* get the filtered list of "available" DNs for this chunk but only if it's replicated */
		if (found && dispatch->hypertable->fd.replication_factor > 1)
		{
			List *chunk_data_nodes =
				ts_chunk_data_node_scan_by_chunk_id_filter(chunk->fd.id, CurrentMemoryContext);

			/*
			 * If the chunk was not created as part of this insert, we need to check whether any
			 * of the chunk's data nodes are currently unavailable and in that case consider the
			 * chunk stale on those data nodes. Do that by removing the AN's chunk-datanode
			 * mapping for the unavailable data nodes.
			 */
			if (dispatch->hypertable->fd.replication_factor > list_length(chunk_data_nodes))
				ts_cm_functions->dist_update_stale_chunk_metadata(chunk, chunk_data_nodes);

			list_free(chunk_data_nodes);
		}

		cis = ts_chunk_insert_state_create(chunk, dispatch);

		/*
		 * We might have been blocked by a compression operation
		 * while trying to fetch the above lock so lets update the
		 * chunk catalog data because the status might have changed.
		 *
		 * This works even in higher levels of isolation since
		 * catalog data is always read from latest snapshot.
		 */
		chunk = ts_chunk_get_by_relid(chunk->table_id, true);
		ts_set_compression_status(cis, chunk);

		ts_subspace_store_add(dispatch->cache, chunk->cube, cis, destroy_chunk_insert_state);
	}
	else if (cis->rel->rd_id == dispatch->prev_cis_oid && cis == dispatch->prev_cis)
	{
		/* got the same item from cache as before */
		cis_changed = false;
	}

	if (found)
	{
		if (cis->chunk_compressed && cis->chunk_data_nodes == NIL)
		{
			/*
			 * If this is an INSERT into a compressed chunk with UNIQUE or
			 * PRIMARY KEY constraints we need to make sure any batches that could
			 * potentially lead to a conflict are in the decompressed chunk so
			 * postgres can do proper constraint checking.
			 */
			if (ts_cm_functions->decompress_batches_for_insert)
			{
				/* Get the chunk if its not already been loaded.
				 * It's needed for decompress_batches_for_insert
				 * which only uses some ids from it.
				 */
				if (chunk == NULL)
					chunk = ts_hypertable_find_chunk_for_point(dispatch->hypertable, point);
				ts_cm_functions->decompress_batches_for_insert(cis, chunk, slot);
				OnConflictAction onconflict_action =
					chunk_dispatch_get_on_conflict_action(dispatch);
				/* mark rows visible */
				if (onconflict_action == ONCONFLICT_UPDATE)
					dispatch->estate->es_output_cid = GetCurrentCommandId(true);
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("functionality not supported under the current \"%s\" license. "
								"Learn more at https://timescale.com/.",
								ts_guc_license),
						 errhint("To access all features and the best time-series "
								 "experience, try out Timescale Cloud")));
		}
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

	return &cscan->scan.plan;
}

static CustomPathMethods chunk_dispatch_path_methods = {
	.CustomName = "ChunkDispatchPath",
	.PlanCustomPath = chunk_dispatch_plan_create,
};

Path *
ts_chunk_dispatch_path_create(PlannerInfo *root, ModifyTablePath *mtpath, Index hypertable_rti,
							  int subpath_index)
{
	ChunkDispatchPath *path = (ChunkDispatchPath *) palloc0(sizeof(ChunkDispatchPath));
#if PG14_LT
	Path *subpath = list_nth(mtpath->subpaths, subpath_index);
#else
	Path *subpath = mtpath->subpath;
#endif
	RangeTblEntry *rte = planner_rt_fetch(hypertable_rti, root);

	memcpy(&path->cpath.path, subpath, sizeof(Path));
	path->cpath.path.type = T_CustomPath;
	path->cpath.path.pathtype = T_CustomScan;
	path->cpath.methods = &chunk_dispatch_path_methods;
	path->cpath.custom_paths = list_make1(subpath);
	path->mtpath = mtpath;
	path->hypertable_rti = hypertable_rti;
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
	state->dispatch = ts_chunk_dispatch_create(ht, estate, eflags);
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
#if PG14_LT
	ModifyTableState *mtstate = state->mtstate;

	/* PG < 14 expects the current target slot to match the result relation. Thus
	 * we need to make sure it is up-to-date with the current chunk here. */
	mtstate->mt_scans[mtstate->mt_whichplan] = cis->slot;
#endif
	state->rri = cis->result_relation_info;
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

	/* Calculate the tuple's point in the N-dimensional hyperspace */
	point = ts_hyperspace_calculate_point(ht->space, slot);

	/* Save the main table's (hypertable's) ResultRelInfo */
	if (!dispatch->hypertable_result_rel_info)
	{
#if PG14_LT
		Assert(RelationGetRelid(estate->es_result_relation_info->ri_RelationDesc) ==
			   state->hypertable_relid);
		dispatch->hypertable_result_rel_info = estate->es_result_relation_info;
#else
		dispatch->hypertable_result_rel_info = dispatch->dispatch_state->mtstate->resultRelInfo;
#endif
	}

	/* Find or create the insert state matching the point */
	cis = ts_chunk_dispatch_get_chunk_insert_state(dispatch,
												   point,
												   slot,
												   on_chunk_insert_state_changed,
												   state);

	/*
	 * Set the result relation in the executor state to the target chunk.
	 * This makes sure that the tuple gets inserted into the correct
	 * chunk. Note that since in PG < 14 the ModifyTable executor saves and restores
	 * the es_result_relation_info this has to be updated every time, not
	 * just when the chunk changes.
	 */
#if PG14_LT
	estate->es_result_relation_info = cis->result_relation_info;
#endif

	MemoryContextSwitchTo(old);

	/* Convert the tuple to the chunk's rowtype, if necessary */
	if (cis->hyper_to_chunk_map != NULL)
		slot = execute_attr_map_slot(cis->hyper_to_chunk_map->attrMap, slot, cis->slot);

	return slot;
}

static void
chunk_dispatch_end(CustomScanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	PlanState *substate = linitial(node->custom_ps);

	ExecEndNode(substate);
	ts_chunk_dispatch_destroy(state->dispatch);
	ts_cache_release(state->hypertable_cache);
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
	ModifyTable *mt_plan = castNode(ModifyTable, mtstate->ps.plan);

	/* Inserts on hypertables should always have one subplan */
#if PG14_LT
	Assert(mtstate->mt_nplans == 1);
#endif
	state->mtstate = mtstate;
	state->arbiter_indexes = mt_plan->arbiterIndexes;
}
