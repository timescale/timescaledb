/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <catalog/pg_class.h>
#include <commands/trigger.h>
#include <nodes/nodes.h>
#include <nodes/extensible.h>
#include <executor/nodeModifyTable.h>

#include "compat/compat.h"
#include "chunk_dispatch_state.h"
#include "chunk_dispatch_plan.h"
#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "chunk.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "dimension.h"
#include "hypertable.h"

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
	if (cis->compress_info != NULL)
		estate->es_result_relation_info = cis->compress_info->orig_result_relation_info;
	else
		estate->es_result_relation_info = cis->result_relation_info;
#endif

	MemoryContextSwitchTo(old);

	/* Convert the tuple to the chunk's rowtype, if necessary */
	if (cis->hyper_to_chunk_map != NULL)
		slot = execute_attr_map_slot(cis->hyper_to_chunk_map->attrMap, slot, cis->slot);

	if (cis->compress_info != NULL)
	{
		/*
		 * When the chunk is compressed, we redirect the insert to the internal compressed
		 * chunk. However, any BEFORE ROW triggers defined on the chunk have to be executed
		 * before we redirect the insert.
		 */
		if (cis->compress_info->orig_result_relation_info->ri_TrigDesc &&
			cis->compress_info->orig_result_relation_info->ri_TrigDesc->trig_insert_before_row)
		{
			bool skip_tuple;
			skip_tuple =
				!ExecBRInsertTriggers(estate, cis->compress_info->orig_result_relation_info, slot);

			if (skip_tuple)
				return NULL;
		}

		if (cis->rel->rd_att->constr && cis->rel->rd_att->constr->has_generated_stored)
			ExecComputeStoredGeneratedCompat(cis->compress_info->orig_result_relation_info,
											 estate,
											 slot,
											 CMD_INSERT);

		if (cis->rel->rd_att->constr)
			ExecConstraints(cis->compress_info->orig_result_relation_info, slot, estate);

#if PG14_LT
		estate->es_result_relation_info = cis->result_relation_info;
#endif
		Assert(ts_cm_functions->compress_row_exec != NULL);
		TupleTableSlot *orig_slot = slot;
		slot = ts_cm_functions->compress_row_exec(cis->compress_info->compress_state, slot);
		/* If we have cagg defined on the hypertable, we have to execute
		 * the function that records invalidations directly as AFTER ROW
		 * triggers do not work with compressed chunks.
		 */
		if (cis->compress_info->has_cagg_trigger)
		{
			Assert(ts_cm_functions->continuous_agg_call_invalidation_trigger);
			HeapTupleTableSlot *hslot = (HeapTupleTableSlot *) orig_slot;
			if (!hslot->tuple)
				hslot->tuple = heap_form_tuple(orig_slot->tts_tupleDescriptor,
											   orig_slot->tts_values,
											   orig_slot->tts_isnull);

			ts_compress_chunk_invoke_cagg_trigger(cis->compress_info, cis->rel, hslot->tuple);
		}
	}
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

ChunkDispatchState *
ts_chunk_dispatch_state_create(Oid hypertable_relid, Plan *subplan)
{
	ChunkDispatchState *state;

	state = (ChunkDispatchState *) newNode(sizeof(ChunkDispatchState), T_CustomScanState);
	state->hypertable_relid = hypertable_relid;
	state->subplan = subplan;
	state->cscan_state.methods = &chunk_dispatch_state_methods;
	return state;
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
