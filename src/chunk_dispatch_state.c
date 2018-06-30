#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <catalog/pg_class.h>
#include <nodes/extensible.h>

#include "chunk_dispatch_state.h"
#include "chunk_dispatch_plan.h"
#include "chunk_dispatch_info.h"
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
	Cache	   *hypertable_cache;
	PlanState  *ps;

	hypertable_cache = hypertable_cache_pin();

	ht = hypertable_cache_get_entry(hypertable_cache, state->hypertable_relid);

	if (NULL == ht)
	{
		cache_release(hypertable_cache);
		elog(ERROR, "no hypertable for relid %d", state->hypertable_relid);
	}
	ps = ExecInitNode(state->subplan, estate, eflags);
	state->hypertable_cache = hypertable_cache;
	state->dispatch = chunk_dispatch_create(ht, estate);
	node->custom_ps = list_make1(ps);
}

static TupleTableSlot *
chunk_dispatch_exec(CustomScanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	TupleTableSlot *slot;
	PlanState  *substate = linitial(node->custom_ps);

	/* Get the next tuple from the subplan state node */
	slot = ExecProcNode(substate);

	if (!TupIsNull(slot))
	{
		Point	   *point;
		ChunkInsertState *cis;
		ChunkDispatch *dispatch = state->dispatch;
		Hypertable *ht = dispatch->hypertable;
		HeapTuple	tuple;
		TupleDesc	tupdesc = slot->tts_tupleDescriptor;
		EState	   *estate = node->ss.ps.state;
		MemoryContext old;

		/* Switch to the executor's per-tuple memory context */
		old = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		tuple = ExecFetchSlotTuple(slot);

		/* Calculate the tuple's point in the N-dimensional hyperspace */
		point = hyperspace_calculate_point(ht->space, tuple, tupdesc);

		/* Save the main table's (hypertable's) ResultRelInfo */
		if (NULL == dispatch->hypertable_result_rel_info)
			dispatch->hypertable_result_rel_info = estate->es_result_relation_info;

		/*
		 * Copy over the index to use in the returning list.
		 */
		dispatch->returning_index = state->parent->mt_whichplan;


		/* Find or create the insert state matching the point */
		cis = chunk_dispatch_get_chunk_insert_state(dispatch, point);

		/*
		 * Update the arbiter indexes for ON CONFLICT statements so that they
		 * match the chunk. Note that this requires updating the existing List
		 * head (not replacing it), or otherwise the ModifyTableState node
		 * won't pick it up.
		 */
		if (cis->arbiter_indexes != NIL)
			state->parent->mt_arbiterindexes = cis->arbiter_indexes;

		/* slot for the "existing" tuple in ON CONFLICT UPDATE IS chunk schema */
		if (cis->tup_conv_map != NULL && state->parent->mt_existing != NULL)
		{
			TupleDesc	chunk_desc = cis->tup_conv_map->outdesc;

			ExecSetSlotDescriptor(state->parent->mt_existing, chunk_desc);
		}

		/*
		 * Set the result relation in the executor state to the target chunk.
		 * This makes sure that the tuple gets inserted into the correct
		 * chunk.
		 */
		estate->es_result_relation_info = cis->result_relation_info;

		MemoryContextSwitchTo(old);

		/* Convert the tuple to the chunk's rowtype, if necessary */
		tuple = chunk_insert_state_convert_tuple(cis, tuple, &slot);
	}

	return slot;
}

static void
chunk_dispatch_end(CustomScanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	PlanState  *substate = linitial(node->custom_ps);

	ExecEndNode(substate);
	chunk_dispatch_destroy(state->dispatch);
	cache_release(state->hypertable_cache);
}

static void
chunk_dispatch_rescan(CustomScanState *node)
{
	PlanState  *substate = linitial(node->custom_ps);

	ExecReScan(substate);
}

static CustomExecMethods chunk_dispatch_state_methods = {
	.CustomName = CHUNK_DISPATCH_STATE_NAME,
	.BeginCustomScan = chunk_dispatch_begin,
	.EndCustomScan = chunk_dispatch_end,
	.ExecCustomScan = chunk_dispatch_exec,
	.ReScanCustomScan = chunk_dispatch_rescan,
};

ChunkDispatchState *
chunk_dispatch_state_create(ChunkDispatchInfo *info, Plan *subplan)
{
	ChunkDispatchState *state;

	state = (ChunkDispatchState *) newNode(sizeof(ChunkDispatchState), T_CustomScanState);
	state->hypertable_relid = info->hypertable_relid;
	state->subplan = subplan;
	state->cscan_state.methods = &chunk_dispatch_state_methods;
	return state;
}

void
chunk_dispatch_state_set_parent(ChunkDispatchState *state, ModifyTableState *parent)
{
	ModifyTable *mt_plan;

	state->parent = parent;
	state->dispatch->arbiter_indexes = parent->mt_arbiterindexes;
	state->dispatch->on_conflict = parent->mt_onconflict;
	state->dispatch->cmd_type = parent->operation;

	/*
	 * Copy over the original expressions for projection infos. In PG 9.6 it
	 * is not possible to get the original expressions back from the
	 * ProjectionInfo structs
	 */

	Assert(IsA(parent->ps.plan, ModifyTable));
	mt_plan = (ModifyTable *) parent->ps.plan;

	state->dispatch->returning_lists = mt_plan->returningLists;
	state->dispatch->on_conflict_set = mt_plan->onConflictSet;

	Assert(mt_plan->onConflictWhere == NULL || IsA(mt_plan->onConflictWhere, List));
	state->dispatch->on_conflict_where = (List *) mt_plan->onConflictWhere;
}
