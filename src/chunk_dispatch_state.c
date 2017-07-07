#include <postgres.h>
#include <utils/lsyscache.h>
#include <catalog/pg_class.h>
#include <nodes/extensible.h>

#include "chunk_dispatch_state.h"
#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "chunk.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "hypertable.h"
#include "chunk_constraint.h"

static void
chunk_dispatch_begin(CustomScanState *node, EState *estate, int eflags)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	Hypertable *ht;
	Cache	   *hypertable_cache;

	hypertable_cache = hypertable_cache_pin();

	ht = hypertable_cache_get_entry(hypertable_cache, state->hypertable_relid);

	if (NULL == ht)
	{
		cache_release(hypertable_cache);
		elog(ERROR, "No hypertable for relid %d", state->hypertable_relid);
	}

	state->hypertable_cache = hypertable_cache;
	state->dispatch = chunk_dispatch_create(ht, estate);
	state->subplan_state = ExecInitNode(state->subplan, estate, eflags);
}

static TupleTableSlot *
chunk_dispatch_exec(CustomScanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	TupleTableSlot *slot;

	slot = ExecProcNode(state->subplan_state);

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

		/* Find or create the insert state matching the point */
		cis = chunk_dispatch_get_chunk_insert_state(dispatch, point);

		/*
		 * Set the result relation in the executor state to the target chunk.
		 * This makes sure that the tuple gets inserted into the correct
		 * chunk.
		 */
		estate->es_result_relation_info = cis->result_relation_info;

		MemoryContextSwitchTo(old);
	}

	return slot;
}

static void
chunk_dispatch_end(CustomScanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;

	ExecEndNode(state->subplan_state);
	chunk_dispatch_destroy(state->dispatch);
	cache_release(state->hypertable_cache);
}

static void
chunk_dispatch_rescan(CustomScanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;

	ExecReScan(state->subplan_state);
}

static CustomExecMethods chunk_dispatch_state_methods = {
	.BeginCustomScan = chunk_dispatch_begin,
	.EndCustomScan = chunk_dispatch_end,
	.ExecCustomScan = chunk_dispatch_exec,
	.ReScanCustomScan = chunk_dispatch_rescan,
};

ChunkDispatchState *
chunk_dispatch_state_create(Oid relid, Plan *subplan)
{
	ChunkDispatchState *state;

	state = (ChunkDispatchState *) newNode(sizeof(ChunkDispatchState), T_CustomScanState);
	state->hypertable_relid = relid;
	state->subplan = subplan;
	state->cscan_state.methods = &chunk_dispatch_state_methods;
	return state;
}
