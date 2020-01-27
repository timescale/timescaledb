/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <catalog/pg_class.h>
#include <nodes/extensible.h>

#include "compat.h"
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

	ht = ts_hypertable_cache_get_cache_and_entry(state->hypertable_relid, false, &hypertable_cache);
	ps = ExecInitNode(state->subplan, estate, eflags);
	state->hypertable_cache = hypertable_cache;
	state->dispatch = ts_chunk_dispatch_create(ht, estate);
	node->custom_ps = list_make1(ps);
}

static TupleTableSlot *
chunk_dispatch_exec(CustomScanState *node)
{
	ChunkDispatchState *state = (ChunkDispatchState *) node;
	TupleTableSlot *slot;
	PlanState *substate = linitial(node->custom_ps);

	/* Get the next tuple from the subplan state node */
	slot = ExecProcNode(substate);

	if (!TupIsNull(slot))
	{
		Point *point;
		ChunkInsertState *cis;
		ChunkDispatch *dispatch = state->dispatch;
		Hypertable *ht = dispatch->hypertable;
		HeapTuple tuple;
		TupleDesc tupdesc = slot->tts_tupleDescriptor;
		EState *estate = node->ss.ps.state;
		MemoryContext old;
		bool cis_changed;

		/* Switch to the executor's per-tuple memory context */
		old = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		tuple = ExecFetchSlotTuple(slot);

		/* Calculate the tuple's point in the N-dimensional hyperspace */
		point = ts_hyperspace_calculate_point(ht->space, tuple, tupdesc);

		/* Save the main table's (hypertable's) ResultRelInfo */
		if (NULL == dispatch->hypertable_result_rel_info)
			dispatch->hypertable_result_rel_info = estate->es_result_relation_info;

		/*
		 * Copy over the index to use in the returning list.
		 */
		dispatch->returning_index = state->parent->mt_whichplan;

		/* Find or create the insert state matching the point */
		cis = ts_chunk_dispatch_get_chunk_insert_state(dispatch, point, &cis_changed);
		if (cis_changed)
		{
			/*
			 * Update the arbiter indexes for ON CONFLICT statements so that they
			 * match the chunk.
			 */
			if (cis->arbiter_indexes != NIL)
			{
				/*
				 * In PG11 several fields were removed from the ModifyTableState
				 * node and ExecInsert function nodes, as they were redundant.
				 * (See:
				 * https://github.com/postgres/postgres/commit/ee0a1fc84eb29c916687dc5bd26909401d3aa8cd).
				 */
#if PG96 || PG10
				state->parent->mt_arbiterindexes = cis->arbiter_indexes;
#else
				Assert(IsA(state->parent->ps.plan, ModifyTable));
				((ModifyTable *) state->parent->ps.plan)->arbiterIndexes = cis->arbiter_indexes;
#endif
			}

			/* slot for the "existing" tuple in ON CONFLICT UPDATE IS chunk schema */

			if (state->parent->mt_existing != NULL)
			{
				TupleDesc chunk_desc;

				if (cis->tup_conv_map && cis->tup_conv_map->outdesc)
					chunk_desc = cis->tup_conv_map->outdesc;
				else
					chunk_desc = RelationGetDescr(cis->rel);
				Assert(chunk_desc != NULL);
				ExecSetSlotDescriptor(state->parent->mt_existing, chunk_desc);
			}
		}
#if defined(USE_ASSERT_CHECKING) && PG11_GE
		if (state->parent->mt_conflproj != NULL)
		{
			TupleTableSlot *slot = get_projection_info_slot_compat(
				ResultRelInfo_OnConflictProjInfoCompat(cis->result_relation_info));

			Assert(state->parent->mt_conflproj == slot);
			Assert(state->parent->mt_existing->tts_tupleDescriptor == RelationGetDescr(cis->rel));
		}
#endif

		/*
		 * Set the result relation in the executor state to the target chunk.
		 * This makes sure that the tuple gets inserted into the correct
		 * chunk. Note that since the ModifyTable executor saves and restores
		 * the es_result_relation_info this has to be updated every time, not
		 * just when the chunk changes.
		 */
		estate->es_result_relation_info = cis->result_relation_info;

		MemoryContextSwitchTo(old);

		/* Convert the tuple to the chunk's rowtype, if necessary */
		tuple = ts_chunk_insert_state_convert_tuple(cis, tuple, &slot);
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
	.CustomName = CHUNK_DISPATCH_STATE_NAME,
	.BeginCustomScan = chunk_dispatch_begin,
	.EndCustomScan = chunk_dispatch_end,
	.ExecCustomScan = chunk_dispatch_exec,
	.ReScanCustomScan = chunk_dispatch_rescan,
};

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

void
ts_chunk_dispatch_state_set_parent(ChunkDispatchState *state, ModifyTableState *parent)
{
	ModifyTable *mt_plan;

	state->parent = parent;
#if !PG96 && !PG10

	/*
	 * Several tuple slots are created with static tupledescs when PG thinks
	 * it's accessing a normal table (and not when it's accessing a
	 * partitioned table) we make copies of these slots and replace them so
	 * that we can modify the tupledesc later in our code.
	 */
	if (parent->mt_existing != NULL)
	{
		TupleDesc existing;

		existing = parent->mt_existing->tts_tupleDescriptor;
		parent->mt_existing = ExecInitExtraTupleSlotCompat(parent->ps.state, NULL);
		ExecSetSlotDescriptor(parent->mt_existing, existing);
	}
	if (parent->mt_conflproj != NULL)
	{
		TupleDesc existing;

		existing = parent->mt_conflproj->tts_tupleDescriptor;

		/*
		 * in this case we must overwrite mt_conflproj because there are
		 * several pointers to it throughout expressions and other
		 * evaluations, and the original tuple will otherwise be stored to the
		 * old slot, whose pointer is saved there.
		 */
		*parent->mt_conflproj = *MakeTupleTableSlot(NULL);
		ExecSetSlotDescriptor(parent->mt_conflproj, existing);
	}
#endif
	state->dispatch->cmd_type = parent->operation;

	/*
	 * Copy over the original expressions for projection infos. In PG 9.6 it
	 * is not possible to get the original expressions back from the
	 * ProjectionInfo structs
	 */

	Assert(IsA(parent->ps.plan, ModifyTable));
	mt_plan = (ModifyTable *) parent->ps.plan;

	state->dispatch->returning_lists = mt_plan->returningLists;
	state->dispatch->on_conflict = mt_plan->onConflictAction;
	state->dispatch->on_conflict_set = mt_plan->onConflictSet;
	state->dispatch->arbiter_indexes = mt_plan->arbiterIndexes;

	Assert(mt_plan->onConflictWhere == NULL || IsA(mt_plan->onConflictWhere, List));
	state->dispatch->on_conflict_where = (List *) mt_plan->onConflictWhere;
}
