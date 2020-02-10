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
#if PG12_GE
	// TODO get from parent using table_slot_callbacks?
	// TODO should this use ExecInitExtraTupleSlot?
	state->slot = MakeSingleTupleTableSlot(NULL, TTSOpsBufferHeapTupleP);
#endif
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
#if PG12_GE
		bool should_free_tuple = false;

		/*
		 * If the tts_ops of the slot we return does not match the tts_ops that
		 * ModifyTable expects, it will try to copy our data into a slot of the
		 * correct kind. For some kinds of slots (BufferHeapTuple) this will not
		 * copy over our TupleDesc, causing the executor to choke later on, when
		 * it realizes it has the wrong TupleDesc in the slot. To prevent this,
		 * we check that we have the correct tts_ops, and update our slot if
		 * it's wrong.
		 */
		if (state->slot->tts_ops != state->parent->mt_scans[state->parent->mt_whichplan]->tts_ops)
		{
			ExecDropSingleTupleTableSlot(state->slot);
			state->slot =
				MakeSingleTupleTableSlot(NULL,
										 state->parent->mt_scans[state->parent->mt_whichplan]
											 ->tts_ops);
		}
#endif

		/* Switch to the executor's per-tuple memory context */
		old = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

#if PG12_LT
		tuple = ExecFetchSlotTuple(slot);
#else /* TODO we should try not materialize a tuple if not needed */
		tuple = ExecFetchSlotHeapTuple(slot, false, &should_free_tuple);
#endif

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

		cis = ts_chunk_dispatch_get_chunk_insert_state(dispatch,
													   point,
													   &cis_changed,
#if PG12_LT
													   NULL
#else
													   state->slot->tts_ops
#endif
		);
		if (cis_changed)
		{
			TupleDesc chunk_desc;

			if (cis->tup_conv_map && cis->tup_conv_map->outdesc)
				chunk_desc = cis->tup_conv_map->outdesc;
			else
				chunk_desc = RelationGetDescr(cis->rel);

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
#if PG12_LT
			if (state->parent->mt_existing != NULL)
			{
				Assert(chunk_desc != NULL);
				ExecSetSlotDescriptor(state->parent->mt_existing, chunk_desc);
			}
#else
			if (cis->result_relation_info->ri_onConflict != NULL)
			{
				if (cis->result_relation_info->ri_onConflict->oc_ProjSlot != NULL)
				{
					Assert(chunk_desc != NULL);
					ExecSetSlotDescriptor(cis->result_relation_info->ri_onConflict->oc_ProjSlot,
										  chunk_desc);
				}

				if (cis->result_relation_info->ri_onConflict->oc_Existing != NULL)
				{
					Assert(chunk_desc != NULL);
					ExecSetSlotDescriptor(cis->result_relation_info->ri_onConflict->oc_Existing,
										  chunk_desc);
				}
			}

			ExecSetSlotDescriptor(state->slot, chunk_desc);
#endif
		}
#if defined(USE_ASSERT_CHECKING) && PG11
		if (state->parent->mt_conflproj != NULL)
		{
			TupleTableSlot *slot = get_projection_info_slot_compat(
				ResultRelInfo_OnConflictProjInfoCompat(cis->result_relation_info));

			Assert(state->parent->mt_conflproj == slot);
			Assert(state->parent->mt_existing->tts_tupleDescriptor == RelationGetDescr(cis->rel));
		}
#endif

#if PG12_GE /* from ExecPrepareTupleRouting */
		if (state->parent->mt_transition_capture != NULL)
		{
			/* TODO BEFORE triggers? */
			// if (partrel->ri_TrigDesc && partrel->ri_TrigDesc->trig_insert_before_row)
			// {
			// 	/*
			// 	* If there are any BEFORE triggers on the partition, we'll have
			// 	* to be ready to convert their result back to tuplestore format.
			// 	*/
			// 	state->parent->mt_transition_capture->tcs_original_insert_tuple = NULL;
			// 	state->parent->mt_transition_capture->tcs_map =
			// 		partrouteinfo->pi_PartitionToRootMap;
			// }
			// else
			{
				/*
				 * Otherwise, just remember the original unconverted tuple, to
				 * avoid a needless round trip conversion.
				 */
				state->parent->mt_transition_capture->tcs_original_insert_tuple = slot;
				state->parent->mt_transition_capture->tcs_map = NULL;
			}
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
#if PG12_LT
		tuple = ts_chunk_insert_state_convert_tuple(cis, tuple, &slot);
#else

		Assert(state->slot->tts_tupleDescriptor == RelationGetDescr(cis->rel));
		if (cis->tup_conv_map != NULL)
			slot = execute_attr_map_slot(cis->tup_conv_map->attrMap, slot, state->slot);
		else
		{
			/* store a virtual tuple in our slot so we have the correct TupleDesc
			 * based on execute_attr_map_slot
			 */
			Datum *invalues;
			bool *inisnull;
			Datum *outvalues;
			bool *outisnull;
			int outnatts = state->slot->tts_tupleDescriptor->natts;

			/* Extract all the values of the in slot. */
			slot_getallattrs(slot);

			/* Before doing the mapping, clear any old contents from the out slot */
			ExecClearTuple(state->slot);

			invalues = slot->tts_values;
			inisnull = slot->tts_isnull;
			outvalues = state->slot->tts_values;
			outisnull = state->slot->tts_isnull;
			memcpy(outvalues, invalues, outnatts * sizeof(*outvalues));
			memcpy(outisnull, inisnull, outnatts * sizeof(*outisnull));

			ExecStoreVirtualTuple(state->slot);

			slot = state->slot;
		}
#endif
	}
	else
	{
		return NULL;
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
#if PG12_GE
	ExecDropSingleTupleTableSlot(state->slot);
#endif
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
#if !PG96 && !PG10 && !PG12

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
		parent->mt_existing = ExecInitExtraTupleSlotCompat(parent->ps.state, NULL, TTSOpsVirtualP);
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
#elif PG12
	if (parent->resultRelInfo->ri_onConflict != NULL)
	{
		if (parent->resultRelInfo->ri_onConflict->oc_Existing != NULL)
		{
			TupleDesc existing;

			existing = parent->resultRelInfo->ri_onConflict->oc_Existing->tts_tupleDescriptor;
			parent->resultRelInfo->ri_onConflict->oc_Existing =
				ExecInitExtraTupleSlotCompat(parent->ps.state,
											 NULL,
											 table_slot_callbacks(
												 parent->resultRelInfo->ri_RelationDesc));
			ExecSetSlotDescriptor(parent->resultRelInfo->ri_onConflict->oc_Existing, existing);
		}

		if (parent->resultRelInfo->ri_onConflict->oc_ProjSlot &&
			TTS_FIXED(parent->resultRelInfo->ri_onConflict->oc_ProjSlot))
		{
			TupleDesc existing;
			TupleTableSlot *new_slot;
			TupleTableSlot *old_slot = parent->resultRelInfo->ri_onConflict->oc_ProjSlot;
			MemoryContext old_context = old_slot->tts_mcxt;
			existing = old_slot->tts_tupleDescriptor;
			Assert(TTS_EMPTY(old_slot));

			/*
			 * in this case we must overwrite mt_conflproj because there are
			 * several pointers to it throughout expressions and other
			 * evaluations, and the original tuple will otherwise be stored to the
			 * old slot, whose pointer is saved there.
			 */
			new_slot = MakeTupleTableSlot(NULL, old_slot->tts_ops);

			/* a TupleTableSlot without a TupleDesc is smaller that one with */
			memcpy(old_slot, new_slot, old_slot->tts_ops->base_slot_size);
			old_slot->tts_mcxt = old_context;
			old_slot->tts_ops->init(old_slot);
			Assert(!TTS_FIXED(old_slot));
			pfree(new_slot);

			Assert(old_slot->tts_values == NULL);
			Assert(old_slot->tts_isnull == NULL);
			Assert(old_slot->tts_tupleDescriptor == NULL);
			ExecSetSlotDescriptor(old_slot, existing);
			Assert(TTS_EMPTY(old_slot));
			Assert(!TTS_FIXED(old_slot));
		}
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
