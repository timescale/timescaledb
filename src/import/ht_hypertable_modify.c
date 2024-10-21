/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from the
 * PostgreSQL database, which is licensed under the open-source PostgreSQL
 * License. Please see the NOTICE at the top level directory for a copy of
 * the PostgreSQL License.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <executor/nodeModifyTable.h>
#include <executor/tuptable.h>
#include <nodes/nodes.h>
#include <storage/lmgr.h>
#include <utils/snapmgr.h>

#include "ht_hypertable_modify.h"

/* clang-format off */

/*
 * ht_ExecUpdatePrologue -- subroutine for ht_ExecUpdate
 *
 * Prepare executor state for UPDATE.  This includes running BEFORE ROW
 * triggers.  We return false if one of them makes the update a no-op;
 * otherwise, return true.
 */
bool
ht_ExecUpdatePrologue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
					  ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
					  TM_Result *result)
{
	Relation resultRelationDesc = resultRelInfo->ri_RelationDesc;

	if (result != NULL)
		*result = TM_Ok;

	ExecMaterializeSlot(slot);

	/*
	 * Open the table's indexes, if we have not done so already, so that
	 * we can add new index entries for the updated tuple.
	 */
	if (resultRelationDesc->rd_rel->relhasindex && resultRelInfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(resultRelInfo, false);

	/* BEFORE ROW UPDATE triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_before_row)
	{
		/* Flush any pending inserts, so rows are visible to the triggers */
		// if (context->estate->es_insert_pending_result_relations != NIL)
		// 	ExecPendingInserts(context->estate);

		return ExecBRUpdateTriggersCompat(context->estate,
						  context->epqstate,
						  resultRelInfo,
						  tupleid,
						  oldtuple,
						  slot,
						  result,
						  &context->tmfd);
	}

	return true;
}

/*
 * ht_ExecUpdatePrepareSlot -- subroutine for ht_ExecUpdate
 *
 * Apply the final modifications to the tuple slot before the update.
 */
void
ht_ExecUpdatePrepareSlot(ResultRelInfo * resultRelInfo, TupleTableSlot * slot, EState * estate)
{
	Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;

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
		ExecComputeStoredGenerated(resultRelInfo, estate, slot, CMD_UPDATE);
}

/*
 * ht_ExecUpdateAct -- subroutine for ht_ExecUpdate
 *
 * Actually update the tuple, when operating on a plain table.  If the table
 * is a partition, and the command was called referencing an ancestor
 * partitioned table, this routine migrates the resulting tuple to another
 * partition.
 *
 * The caller is in charge of keeping indexes current as necessary.  The
 * caller is also in charge of doing EvalPlanQual if the tuple is found to be
 * concurrently updated.  However, in case of a cross-partition update, this
 * routine does it.
 *
 * Caller is in charge of doing EvalPlanQual as necessary, and of keeping
 * indexes current for the update.
 */
TM_Result
ht_ExecUpdateAct(ModifyTableContext * context, ResultRelInfo * resultRelInfo, ItemPointer tupleid,
		 HeapTuple oldtuple, TupleTableSlot * slot, bool canSetTag, UpdateContext * updateCxt){
	EState	       *estate = context->estate;
	Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;
	bool		partition_constraint_failed;
	TM_Result	result;

	updateCxt->crossPartUpdate = false;

	/*
	 * If we move the tuple to a new partition, we loop back here to recompute
	 * GENERATED values (which are allowed to be different across partitions)
	 * and recheck any RLS policies and constraints.  We do not fire any
	 * BEFORE triggers of the new partition, however.
	 */

	/* Fill in GENERATEd columns */
	ht_ExecUpdatePrepareSlot(resultRelInfo, slot, estate);

	/* ensure slot is independent, consider e.g. EPQ */
	ExecMaterializeSlot(slot);

	/*
	 * If partition constraint fails, this row might get moved to another
	 * partition, in which case we should check the RLS CHECK policy just
	 * before inserting into the new partition, rather than doing it
	 * here. This is because a trigger on that partition might again
	 * change the row. So skip the WCO checks if the partition constraint
	 * fails.
	 */
	partition_constraint_failed = resultRelationDesc->rd_rel->relispartition &&
		!ExecPartitionCheck(resultRelInfo, slot, estate, false);

	/* Check any RLS UPDATE WITH CHECK policies */
	if (!partition_constraint_failed &&
		resultRelInfo->ri_WithCheckOptions != NIL)
	{
		/*
		 * ExecWithCheckOptions() will skip any WCOs which are not of
		 * the kind we are looking for at this point.
		 */
		ExecWithCheckOptions(WCO_RLS_UPDATE_CHECK, resultRelInfo, slot, estate);
	}

	/*
	 * If a partition check failed, try to move the row into the right
	 * partition.
	 */
	if (partition_constraint_failed) {
		elog(ERROR, "cross chunk updates not supported");
	}

	/*
	 * Check the constraints of the tuple.  We've already checked the
	 * partition constraint above; however, we must still ensure the
	 * tuple passes all other constraints, so we will call
	 * ExecConstraints() and have it validate all remaining checks.
	 */
	if (resultRelationDesc->rd_att->constr)
		ExecConstraints(resultRelInfo, slot, estate);

	/*
	 * replace the heap tuple
	 *
	 * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check
	 * that the row to be updated is visible to that snapshot, and throw
	 * a can't-serialize error if not. This is a special-case behavior
	 * needed for referential integrity updates in transaction-snapshot
	 * mode transactions.
	 */
	result = table_tuple_update(resultRelationDesc,
				    tupleid,
				    slot,
				    estate->es_output_cid,
				    estate->es_snapshot,
				    estate->es_crosscheck_snapshot,
				    true /* wait for commit */ ,
				    &context->tmfd,
				    &context->lockmode,
				    &updateCxt->updateIndexes);
	if (result == TM_Ok)
		updateCxt->updated = true;

	return result;
}


/*
 * ht_ExecUpdateEpilogue -- subroutine for ht_ExecUpdate
 *
 * Closing steps of updating a tuple.  Must be called if ht_ExecUpdateAct
 * returns indicating that the tuple was updated.
 */
void
ht_ExecUpdateEpilogue(ModifyTableContext * context, UpdateContext * updateCxt,
     ResultRelInfo * resultRelInfo, ItemPointer tupleid, HeapTuple oldtuple,
		      TupleTableSlot * slot, List * recheckIndexes)
{
	ModifyTableState *mtstate = context->mtstate;

	/* insert index entries for tuple if necessary */
	bool onlySummarizing = false;
#if PG16_LT
	bool updateIndexes = updateCxt->updateIndexes;
	(void) onlySummarizing; /* onlySummarizing is unused in versions < PG16 */
#else
	bool updateIndexes = (updateCxt->updateIndexes != TU_None);
	onlySummarizing = (updateCxt->updateIndexes == TU_Summarizing);
#endif
	if (resultRelInfo->ri_NumIndices > 0 && updateIndexes)
		recheckIndexes = ExecInsertIndexTuplesCompat(resultRelInfo,
													 slot,
													 context->estate,
													 true,
													 false,
													 NULL,
													 NIL,
													 onlySummarizing);

	/* AFTER ROW UPDATE Triggers */
	ExecARUpdateTriggersCompat(context->estate,
				   resultRelInfo,
				   NULL,
				   NULL,
				   tupleid,
				   oldtuple,
				   slot,
				   recheckIndexes,
				   mtstate->operation == CMD_INSERT ?
				   mtstate->mt_oc_transition_capture :
				   mtstate->mt_transition_capture,
				   false	/* is_crosspart_update */
		);

	list_free(recheckIndexes);

	/*
	 * Check any WITH CHECK OPTION constraints from parent views.  We are
	 * required to do this after testing all constraints and uniqueness
	 * violations per the SQL spec, so we do it after actually updating
	 * the record in the heap and all indexes.
	 *
	 * ExecWithCheckOptions() will skip any WCOs which are not of the
	 * kind we are looking for at this point.
	 */
	if (resultRelInfo->ri_WithCheckOptions != NIL)
		ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, context->estate);
}

/*
 * ht_ExecDeletePrologue -- subroutine for ht_ExecDelete
 *
 * Prepare executor state for DELETE.  Actually, the only thing we have to do
 * here is execute BEFORE ROW triggers.  We return false if one of them makes
 * the delete a no-op; otherwise, return true.
 */
bool
ht_ExecDeletePrologue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
					  ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot **epqreturnslot,
					  TM_Result *result)
{
	/* BEFORE ROW DELETE triggers */
	if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_delete_before_row)
		return ExecBRDeleteTriggersCompat(context->estate,
										  context->epqstate,
										  resultRelInfo,
										  tupleid,
										  oldtuple,
										  epqreturnslot,
										  result,
										  &context->tmfd);

	return true;
}

/*
 * ht_ExecDeleteAct -- subroutine for ht_ExecDelete
 *
 * Actually delete the tuple from a plain table.
 *
 * Caller is in charge of doing EvalPlanQual as necessary
 */
TM_Result
ht_ExecDeleteAct(ModifyTableContext * context, ResultRelInfo * resultRelInfo, ItemPointer tupleid,
		 bool changingPart){
	EState	       *estate = context->estate;

	return table_tuple_delete(resultRelInfo->ri_RelationDesc,
				  tupleid,
				  estate->es_output_cid,
				  estate->es_snapshot,
				  estate->es_crosscheck_snapshot,
				  true /* wait for commit */ ,
				  &context->tmfd,
				  changingPart);
}

/*
 * ht_ExecDeleteEpilogue -- subroutine for ht_ExecDelete
 *
 * Closing steps of tuple deletion; this invokes AFTER FOR EACH ROW triggers,
 * including the UPDATE triggers if the deletion is being done as part of a
 * cross-partition tuple move.
 */
void
ht_ExecDeleteEpilogue(ModifyTableContext * context, ResultRelInfo * resultRelInfo, ItemPointer tupleid,
		      HeapTuple oldtuple)
{
	ModifyTableState *mtstate = context->mtstate;
	EState	       *estate = context->estate;
	TransitionCaptureState *ar_delete_trig_tcs;

	/*
	 * If this delete is the result of a partition key update that moved
	 * the tuple to a new partition, put this row into the transition OLD
	 * TABLE, if there is one. We need to do this separately for DELETE
	 * and INSERT because they happen on different tables.
	 */
	ar_delete_trig_tcs = mtstate->mt_transition_capture;
	if (mtstate->operation == CMD_UPDATE && mtstate->mt_transition_capture &&
	    mtstate->mt_transition_capture->tcs_update_old_table) {
		ExecARUpdateTriggersCompat(estate,
					   resultRelInfo,
					   NULL,
					   NULL,
					   tupleid,
					   oldtuple,
					   NULL,
					   NULL,
					   mtstate->mt_transition_capture,
					   false);

		/*
		 * We've already captured the NEW TABLE row, so make sure any
		 * AR DELETE trigger fired below doesn't capture it again.
		 */
		ar_delete_trig_tcs = NULL;
	}

	/* AFTER ROW DELETE Triggers */
	ExecARDeleteTriggersCompat(estate, resultRelInfo, tupleid, oldtuple, ar_delete_trig_tcs, false);
}

#if PG17_GE
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
#endif

#if PG15_GE && PG17_LT
static TupleTableSlot * mergeGetUpdateNewTuple(ResultRelInfo * relinfo, TupleTableSlot * planSlot,
		    TupleTableSlot * oldSlot, MergeActionState * relaction);
#endif

#if PG15_GE

TupleTableSlot *ExecInsert(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
						   ChunkDispatchState* cds,
						   TupleTableSlot * slot, bool canSetTag);

/*
 * Check and execute the first qualifying MATCHED action. The current target
 * tuple is identified by tupleid.
 *
 * We start from the first WHEN MATCHED action and check if the WHEN quals
 * pass, if any. If the WHEN quals for the first action do not pass, we check
 * the second, then the third and so on. If we reach to the end, no action is
 * taken and we return true, indicating that no further action is required
 * for this tuple.
 *
 * If we do find a qualifying action, then we attempt to execute the action.
 *
 * If the tuple is concurrently updated, EvalPlanQual is run with the updated
 * tuple to recheck the join quals. Note that the additional quals associated
 * with individual actions are evaluated by this routine via ExecQual, while
 * EvalPlanQual checks for the join quals. If EvalPlanQual tells us that the
 * updated tuple still passes the join quals, then we restart from the first
 * action to look for a qualifying action. Otherwise, we return false --
 * meaning that a NOT MATCHED action must now be executed for the current
 * source tuple.
 */

TupleTableSlot*
ht_ExecMergeMatched(ModifyTableContext * context, ResultRelInfo * resultRelInfo, ItemPointer tupleid,
		    HeapTuple oldtuple, bool canSetTag, bool *matched)
{

	ModifyTableState *mtstate = context->mtstate;
#if PG17_GE
	List	  **mergeActions = resultRelInfo->ri_MergeActions;
#if PG_VERSION_NUM >= 170100
	ItemPointerData lockedtid;
#endif
#else
	List	  *mergeActions = resultRelInfo->ri_matchedMergeAction;
#endif
	List	   *actionStates;
	TupleTableSlot *newslot = NULL;
	TupleTableSlot *rslot = NULL;
	EState	       *estate = context->estate;
	ExprContext    *econtext = mtstate->ps.ps_ExprContext;
	bool		isNull;
	EPQState       *epqstate = &mtstate->mt_epqstate;
	ListCell       *l;

	/* Expect matched to be true on entry */
	Assert(*matched);

#if PG17_GE
	/*
	 * If there are no WHEN MATCHED or WHEN NOT MATCHED BY SOURCE actions, we
	 * are done.
	 */
	if (mergeActions[MERGE_WHEN_MATCHED] == NIL &&
		mergeActions[MERGE_WHEN_NOT_MATCHED_BY_SOURCE] == NIL)
		return NULL;
#else
	/*
	 * If there are no WHEN MATCHED actions, we are done.
	 */
	if (mergeActions == NIL)
	{
		*matched = true;
		return NULL;
	}
#endif
	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject. The target's existing tuple is installed in the scantuple.
	 * This target relation's slot is required only in the case of a MATCHED
	 * or NOT MATCHED BY SOURCE tuple and UPDATE/DELETE actions.
	 */
	econtext->ecxt_scantuple = resultRelInfo->ri_oldTupleSlot;
	econtext->ecxt_innertuple = context->planSlot;
	econtext->ecxt_outertuple = NULL;

#if PG17_GE
	/*
	 * This routine is only invoked for matched target rows, so we should
	 * either have the tupleid of the target row, or an old tuple from the
	 * target wholerow junk attr.
	 */
	Assert(tupleid != NULL || oldtuple != NULL);
#if PG_VERSION_NUM >= 170100
	ItemPointerSetInvalid(&lockedtid);
#endif
	if (oldtuple != NULL)
	{
#if PG_VERSION_NUM >= 170100
		Assert(!resultRelInfo->ri_needLockTagTuple);
#endif
		ExecForceStoreHeapTuple(oldtuple, resultRelInfo->ri_oldTupleSlot,
								false);
	}
	else
	{
#if PG_VERSION_NUM >= 170100
		if (resultRelInfo->ri_needLockTagTuple)
		{
			/*
			 * This locks even for CMD_DELETE, for CMD_NOTHING, and for tuples
			 * that don't match mas_whenqual.  MERGE on system catalogs is a
			 * minor use case, so don't bother optimizing those.
			 */
			LockTuple(resultRelInfo->ri_RelationDesc, tupleid,
					  InplaceUpdateTupleLock);
			lockedtid = *tupleid;
		}
#endif
		if (!table_tuple_fetch_row_version(resultRelInfo->ri_RelationDesc,
										   tupleid,
										   SnapshotAny,
										   resultRelInfo->ri_oldTupleSlot))
			elog(ERROR, "failed to fetch the target tuple");
	}

	/*
	 * Test the join condition.  If it's satisfied, perform a MATCHED action.
	 * Otherwise, perform a NOT MATCHED BY SOURCE action.
	 *
	 * Note that this join condition will be NULL if there are no NOT MATCHED
	 * BY SOURCE actions --- see transform_MERGE_to_join().  In that case, we
	 * need only consider MATCHED actions here.
	 */
	if (ExecQual(resultRelInfo->ri_MergeJoinCondition, econtext))
		actionStates = mergeActions[MERGE_WHEN_MATCHED];
	else
		actionStates = mergeActions[MERGE_WHEN_NOT_MATCHED_BY_SOURCE];
#else
	actionStates = mergeActions;
#endif

lmerge_matched:

	foreach(l, actionStates)
	{
		MergeActionState *relaction = (MergeActionState *) lfirst(l);
		CmdType		commandType = relaction->mas_action->commandType;
		List	       *recheckIndexes = NIL;
		TM_Result	result;
		UpdateContext	updateCxt = {0};

		/*
		 * Test condition, if any.
		 *
		 * In the absence of any condition, we perform the action
		 * unconditionally (no need to check separately since
		 * ExecQual() will return true if there are no conditions to
		 * evaluate).
		 */
		if (!ExecQual(relaction->mas_whenqual, econtext))
			continue;

		/*
		 * Check if the existing target tuple meets the USING checks
		 * of UPDATE/DELETE RLS policies. If those checks fail, we
		 * throw an error.
		 *
		 * The WITH CHECK quals for UPDATE RLS policies are applied in
		 * ExecUpdate() and hence we need not do anything special to handle
		 * them.
		 *
		 * NOTE: We must do this after WHEN quals are evaluated, so
		 * that we check policies only when they matter.
		 */
		if (resultRelInfo->ri_WithCheckOptions && commandType != CMD_NOTHING)
		{
			ExecWithCheckOptions(commandType == CMD_UPDATE ? WCO_RLS_MERGE_UPDATE_CHECK :
					     WCO_RLS_MERGE_DELETE_CHECK,
					     resultRelInfo,
					     resultRelInfo->ri_oldTupleSlot,
					     context->mtstate->ps.state);
		}

		/* Perform stated action */
		switch (commandType) {
		case CMD_UPDATE:

			/*
			 * Project the output tuple, and use that to update
			 * the table. We don't need to filter out junk
			 * attributes, because the UPDATE action's targetlist
			 * doesn't have any.
			 */
			newslot = ExecProject(relaction->mas_proj);

#if PG17_GE
			mtstate->mt_merge_action = relaction;
#else
			context->relaction = relaction;
			context->GetUpdateNewTuple = mergeGetUpdateNewTuple;
			context->cpUpdateRetrySlot = NULL;
#endif

			if (!ht_ExecUpdatePrologue(context, resultRelInfo, tupleid, NULL, newslot, &result))
			{
#if PG16_LT
				result = TM_Ok;
#else
				if (result == TM_Ok)
					goto out;	/* "do nothing" */
#endif

				/* if not TM_OK, it is concurrent update/delete */
				break;
			}
#if PG17_LT
			ht_ExecUpdatePrepareSlot(resultRelInfo, newslot, context->estate);
#endif
			result = ht_ExecUpdateAct(context,
						  resultRelInfo,
						  tupleid,
						  NULL,
						  newslot,
						  mtstate->canSetTag,
						  &updateCxt);
			if (result == TM_Ok && updateCxt.updated) {
				ht_ExecUpdateEpilogue(context,
						      &updateCxt,
						      resultRelInfo,
						      tupleid,
						      NULL,
						      newslot,
						      recheckIndexes);
				mtstate->mt_merge_updated = 1;
			}

			break;

		case CMD_DELETE:
#if PG17_GE
			mtstate->mt_merge_action = relaction;
#else
			context->relaction = relaction;
#endif
			if (!ht_ExecDeletePrologue(context, resultRelInfo, tupleid, NULL, NULL, &result))
			{
#if PG16_LT
				result = TM_Ok;
#else
				if (result == TM_Ok)
					goto out; /* "do nothing" */

				/* if not TM_OK, it is concurrent update/delete */
#endif
				break;
			}
			result = ht_ExecDeleteAct(context, resultRelInfo, tupleid, false);
			if (result == TM_Ok) {
				ht_ExecDeleteEpilogue(context, resultRelInfo, tupleid, NULL);
				mtstate->mt_merge_deleted = 1;
			}
			break;

		case CMD_NOTHING:
			/* Doing nothing is always OK */
			result = TM_Ok;
			break;

		default:
			elog(ERROR, "unknown action in MERGE WHEN MATCHED clause");
		}

		switch (result) {
		case TM_Ok:
			/* all good; perform final actions */
			if (canSetTag && commandType != CMD_NOTHING)
				(estate->es_processed)++;

			break;

		case TM_SelfModified:
			/*
			 * The target tuple was already updated or deleted by the
			 * current command, or by a later command in the current
			 * transaction.  The former case is explicitly disallowed by
			 * the SQL standard for MERGE, which insists that the MERGE
			 * join condition should not join a target row to more than
			 * one source row.
			 *
			 * The latter case arises if the tuple is modified by a
			 * command in a BEFORE trigger, or perhaps by a command in a
			 * volatile function used in the query.  In such situations we
			 * should not ignore the MERGE action, but it is equally
			 * unsafe to proceed.  We don't want to discard the original
			 * MERGE action while keeping the triggered actions based on
			 * it; and it would be no better to allow the original MERGE
			 * action while discarding the updates that it triggered.  So
			 * throwing an error is the only safe course.
			 */
			if (context->tmfd.cmax != estate->es_output_cid)
								ereport(ERROR,
										(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
										 errmsg("tuple to be updated or deleted was already modified by an operation triggered by the current command"),
										 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

			if (TransactionIdIsCurrentTransactionId(context->tmfd.xmax))
				ereport(ERROR,
				    (errcode(ERRCODE_CARDINALITY_VIOLATION),
				/* translator: %s is a SQL command name */
				     errmsg("%s command cannot affect row a second time", "MERGE"),
				     errhint("Ensure that not more than one source row matches any one "
					     "target row.")));
			/* This shouldn't happen */
			elog(ERROR, "attempted to update or delete invisible tuple");
			break;

		case TM_Deleted:
			if (IsolationUsesXactSnapshot())
				ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("could not serialize access due to concurrent delete")));

			/*
			 * If the tuple was already deleted, return to let
			 * caller handle it under NOT MATCHED clauses.
			 */
			*matched = false;
			return NULL;


		case TM_Updated:
			{
				bool		was_matched;
				Relation	resultRelationDesc;
				TupleTableSlot *epqslot,
							*inputslot;
				LockTupleMode lockmode;

#if PG17_GE
				/*
					* The target tuple was concurrently updated by some other
					* transaction.  If we are currently processing a MATCHED
					* action, use EvalPlanQual() with the new version of the
					* tuple and recheck the join qual, to detect a change
					* from the MATCHED to the NOT MATCHED cases.  If we are
					* already processing a NOT MATCHED BY SOURCE action, we
					* skip this (cannot switch from NOT MATCHED BY SOURCE to
					* MATCHED).
					*/
				was_matched = relaction->mas_action->matchKind == MERGE_WHEN_MATCHED;
#else
				was_matched = true;
				/*
				 * If cpUpdateRetrySlot is set,
				 * ExecCrossPartitionUpdate() must have
				 * detected that the tuple was concurrently
				 * updated, so we restart the search for an
				 * appropriate WHEN MATCHED clause to process
				 * the updated tuple.
				 *
				 * In this case, ExecDelete() would already
				 * have performed EvalPlanQual() on the
				 * latest version of the tuple, which in turn
				 * would already have been loaded into
				 * ri_oldTupleSlot, so no need to do either
				 * of those things.
				 *
				 * XXX why do we not check the WHEN NOT
				 * MATCHED list in this case?
				 */
				if (!TupIsNull(context->cpUpdateRetrySlot))
					goto lmerge_matched;

				/*
				 * Otherwise, we run the EvalPlanQual() with
				 * the new version of the tuple. If
				 * EvalPlanQual() does not return a tuple,
				 * then we switch to the NOT MATCHED list of
				 * actions. If it does return a tuple and the
				 * join qual is still satisfied, then we just
				 * need to recheck the MATCHED actions,
				 * starting from the top, and execute the
				 * first qualifying action.
				 */

#endif
				resultRelationDesc = resultRelInfo->ri_RelationDesc;
				lockmode = ExecUpdateLockMode(estate, resultRelInfo);

				if (was_matched)
					inputslot = EvalPlanQualSlot(epqstate, resultRelationDesc,
													resultRelInfo->ri_RangeTableIndex);
				else
					inputslot = resultRelInfo->ri_oldTupleSlot;

				result = table_tuple_lock(resultRelationDesc, tupleid,
											estate->es_snapshot,
											inputslot, estate->es_output_cid,
											lockmode, LockWaitBlock,
											TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
											&context->tmfd);
				switch (result)
				{
					case TM_Ok:
#if PG17_GE
						/*
							* If the tuple was updated and migrated to
							* another partition concurrently, the current
							* MERGE implementation can't follow.  There's
							* probably a better way to handle this case, but
							* it'd require recognizing the relation to which
							* the tuple moved, and setting our current
							* resultRelInfo to that.
							*/
						if (ItemPointerIndicatesMovedPartitions(&context->tmfd.ctid))
							ereport(ERROR,
									(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
										errmsg("tuple to be merged was already moved to another partition due to concurrent update")));
#endif

						/*
							* If this was a MATCHED case, use EvalPlanQual()
							* to recheck the join condition.
							*/
						if (was_matched)
						{
							epqslot = EvalPlanQual(epqstate,
													resultRelationDesc,
													resultRelInfo->ri_RangeTableIndex,
													inputslot);

							/*
								* If the subplan didn't return a tuple, then
								* we must be dealing with an inner join for
								* which the join condition no longer matches.
								* This can only happen if there are no NOT
								* MATCHED actions, and so there is nothing
								* more to do.
								*/
							if (TupIsNull(epqslot))
#if PG17_GE
								goto out;
#else
								return false;
#endif

							/*
								* If we got a NULL ctid from the subplan, the
								* join quals no longer pass and we switch to
								* the NOT MATCHED BY SOURCE case.
								*/
							(void) ExecGetJunkAttribute(epqslot,
														resultRelInfo->ri_RowIdAttNo,
														&isNull);
							if (isNull)
#if PG17_GE
								*matched = false;
#else
								return false;
#endif

#if PG17_GE
							/*
								* Otherwise, recheck the join quals to see if
								* we need to switch to the NOT MATCHED BY
								* SOURCE case.
								*/
#if PG_VERSION_NUM >= 170100
							if (resultRelInfo->ri_needLockTagTuple)
							{
								if (ItemPointerIsValid(&lockedtid))
									UnlockTuple(resultRelInfo->ri_RelationDesc, &lockedtid,
												InplaceUpdateTupleLock);
								LockTuple(resultRelInfo->ri_RelationDesc, &context->tmfd.ctid,
											InplaceUpdateTupleLock);
								lockedtid = context->tmfd.ctid;
							}
#endif
							if (!table_tuple_fetch_row_version(resultRelationDesc,
																&context->tmfd.ctid,
																SnapshotAny,
																resultRelInfo->ri_oldTupleSlot))
								elog(ERROR, "failed to fetch the target tuple");

							if (*matched)
								*matched = ExecQual(resultRelInfo->ri_MergeJoinCondition,
													econtext);

							/* Switch lists, if necessary */
							if (!*matched)
								actionStates = mergeActions[MERGE_WHEN_NOT_MATCHED_BY_SOURCE];
#endif
						}

						/*
							* Loop back and process the MATCHED or NOT
							* MATCHED BY SOURCE actions from the start.
							*/
						goto lmerge_matched;

					case TM_Deleted:

						/*
							* tuple already deleted; tell caller to run NOT
							* MATCHED [BY TARGET] actions
							*/
						*matched = false;
						goto out;

					case TM_SelfModified:

						/*
							* This can be reached when following an update
							* chain from a tuple updated by another session,
							* reaching a tuple that was already updated or
							* deleted by the current command, or by a later
							* command in the current transaction. As above,
							* this should always be treated as an error.
							*/
						if (context->tmfd.cmax != estate->es_output_cid)
							ereport(ERROR,
									(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
										errmsg("tuple to be updated or deleted was already modified by an operation triggered by the current command"),
										errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

						if (TransactionIdIsCurrentTransactionId(context->tmfd.xmax))
							ereport(ERROR,
									(errcode(ERRCODE_CARDINALITY_VIOLATION),
							/* translator: %s is a SQL command name */
										errmsg("%s command cannot affect row a second time",
											"MERGE"),
										errhint("Ensure that not more than one source row matches any one target row.")));

						/* This shouldn't happen */
						elog(ERROR, "attempted to update or delete invisible tuple");
						goto out;

					default:
						/* see table_tuple_lock call in ExecDelete() */
						elog(ERROR, "unexpected table_tuple_lock status: %u",
								result);
						goto out;
				}
			}

		case TM_Invisible:
		case TM_WouldBlock:
		case TM_BeingModified:
			/* these should not occur */
			elog(ERROR, "unexpected tuple operation result: %d", result);
			break;
		}

#if PG17_GE
	/* Process RETURNING if present */
	if (resultRelInfo->ri_projectReturning)
	{
		switch (commandType)
		{
			case CMD_UPDATE:
				/* Variable newslot should be set for CMD_UPDATE above */
				Assert(newslot != NULL);
				rslot = ExecProcessReturning(resultRelInfo, newslot,
											 context->planSlot);
				break;

			case CMD_DELETE:
				rslot = ExecProcessReturning(resultRelInfo,
											 resultRelInfo->ri_oldTupleSlot,
											 context->planSlot);
				break;

			case CMD_NOTHING:
				break;

			default:
				elog(ERROR, "unrecognized commandType: %d",
					 (int) commandType);
		}
	}
#endif

		/*
		 * We've activated one of the WHEN clauses, so we don't
		 * search further. This is required behaviour, not an
		 * optimization.
		 */
		break;
	}


	/*
	 * Successfully executed an action or no qualifying action was found.
	 */
out:
#if PG_VERSION_NUM >= 170100
	if (ItemPointerIsValid(&lockedtid))
		UnlockTuple(resultRelInfo->ri_RelationDesc, &lockedtid,
					InplaceUpdateTupleLock);
#endif

	return rslot;
}

/*
 * Execute the first qualifying NOT MATCHED action.
 */
TupleTableSlot*
ht_ExecMergeNotMatched(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
		       ChunkDispatchState * cds, bool canSetTag)
{
	ModifyTableState *mtstate = context->mtstate;
	ExprContext    *econtext = mtstate->ps.ps_ExprContext;
	List	       *actionStates = NIL;
	ListCell       *l;
	TupleTableSlot *rslot = NULL;

	/*
	 * For INSERT actions, the root relation's merge action is OK since
	 * the INSERT's targetlist and the WHEN conditions can only refer to
	 * the source relation and hence it does not matter which result
	 * relation we work with.
	 *
	 * XXX does this mean that we can avoid creating copies of
	 * actionStates on partitioned tables, for not-matched actions?
	 *
	 * XXX do we need an additional support of NOT MATCHED BY SOURCE
	 * for PG >= 17? See PostgreSQL commit 0294df2f1f84
	 */
#if PG17_GE
	actionStates = cds->rri->ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET];
#else
	actionStates = cds->rri->ri_notMatchedMergeAction;
#endif
	/*
	 * Make source tuple available to ExecQual and ExecProject. We don't
	 * need the target tuple, since the WHEN quals and targetlist can't
	 * refer to the target columns.
	 */
	econtext->ecxt_scantuple = NULL;
	econtext->ecxt_innertuple = context->planSlot;
	econtext->ecxt_outertuple = NULL;

	foreach(l, actionStates) {
		MergeActionState *action = (MergeActionState *) lfirst(l);
		CmdType		commandType = action->mas_action->commandType;
		TupleTableSlot *newslot;

		/*
		 * Test condition, if any.
		 *
		 * In the absence of any condition, we perform the action
		 * unconditionally (no need to check separately since
		 * ExecQual() will return true if there are no conditions to
		 * evaluate).
		 */
		if (!ExecQual(action->mas_whenqual, econtext))
			continue;

		/* Perform stated action */
		switch (commandType) {
		case CMD_INSERT:

			/*
			 * Project the tuple.  In case of a partitioned
			 * table, the projection was already built to use the
			 * root's descriptor, so we don't need to map the
			 * tuple here.
			 */
			newslot = ExecProject(action->mas_proj);
#if PG17_GE
			mtstate->mt_merge_action = action;
#else
			context->relaction = action;
#endif
			if (cds->is_dropped_attr_exists)
			{
				AttrMap *map;
				TupleDesc parenttupdesc, chunktupdesc;
				TupleTableSlot *chunk_slot = NULL;

				parenttupdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
				chunktupdesc = RelationGetDescr(cds->rri->ri_RelationDesc);
				/* map from parent to chunk */
#if PG16_LT
				map = build_attrmap_by_name_if_req(parenttupdesc, chunktupdesc);
#else
				map = build_attrmap_by_name_if_req(parenttupdesc, chunktupdesc, false);
#endif
				if (map != NULL)
					chunk_slot =
						execute_attr_map_slot(map,
												newslot,
												MakeSingleTupleTableSlot(chunktupdesc,
																		&TTSOpsVirtual));
				rslot =  ExecInsert(context,
									resultRelInfo,
									cds,
									(chunk_slot ? chunk_slot : newslot),
									canSetTag);
				if (chunk_slot)
					ExecDropSingleTupleTableSlot(chunk_slot);
			}
			else
				rslot = ExecInsert(context, resultRelInfo, cds, newslot, canSetTag);
			mtstate->mt_merge_inserted = 1;
			break;
		case CMD_NOTHING:
			/* Do nothing */
			break;
		default:
			elog(ERROR, "unknown action in MERGE WHEN NOT MATCHED clause");
		}

		/*
		 * We've activated one of the WHEN clauses, so we don't
		 * search further. This is required behaviour, not an
		 * optimization.
		 */
		break;
	}

	return rslot;
}

/*
 * Perform MERGE.
 */
TupleTableSlot *
ht_ExecMerge(ModifyTableContext * context, ResultRelInfo * resultRelInfo, ChunkDispatchState * cds,
	     ItemPointer tupleid, HeapTuple oldtuple, bool canSetTag)
{
	bool		matched;
	TupleTableSlot* rslot = NULL;

	/*-----
	 * If we are dealing with a WHEN MATCHED case (tupleid is valid), we
	 * execute the first action for which the additional WHEN MATCHED AND
	 * quals pass.  If an action without quals is found, that action is
	 * executed.
	 *
	 * Similarly, if we are dealing with WHEN NOT MATCHED case, we look at
	 * the given WHEN NOT MATCHED actions in sequence until one passes.
	 *
	 * Things get interesting in case of concurrent update/delete of the
	 * target tuple. Such concurrent update/delete is detected while we are
	 * executing a WHEN MATCHED action.
	 *
	 * A concurrent update can:
	 *
	 * 1. modify the target tuple so that it no longer satisfies the
	 *    additional quals attached to the current WHEN MATCHED action
	 *
	 *    In this case, we are still dealing with a WHEN MATCHED case.
	 *    We recheck the list of WHEN MATCHED actions from the start and
	 *    choose the first one that satisfies the new target tuple.
	 *
	 * 2. modify the target tuple so that the join quals no longer pass and
	 *    hence the source tuple no longer has a match.
	 *
	 *    In this case, the source tuple no longer matches the target tuple,
	 *    so we now instead find a qualifying WHEN NOT MATCHED action to
	 *    execute.
	 *
	 * XXX Hmmm, what if the updated tuple would now match one that was
	 * considered NOT MATCHED so far?
	 *
	 * A concurrent delete changes a WHEN MATCHED case to WHEN NOT MATCHED.
	 *
	 * ht_ExecMergeMatched takes care of following the update chain and
	 * re-finding the qualifying WHEN MATCHED action, as long as the updated
	 * target tuple still satisfies the join quals, i.e., it remains a WHEN
	 * MATCHED case. If the tuple gets deleted or the join quals fail, it
	 * returns and we try ht_ExecMergeNotMatched. Given that ht_ExecMergeMatched
	 * always make progress by following the update chain and we never switch
	 * from ht_ExecMergeNotMatched to ht_ExecMergeMatched, there is no risk of a
	 * livelock.
	 */
#if PG17_GE
	matched = tupleid != NULL || oldtuple != NULL;
#else
	matched = tupleid != NULL;
#endif
	if (matched)
		rslot = ht_ExecMergeMatched(context, resultRelInfo, tupleid, oldtuple, canSetTag, &matched);


	/*
	 * Either we were dealing with a NOT MATCHED tuple or
	 * ht_ExecMergeMatched() returned "false", indicating the previously
	 * MATCHED tuple no longer matches.
	 */
	if (!matched)
	{
#if PG17_GE
		if(rslot == NULL)
			rslot = ht_ExecMergeNotMatched(context, resultRelInfo, cds, canSetTag);
		else
			context->mtstate->mt_merge_pending_not_matched = context->planSlot;
#else
		(void) ht_ExecMergeNotMatched(context, resultRelInfo, cds, canSetTag);
#endif
	}

	return rslot;
}

#endif

#if PG15_GE && PG17_LT
/*
 * Callback for ModifyTableContext->GetUpdateNewTuple for use by MERGE.  It
 * computes the updated tuple by projecting from the current merge action's
 * projection.
 */
static TupleTableSlot *
mergeGetUpdateNewTuple(ResultRelInfo * relinfo, TupleTableSlot * planSlot, TupleTableSlot * oldSlot,
		       MergeActionState * relaction) {
	ExprContext    *econtext = relaction->mas_proj->pi_exprContext;

	econtext->ecxt_scantuple = oldSlot;
	econtext->ecxt_innertuple = planSlot;

	return ExecProject(relaction->mas_proj);
}
#endif
