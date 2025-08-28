/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file is based on postgresql backend/executor/nodeModifyTable.c
 * Ordering of functions in this file is based on the order of functions in
 * nodeModifyTable.c
 */

/* clang-format off */

/* INTERFACE ROUTINES
 *		ExecInitModifyTable - initialize the ModifyTable node
 *		ExecModifyTable		- retrieve the next tuple from the node
 *		ExecEndModifyTable	- shut down the ModifyTable node
 *		ExecReScanModifyTable - rescan the ModifyTable node
 *
 *	 NOTES
 *		The ModifyTable node receives input from its outerPlan, which is
 *		the data to insert for INSERT cases, the changed columns' new
 *		values plus row-locating info for UPDATE and MERGE cases, or just the
 *		row-locating info for DELETE cases.
 *
 *		The relation to modify can be an ordinary table, a foreign table, or a
 *		view.  If it's a view, either it has sufficient INSTEAD OF triggers or
 *		this node executes only MERGE ... DO NOTHING.  If the original MERGE
 *		targeted a view not in one of those two categories, earlier processing
 *		already pointed the ModifyTable result relation to an underlying
 *		relation of that other view.  This node does process
 *		ri_WithCheckOptions, which may have expressions from those other,
 *		automatically updatable views.
 *
 *		MERGE runs a join between the source relation and the target table.
 *		If any WHEN NOT MATCHED [BY TARGET] clauses are present, then the join
 *		is an outer join that might output tuples without a matching target
 *		tuple.  In this case, any unmatched target tuples will have NULL
 *		row-locating info, and only INSERT can be run.  But for matched target
 *		tuples, the row-locating info is used to determine the tuple to UPDATE
 *		or DELETE.  When all clauses are WHEN MATCHED or WHEN NOT MATCHED BY
 *		SOURCE, all tuples produced by the join will include a matching target
 *		tuple, so all tuples contain row-locating info.
 *
 *		If the query specifies RETURNING, then the ModifyTable returns a
 *		RETURNING tuple after completing each row insert, update, or delete.
 *		It must be called again to continue the operation.  Without RETURNING,
 *		we just loop within the node until all the work is done, then
 *		return NULL.  This avoids useless call/return overhead.
 */

#include <postgres.h>
#include <access/tupdesc.h>
#include <access/xact.h>
#include <catalog/pg_attribute.h>
#include <catalog/pg_type.h>
#include <executor/execPartition.h>
#include <executor/nodeModifyTable.h>
#include <foreign/foreign.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pg_list.h>
#include <nodes/plannodes.h>
#include <optimizer/optimizer.h>
#include <optimizer/plancat.h>
#include <parser/parsetree.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>

#include "cross_module_fn.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "modify_hypertable.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/chunk_dispatch/chunk_dispatch.h"
#include "utils.h"

/*
 * Context struct for a ModifyTable operation, containing basic execution
 * state and some output variables populated by ExecUpdateAct() and
 * ExecDeleteAct() to report the result of their actions to callers.
 */
typedef struct ModifyTableContext
{
	/* Operation state */
	ModifyTableState *mtstate;
	EPQState *epqstate;
	EState *estate;

	/*
	 * Slot containing tuple obtained from ModifyTable's subplan.  Used to
	 * access "junk" columns that are not going to be stored.
	 */
	TupleTableSlot *planSlot;

	/* MERGE specific */
	MergeActionState *relaction; /* MERGE action in progress */
	/*
	 * Information about the changes that were made concurrently to a tuple
	 * being updated or deleted
	 */
	TM_FailureData tmfd;

	/*
	 * The tuple produced by EvalPlanQual to retry from, if a
	 * cross-partition UPDATE requires it
	 */
	TupleTableSlot *cpUpdateRetrySlot;

	/*
	 * The tuple projected by the INSERT's RETURNING clause, when doing a
	 * cross-partition UPDATE
	 */
	TupleTableSlot *cpUpdateReturningSlot;

	/*
	 * Lock mode to acquire on the latest tuple version before performing
	 * EvalPlanQual on it
	 */
	LockTupleMode lockmode;
} ModifyTableContext;

/*
 * Context struct containing output data specific to UPDATE operations.
 */
typedef struct UpdateContext
{
	bool		crossPartUpdate;	/* was it a cross-partition update? */
#if PG16_LT
	bool updateIndexes; /* index update required? */
#else
	TU_UpdateIndexes updateIndexes; /* Which index updates are required? */
#endif

	/*
	 * Lock mode to acquire on the latest tuple version before performing
	 * EvalPlanQual on it
	 */
	LockTupleMode lockmode;
} UpdateContext;


static void ExecBatchInsert(ModifyTableState *mtstate,
							ResultRelInfo *resultRelInfo,
							TupleTableSlot **slots,
							TupleTableSlot **planSlots,
							int numSlots,
							EState *estate,
							bool canSetTag);
static void ExecPendingInserts(EState *estate);
/*
static void ExecCrossPartitionUpdateForeignKey(ModifyTableContext *context,
											   ResultRelInfo *sourcePartInfo,
											   ResultRelInfo *destPartInfo,
											   ItemPointer tupleid,
											   TupleTableSlot *oldslot,
											   TupleTableSlot *newslot);
*/
static bool ExecOnConflictUpdate(ModifyTableContext *context,
								 ResultRelInfo *resultRelInfo,
								 ItemPointer conflictTid,
								 TupleTableSlot *excludedSlot,
								 bool canSetTag,
								 TupleTableSlot **returning);

static TupleTableSlot *ExecPrepareTupleRouting(ModifyTableState *mtstate,
											   EState *estate,
											   ChunkDispatchState *cds,
											   ResultRelInfo *targetRelInfo,
											   TupleTableSlot *slot,
											   ResultRelInfo **partRelInfo);

static TupleTableSlot *ExecMerge(ModifyTableContext *context,
								 ResultRelInfo *resultRelInfo,
								 ChunkDispatchState *cds,
								 ItemPointer tupleid,
								 HeapTuple oldtuple,
								 bool canSetTag);
/*
static void ExecInitMerge(ModifyTableState *mtstate, EState *estate);
*/
static TupleTableSlot *ExecMergeMatched(ModifyTableContext *context,
										ResultRelInfo *resultRelInfo,
										ItemPointer tupleid,
										HeapTuple oldtuple,
										bool canSetTag,
										bool *matched);
static TupleTableSlot *ExecMergeNotMatched(ModifyTableContext *context,
										   ResultRelInfo *resultRelInfo,
										   ChunkDispatchState *cds,
										   bool canSetTag);


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
 */
static void
ExecCheckPlanOutput(Relation resultRel, List *targetList)
{
	TupleDesc	resultDesc = RelationGetDescr(resultRel);
	int			attno = 0;
	ListCell   *lc;

	foreach(lc, targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr;

		Assert(!tle->resjunk);	/* caller removed junk items already */

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
						 errdetail("Table has type %s at ordinal position %d, but query expects %s.",
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
			if (!IsA(tle->expr, Const) ||
				!((Const *) tle->expr)->constisnull)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("table row type and query-specified row type do not match"),
						 errdetail("Query provides a value for a dropped column at ordinal position %d.",
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
 */
static TupleTableSlot *
ExecProcessReturning(ResultRelInfo *resultRelInfo,
					 TupleTableSlot *tupleSlot,
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
	econtext->ecxt_scantuple->tts_tableOid =
		RelationGetRelid(resultRelInfo->ri_RelationDesc);

	/* Compute the RETURNING expressions */
	return ExecProject(projectReturning);
}

/*
 * ExecCheckTupleVisible -- verify tuple is visible
 *
 * It would not be consistent with guarantees of the higher isolation levels to
 * proceed with avoiding insertion (taking speculative insertion's alternative
 * path) on the basis of another tuple that is not visible to MVCC snapshot.
 * Check for the need to raise a serialization failure, and do so as necessary.
 */
static void
ExecCheckTupleVisible(EState *estate,
					  Relation rel,
					  TupleTableSlot *slot)
{
	if (!IsolationUsesXactSnapshot())
		return;

	if (!table_tuple_satisfies_snapshot(rel, slot, estate->es_snapshot))
	{
		Datum		xminDatum;
		TransactionId xmin;
		bool		isnull;

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
 */
static void
ExecCheckTIDVisible(EState *estate,
					ResultRelInfo *relinfo,
					ItemPointer tid,
					TupleTableSlot *tempSlot)
{
	Relation	rel = relinfo->ri_RelationDesc;

	/* Redundantly check isolation level */
	if (!IsolationUsesXactSnapshot())
		return;

	if (!table_tuple_fetch_row_version(rel, tid, SnapshotAny, tempSlot))
		elog(ERROR, "failed to fetch conflicting tuple for ON CONFLICT");
	ExecCheckTupleVisible(estate, rel, tempSlot);
	ExecClearTuple(tempSlot);
}

/*
 * ExecInitInsertProjection
 *		Do one-time initialization of projection data for INSERT tuples.
 *
 * INSERT queries may need a projection to filter out junk attrs in the tlist.
 *
 * This is also a convenient place to verify that the
 * output of an INSERT matches the target table.
 */
static void
ExecInitInsertProjection(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	Plan	   *subplan = outerPlan(node);
	EState	   *estate = mtstate->ps.state;
	List	   *insertTargetList = NIL;
	bool		need_projection = false;
	ListCell   *l;

	/* Extract non-junk columns of the subplan's result tlist. */
	foreach(l, subplan->targetlist)
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
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);

	/* Build ProjectionInfo if needed (it probably isn't). */
	if (need_projection)
	{
		TupleDesc	relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

		/* need an expression context to do the projection */
		if (mtstate->ps.ps_ExprContext == NULL)
			ExecAssignExprContext(estate, &mtstate->ps);

		resultRelInfo->ri_projectNew =
			ExecBuildProjectionInfo(insertTargetList,
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
 */
static void
ExecInitUpdateProjection(ModifyTableState *mtstate,
						 ResultRelInfo *resultRelInfo)
{
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	Plan	   *subplan = outerPlan(node);
	EState	   *estate = mtstate->ps.state;
	TupleDesc	relDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);
	int			whichrel;
	List	   *updateColnos;

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
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);
	resultRelInfo->ri_newTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);

	/* need an expression context to do the projection */
	if (mtstate->ps.ps_ExprContext == NULL)
		ExecAssignExprContext(estate, &mtstate->ps);

	resultRelInfo->ri_projectNew =
		ExecBuildUpdateProjection(subplan->targetlist,
								  false,	/* subplan did the evaluation */
								  updateColnos,
								  relDesc,
								  mtstate->ps.ps_ExprContext,
								  resultRelInfo->ri_newTupleSlot,
								  &mtstate->ps);

	resultRelInfo->ri_projectNewInfoValid = true;
}

/*
 * ExecGetInsertNewTuple
 *		This prepares a "new" tuple ready to be inserted into given result
 *		relation, by removing any junk columns of the plan's output tuple
 *		and (if necessary) coercing the tuple to the right tuple format.
 */
static TupleTableSlot *
ExecGetInsertNewTuple(ResultRelInfo *relinfo,
					  TupleTableSlot *planSlot)
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
 * ExecPrepareTupleRouting --- prepare for routing one tuple
 *
 * Determine the partition in which the tuple in slot is to be inserted,
 * and return its ResultRelInfo in *partRelInfo.  The return value is
 * a slot holding the tuple of the partition rowtype.
 *
 * This also sets the transition table information in mtstate based on the
 * selected partition.
 */
static TupleTableSlot *
ExecPrepareTupleRouting(ModifyTableState *mtstate,
						EState *estate,
						ChunkDispatchState *cds,
						ResultRelInfo *targetRelInfo,
						TupleTableSlot *slot,
						ResultRelInfo **partRelInfo)
{
	ChunkInsertState *cis = cds->cis;
	/* Convert the tuple to the chunk's rowtype, if necessary */
	if (cis->hyper_to_chunk_map != NULL && cds->is_dropped_attr_exists == false)
		slot = execute_attr_map_slot(cis->hyper_to_chunk_map->attrMap, slot, cis->slot);

	*partRelInfo = cds->rri;
	return slot;
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
ExecInsert(ModifyTableContext *context,
		   ResultRelInfo *resultRelInfo,
		   ChunkDispatchState *cds,
		   TupleTableSlot *slot,
		   bool canSetTag)
{
	ModifyTableState *mtstate = context->mtstate;
	EState	   *estate = context->estate;
	Relation	resultRelationDesc;
	List	   *recheckIndexes = NIL;
	TupleTableSlot *planSlot = context->planSlot;
	TupleTableSlot *result = NULL;
	TransitionCaptureState *ar_insert_trig_tcs;
	ModifyTable *node = (ModifyTable *) mtstate->ps.plan;
	OnConflictAction onconflict = node->onConflictAction;
	MemoryContext oldContext;
	bool skip_generated_column_computations = false;

	Assert(!mtstate->mt_partition_tuple_routing);

	/*
	 * If the input result relation is a partitioned table, find the leaf
	 * partition to insert the tuple into.
	 */
	if (cds)
	{
		ResultRelInfo *partRelInfo;

		slot = ExecPrepareTupleRouting(mtstate, estate, cds,
									   resultRelInfo, slot,
									   &partRelInfo);
		resultRelInfo = partRelInfo;

		skip_generated_column_computations = cds->cis->skip_generated_column_computations;
	}

	ExecMaterializeSlot(slot);

	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	/*
	 * Open the table's indexes, if we have not done so already, so that we
	 * can add new index entries for the inserted tuple.
	 */
	if (resultRelationDesc->rd_rel->relhasindex &&
		resultRelInfo->ri_IndexRelationDescs == NULL)
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
	if (resultRelInfo->ri_TrigDesc &&
			resultRelInfo->ri_TrigDesc->trig_insert_before_row)
	{
		if (!ExecBRInsertTriggers(estate, resultRelInfo, slot))
			return NULL; /* "do nothing" */
	}

	/* INSTEAD OF ROW INSERT Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_insert_instead_row)
	{
		if (!ExecIRInsertTriggers(estate, resultRelInfo, slot))
			return NULL;		/* "do nothing" */
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
			ExecComputeStoredGenerated(resultRelInfo, estate, slot,
									   CMD_INSERT);

		/*
		 * If the FDW supports batching, and batching is requested, accumulate
		 * rows and insert them in batches. Otherwise use the per-row inserts.
		 */
		if (resultRelInfo->ri_BatchSize > 1)
		{
			/*
			 * When we've reached the desired batch size, perform the
			 * insertion.
			 */
			if (resultRelInfo->ri_NumSlots == resultRelInfo->ri_BatchSize)
			{
				ExecBatchInsert(mtstate, resultRelInfo,
								resultRelInfo->ri_Slots,
								resultRelInfo->ri_PlanSlots,
								resultRelInfo->ri_NumSlots,
								estate, canSetTag);
				resultRelInfo->ri_NumSlots = 0;
			}

			oldContext = MemoryContextSwitchTo(estate->es_query_cxt);

			if (resultRelInfo->ri_Slots == NULL)
			{
				resultRelInfo->ri_Slots = palloc(sizeof(TupleTableSlot *) *
												 resultRelInfo->ri_BatchSize);
				resultRelInfo->ri_PlanSlots = palloc(sizeof(TupleTableSlot *) *
													 resultRelInfo->ri_BatchSize);
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
				TupleDesc	tdesc = CreateTupleDescCopy(slot->tts_tupleDescriptor);
				TupleDesc	plan_tdesc =
					CreateTupleDescCopy(planSlot->tts_tupleDescriptor);

				resultRelInfo->ri_Slots[resultRelInfo->ri_NumSlots] =
					MakeSingleTupleTableSlot(tdesc, slot->tts_ops);

				resultRelInfo->ri_PlanSlots[resultRelInfo->ri_NumSlots] =
					MakeSingleTupleTableSlot(plan_tdesc, planSlot->tts_ops);

				/* remember how many batch slots we initialized */
				resultRelInfo->ri_NumSlotsInitialized++;
			}

			ExecCopySlot(resultRelInfo->ri_Slots[resultRelInfo->ri_NumSlots],
						 slot);

			ExecCopySlot(resultRelInfo->ri_PlanSlots[resultRelInfo->ri_NumSlots],
						 planSlot);

			resultRelInfo->ri_NumSlots++;

			MemoryContextSwitchTo(oldContext);

			return NULL;
		}

		/*
		 * insert into foreign table: let the FDW do it
		 */
		slot = resultRelInfo->ri_FdwRoutine->ExecForeignInsert(estate,
															   resultRelInfo,
															   slot,
															   planSlot);

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
		 * NOTE: we are skipping generation if we detect that we went through
		 * compressed chunk uniqueness check which would have already
		 * triggered generating the columns.
		 */
		if (resultRelationDesc->rd_att->constr &&
			resultRelationDesc->rd_att->constr->has_generated_stored &&
			!skip_generated_column_computations
		)
			ExecComputeStoredGenerated(resultRelInfo, estate, slot,
									   CMD_INSERT);

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
		else if (mtstate->operation == CMD_MERGE)
			wco_kind = (
#if PG17_GE
						   mtstate->mt_merge_action->mas_action->commandType
#else
						   context->relaction->mas_action->commandType
#endif
						   == CMD_UPDATE) ?
						   WCO_RLS_UPDATE_CHECK :
						   WCO_RLS_INSERT_CHECK;
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
#if PG18_GE
										   NULL,
#endif
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
		ExecARUpdateTriggers(estate,
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
 */
static void
ExecBatchInsert(ModifyTableState *mtstate,
				ResultRelInfo *resultRelInfo,
				TupleTableSlot **slots,
				TupleTableSlot **planSlots,
				int numSlots,
				EState *estate,
				bool canSetTag)
{
	int			i;
	int			numInserted = numSlots;
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
		 * AFTER ROW Triggers might reference the tableoid column, so
		 * (re-)initialize tts_tableOid before evaluating them.
		 */
		slot->tts_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

		/* AFTER ROW INSERT Triggers */
		ExecARInsertTriggers(estate, resultRelInfo, slot, NIL,
							 mtstate->mt_transition_capture);

		/*
		 * Check any WITH CHECK OPTION constraints from parent views.  See the
		 * comment in ExecInsert.
		 */
		if (resultRelInfo->ri_WithCheckOptions != NIL)
			ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo, slot, estate);
	}

	if (canSetTag && numInserted > 0)
		estate->es_processed += numInserted;

	/* Clean up all the slots, ready for the next batch */
	for (i = 0; i < numSlots; i++)
	{
		ExecClearTuple(slots[i]);
		ExecClearTuple(planSlots[i]);
	}
	resultRelInfo->ri_NumSlots = 0;
}

/*
 * ExecPendingInserts -- flushes all pending inserts to the foreign tables
 */
static void
ExecPendingInserts(EState *estate)
{
	ListCell   *l1,
			   *l2;

	forboth(l1, estate->es_insert_pending_result_relations,
			l2, estate->es_insert_pending_modifytables)
	{
		ResultRelInfo *resultRelInfo = (ResultRelInfo *) lfirst(l1);
		ModifyTableState *mtstate = (ModifyTableState *) lfirst(l2);

		Assert(mtstate);
		ExecBatchInsert(mtstate, resultRelInfo,
						resultRelInfo->ri_Slots,
						resultRelInfo->ri_PlanSlots,
						resultRelInfo->ri_NumSlots,
						estate, mtstate->canSetTag);
	}

	list_free(estate->es_insert_pending_result_relations);
	list_free(estate->es_insert_pending_modifytables);
	estate->es_insert_pending_result_relations = NIL;
	estate->es_insert_pending_modifytables = NIL;
}

/*
 * ExecDeletePrologue -- subroutine for ExecDelete
 *
 * Prepare executor state for DELETE.  Actually, the only thing we have to do
 * here is execute BEFORE ROW triggers.  We return false if one of them makes
 * the delete a no-op; otherwise, return true.
 */
static bool
ExecDeletePrologue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
				   ItemPointer tupleid, HeapTuple oldtuple,
				   TupleTableSlot **epqreturnslot, TM_Result *result)
{
	if (result)
		*result = TM_Ok;

	/* BEFORE ROW DELETE triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_delete_before_row)
	{
		/* Flush any pending inserts, so rows are visible to the triggers */
		if (context->estate->es_insert_pending_result_relations != NIL)
			ExecPendingInserts(context->estate);

		return ExecBRDeleteTriggersCompat(context->estate, context->epqstate,
									resultRelInfo, tupleid, oldtuple,
									epqreturnslot, result, &context->tmfd,
									context->mtstate->operation == CMD_MERGE);
	}

	return true;
}

/*
 * ExecDeleteAct -- subroutine for ExecDelete
 *
 * Actually delete the tuple from a plain table.
 *
 * Caller is in charge of doing EvalPlanQual as necessary
 */
static TM_Result
ExecDeleteAct(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
			  ItemPointer tupleid, bool changingPart)
{
	EState	   *estate = context->estate;

	return table_tuple_delete(resultRelInfo->ri_RelationDesc, tupleid,
							  estate->es_output_cid,
							  estate->es_snapshot,
							  estate->es_crosscheck_snapshot,
							  true /* wait for commit */ ,
							  &context->tmfd,
							  changingPart);
}

/*
 * ExecDeleteEpilogue -- subroutine for ExecDelete
 *
 * Closing steps of tuple deletion; this invokes AFTER FOR EACH ROW triggers,
 * including the UPDATE triggers if the deletion is being done as part of a
 * cross-partition tuple move.
 */
static void
ExecDeleteEpilogue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
				   ItemPointer tupleid, HeapTuple oldtuple, bool changingPart)
{
	ModifyTableState *mtstate = context->mtstate;
	EState	   *estate = context->estate;
	TransitionCaptureState *ar_delete_trig_tcs;

	/*
	 * If this delete is the result of a partition key update that moved the
	 * tuple to a new partition, put this row into the transition OLD TABLE,
	 * if there is one. We need to do this separately for DELETE and INSERT
	 * because they happen on different tables.
	 */
	ar_delete_trig_tcs = mtstate->mt_transition_capture;
	if (mtstate->operation == CMD_UPDATE && mtstate->mt_transition_capture &&
		mtstate->mt_transition_capture->tcs_update_old_table)
	{
		ExecARUpdateTriggers(estate, resultRelInfo,
							 NULL, NULL,
							 tupleid, oldtuple,
							 NULL, NULL, mtstate->mt_transition_capture,
							 false);

		/*
		 * We've already captured the OLD TABLE row, so make sure any AR
		 * DELETE trigger fired below doesn't capture it again.
		 */
		ar_delete_trig_tcs = NULL;
	}

	/* AFTER ROW DELETE Triggers */
	ExecARDeleteTriggers(estate, resultRelInfo, tupleid, oldtuple,
						 ar_delete_trig_tcs, changingPart);
}

/* ----------------------------------------------------------------
 *		ExecDelete
 *
 *		DELETE is like UPDATE, except that we delete the tuple and no
 *		index modifications are needed.
 *
 *		When deleting from a table, tupleid identifies the tuple to delete and
 *		oldtuple is NULL.  When deleting through a view INSTEAD OF trigger,
 *		oldtuple is passed to the triggers and identifies what to delete, and
 *		tupleid is invalid.  When deleting from a foreign table, tupleid is
 *		invalid; the FDW has to figure out which row to delete using data from
 *		the planSlot.  oldtuple is passed to foreign table triggers; it is
 *		NULL when the foreign table has no relevant triggers.  We use
 *		tupleDeleted to indicate whether the tuple is actually deleted,
 *		callers can use it to decide whether to continue the operation.  When
 *		this DELETE is a part of an UPDATE of partition-key, then the slot
 *		returned by EvalPlanQual() is passed back using output parameter
 *		epqreturnslot.
 *
 *		Returns RETURNING result if any, otherwise NULL.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecDelete(ModifyTableContext *context,
		   ResultRelInfo *resultRelInfo,
		   ItemPointer tupleid,
		   HeapTuple oldtuple,
		   bool processReturning,
		   bool changingPart,
		   bool canSetTag,
		   TM_Result *tmresult,
		   bool *tupleDeleted,
		   TupleTableSlot **epqreturnslot)
{
	EState	   *estate = context->estate;
	Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;
	TupleTableSlot *slot = NULL;
	TM_Result	result;

	if (tupleDeleted)
		*tupleDeleted = false;

	/*
	 * Prepare for the delete.  This includes BEFORE ROW triggers, so we're
	 * done if it says we are.
	 */
	if (!ExecDeletePrologue(context, resultRelInfo, tupleid, oldtuple,
							epqreturnslot, tmresult))
		return NULL;

	/* INSTEAD OF ROW DELETE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_delete_instead_row)
	{
		bool		dodelete;

		Assert(oldtuple != NULL);
		dodelete = ExecIRDeleteTriggers(estate, resultRelInfo, oldtuple);

		if (!dodelete)			/* "do nothing" */
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

		if (slot == NULL)		/* "do nothing" */
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
ldelete:
		result = ExecDeleteAct(context, resultRelInfo, tupleid, changingPart);

		if (tmresult)
			*tmresult = result;

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
							 errmsg("tuple to be deleted was already modified by an operation triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

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
					inputslot = EvalPlanQualSlot(context->epqstate, resultRelationDesc,
												 resultRelInfo->ri_RangeTableIndex);

					result = table_tuple_lock(resultRelationDesc, tupleid,
											  estate->es_snapshot,
											  inputslot, estate->es_output_cid,
											  LockTupleExclusive, LockWaitBlock,
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
										 errmsg("tuple to be deleted was already modified by an operation triggered by the current command"),
										 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
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
							elog(ERROR, "unexpected table_tuple_lock status: %u",
								 result);
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
				/* tuple already deleted; nothing to do */
				return NULL;

			default:
				elog(ERROR, "unrecognized table_tuple_delete status: %u",
					 result);
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

	ExecDeleteEpilogue(context, resultRelInfo, tupleid, oldtuple, changingPart);

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
				if (!table_tuple_fetch_row_version(resultRelationDesc, tupleid,
												   SnapshotAny, slot))
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

/*
 * ExecUpdatePrologue -- subroutine for ExecUpdate
 *
 * Prepare executor state for UPDATE.  This includes running BEFORE ROW
 * triggers.  We return false if one of them makes the update a no-op;
 * otherwise, return true.
 */
static bool
ExecUpdatePrologue(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
				   ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
				   TM_Result *result)
{
	Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;

	if (result)
		*result = TM_Ok;

	ExecMaterializeSlot(slot);

	/*
	 * Open the table's indexes, if we have not done so already, so that we
	 * can add new index entries for the updated tuple.
	 */
	if (resultRelationDesc->rd_rel->relhasindex &&
		resultRelInfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(resultRelInfo, false);

	/* BEFORE ROW UPDATE triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_before_row)
	{
		/* Flush any pending inserts, so rows are visible to the triggers */
		if (context->estate->es_insert_pending_result_relations != NIL)
			ExecPendingInserts(context->estate);

		return ExecBRUpdateTriggersCompat(context->estate, context->epqstate,
									resultRelInfo, tupleid, oldtuple, slot,
									result, &context->tmfd,
									context->mtstate->operation == CMD_MERGE);
	}

	return true;
}

/*
 * ExecUpdatePrepareSlot -- subroutine for ExecUpdateAct
 *
 * Apply the final modifications to the tuple slot before the update.
 * (This is split out because we also need it in the foreign-table code path.)
 */
static void
ExecUpdatePrepareSlot(ResultRelInfo *resultRelInfo,
					  TupleTableSlot *slot,
					  EState *estate)
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
		ExecComputeStoredGenerated(resultRelInfo, estate, slot,
								   CMD_UPDATE);
}

/*
 * ExecUpdateAct -- subroutine for ExecUpdate
 *
 * Actually update the tuple, when operating on a plain table.  If the
 * table is a partition, and the command was called referencing an ancestor
 * partitioned table, this routine migrates the resulting tuple to another
 * partition.
 *
 * The caller is in charge of keeping indexes current as necessary.  The
 * caller is also in charge of doing EvalPlanQual if the tuple is found to
 * be concurrently updated.  However, in case of a cross-partition update,
 * this routine does it.
 */
static TM_Result
ExecUpdateAct(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
			  ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
			  bool canSetTag, UpdateContext *updateCxt)
{
	EState	   *estate = context->estate;
	Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;
	bool		partition_constraint_failed;
	TM_Result	result;

	updateCxt->crossPartUpdate = false;

	/* Fill in GENERATEd columns */
	ExecUpdatePrepareSlot(resultRelInfo, slot, estate);

	/* ensure slot is independent, consider e.g. EPQ */
	ExecMaterializeSlot(slot);

	/*
	 * If partition constraint fails, this row might get moved to another
	 * partition, in which case we should check the RLS CHECK policy just
	 * before inserting into the new partition, rather than doing it here.
	 * This is because a trigger on that partition might again change the row.
	 * So skip the WCO checks if the partition constraint fails.
	 */
	partition_constraint_failed =
		resultRelationDesc->rd_rel->relispartition &&
		!ExecPartitionCheck(resultRelInfo, slot, estate, false);

	/* Check any RLS UPDATE WITH CHECK policies */
	if (!partition_constraint_failed &&
		resultRelInfo->ri_WithCheckOptions != NIL)
	{
		/*
		 * ExecWithCheckOptions() will skip any WCOs which are not of the kind
		 * we are looking for at this point.
		 */
		ExecWithCheckOptions(WCO_RLS_UPDATE_CHECK,
							 resultRelInfo, slot, estate);
	}

	/*
	 * If a partition check failed, try to move the row into the right
	 * partition.
	 */
	if (partition_constraint_failed)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot update partition key of hypertable"),
				 errdetail("The partition constraint failed, and the row was not moved to another partition."),
				 errhint("Use DELETE and INSERT to change the partition key.")));
	}

	/*
	 * Check the constraints of the tuple.  We've already checked the
	 * partition constraint above; however, we must still ensure the tuple
	 * passes all other constraints, so we will call ExecConstraints() and
	 * have it validate all remaining checks.
	 */
	if (resultRelationDesc->rd_att->constr)
		ExecConstraints(resultRelInfo, slot, estate);

	/*
	 * replace the heap tuple
	 *
	 * Note: if es_crosscheck_snapshot isn't InvalidSnapshot, we check that
	 * the row to be updated is visible to that snapshot, and throw a
	 * can't-serialize error if not. This is a special-case behavior needed
	 * for referential integrity updates in transaction-snapshot mode
	 * transactions.
	 */
	result = table_tuple_update(resultRelationDesc, tupleid, slot,
								estate->es_output_cid,
								estate->es_snapshot,
								estate->es_crosscheck_snapshot,
								true /* wait for commit */ ,
								&context->tmfd, &updateCxt->lockmode,
								&updateCxt->updateIndexes);

	return result;
}

/*
 * ExecUpdateEpilogue -- subroutine for ExecUpdate
 *
 * Closing steps of updating a tuple.  Must be called if ExecUpdateAct
 * returns indicating that the tuple was updated.
 */
static void
ExecUpdateEpilogue(ModifyTableContext *context, UpdateContext *updateCxt,
				   ResultRelInfo *resultRelInfo, ItemPointer tupleid,
				   HeapTuple oldtuple, TupleTableSlot *slot)
{
	ModifyTableState *mtstate = context->mtstate;
	List	   *recheckIndexes = NIL;

	/* insert index entries for tuple if necessary */
#if PG15
	if (resultRelInfo->ri_NumIndices > 0 && updateCxt->updateIndexes)
		recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
											   slot, context->estate,
											   true, false,
											   NULL, NIL);
#else
	if (resultRelInfo->ri_NumIndices > 0 && (updateCxt->updateIndexes != TU_None))
		recheckIndexes = ExecInsertIndexTuples(resultRelInfo,
											   slot, context->estate,
											   true, false,
											   NULL, NIL,
											   (updateCxt->updateIndexes == TU_Summarizing));
#endif

	/* AFTER ROW UPDATE Triggers */
	ExecARUpdateTriggers(context->estate, resultRelInfo,
						 NULL, NULL,
						 tupleid, oldtuple, slot,
						 recheckIndexes,
						 mtstate->operation == CMD_INSERT ?
						 mtstate->mt_oc_transition_capture :
						 mtstate->mt_transition_capture,
						 false);

	list_free(recheckIndexes);

	/*
	 * Check any WITH CHECK OPTION constraints from parent views.  We are
	 * required to do this after testing all constraints and uniqueness
	 * violations per the SQL spec, so we do it after actually updating the
	 * record in the heap and all indexes.
	 *
	 * ExecWithCheckOptions() will skip any WCOs which are not of the kind we
	 * are looking for at this point.
	 */
	if (resultRelInfo->ri_WithCheckOptions != NIL)
		ExecWithCheckOptions(WCO_VIEW_CHECK, resultRelInfo,
							 slot, context->estate);
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
 *		When updating a table, tupleid identifies the tuple to update and
 *		oldtuple is NULL.  When updating through a view INSTEAD OF trigger,
 *		oldtuple is passed to the triggers and identifies what to update, and
 *		tupleid is invalid.  When updating a foreign table, tupleid is
 *		invalid; the FDW has to figure out which row to update using data from
 *		the planSlot.  oldtuple is passed to foreign table triggers; it is
 *		NULL when the foreign table has no relevant triggers.
 *
 *		slot contains the new tuple value to be stored.
 *		planSlot is the output of the ModifyTable's subplan; we use it
 *		to access values from other input tables (for RETURNING),
 *		row-ID junk columns, etc.
 *
 *		Returns RETURNING result if any, otherwise NULL.  On exit, if tupleid
 *		had identified the tuple to update, it will identify the tuple
 *		actually updated after EvalPlanQual.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecUpdate(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
		   ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot *slot,
		   bool canSetTag)
{
	EState	   *estate = context->estate;
	Relation	resultRelationDesc = resultRelInfo->ri_RelationDesc;
	UpdateContext updateCxt = {0};
	TM_Result	result;

	/*
	 * abort the operation if not running transactions
	 */
	if (IsBootstrapProcessingMode())
		elog(ERROR, "cannot UPDATE during bootstrap");

	/*
	 * Prepare for the update.  This includes BEFORE ROW triggers, so we're
	 * done if it says we are.
	 */
	if (!ExecUpdatePrologue(context, resultRelInfo, tupleid, oldtuple, slot, NULL))
		return NULL;

	/* INSTEAD OF ROW UPDATE Triggers */
	if (resultRelInfo->ri_TrigDesc &&
		resultRelInfo->ri_TrigDesc->trig_update_instead_row)
	{
		if (!ExecIRUpdateTriggers(estate, resultRelInfo,
								  oldtuple, slot))
			return NULL;		/* "do nothing" */
	}
	else if (resultRelInfo->ri_FdwRoutine)
	{
		/* Fill in GENERATEd columns */
		ExecUpdatePrepareSlot(resultRelInfo, slot, estate);

		/*
		 * update in foreign table: let the FDW do it
		 */
		slot = resultRelInfo->ri_FdwRoutine->ExecForeignUpdate(estate,
															   resultRelInfo,
															   slot,
															   context->planSlot);

		if (slot == NULL)		/* "do nothing" */
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
#if PG16_GE
		ItemPointerData lockedtid;
#endif

		/*
		 * If we generate a new candidate tuple after EvalPlanQual testing, we
		 * must loop back here to try again.  (We don't need to redo triggers,
		 * however.  If there are any BEFORE triggers then trigger.c will have
		 * done table_tuple_lock to lock the correct tuple, so there's no need
		 * to do them again.)
		 */
redo_act:
#if PG16_GE
		lockedtid = *tupleid;
#endif
		result = ExecUpdateAct(context, resultRelInfo, tupleid, oldtuple, slot,
							   canSetTag, &updateCxt);

		/*
		 * If ExecUpdateAct reports that a cross-partition update was done,
		 * then the RETURNING tuple (if any) has been projected and there's
		 * nothing else for us to do.
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
							 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));

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
					inputslot = EvalPlanQualSlot(context->epqstate, resultRelationDesc,
												 resultRelInfo->ri_RangeTableIndex);

					result = table_tuple_lock(resultRelationDesc, tupleid,
											  estate->es_snapshot,
											  inputslot, estate->es_output_cid,
											  updateCxt.lockmode, LockWaitBlock,
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
								ExecInitUpdateProjection(context->mtstate,
														 resultRelInfo);

#if PG16_GE
							if (resultRelInfo->ri_needLockTagTuple)
							{
								UnlockTuple(resultRelationDesc,
											&lockedtid, InplaceUpdateTupleLock);
								LockTuple(resultRelationDesc,
										  tupleid, InplaceUpdateTupleLock);
							}
#endif

							/* Fetch the most recent version of old tuple. */
							oldSlot = resultRelInfo->ri_oldTupleSlot;
							if (!table_tuple_fetch_row_version(resultRelationDesc,
															   tupleid,
															   SnapshotAny,
															   oldSlot))
								elog(ERROR, "failed to fetch tuple being updated");
							slot = ExecGetUpdateNewTuple(resultRelInfo,
														 epqslot, oldSlot);
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
										 errmsg("tuple to be updated was already modified by an operation triggered by the current command"),
										 errhint("Consider using an AFTER trigger instead of a BEFORE trigger to propagate changes to other rows.")));
							return NULL;

						default:
							/* see table_tuple_lock call in ExecDelete() */
							elog(ERROR, "unexpected table_tuple_lock status: %u",
								 result);
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
				elog(ERROR, "unrecognized table_tuple_update status: %u",
					 result);
				return NULL;
		}
	}

	if (canSetTag)
		(estate->es_processed)++;

	ExecUpdateEpilogue(context, &updateCxt, resultRelInfo, tupleid, oldtuple,
					   slot);

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
 */
static bool
ExecOnConflictUpdate(ModifyTableContext *context,
					 ResultRelInfo *resultRelInfo,
					 ItemPointer conflictTid,
					 TupleTableSlot *excludedSlot,
					 bool canSetTag,
					 TupleTableSlot **returning)
{
	ModifyTableState *mtstate = context->mtstate;
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	Relation	relation = resultRelInfo->ri_RelationDesc;
	ExprState  *onConflictSetWhere = resultRelInfo->ri_onConflict->oc_WhereClause;
	TupleTableSlot *existing = resultRelInfo->ri_onConflict->oc_Existing;
	TM_FailureData tmfd;
	LockTupleMode lockmode;
	TM_Result	test;
	Datum		xminDatum;
	TransactionId xmin;
	bool		isnull;

	/*
	 * Parse analysis should have blocked ON CONFLICT for all system
	 * relations, which includes these.  There's no fundamental obstacle to
	 * supporting this; we'd just need to handle LOCKTAG_TUPLE like the other
	 * ExecUpdate() caller.
	 */
#if PG16_GE
	Assert(!resultRelInfo->ri_needLockTagTuple);
#endif

	/* Determine lock mode to use */
	lockmode = ExecUpdateLockMode(context->estate, resultRelInfo);

	/*
	 * Lock tuple for update.  Don't follow updates when tuple cannot be
	 * locked without doing so.  A row locking conflict here means our
	 * previous conclusion that the tuple is conclusively committed is not
	 * true anymore.
	 */
	test = table_tuple_lock(relation, conflictTid,
							context->estate->es_snapshot,
							existing, context->estate->es_output_cid,
							lockmode, LockWaitBlock, 0,
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
			 * occurring.  These problems are why the SQL standard similarly
			 * specifies that for SQL MERGE, an exception must be raised in
			 * the event of an attempt to update the same row twice.
			 */
			xminDatum = slot_getsysattr(existing,
										MinTransactionIdAttributeNumber,
										&isnull);
			Assert(!isnull);
			xmin = DatumGetTransactionId(xminDatum);

			if (TransactionIdIsCurrentTransactionId(xmin))
				ereport(ERROR,
						(errcode(ERRCODE_CARDINALITY_VIOLATION),
				/* translator: %s is a SQL command name */
						 errmsg("%s command cannot affect row a second time",
								"ON CONFLICT DO UPDATE"),
						 errhint("Ensure that no rows proposed for insertion within the same command have duplicate constrained values.")));

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
		ExecClearTuple(existing);	/* see return below */
		InstrCountFiltered1(&mtstate->ps, 1);
		return true;			/* done with the tuple */
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
		ExecWithCheckOptions(WCO_RLS_CONFLICT_CHECK, resultRelInfo,
							 existing,
							 mtstate->ps.state);
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
	*returning = ExecUpdate(context, resultRelInfo,
							conflictTid, NULL,
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


static void fireASTriggers(ModifyTableState *node);
static void fireBSTriggers(ModifyTableState *node);

static ChunkDispatchState *
get_chunk_dispatch_state(PlanState *substate)
{
	switch (nodeTag(substate))
	{
		case T_CustomScanState:
		{
			if (ts_is_chunk_dispatch_state(substate))
				return (ChunkDispatchState *) substate;
			break;
		}
		case T_ResultState:
			return get_chunk_dispatch_state(castNode(ResultState, substate)->ps.lefttree);
		default:
			break;
	}

	return NULL;
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
TupleTableSlot *
ExecModifyTable(CustomScanState *cs_node, PlanState *pstate)
{
	ModifyHypertableState *ht_state = (ModifyHypertableState *) cs_node;
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

	if (operation == CMD_INSERT || operation == CMD_MERGE)
	{
		cds = get_chunk_dispatch_state(subplanstate);
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
		/* Modify snapshot only if something got decompressed */
		if (ts_cm_functions->decompress_target_segments &&
			ts_cm_functions->decompress_target_segments(ht_state))
		{
			ht_state->comp_chunks_processed = true;
			/*
			 * save snapshot set during ExecutorStart(), since this is the same
			 * snapshot used to SeqScan of uncompressed chunks
			 */
			ht_state->snapshot = estate->es_snapshot;

			CommandCounterIncrement();
			/* use a static copy of current transaction snapshot
			 * this needs to be a copy so we don't read trigger updates
			 */
			estate->es_snapshot = RegisterSnapshot(GetTransactionSnapshot());
			/* mark rows visible */
			estate->es_output_cid = GetCurrentCommandId(true);

			if (ts_guc_max_tuples_decompressed_per_dml > 0)
			{
				if (ht_state->tuples_decompressed > ts_guc_max_tuples_decompressed_per_dml)
				{
					ereport(ERROR,
							(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
							 errmsg("tuple decompression limit exceeded by operation"),
							 errdetail("current limit: %d, tuples decompressed: %lld",
									   ts_guc_max_tuples_decompressed_per_dml,
									   (long long int) ht_state->tuples_decompressed),
							 errhint("Consider increasing "
									 "timescaledb.max_tuples_decompressed_per_dml_transaction or "
									 "set to 0 (unlimited).")));
				}
			}
		}
		/* Account for tuples deleted via batch DELETE in compressed chunks */
		if (operation == CMD_DELETE && ht_state->tuples_deleted > 0)
			estate->es_processed += ht_state->tuples_deleted;
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

#if PG17_GE
		/*
		 * If there is a pending MERGE ... WHEN NOT MATCHED [BY TARGET] action
		 * to execute, do so now --- see the comments in ExecMerge().
		 */
		if (node->mt_merge_pending_not_matched != NULL)
		{
			context.planSlot = node->mt_merge_pending_not_matched;

			slot = ExecMergeNotMatched(&context, node->resultRelInfo, cds, node->canSetTag);

			/* Clear the pending action */
			node->mt_merge_pending_not_matched = NULL;

			/*
			 * If we got a RETURNING result, return it to the caller.  We'll
			 * continue the work on next call.
			 */
			if (slot)
				return slot;

			continue; /* continue with the next tuple */
		}
#endif

		context.planSlot = ExecProcNode(subplanstate);

		if (cds && cds->rri && operation == CMD_INSERT && cds->cis->skip_current_tuple)
		{
			cds->cis->skip_current_tuple = false;
			if (node->ps.instrument)
				node->ps.instrument->ntuples2++;
			continue;
		}

		/* No more tuples to process? */
		if (TupIsNull(context.planSlot))
			break;

		/*
		 * copy INSERT merge action list to result relation info of corresponding chunk
		 *
		 * XXX do we need an additional support of NOT MATCHED BY SOURCE
		 * for PG >= 17? See PostgreSQL commit 0294df2f1f84
		 */
		if (cds && cds->rri && operation == CMD_MERGE)
#if PG17_GE
			cds->rri->ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET] =
				resultRelInfo->ri_MergeActions[MERGE_WHEN_NOT_MATCHED_BY_TARGET];
#else
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
				if (operation == CMD_MERGE)
				{
					EvalPlanQualSetSlot(&node->mt_epqstate, context.planSlot);
					slot =
						ExecMerge(&context, node->resultRelInfo, cds, NULL, NULL, node->canSetTag);
					if (slot)
						return slot;
					continue;
				}
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
		if (operation == CMD_UPDATE || operation == CMD_DELETE || operation == CMD_MERGE)
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

				/*
				 * For commands other than MERGE, any tuples having a null row
				 * identifier are errors.  For MERGE, we may need to handle
				 * them as WHEN NOT MATCHED clauses if any, so do that.
				 *
				 * Note that we use the node's toplevel resultRelInfo, not any
				 * specific partition's.
				 */
				if (isNull)
				{
					if (operation == CMD_MERGE)
					{
						EvalPlanQualSetSlot(&node->mt_epqstate, context.planSlot);
						slot = ExecMerge(&context,
										 node->resultRelInfo,
										 cds,
										 NULL,
										 NULL,
										 node->canSetTag);
						if (slot)
							return slot;
						continue;
					}
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
#if PG17_GE
				if (isNull)
				{
					if (operation == CMD_MERGE)
					{
						EvalPlanQualSetSlot(&node->mt_epqstate, context.planSlot);
						slot = ExecMerge(&context,
										 node->resultRelInfo,
										 cds,
										 NULL,
										 NULL,
										 node->canSetTag);
						if (slot)
							return slot;
						continue;
					}
					elog(ERROR, "wholerow is NULL");
				}
#else
				/* shouldn't ever get a null result... */
				if (isNull)
					elog(ERROR, "wholerow is NULL");
#endif

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
				slot = ExecInsert(&context, resultRelInfo, cds, slot, node->canSetTag);
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
				slot = ExecGetUpdateNewTuple(resultRelInfo, context.planSlot, oldSlot);
				context.relaction = NULL;
				/* Now apply the update. */
				slot =
					ExecUpdate(&context, resultRelInfo, tupleid, oldtuple, slot, node->canSetTag);
				break;
			case CMD_DELETE:
				slot = ExecDelete(&context, resultRelInfo, tupleid, oldtuple,
								  true, false, node->canSetTag, NULL, NULL, NULL);
				break;
			case CMD_MERGE:
				slot = ExecMerge(&context, resultRelInfo, cds, tupleid, oldtuple, node->canSetTag);
				break;
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
		UnregisterSnapshot(estate->es_snapshot);
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
		case CMD_MERGE:
			if (node->mt_merge_subcommands & MERGE_INSERT)
				ExecBSInsertTriggers(node->ps.state, resultRelInfo);
			if (node->mt_merge_subcommands & MERGE_UPDATE)
				ExecBSUpdateTriggers(node->ps.state, resultRelInfo);
			if (node->mt_merge_subcommands & MERGE_DELETE)
				ExecBSDeleteTriggers(node->ps.state, resultRelInfo);
			break;
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
		case CMD_MERGE:
			if (node->mt_merge_subcommands & MERGE_INSERT)
				ExecASInsertTriggers(node->ps.state, resultRelInfo, node->mt_transition_capture);
			if (node->mt_merge_subcommands & MERGE_UPDATE)
				ExecASUpdateTriggers(node->ps.state, resultRelInfo, node->mt_transition_capture);
			if (node->mt_merge_subcommands & MERGE_DELETE)
				ExecASDeleteTriggers(node->ps.state, resultRelInfo, node->mt_transition_capture);
			break;
		default:
			elog(ERROR, "unknown operation");
			break;
	}
}


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

TupleTableSlot *
ExecMergeMatched(ModifyTableContext *context, ResultRelInfo *resultRelInfo, ItemPointer tupleid,
				 HeapTuple oldtuple, bool canSetTag, bool *matched)
{
	ModifyTableState *mtstate = context->mtstate;
	TupleTableSlot *newslot = NULL;
	EState *estate = context->estate;
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	bool isNull;
	EPQState *epqstate = &mtstate->mt_epqstate;
	ListCell *l;

	TupleTableSlot *rslot = NULL;

	Assert(*matched == true);

	/*
	 * If there are no WHEN MATCHED actions, we are done.
	 */
#if PG17_GE
	if (resultRelInfo->ri_MergeActions[MERGE_WHEN_MATCHED] == NIL)
		return NULL;
#else
	if (resultRelInfo->ri_matchedMergeAction == NIL)
	{
		*matched = true;
		return NULL;
	}
#endif
	/*
	 * Make tuple and any needed join variables available to ExecQual and
	 * ExecProject. The target's existing tuple is installed in the
	 * scantuple. Again, this target relation's slot is required only in
	 * the case of a MATCHED tuple and UPDATE/DELETE actions.
	 */
	econtext->ecxt_scantuple = resultRelInfo->ri_oldTupleSlot;
	econtext->ecxt_innertuple = context->planSlot;
	econtext->ecxt_outertuple = NULL;

lmerge_matched:;

/*
 * This routine is only invoked for matched rows, and we must have
 * found the tupleid of the target row in that case; fetch that
 * tuple.
 *
 * We use SnapshotAny for this because we might get called again
 * after EvalPlanQual returns us a new tuple, which may not be
 * visible to our MVCC snapshot.
 */

#if PG17_GE
	if (oldtuple != NULL)
		ExecForceStoreHeapTuple(oldtuple, resultRelInfo->ri_oldTupleSlot, false);
	else
#endif
		if (!table_tuple_fetch_row_version(resultRelInfo->ri_RelationDesc,
										   tupleid,
										   SnapshotAny,
										   resultRelInfo->ri_oldTupleSlot))
		elog(ERROR, "failed to fetch the target tuple");
#if PG17_GE
	foreach (l, resultRelInfo->ri_MergeActions[MERGE_WHEN_MATCHED])
	{
#else
	foreach (l, resultRelInfo->ri_matchedMergeAction)
	{
#endif
		MergeActionState *relaction = (MergeActionState *) lfirst(l);
		CmdType commandType = relaction->mas_action->commandType;
		TM_Result result;
		UpdateContext updateCxt = { 0 };

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
		 * The WITH CHECK quals are applied in ExecUpdate() and hence
		 * we need not do anything special to handle them.
		 *
		 * NOTE: We must do this after WHEN quals are evaluated, so
		 * that we check policies only when they matter.
		 */
		if (resultRelInfo->ri_WithCheckOptions)
		{
			ExecWithCheckOptions(commandType == CMD_UPDATE ? WCO_RLS_MERGE_UPDATE_CHECK :
															 WCO_RLS_MERGE_DELETE_CHECK,
								 resultRelInfo,
								 resultRelInfo->ri_oldTupleSlot,
								 context->mtstate->ps.state);
		}

		/* Perform stated action */
		switch (commandType)
		{
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
#endif
				context->cpUpdateRetrySlot = NULL;

				if (!ExecUpdatePrologue(context, resultRelInfo, tupleid, NULL, newslot, &result))
				{
#if PG16_LT
					result = TM_Ok;
#else
					if (result == TM_Ok)
						return NULL;
#endif

					/* if not TM_OK, it is concurrent update/delete */
					break;
				}
				ExecUpdatePrepareSlot(resultRelInfo, newslot, context->estate);
				result = ExecUpdateAct(context,
										  resultRelInfo,
										  tupleid,
										  NULL,
										  newslot,
										  mtstate->canSetTag,
										  &updateCxt);
				if (result == TM_Ok)
				{
					ExecUpdateEpilogue(context,
									   &updateCxt,
									   resultRelInfo,
									   tupleid,
									   NULL,
									   newslot);
					mtstate->mt_merge_updated += 1;
				}

				break;

			case CMD_DELETE:
#if PG17_GE
				mtstate->mt_merge_action = relaction;
#else
				context->relaction = relaction;
#endif
				if (!ExecDeletePrologue(context, resultRelInfo, tupleid, NULL, NULL, &result))
				{
#if PG16_LT
					result = TM_Ok;
#else
					if (result == TM_Ok)
						return NULL; /* "do nothing" */

						/* if not TM_OK, it is concurrent update/delete */
#endif
					break;
				}
				result = ExecDeleteAct(context, resultRelInfo, tupleid, false);
				if (result == TM_Ok)
				{
					ExecDeleteEpilogue(context, resultRelInfo, tupleid, NULL, false);
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

		switch (result)
		{
			case TM_Ok:
				/* all good; perform final actions */
				if (canSetTag)
					(estate->es_processed)++;

				break;

			case TM_SelfModified:
				if (context->tmfd.cmax != estate->es_output_cid)
					ereport(ERROR,
							(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
							 errmsg("tuple to be updated or deleted was already modified by an "
									"operation triggered by the current command"),
							 errhint("Consider using an AFTER trigger instead of a BEFORE trigger "
									 "to propagate changes to other rows.")));

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
				Relation resultRelationDesc;
				TupleTableSlot *epqslot, *inputslot;
				LockTupleMode lockmode;

				/*
				 * The target tuple was concurrently updated
				 * by some other transaction.
				 */

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
				resultRelationDesc = resultRelInfo->ri_RelationDesc;
				lockmode = ExecUpdateLockMode(estate, resultRelInfo);

				inputslot = EvalPlanQualSlot(epqstate,
											 resultRelationDesc,
											 resultRelInfo->ri_RangeTableIndex);

				result = table_tuple_lock(resultRelationDesc,
										  tupleid,
										  estate->es_snapshot,
										  inputslot,
										  estate->es_output_cid,
										  lockmode,
										  LockWaitBlock,
										  TUPLE_LOCK_FLAG_FIND_LAST_VERSION,
										  &context->tmfd);
				switch (result)
				{
					case TM_Ok:
						// TODO: update this to match PG17
						epqslot = EvalPlanQual(epqstate,
											   resultRelationDesc,
											   resultRelInfo->ri_RangeTableIndex,
											   inputslot);

						/*
						 * If we got no tuple, or the tuple
						 * we get has a NULL ctid, go back to
						 * caller: this one is not a MATCHED
						 * tuple anymore, so they can retry
						 * with NOT MATCHED actions.
						 */
						if (TupIsNull(epqslot))
							return false;

						(void) ExecGetJunkAttribute(epqslot, resultRelInfo->ri_RowIdAttNo, &isNull);
						if (isNull)
							return false;

						/*
						 * When a tuple was updated and
						 * migrated to another partition
						 * concurrently, the current MERGE
						 * implementation can't follow.
						 * There's probably a better way to
						 * handle this case, but it'd require
						 * recognizing the relation to which
						 * the tuple moved, and setting our
						 * current resultRelInfo to that.
						 */
						if (ItemPointerIndicatesMovedPartitions(&context->tmfd.ctid))
							ereport(ERROR,
									(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
									 errmsg("tuple to be deleted was already moved to another "
											"partition due to concurrent update")));

						/*
						 * A non-NULL ctid means that we are
						 * still dealing with MATCHED case.
						 * Restart the loop so that we apply
						 * all the MATCHED rules again, to
						 * ensure that the first qualifying
						 * WHEN MATCHED action is executed.
						 *
						 * Update tupleid to that of the new
						 * tuple, for the refetch we do at
						 * the top.
						 */
						Ensure(tupleid != NULL, "matched tupleid during merge cannot be null");
						ItemPointerCopy(&context->tmfd.ctid, tupleid);
						goto lmerge_matched;

					case TM_Deleted:

						/*
						 * tuple already deleted; tell caller
						 * to run NOT MATCHED actions
						 */
						*matched = false;
						return NULL;

					case TM_SelfModified:

						/*
						 * This can be reached when following
						 * an update chain from a tuple
						 * updated by another session,
						 * reaching a tuple that was already
						 * updated in this transaction. If
						 * previously modified by this
						 * command, ignore the redundant
						 * update, otherwise error out.
						 *
						 * See also response to
						 * TM_SelfModified in
						 * ht_ExecUpdate().
						 */
						if (context->tmfd.cmax != estate->es_output_cid)
							ereport(ERROR,
									(errcode(ERRCODE_TRIGGERED_DATA_CHANGE_VIOLATION),
									 errmsg("tuple to be updated or deleted was already modified "
											"by an operation triggered by the current command"),
									 errhint("Consider using an AFTER trigger instead of a BEFORE "
											 "trigger to propagate changes to other rows.")));
						if (TransactionIdIsCurrentTransactionId(context->tmfd.xmax))
							ereport(ERROR,
									(errcode(ERRCODE_CARDINALITY_VIOLATION),
									 /* translator: %s is a SQL command name */
									 errmsg("%s command cannot affect row a second time", "MERGE"),
									 errhint("Ensure that not more than one source row matches any "
											 "one target row.")));

						/* This shouldn't happen */
						elog(ERROR, "attempted to update or delete invisible tuple");
						return NULL;

					default:
						/*
						 * see table_tuple_lock call in
						 * ht_ExecDelete()
						 */
						elog(ERROR, "unexpected table_tuple_lock status: %u", result);
						return NULL;
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
					rslot = ExecProcessReturning(resultRelInfo, newslot, context->planSlot);
					break;

				case CMD_DELETE:
					rslot = ExecProcessReturning(resultRelInfo,
												 resultRelInfo->ri_oldTupleSlot,
												 context->planSlot);
					break;

				case CMD_NOTHING:
					break;

				default:
					elog(ERROR, "unrecognized commandType: %d", (int) commandType);
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
	return rslot;
}

/*
 * Execute the first qualifying NOT MATCHED action.
 */
static TupleTableSlot *
ExecMergeNotMatched(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
					ChunkDispatchState *cds, bool canSetTag)
{
	ModifyTableState *mtstate = context->mtstate;
	ExprContext *econtext = mtstate->ps.ps_ExprContext;
	List *actionStates = NIL;
	ListCell *l;
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

	foreach (l, actionStates)
	{
		MergeActionState *action = (MergeActionState *) lfirst(l);
		CmdType commandType = action->mas_action->commandType;
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
		switch (commandType)
		{
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
					rslot = ExecInsert(context,
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
ExecMerge(ModifyTableContext *context, ResultRelInfo *resultRelInfo, ChunkDispatchState *cds,
		  ItemPointer tupleid, HeapTuple oldtuple, bool canSetTag)
{
	bool matched;
	TupleTableSlot *rslot = NULL;

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
	 * ExecMergeMatched takes care of following the update chain and
	 * re-finding the qualifying WHEN MATCHED action, as long as the updated
	 * target tuple still satisfies the join quals, i.e., it remains a WHEN
	 * MATCHED case. If the tuple gets deleted or the join quals fail, it
	 * returns and we try ExecMergeNotMatched. Given that ExecMergeMatched
	 * always make progress by following the update chain and we never switch
	 * from ExecMergeNotMatched to ExecMergeMatched, there is no risk of a
	 * livelock.
	 */
#if PG17_GE
	matched = tupleid != NULL || oldtuple != NULL;
#else
	matched = tupleid != NULL;
#endif
	if (matched)
		rslot = ExecMergeMatched(context, resultRelInfo, tupleid, oldtuple, canSetTag, &matched);

	/*
	 * Either we were dealing with a NOT MATCHED tuple or
	 * ExecMergeMatched() returned "false", indicating the previously
	 * MATCHED tuple no longer matches.
	 */
	if (!matched)
	{
#if PG17_GE
		if (rslot == NULL)
			rslot = ExecMergeNotMatched(context, resultRelInfo, cds, canSetTag);
		else
			context->mtstate->mt_merge_pending_not_matched = context->planSlot;
#else
		(void) ExecMergeNotMatched(context, resultRelInfo, cds, canSetTag);
#endif
	}

	return rslot;
}
