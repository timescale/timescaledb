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

#include "compat/compat.h"
#include "nodes/chunk_dispatch/chunk_dispatch.h"

#if PG14_GE
/* clang-format off */
/*
 * Context struct for a ModifyTable operation, containing basic execution
 * state and some output variables populated by ExecUpdateAct() and
 * ExecDeleteAct() to report the result of their actions to callers.
 */
typedef struct ModifyTableContext {
	/* Operation state */
	ModifyTableState *mtstate;
	EPQState       *epqstate;
	EState	       *estate;

	/*
	 * Slot containing tuple obtained from ModifyTable's subplan.  Used
	 * to access "junk" columns that are not going to be stored.
	 */
	TupleTableSlot *planSlot;

	/*
	 * During EvalPlanQual, project and return the new version of the new
	 * tuple
	 */
#if PG15_GE
	TupleTableSlot *(*GetUpdateNewTuple) (ResultRelInfo * resultRelInfo, TupleTableSlot * epqslot,
		    TupleTableSlot * oldSlot, MergeActionState * relaction);

	/* MERGE specific */
	MergeActionState *relaction;	/* MERGE action in progress */
#endif
	/*
	 * Information about the changes that were made concurrently to a
	 * tuple being updated or deleted
	 */
	TM_FailureData	tmfd;

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
	LockTupleMode	lockmode;
}		ModifyTableContext;

/*
 * Context struct containing output data specific to UPDATE operations.
 */
typedef struct UpdateContext
{
	bool updated; /* did UPDATE actually occur? */
#if PG16_LT
	bool updateIndexes; /* index update required? */
#else
	TU_UpdateIndexes updateIndexes; /* result codes */
#endif

	bool crossPartUpdate; /* was it a cross-partition
						   * update? */
} UpdateContext;

bool		ht_ExecUpdatePrologue(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
	    ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot * slot, TM_Result *result);
void		ht_ExecUpdatePrepareSlot(ResultRelInfo * resultRelInfo, TupleTableSlot * slot, EState * estate);
TM_Result	ht_ExecUpdateAct(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
	     ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot * slot,
				 bool canSetTag, UpdateContext * updateCxt);
void		ht_ExecUpdateEpilogue(ModifyTableContext * context, UpdateContext * updateCxt,
     ResultRelInfo * resultRelInfo, ItemPointer tupleid, HeapTuple oldtuple,
			      TupleTableSlot * slot, List * recheckIndexes);

bool		ht_ExecDeletePrologue(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
  ItemPointer tupleid, HeapTuple oldtuple, TupleTableSlot * *epqreturnslot, TM_Result *result);
TM_Result	ht_ExecDeleteAct(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
				 ItemPointer tupleid, bool changingPart);
void		ht_ExecDeleteEpilogue(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
				   ItemPointer tupleid, HeapTuple oldtuple);

#endif

#if PG15_GE
/* MERGE specific */
TupleTableSlot *ht_ExecMerge(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
	     ChunkDispatchState * cds, ItemPointer tupleid, bool canSetTag);
bool		ht_ExecMergeMatched(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
				    ItemPointer tupleid, bool canSetTag);
void		ht_ExecMergeNotMatched(ModifyTableContext * context, ResultRelInfo * resultRelInfo,
				  ChunkDispatchState * cds, bool canSetTag);
#endif
