#include <postgres.h>

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <access/heapam.h>
#include <access/sysattr.h>
#include <access/xact.h>
#include <access/hio.h>
#include <commands/copy.h>
#include <commands/trigger.h>
#include <executor/executor.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <storage/bufmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/rls.h>

#include "hypertable.h"
#include "copy.h"
#include "dimension.h"
#include "chunk_insert_state.h"
#include "chunk_dispatch.h"
#include "subspace_store.h"
#include "compat.h"

/*
 * Copy from a file to a hypertable.
 *
 * Unfortunately, there aren't any good hooks in the regular COPY code to insert
 * our chunk dispatching. so most of this code is a straight-up copy of the
 * regular PostgreSQL source code for the COPY command (command/copy.c), albeit
 * with minor modifications.
 *
 */

typedef struct CopyChunkState
{
	EState	   *estate;
	ChunkDispatch *dispatch;
	CopyState	cstate;
} CopyChunkState;

static CopyChunkState *
copy_chunk_state_create(Hypertable *ht, Relation rel, CopyState cstate)
{
	CopyChunkState *ccstate;
	EState	   *estate = CreateExecutorState();

	ccstate = palloc(sizeof(CopyChunkState));
	ccstate->estate = estate;
	ccstate->dispatch = chunk_dispatch_create(ht, estate, NULL);
	ccstate->cstate = cstate;

	return ccstate;
}

static void
copy_chunk_state_destroy(CopyChunkState *ccstate)
{
	chunk_dispatch_destroy(ccstate->dispatch);
	FreeExecutorState(ccstate->estate);
}

/*
 * Copy FROM file to relation.
 */
static uint64
timescaledb_CopyFrom(CopyState cstate, Relation main_rel, List *range_table, Hypertable *ht)
{
	HeapTuple	tuple;
	TupleDesc	tupDesc;
	Datum	   *values;
	bool	   *nulls;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *saved_resultRelInfo = NULL;
	CopyChunkState *ccstate = copy_chunk_state_create(ht, main_rel, cstate);
	EState	   *estate = ccstate->estate;		/* for ExecConstraints() */
	ExprContext *econtext;
	TupleTableSlot *myslot;
	MemoryContext oldcontext = CurrentMemoryContext;
	ChunkInsertState *prev_cis = NULL;

	ErrorContextCallback errcallback;
	CommandId	mycid = GetCurrentCommandId(true);
	int			hi_options = 0; /* start with default heap_insert options */
	BulkInsertState bistate;
	uint64		processed = 0;

	if (main_rel->rd_rel->relkind != RELKIND_RELATION)
	{
		if (main_rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to view \"%s\"",
							RelationGetRelationName(main_rel))));
		else if (main_rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to materialized view \"%s\"",
							RelationGetRelationName(main_rel))));
		else if (main_rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to foreign table \"%s\"",
							RelationGetRelationName(main_rel))));
		else if (main_rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to sequence \"%s\"",
							RelationGetRelationName(main_rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to non-table relation \"%s\"",
							RelationGetRelationName(main_rel))));
	}

	tupDesc = RelationGetDescr(main_rel);

	/*----------
	 * Check to see if we can avoid writing WAL
	 *
	 * If archive logging/streaming is not enabled *and* either
	 *	- table was created in same transaction as this COPY
	 *	- data is being written to relfilenode created in this transaction
	 * then we can skip writing WAL.  It's safe because if the transaction
	 * doesn't commit, we'll discard the table (or the new relfilenode file).
	 * If it does commit, we'll have done the heap_sync at the bottom of this
	 * routine first.
	 *
	 * As mentioned in comments in utils/rel.h, the in-same-transaction test
	 * is not always set correctly, since in rare cases rd_newRelfilenodeSubid
	 * can be cleared before the end of the transaction. The exact case is
	 * when a relation sets a new relfilenode twice in same transaction, yet
	 * the second one fails in an aborted subtransaction, e.g.
	 *
	 * BEGIN;
	 * TRUNCATE t;
	 * SAVEPOINT save;
	 * TRUNCATE t;
	 * ROLLBACK TO save;
	 * COPY ...
	 *
	 * Also, if the target file is new-in-transaction, we assume that checking
	 * FSM for free space is a waste of time, even if we must use WAL because
	 * of archiving.  This could possibly be wrong, but it's unlikely.
	 *
	 * The comments for heap_insert and RelationGetBufferForTuple specify that
	 * skipping WAL logging is only safe if we ensure that our tuples do not
	 * go into pages containing tuples from any other transactions --- but this
	 * must be the case if we have a new table or new relfilenode, so we need
	 * no additional work to enforce that.
	 *----------
	 */
	/* createSubid is creation check, newRelfilenodeSubid is truncation check */
	if (main_rel->rd_createSubid != InvalidSubTransactionId ||
		main_rel->rd_newRelfilenodeSubid != InvalidSubTransactionId)
	{
		hi_options |= HEAP_INSERT_SKIP_FSM;
		if (!XLogIsNeeded())
			hi_options |= HEAP_INSERT_SKIP_WAL;
	}

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfoCompat(resultRelInfo,
							main_rel,
							1,	/* dummy rangetable index */
							0);

	ExecOpenIndices(resultRelInfo, false);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
	estate->es_range_table = range_table;

	/* Set up a tuple slot too */
	myslot = ExecInitExtraTupleSlot(estate);
	ExecSetSlotDescriptor(myslot, tupDesc);
	/* Triggers might need a slot as well */
	estate->es_trig_tuple_slot = ExecInitExtraTupleSlot(estate);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	values = (Datum *) palloc(tupDesc->natts * sizeof(Datum));
	nulls = (bool *) palloc(tupDesc->natts * sizeof(bool));

	bistate = GetBulkInsertState();
	econtext = GetPerTupleExprContext(estate);

	/* Set up callback to identify error line number */
	errcallback.callback = CopyFromErrorCallback;
	errcallback.arg = (void *) ccstate->cstate;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	for (;;)
	{
		TupleTableSlot *slot;
		bool		skip_tuple;
		Oid			loaded_oid = InvalidOid;
		Point	   *point;
		ChunkDispatch *dispatch = ccstate->dispatch;
		ChunkInsertState *cis;

		CHECK_FOR_INTERRUPTS();

		/* Reset the per-tuple exprcontext */
		ResetPerTupleExprContext(estate);

		/* Switch into its memory context */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		if (!NextCopyFrom(ccstate->cstate, econtext, values, nulls, &loaded_oid))
			break;

		/* And now we can form the input tuple. */
		tuple = heap_form_tuple(tupDesc, values, nulls);

		if (loaded_oid != InvalidOid)
			HeapTupleSetOid(tuple, loaded_oid);

		/* Calculate the tuple's point in the N-dimensional hyperspace */
		point = hyperspace_calculate_point(ht->space, tuple, tupDesc);

		/* Save the main table's (hypertable's) ResultRelInfo */
		if (NULL == dispatch->hypertable_result_rel_info)
			dispatch->hypertable_result_rel_info = estate->es_result_relation_info;

		/* Find or create the insert state matching the point */
		cis = chunk_dispatch_get_chunk_insert_state(dispatch, point, CMD_INSERT);

		Assert(cis != NULL);

		if (cis != prev_cis)
		{
			/* Different chunk so must release BulkInsertState */
			if (bistate->current_buf != InvalidBuffer)
				ReleaseBuffer(bistate->current_buf);
			bistate->current_buf = InvalidBuffer;
		}

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(oldcontext);

		/* Place tuple in tuple slot --- but slot shouldn't free it */
		slot = myslot;
		ExecStoreTuple(tuple, slot, InvalidBuffer, false);

		/* Convert the tuple to match the chunk's rowtype */
		tuple = chunk_insert_state_convert_tuple(cis, tuple, &slot);

		/*
		 * Set the result relation in the executor state to the target chunk.
		 * This makes sure that the tuple gets inserted into the correct
		 * chunk.
		 */
		saved_resultRelInfo = resultRelInfo;
		resultRelInfo = cis->result_relation_info;
		estate->es_result_relation_info = resultRelInfo;
		prev_cis = cis;

		/*
		 * Constraints might reference the tableoid column, so initialize
		 * t_tableOid before evaluating them.
		 */
		tuple->t_tableOid = RelationGetRelid(resultRelInfo->ri_RelationDesc);

		skip_tuple = false;

		/* BEFORE ROW INSERT Triggers */
		if (resultRelInfo->ri_TrigDesc &&
			resultRelInfo->ri_TrigDesc->trig_insert_before_row)
		{
			slot = ExecBRInsertTriggers(estate, resultRelInfo, slot);

			if (slot == NULL)	/* "do nothing" */
				skip_tuple = true;
			else	/* trigger might have changed tuple */
				tuple = ExecMaterializeSlot(slot);
		}

		if (!skip_tuple)
		{
			/* Check the constraints of the tuple */
			if (main_rel->rd_att->constr)
				ExecConstraints(resultRelInfo, slot, estate);

			{
				List	   *recheckIndexes = NIL;

				/* OK, store the tuple and create index entries for it */
				heap_insert(resultRelInfo->ri_RelationDesc, tuple, mycid,
							hi_options, bistate);

				if (resultRelInfo->ri_NumIndices > 0)
					recheckIndexes = ExecInsertIndexTuples(slot, &(tuple->t_self),
														 estate, false, NULL,
														   NIL);

				/* AFTER ROW INSERT Triggers */
				ExecARInsertTriggersCompat(estate, resultRelInfo, tuple, recheckIndexes);

				list_free(recheckIndexes);
			}

			/*
			 * We count only tuples not suppressed by a BEFORE INSERT trigger;
			 * this is the same definition used by execMain.c for counting
			 * tuples inserted by an INSERT command.
			 */
			processed++;

			if (saved_resultRelInfo)
			{
				resultRelInfo = saved_resultRelInfo;
				estate->es_result_relation_info = resultRelInfo;
			}
		}
	}
	/* Done, clean up */
	error_context_stack = errcallback.previous;

	FreeBulkInsertState(bistate);

	MemoryContextSwitchTo(oldcontext);

	/*
	 * if (cstate->copy_dest == COPY_OLD_FE) pq_endmsgread();
	 */
	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggersCompat(estate, resultRelInfo);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	pfree(values);
	pfree(nulls);

	ExecResetTupleTable(estate->es_tupleTable, false);

	ExecCloseIndices(resultRelInfo);
	copy_chunk_state_destroy(ccstate);

	/*
	 * If we skipped writing WAL, then we need to sync the heap (but not
	 * indexes since those use WAL anyway)
	 */
	if (hi_options & HEAP_INSERT_SKIP_WAL)
		heap_sync(main_rel);

	return processed;
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * rel can be NULL ... it's only used for error reports.
 */
static List *
timescaledb_CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		Form_pg_attribute *attr = tupDesc->attrs;
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (attr[i]->attisdropped)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				if (tupDesc->attrs[i]->attisdropped)
					continue;
				if (namestrcmp(&(tupDesc->attrs[i]->attname), name) == 0)
				{
					attnum = tupDesc->attrs[i]->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
					errmsg("column \"%s\" of relation \"%s\" does not exist",
						   name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}

Oid
timescaledb_DoCopy(const CopyStmt *stmt, const char *queryString, uint64 *processed, Hypertable *ht)
{
	CopyState	cstate;
	bool		is_from = stmt->is_from;
	bool		pipe = (stmt->filename == NULL);
	Relation	rel;
	Oid			relid;
	List	   *range_table = NIL;

	/* Disallow COPY to/from file or program except to superusers. */
	if (!pipe && !superuser())
	{
		if (stmt->is_program)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from an external program"),
					 errhint("Anyone can COPY to stdout or from stdin. "
						   "psql's \\copy command also works for anyone.")));
		else
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("must be superuser to COPY to or from a file"),
					 errhint("Anyone can COPY to stdout or from stdin. "
						   "psql's \\copy command also works for anyone.")));
	}

	if (!is_from || NULL == stmt->relation)
	{
		elog(ERROR, "timescale DoCopy should only be called for COPY FROM");
	}

	TupleDesc	tupDesc;
	AclMode		required_access = (is_from ? ACL_INSERT : ACL_SELECT);
	List	   *attnums;
	ListCell   *cur;
	RangeTblEntry *rte;

	Assert(!stmt->query);

	/* Open and lock the relation, using the appropriate lock type. */
	rel = heap_openrv(stmt->relation,
					  (is_from ? RowExclusiveLock : AccessShareLock));

	relid = RelationGetRelid(rel);

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->requiredPerms = required_access;
	range_table = list_make1(rte);

	tupDesc = RelationGetDescr(rel);
	attnums = timescaledb_CopyGetAttnums(tupDesc, rel, stmt->attlist);
	foreach(cur, attnums)
	{
		int			attno = lfirst_int(cur) -
		FirstLowInvalidHeapAttributeNumber;

		if (is_from)
			rte->insertedCols = bms_add_member(rte->insertedCols, attno);
		else
			rte->selectedCols = bms_add_member(rte->selectedCols, attno);
	}
	ExecCheckRTPerms(range_table, true);

	/*
	 * Permission check for row security policies.
	 *
	 * check_enable_rls will ereport(ERROR) if the user has requested
	 * something invalid and will otherwise indicate if we should enable RLS
	 * (returns RLS_ENABLED) or not for this COPY statement.
	 *
	 * If the relation has a row security policy and we are to apply it then
	 * perform a "query" copy and allow the normal query processing to handle
	 * the policies.
	 *
	 * If RLS is not enabled for this, then just fall through to the normal
	 * non-filtering relation handling.
	 */
	if (check_enable_rls(rte->relid, InvalidOid, false) == RLS_ENABLED)
	{
		/* is_from is true here */
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY FROM not supported with row-level security"),
				 errhint("Use INSERT statements instead.")));
	}

	Assert(rel);

	/* check read-only transaction and parallel mode */
	if (XactReadOnly && !rel->rd_islocaltemp)
		PreventCommandIfReadOnly("COPY FROM");
	PreventCommandIfParallelMode("COPY FROM");

#if PG10
	{
		ParseState *pstate = make_parsestate(NULL);

		pstate->p_sourcetext = queryString;

		cstate = BeginCopyFrom(pstate, rel, stmt->filename, stmt->is_program,
							   NULL, stmt->attlist, stmt->options);
		free_parsestate(pstate);
	}
#elif PG96
	cstate = BeginCopyFrom(rel, stmt->filename, stmt->is_program,
						   stmt->attlist, stmt->options);
#endif
	*processed = timescaledb_CopyFrom(cstate, rel, range_table, ht);	/* copy from file to
																		 * database */
	EndCopyFrom(cstate);

	/*
	 * Close the relation. If reading, we can release the AccessShareLock we
	 * got; if writing, we should hold the lock until end of transaction to
	 * ensure that updates will be committed before lock is released.
	 */
	heap_close(rel, (is_from ? NoLock : AccessShareLock));

	return relid;
}
