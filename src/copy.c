/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <access/heapam.h>
#include <access/sysattr.h>
#include <access/hio.h>
#include <access/xact.h>
#include <commands/copy.h>
#include <commands/trigger.h>
#include <commands/tablecmds.h>
#include <executor/executor.h>
#include <executor/nodeModifyTable.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <storage/bufmgr.h>
#include <utils/builtins.h>
#include <utils/guc.h>
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

typedef struct CopyChunkState CopyChunkState;

typedef bool (*CopyFromFunc)(CopyChunkState *ccstate, ExprContext *econtext, Datum *values,
							 bool *nulls);

typedef struct CopyChunkState
{
	Relation rel;
	EState *estate;
	ChunkDispatch *dispatch;
	CopyFromFunc next_copy_from;
	CopyState cstate;
	TableScanDesc scandesc;
} CopyChunkState;

static CopyChunkState *
copy_chunk_state_create(Hypertable *ht, Relation rel, CopyFromFunc from_func, CopyState cstate,
						TableScanDesc scandesc)
{
	CopyChunkState *ccstate;
	EState *estate = CreateExecutorState();

	ccstate = palloc(sizeof(CopyChunkState));
	ccstate->rel = rel;
	ccstate->estate = estate;
	ccstate->dispatch = ts_chunk_dispatch_create(ht, estate);
	ccstate->cstate = cstate;
	ccstate->scandesc = scandesc;
	ccstate->next_copy_from = from_func;

	return ccstate;
}

static void
copy_chunk_state_destroy(CopyChunkState *ccstate)
{
	ts_chunk_dispatch_destroy(ccstate->dispatch);
	FreeExecutorState(ccstate->estate);
}

static bool
next_copy_from(CopyChunkState *ccstate, ExprContext *econtext, Datum *values, bool *nulls)
{
	Assert(ccstate->cstate != NULL);
#if PG12_GE
	return NextCopyFrom(ccstate->cstate, econtext, values, nulls);
#else
	return NextCopyFrom(ccstate->cstate, econtext, values, nulls, NULL);
#endif
}

/*
 * Change to another chunk for inserts.
 *
 * Called every time we switch to another chunk for inserts.
 */
static void
on_chunk_insert_state_changed(ChunkInsertState *state, void *data)
{
	BulkInsertState bistate = data;

	/* Different chunk so must release BulkInsertState */
	if (bistate->current_buf != InvalidBuffer)
		ReleaseBuffer(bistate->current_buf);
	bistate->current_buf = InvalidBuffer;
}

/*
 * Copy FROM file to relation.
 */
static uint64
copyfrom(CopyChunkState *ccstate, List *range_table, Hypertable *ht)
{
	Datum *values;
	bool *nulls;
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *saved_resultRelInfo = NULL;
	EState *estate = ccstate->estate; /* for ExecConstraints() */
	ExprContext *econtext;
	TupleTableSlot *singleslot;
	MemoryContext oldcontext = CurrentMemoryContext;
	ErrorContextCallback errcallback = { 0 };
	CommandId mycid = GetCurrentCommandId(true);
	int ti_options = 0; /* start with default options for insert */
	BulkInsertState bistate;
	uint64 processed = 0;

	if (ccstate->rel->rd_rel->relkind != RELKIND_RELATION)
	{
		if (ccstate->rel->rd_rel->relkind == RELKIND_VIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to view \"%s\"", RelationGetRelationName(ccstate->rel))));
		else if (ccstate->rel->rd_rel->relkind == RELKIND_MATVIEW)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to materialized view \"%s\"",
							RelationGetRelationName(ccstate->rel))));
		else if (ccstate->rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to foreign table \"%s\"",
							RelationGetRelationName(ccstate->rel))));
		else if (ccstate->rel->rd_rel->relkind == RELKIND_SEQUENCE)
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to sequence \"%s\"",
							RelationGetRelationName(ccstate->rel))));
		else
			ereport(ERROR,
					(errcode(ERRCODE_WRONG_OBJECT_TYPE),
					 errmsg("cannot copy to non-table relation \"%s\"",
							RelationGetRelationName(ccstate->rel))));
	}

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
	if (ccstate->rel->rd_createSubid != InvalidSubTransactionId ||
		ccstate->rel->rd_newRelfilenodeSubid != InvalidSubTransactionId)
	{
		ti_options |= HEAP_INSERT_SKIP_FSM;
		if (!XLogIsNeeded())
			ti_options |= HEAP_INSERT_SKIP_WAL;
	}

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */
	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfoCompat(resultRelInfo,
							ccstate->rel,
							0, /* dummy rangetable index - original was 1
								* which isn't dummy-nuf */
							0);

	CheckValidResultRelCompat(resultRelInfo, CMD_INSERT);

	ExecOpenIndices(resultRelInfo, false);

	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
	estate->es_range_table = range_table;

#if PG12_GE
	ExecInitRangeTable(estate, estate->es_range_table);
#else

	/* Triggers might need a slot as well. In PG12, the trigger slots were
	 * moved to the ResultRelInfo and are lazily initialized during
	 * executor execution. */
	estate->es_trig_tuple_slot = ExecInitExtraTupleSlotCompat(estate, NULL, NULL);
#endif

	singleslot = table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	values = (Datum *) palloc(RelationGetDescr(ccstate->rel)->natts * sizeof(Datum));
	nulls = (bool *) palloc(RelationGetDescr(ccstate->rel)->natts * sizeof(bool));

	bistate = GetBulkInsertState();
	econtext = GetPerTupleExprContext(estate);

	/* Set up callback to identify error line number.
	 *
	 * It is not necessary to add an entry to the error context stack if we do
	 * not have a CopyState. In that case, we just use the existing error
	 * already on the context stack. */
	if (ccstate->cstate)
	{
		errcallback.callback = CopyFromErrorCallback;
		errcallback.arg = (void *) ccstate->cstate;
		errcallback.previous = error_context_stack;
		error_context_stack = &errcallback;
	}

	for (;;)
	{
		TupleTableSlot *myslot;
		bool skip_tuple;
		Point *point;
		ChunkDispatch *dispatch = ccstate->dispatch;
		ChunkInsertState *cis;

		CHECK_FOR_INTERRUPTS();

		/* Reset the per-tuple exprcontext */
		ResetPerTupleExprContext(estate);
		myslot = singleslot;

		/* Switch into its memory context */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		ExecClearTuple(myslot);

		if (!ccstate->next_copy_from(ccstate, econtext, myslot->tts_values, myslot->tts_isnull))
			break;

		ExecStoreVirtualTuple(myslot);

		/* Calculate the tuple's point in the N-dimensional hyperspace */
		point = ts_hyperspace_calculate_point(ht->space, myslot);

		/* Save the main table's (hypertable's) ResultRelInfo */
		if (NULL == dispatch->hypertable_result_rel_info)
			dispatch->hypertable_result_rel_info = estate->es_result_relation_info;

		/* Find or create the insert state matching the point */
		cis = ts_chunk_dispatch_get_chunk_insert_state(dispatch,
													   point,
													   on_chunk_insert_state_changed,
													   bistate);

		Assert(cis != NULL);

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(oldcontext);

		/* Convert the tuple to match the chunk's rowtype */
		if (NULL != cis->hyper_to_chunk_map)
			myslot = execute_attr_map_slot(cis->hyper_to_chunk_map->attrMap, myslot, cis->slot);

		/*
		 * Set the result relation in the executor state to the target chunk.
		 * This makes sure that the tuple gets inserted into the correct
		 * chunk.
		 */
		saved_resultRelInfo = resultRelInfo;
		resultRelInfo = cis->result_relation_info;
		estate->es_result_relation_info = resultRelInfo;

		/* Set the right relation for triggers */
		ts_tuptableslot_set_table_oid(myslot, RelationGetRelid(resultRelInfo->ri_RelationDesc));

		skip_tuple = false;

		/* BEFORE ROW INSERT Triggers */
		if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_insert_before_row)
		{
			ExecBRInsertTriggersCompat(estate, resultRelInfo, myslot);

			if (myslot == NULL) /* "do nothing" */
				skip_tuple = true;
		}

		if (!skip_tuple)
		{
			bool has_instead_insert_row_trig = false;
			/*
			 * If there is an INSTEAD OF INSERT ROW trigger, let it handle the
			 * tuple.  Otherwise, proceed with inserting the tuple into the
			 * table or foreign table.
			 */
			if (has_instead_insert_row_trig)
			{
				ExecIRInsertTriggers(estate, resultRelInfo, myslot);
			}
			else
			{
				List *recheckIndexes = NIL;

#if PG12_GE
				/* Compute stored generated columns */
				if (resultRelInfo->ri_RelationDesc->rd_att->constr &&
					resultRelInfo->ri_RelationDesc->rd_att->constr->has_generated_stored)
					ExecComputeStoredGenerated(estate, myslot);
#endif
				/*
				 * If the target is a plain table, check the constraints of
				 * the tuple.
				 */
				if (resultRelInfo->ri_FdwRoutine == NULL &&
					resultRelInfo->ri_RelationDesc->rd_att->constr)
					ExecConstraints(resultRelInfo, myslot, estate);

				/* OK, store the tuple and create index entries for it */
				table_tuple_insert(resultRelInfo->ri_RelationDesc,
								   myslot,
								   mycid,
								   ti_options,
								   bistate);

				if (resultRelInfo->ri_NumIndices > 0)
					recheckIndexes = ExecInsertIndexTuplesCompat(myslot, estate, false, NULL, NIL);

				/* AFTER ROW INSERT Triggers */
				ExecARInsertTriggersCompat(estate,
										   resultRelInfo,
										   myslot,
										   recheckIndexes,
										   NULL /* transition capture */);

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

	estate->es_result_relation_info = ccstate->dispatch->hypertable_result_rel_info;

	/* Done, clean up */
	if (errcallback.previous)
		error_context_stack = errcallback.previous;

	FreeBulkInsertState(bistate);

	MemoryContextSwitchTo(oldcontext);

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggersCompat(estate, resultRelInfo);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	pfree(values);
	pfree(nulls);

	ExecResetTupleTable(estate->es_tupleTable, false);

	ExecCloseIndices(resultRelInfo);
#if PG96
	{
		/*
		 * es_trig_target_relations sometimes created in
		 * ExecGetTriggerResultRel on chunks. During copy to regular tables,
		 * this never happens because the ResultRelInfo always already exists
		 * for the regular table.
		 */
		ListCell *l;

		foreach (l, estate->es_trig_target_relations)
		{
			ResultRelInfo *resultRelInfo = (ResultRelInfo *) lfirst(l);

			/* Close indices and then the relation itself */
			ExecCloseIndices(resultRelInfo);
			table_close(resultRelInfo->ri_RelationDesc, NoLock);
		}
	}
#else
	/* Close any trigger target relations */
	ExecCleanUpTriggerState(estate);
#endif

	copy_chunk_state_destroy(ccstate);

	/*
	 * If we skipped writing WAL, then we need to sync the heap (but not
	 * indexes since those use WAL anyway)
	 */
	if (ti_options & HEAP_INSERT_SKIP_WAL)
		heap_sync(ccstate->rel);

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
	List *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int attr_count = tupDesc->natts;
		int i;

		for (i = 0; i < attr_count; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupDesc, i);

			if (attr->attisdropped)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell *l;

		foreach (l, attnamelist)
		{
			char *name = strVal(lfirst(l));
			int attnum;
			int i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(tupDesc, i);

				if (attr->attisdropped)
					continue;
				if (namestrcmp(&(attr->attname), name) == 0)
				{
					attnum = attr->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									name,
									RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist", name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once", name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}

static void
copy_security_check(Relation rel, List *attnums)
{
	List *range_table = NIL;
	ListCell *cur;
	RangeTblEntry *rte;
	char *xactReadOnly;

	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->requiredPerms = ACL_INSERT;
	range_table = list_make1(rte);

	foreach (cur, attnums)
	{
		int attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;

		rte->insertedCols = bms_add_member(rte->insertedCols, attno);
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
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY FROM not supported with row-level security"),
				 errhint("Use INSERT statements instead.")));
	}

	/* check read-only transaction and parallel mode */
	xactReadOnly = GetConfigOptionByName("transaction_read_only", NULL, false);

	if (strncmp(xactReadOnly, "on", sizeof("on")) == 0 && !rel->rd_islocaltemp)
		PreventCommandIfReadOnly("COPY FROM");
	PreventCommandIfParallelMode("COPY FROM");
}

void
timescaledb_DoCopy(const CopyStmt *stmt, const char *queryString, uint64 *processed, Hypertable *ht)
{
	CopyChunkState *ccstate;
	CopyState cstate;
	bool pipe = (stmt->filename == NULL);
	Relation rel;
	List *range_table = NIL;
	List *attnums = NIL;

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

	if (!stmt->is_from || NULL == stmt->relation)
		elog(ERROR, "timescale DoCopy should only be called for COPY FROM");

	Assert(!stmt->query);

	/*
	 * We never actually write to the main table, but we need RowExclusiveLock
	 * to ensure no one else is
	 */
	rel = table_openrv(stmt->relation, RowExclusiveLock);

	attnums = timescaledb_CopyGetAttnums(RelationGetDescr(rel), rel, stmt->attlist);

	copy_security_check(rel, attnums);

#if PG96
	cstate = BeginCopyFrom(rel, stmt->filename, stmt->is_program, stmt->attlist, stmt->options);
#else
	{
		ParseState *pstate = make_parsestate(NULL);

		pstate->p_sourcetext = queryString;
		cstate = BeginCopyFrom(pstate,
							   rel,
							   stmt->filename,
							   stmt->is_program,
							   NULL,
							   stmt->attlist,
							   stmt->options);
		free_parsestate(pstate);
	}
#endif
	ccstate = copy_chunk_state_create(ht, rel, next_copy_from, cstate, NULL);
	*processed = copyfrom(ccstate, range_table, ht);
	EndCopyFrom(cstate);

	table_close(rel, NoLock);
}

static bool
next_copy_from_table_to_chunks(CopyChunkState *ccstate, ExprContext *econtext, Datum *values,
							   bool *nulls)
{
	TableScanDesc scandesc = ccstate->scandesc;
	HeapTuple tuple;

	Assert(scandesc != NULL);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	if (!HeapTupleIsValid(tuple))
		return false;

	heap_deform_tuple(tuple, RelationGetDescr(ccstate->rel), values, nulls);

	return true;
}

/*
 * Move data from the given hypertable's main table to chunks.
 *
 * The data moving is essentially a COPY from the main table to the chunks
 * followed by a TRUNCATE on the main table.
 */
void
timescaledb_move_from_table_to_chunks(Hypertable *ht, LOCKMODE lockmode)
{
	Relation rel;
	CopyChunkState *ccstate;
	TableScanDesc scandesc;
	Snapshot snapshot;
	List *attnums = NIL;
	List *range_table = NIL;
	RangeVar rv = {
		.schemaname = NameStr(ht->fd.schema_name),
		.relname = NameStr(ht->fd.table_name),
#if PG96
		.inhOpt = INH_NO,
#else
		.inh = false, /* Don't recurse */
#endif
	};

	TruncateStmt stmt = {
		.type = T_TruncateStmt,
		.relations = list_make1(&rv),
		.behavior = DROP_RESTRICT,
	};
	int i;

	rel = table_open(ht->main_table_relid, lockmode);

	for (i = 0; i < rel->rd_att->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(rel->rd_att, i);

		attnums = lappend_int(attnums, attr->attnum);
	}

	copy_security_check(rel, attnums);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scandesc = table_beginscan(rel, snapshot, 0, NULL);
	ccstate = copy_chunk_state_create(ht, rel, next_copy_from_table_to_chunks, NULL, scandesc);
	copyfrom(ccstate, range_table, ht);
	heap_endscan(scandesc);
	UnregisterSnapshot(snapshot);
	table_close(rel, lockmode);

	ExecuteTruncate(&stmt);
}
