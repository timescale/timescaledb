/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 *
 * The code copies data to a hypertable or migrates existing data from
 * a table to a hypertable when create_hypertable(..., migrate_data =>
 * 'true', ...) is called.
 *
 * Unfortunately, there aren't any good hooks in the regular COPY code to
 * insert our chunk dispatching. So, most of this code is a straight-up
 * copy of the regular PostgreSQL source code for the COPY command
 * (command/copy.c and command/copyfrom.c), albeit with minor modifications.
 */

#include <postgres.h>

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <access/heapam.h>
#include <access/hio.h>
#include <access/sysattr.h>
#include <access/xact.h>
#include <catalog/pg_trigger_d.h>
#include <commands/copy.h>
#include <commands/tablecmds.h>
#include <commands/trigger.h>
#include <executor/executor.h>
#include <executor/nodeModifyTable.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <optimizer/optimizer.h>
#include <parser/parse_coerce.h>
#include <parser/parse_collate.h>
#include <parser/parse_expr.h>
#include <parser/parse_relation.h>
#include <storage/bufmgr.h>
#include <storage/smgr.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/guc.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/rls.h>

#include "compat/compat.h"
#include "copy.h"
#include "cross_module_fn.h"
#include "dimension.h"
#include "hypertable.h"
#include "nodes/chunk_dispatch/chunk_dispatch.h"
#include "nodes/chunk_dispatch/chunk_insert_state.h"
#include "subspace_store.h"

#if PG14_GE
#include <commands/copyfrom_internal.h>
#endif

/*
 * No more than this many tuples per TSCopyMultiInsertBuffer
 *
 * Caution: Don't make this too big, as we could end up with this many
 * TSCopyMultiInsertBuffer items stored in TSCopyMultiInsertInfo's
 * multiInsertBuffers list.  Increasing this can cause quadratic growth in
 * memory requirements during copies into partitioned tables with a large
 * number of partitions.
 */
#define MAX_BUFFERED_TUPLES 1000

/*
 * Flush buffers if there are >= this many bytes, as counted by the input
 * size, of tuples stored.
 */
#define MAX_BUFFERED_BYTES 65535

/* Trim the list of buffers back down to this number after flushing */
#define MAX_PARTITION_BUFFERS 32

/* Stores multi-insert data related to a single relation in CopyFrom. */
typedef struct TSCopyMultiInsertBuffer
{
	/*
	 * Tuple description for inserted tuple slots. We use a copy of the result
	 * relation tupdesc to disable reference counting for this tupdesc. It is
	 * not needed and is wasting a lot of CPU in ResourceOwner.
	 */
	TupleDesc tupdesc;
	TupleTableSlot *slots[MAX_BUFFERED_TUPLES]; /* Array to store tuples */
	Point *point;								/* The point in space of this buffer */
	BulkInsertState bistate;					/* BulkInsertState for this buffer */
	int nused;									/* number of 'slots' containing tuples */
	uint64 linenos[MAX_BUFFERED_TUPLES];		/* Line # of tuple in copy
												 * stream */
} TSCopyMultiInsertBuffer;

/*
 * Stores one or many TSCopyMultiInsertBuffers and details about the size and
 * number of tuples which are stored in them.  This allows multiple buffers to
 * exist at once when COPYing into a partitioned table.
 *
 * The HTAB is used to store the relationship between a chunk and a
 * TSCopyMultiInsertBuffer beyond the lifetime of the ChunkInsertState.
 *
 * Chunks can be closed (e.g., due to timescaledb.max_open_chunks_per_insert).
 * When ts_chunk_dispatch_get_chunk_insert_state is called again for a closed
 * chunk, a new ChunkInsertState is returned.
 */
typedef struct TSCopyMultiInsertInfo
{
	HTAB *multiInsertBuffers; /* Maps the chunk ids to the buffers (chunkid ->
								 TSCopyMultiInsertBuffer) */
	int bufferedTuples;		  /* number of tuples buffered over all buffers */
	int bufferedBytes;		  /* number of bytes from all buffered tuples */
	CopyChunkState *ccstate;  /* Copy chunk state for this TSCopyMultiInsertInfo */
	EState *estate;			  /* Executor state used for COPY */
	CommandId mycid;		  /* Command Id used for COPY */
	int ti_options;			  /* table insert options */
	Hypertable *ht;			  /* The hypertable for the inserts */
} TSCopyMultiInsertInfo;

/*
 * The entry of the multiInsertBuffers HTAB.
 */
typedef struct MultiInsertBufferEntry
{
	int32 key;
	TSCopyMultiInsertBuffer *buffer;
} MultiInsertBufferEntry;

/*
 * Represents the heap insert method to be used during COPY FROM.
 */
#if PG14_LT
typedef enum CopyInsertMethod
{
	CIM_SINGLE,			  /* use table_tuple_insert or fdw routine */
	CIM_MULTI_CONDITIONAL /* use table_multi_insert only if valid */
} CopyInsertMethod;
#endif

/*
 * Change to another chunk for inserts.
 *
 * Called every time we switch to another chunk for inserts.
 */
static void
on_chunk_insert_state_changed(ChunkInsertState *state, void *data)
{
	BulkInsertState bistate = data;

	/* Chunk changed, so release the buffer held in BulkInsertState */
	ReleaseBulkInsertStatePin(bistate);
}

static CopyChunkState *
copy_chunk_state_create(Hypertable *ht, Relation rel, CopyFromFunc from_func, CopyFromState cstate,
						TableScanDesc scandesc)
{
	CopyChunkState *ccstate;
	EState *estate = CreateExecutorState();

	ccstate = palloc(sizeof(CopyChunkState));
	ccstate->rel = rel;
	ccstate->estate = estate;
	ccstate->dispatch = ts_chunk_dispatch_create(ht, estate, 0);
	ccstate->cstate = cstate;
	ccstate->scandesc = scandesc;
	ccstate->next_copy_from = from_func;
	ccstate->where_clause = NULL;

	return ccstate;
}

/*
 * Allocate memory and initialize a new TSCopyMultiInsertBuffer for this
 * ResultRelInfo.
 */
static TSCopyMultiInsertBuffer *
TSCopyMultiInsertBufferInit(ChunkInsertState *cis, Point *point)
{
	TSCopyMultiInsertBuffer *buffer;

	buffer = (TSCopyMultiInsertBuffer *) palloc(sizeof(TSCopyMultiInsertBuffer));
	memset(buffer->slots, 0, sizeof(TupleTableSlot *) * MAX_BUFFERED_TUPLES);
	buffer->bistate = GetBulkInsertState();
	buffer->nused = 0;

	buffer->point = palloc(POINT_SIZE(point->num_coords));
	memcpy(buffer->point, point, POINT_SIZE(point->num_coords));

	/*
	 * Make a non-refcounted copy of tupdesc to avoid spending CPU in
	 * ResourceOwner when creating a big number of table slots. This happens
	 * because each new slot pins its tuple descriptor using PinTupleDesc, and
	 * for reference-counting tuples this involves adding a new reference to
	 * ResourceOwner, which is not very efficient for a large number of
	 * references.
	 */
	buffer->tupdesc = CreateTupleDescCopyConstr(cis->rel->rd_att);
	Assert(buffer->tupdesc->tdrefcount == -1);

	return buffer;
}

/*
 * Get the existing TSCopyMultiInsertBuffer for the chunk or create a new one.
 */
static inline TSCopyMultiInsertBuffer *
TSCopyMultiInsertInfoGetOrSetupBuffer(TSCopyMultiInsertInfo *miinfo, ChunkInsertState *cis,
									  Point *point)
{
	bool found;
	int32 chunk_id;

	Assert(miinfo != NULL);
	Assert(cis != NULL);
	Assert(point != NULL);

	chunk_id = cis->chunk_id;
	MultiInsertBufferEntry *entry =
		hash_search(miinfo->multiInsertBuffers, &chunk_id, HASH_ENTER, &found);

	/* No insert buffer for this chunk exists, create a new one */
	if (!found)
	{
		entry->buffer = TSCopyMultiInsertBufferInit(cis, point);
	}

	return entry->buffer;
}

/*
 * Create a new HTAB that maps from the chunk_id to the multi-insert buffers.
 */
static HTAB *
TSCopyCreateNewInsertBufferHashMap()
{
	struct HASHCTL hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(MultiInsertBufferEntry),
		.hcxt = CurrentMemoryContext,
	};

	return hash_create("COPY insert buffer", 20, &hctl, HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
}

/*
 * Initialize an already allocated TSCopyMultiInsertInfo.
 */
static void
TSCopyMultiInsertInfoInit(TSCopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						  CopyChunkState *ccstate, EState *estate, CommandId mycid, int ti_options,
						  Hypertable *ht)
{
	miinfo->multiInsertBuffers = TSCopyCreateNewInsertBufferHashMap();
	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;
	miinfo->ccstate = ccstate;
	miinfo->estate = estate;
	miinfo->mycid = mycid;
	miinfo->ti_options = ti_options;
	miinfo->ht = ht;
}

/*
 * Returns true if the buffers are full.
 */
static inline bool
TSCopyMultiInsertInfoIsFull(TSCopyMultiInsertInfo *miinfo)
{
	if (miinfo->bufferedTuples >= MAX_BUFFERED_TUPLES ||
		miinfo->bufferedBytes >= MAX_BUFFERED_BYTES)
		return true;

	return false;
}

/*
 * Write the tuples stored in 'buffer' out to the table.
 */
static inline int
TSCopyMultiInsertBufferFlush(TSCopyMultiInsertInfo *miinfo, TSCopyMultiInsertBuffer *buffer)
{
	MemoryContext oldcontext;
	int i;

	Assert(miinfo != NULL);
	Assert(buffer != NULL);

	EState *estate = miinfo->estate;
	CommandId mycid = miinfo->mycid;
	int ti_options = miinfo->ti_options;
	int nused = buffer->nused;
	TupleTableSlot **slots = buffer->slots;

	/*
	 * table_multi_insert and reinitialization of the chunk insert state may
	 * leak memory, so switch to short-lived memory context before calling it.
	 */
	oldcontext = MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

	/*
	 * A chunk can be closed while buffering the tuples. Even when the chunk
	 * insert state is moved to the copy memory context, the underlying
	 * table is closed and pointers (e.g., result_relation_info point) to invalid
	 * addresses. Re-reading the chunk insert state ensures that the table is
	 * open and the pointers are valid.
	 *
	 * No callback on changed chunk is needed, the bulk insert state buffer is
	 * freed in TSCopyMultiInsertBufferCleanup().
	 */
	ChunkInsertState *cis =
		ts_chunk_dispatch_get_chunk_insert_state(miinfo->ccstate->dispatch,
												 buffer->point,
												 buffer->slots[0],
												 NULL /* on chunk changed function */,
												 NULL /* payload for on chunk changed function */);

	ResultRelInfo *resultRelInfo = cis->result_relation_info;

	/*
	 * Add context information to the copy state, which is used to display
	 * error messages with additional details. Providing this information is
	 * only possible in PG >= 14. Until PG13 the CopyFromState structure is
	 * kept internally in copy.c and no access to its members is possible.
	 * Since PG14, the structure is stored in copyfrom_internal.h and the
	 * members can be accessed.
	 */
#if PG14_GE
	uint64 save_cur_lineno = 0;
	bool line_buf_valid = false;
	CopyFromState cstate = miinfo->ccstate->cstate;

	/* cstate can be NULL in calls that are invoked from timescaledb_move_from_table_to_chunks. */
	if (cstate != NULL)
	{
		line_buf_valid = cstate->line_buf_valid;
		save_cur_lineno = cstate->cur_lineno;

		cstate->line_buf_valid = false;
	}
#endif

#if PG14_LT
	estate->es_result_relation_info = resultRelInfo;
#endif

	table_multi_insert(resultRelInfo->ri_RelationDesc,
					   slots,
					   nused,
					   mycid,
					   ti_options,
					   buffer->bistate);
	MemoryContextSwitchTo(oldcontext);

	for (i = 0; i < nused; i++)
	{
#if PG14_GE
		if (cstate != NULL)
			cstate->cur_lineno = buffer->linenos[i];
#endif
		/*
		 * If there are any indexes, update them for all the inserted tuples,
		 * and run AFTER ROW INSERT triggers.
		 */
		if (resultRelInfo->ri_NumIndices > 0)
		{
			List *recheckIndexes;

			recheckIndexes = ExecInsertIndexTuplesCompat(resultRelInfo,
														 buffer->slots[i],
														 estate,
														 false,
														 false,
														 NULL,
														 NIL,
														 false);

			ExecARInsertTriggers(estate,
								 resultRelInfo,
								 slots[i],
								 recheckIndexes,
								 NULL /* transition capture */);
			list_free(recheckIndexes);
		}

		/*
		 * There's no indexes, but see if we need to run AFTER ROW INSERT
		 * triggers anyway.
		 */
		else if (resultRelInfo->ri_TrigDesc != NULL &&
				 (resultRelInfo->ri_TrigDesc->trig_insert_after_row ||
				  resultRelInfo->ri_TrigDesc->trig_insert_new_table))
		{
			ExecARInsertTriggers(estate,
								 resultRelInfo,
								 slots[i],
								 NIL,
								 NULL /* transition capture */);
		}

		ExecClearTuple(slots[i]);
	}

	/* Mark that all slots are free */
	buffer->nused = 0;

	/* Chunk could be closed on a subsequent call of ts_chunk_dispatch_get_chunk_insert_state
	 * (e.g., due to timescaledb.max_open_chunks_per_insert). So, ensure the bulk insert is
	 * finished after the flush is complete.
	 */
	ResultRelInfo *result_relation_info = cis->result_relation_info;
	Assert(result_relation_info != NULL);
	table_finish_bulk_insert(result_relation_info->ri_RelationDesc, miinfo->ti_options);

	/* Reset cur_lineno and line_buf_valid to what they were */
#if PG14_GE
	if (cstate != NULL)
	{
		cstate->line_buf_valid = line_buf_valid;
		cstate->cur_lineno = save_cur_lineno;
	}
#endif

	return cis->chunk_id;
}

/*
 * Drop used slots and free member for this buffer.
 *
 * The buffer must be flushed before cleanup.
 */
static inline void
TSCopyMultiInsertBufferCleanup(TSCopyMultiInsertInfo *miinfo, TSCopyMultiInsertBuffer *buffer)
{
	int i;

	/* Ensure buffer was flushed */
	Assert(buffer->nused == 0);

	FreeBulkInsertState(buffer->bistate);

	/* Since we only create slots on demand, just drop the non-null ones. */
	for (i = 0; i < MAX_BUFFERED_TUPLES && buffer->slots[i] != NULL; i++)
		ExecDropSingleTupleTableSlot(buffer->slots[i]);

	pfree(buffer->point);
	FreeTupleDesc(buffer->tupdesc);
	pfree(buffer);
}

/* list_sort comparator to sort TSCopyMultiInsertBuffer by usage */
static int
TSCmpBuffersByUsage(const ListCell *a, const ListCell *b)
{
	int b1 = ((const TSCopyMultiInsertBuffer *) lfirst(a))->nused;
	int b2 = ((const TSCopyMultiInsertBuffer *) lfirst(b))->nused;

	Assert(b1 >= 0);
	Assert(b2 >= 0);

	return (b1 > b2) ? 1 : (b1 == b2) ? 0 : -1;
}

/*
 * Flush all buffers by writing the tuples to the chunks. In addition, trim down the
 * amount of multi-insert buffers to MAX_PARTITION_BUFFERS by deleting the least used
 * buffers (the buffers that store least tuples).
 */
static inline void
TSCopyMultiInsertInfoFlush(TSCopyMultiInsertInfo *miinfo, ChunkInsertState *cur_cis)
{
	HASH_SEQ_STATUS status;
	MultiInsertBufferEntry *entry;
	int current_multi_insert_buffers;
	int buffers_to_delete;
	bool found;
	int32 flushed_chunk_id;
	List *buffer_list = NIL;
	ListCell *lc;

	current_multi_insert_buffers = hash_get_num_entries(miinfo->multiInsertBuffers);

	/* Create a list of buffers that can be sorted by usage */
	hash_seq_init(&status, miinfo->multiInsertBuffers);
	for (entry = hash_seq_search(&status); entry != NULL; entry = hash_seq_search(&status))
	{
		buffer_list = lappend(buffer_list, entry->buffer);
	}

	buffers_to_delete = Max(current_multi_insert_buffers - MAX_PARTITION_BUFFERS, 0);

	/* Sorting is only needed if we want to remove the least used buffers */
	if (buffers_to_delete > 0)
		list_sort(buffer_list, TSCmpBuffersByUsage);

	/* Flush buffers and delete them if needed */
	foreach (lc, buffer_list)
	{
		TSCopyMultiInsertBuffer *buffer = (TSCopyMultiInsertBuffer *) lfirst(lc);
		flushed_chunk_id = TSCopyMultiInsertBufferFlush(miinfo, buffer);

		if (buffers_to_delete > 0)
		{
			/*
			 * Reduce active multi-insert buffers. However, the current used buffer
			 * should not be deleted because it might reused for the next insert.
			 */
			if (cur_cis == NULL || flushed_chunk_id != cur_cis->chunk_id)
			{
				TSCopyMultiInsertBufferCleanup(miinfo, buffer);
				hash_search(miinfo->multiInsertBuffers, &flushed_chunk_id, HASH_REMOVE, &found);
				Assert(found);
				buffers_to_delete--;
			}
		}
	}

	list_free(buffer_list);

	/* All buffers have been flushed */
	miinfo->bufferedTuples = 0;
	miinfo->bufferedBytes = 0;
}

/*
 * All existing buffers are flushed and the multi-insert states
 * are freed. So, delete old hash map and create a new one for further
 * inserts.
 */
static inline void
TSCopyMultiInsertInfoFlushAndCleanup(TSCopyMultiInsertInfo *miinfo)
{
	TSCopyMultiInsertInfoFlush(miinfo, NULL);

	HASH_SEQ_STATUS status;
	MultiInsertBufferEntry *entry;

	hash_seq_init(&status, miinfo->multiInsertBuffers);

	for (entry = hash_seq_search(&status); entry != NULL; entry = hash_seq_search(&status))
	{
		TSCopyMultiInsertBuffer *buffer = entry->buffer;
		TSCopyMultiInsertBufferCleanup(miinfo, buffer);
	}

	hash_destroy(miinfo->multiInsertBuffers);
}

/*
 * Get the next TupleTableSlot that the next tuple should be stored in.
 *
 * Callers must ensure that the buffer is not full.
 *
 * Note: 'miinfo' is unused but has been included for consistency with the
 * other functions in this area.
 */
static inline TupleTableSlot *
TSCopyMultiInsertInfoNextFreeSlot(TSCopyMultiInsertInfo *miinfo,
								  ResultRelInfo *result_relation_info,
								  TSCopyMultiInsertBuffer *buffer)
{
	int nused = buffer->nused;

	Assert(buffer != NULL);
	Assert(nused < MAX_BUFFERED_TUPLES);

	if (buffer->slots[nused] == NULL)
	{
		const TupleTableSlotOps *tts_cb =
			table_slot_callbacks(result_relation_info->ri_RelationDesc);
		buffer->slots[nused] = MakeSingleTupleTableSlot(buffer->tupdesc, tts_cb);
	}
	return buffer->slots[nused];
}

/*
 * Record the previously reserved TupleTableSlot that was reserved by
 * TSCopyMultiInsertInfoNextFreeSlot as being consumed.
 */
static inline void
TSCopyMultiInsertInfoStore(TSCopyMultiInsertInfo *miinfo, ResultRelInfo *rri,
						   TSCopyMultiInsertBuffer *buffer, TupleTableSlot *slot,
						   CopyFromState cstate)
{
	Assert(buffer != NULL);
	Assert(slot == buffer->slots[buffer->nused]);

	/* Store the line number so we can properly report any errors later */
	uint64 lineno = 0;

	/* The structure CopyFromState is private in PG < 14. So we can not access
	 * the members like the line number or the size of the tuple.
	 */
#if PG14_GE
	if (cstate != NULL)
		lineno = cstate->cur_lineno;
#endif
	buffer->linenos[buffer->nused] = lineno;

	/* Record this slot as being used */
	buffer->nused++;

	/* Update how many tuples are stored and their size */
	miinfo->bufferedTuples++;

	/*
	 * Note: There is no reliable way to determine the in-memory size of a virtual
	 * tuple. So, we perform flushing in PG < 14 only based on the number of buffered
	 * tuples and not based on the size.
	 */
#if PG14_GE
	if (cstate != NULL)
	{
		int tuplen = cstate->line_buf.len;
		miinfo->bufferedBytes += tuplen;
	}
#endif
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
	return NextCopyFrom(ccstate->cstate, econtext, values, nulls);
}

/*
 * Error context callback when copying from table to chunk.
 */
static void
copy_table_to_chunk_error_callback(void *arg)
{
	TableScanDesc scandesc = (TableScanDesc) arg;
	errcontext("copying from table %s", RelationGetRelationName(scandesc->rs_rd));
}

/*
 * Tests if there are other before insert row triggers besides the
 * ts_insert_blocker trigger.
 */
static inline bool
has_other_before_insert_row_trigger_than_ts(ResultRelInfo *resultRelInfo)
{
	TriggerDesc *trigdesc = resultRelInfo->ri_TrigDesc;
	int i;

	if (trigdesc == NULL)
		return false;

	if (!trigdesc->trig_insert_before_row)
		return false;

	for (i = 0; i < trigdesc->numtriggers; i++)
	{
		Trigger *trigger = &trigdesc->triggers[i];
		if (!TRIGGER_TYPE_MATCHES(trigger->tgtype,
								  TRIGGER_TYPE_ROW,
								  TRIGGER_TYPE_BEFORE,
								  TRIGGER_TYPE_INSERT))
			continue;

		/* Ignore the ts_insert_block trigger */
		if (strncmp(trigger->tgname, INSERT_BLOCKER_NAME, NAMEDATALEN) == 0)
			continue;

		/* At least one trigger exists */
		return true;
	}

	return false;
}

/*
 * Use COPY FROM to copy data from file to relation.
 */
static uint64
copyfrom(CopyChunkState *ccstate, ParseState *pstate, Hypertable *ht, MemoryContext copycontext,
		 void (*callback)(void *), void *arg)
{
	ResultRelInfo *resultRelInfo;
	ResultRelInfo *saved_resultRelInfo = NULL;
	EState *estate = ccstate->estate; /* for ExecConstraints() */
	ExprContext *econtext;
	TupleTableSlot *singleslot;
	MemoryContext oldcontext = CurrentMemoryContext;
	ErrorContextCallback errcallback = {
		.callback = callback,
		.arg = arg,
	};
	CommandId mycid = GetCurrentCommandId(true);
	CopyInsertMethod insertMethod;				   /* The insert method for the table */
	CopyInsertMethod currentTupleInsertMethod;	   /* The insert method of the current tuple */
	TSCopyMultiInsertInfo multiInsertInfo = { 0 }; /* pacify compiler */
	int ti_options = 0;							   /* start with default options for insert */
	BulkInsertState bistate = NULL;
	uint64 processed = 0;
	bool has_before_insert_row_trig;
	bool has_instead_insert_row_trig;
	ExprState *qualexpr = NULL;
	ChunkDispatch *dispatch = ccstate->dispatch;

	Assert(pstate->p_rtable);

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
#if PG16_LT
		ccstate->rel->rd_newRelfilenodeSubid != InvalidSubTransactionId)
#else
		ccstate->rel->rd_newRelfilelocatorSubid != InvalidSubTransactionId)
#endif
	{
		ti_options |= HEAP_INSERT_SKIP_FSM;
	}

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 *
	 * WARNING. The dummy rangetable index is decremented by 1 (unchecked)
	 * inside `ExecConstraints` so unless you want to have a overflow, keep it
	 * above zero. See `rt_fetch` in parsetree.h.
	 */
	resultRelInfo = makeNode(ResultRelInfo);

#if PG14_LT
	InitResultRelInfo(resultRelInfo,
					  ccstate->rel,
					  /* RangeTableIndex */ 1,
					  NULL,
					  0);
#else
#if PG16_LT
	ExecInitRangeTable(estate, pstate->p_rtable);
#else
	Assert(pstate->p_rteperminfos != NULL);
	ExecInitRangeTable(estate, pstate->p_rtable, pstate->p_rteperminfos);
#endif
	ExecInitResultRelation(estate, resultRelInfo, 1);
#endif

	CheckValidResultRel(resultRelInfo, CMD_INSERT);

	ExecOpenIndices(resultRelInfo, false);

#if PG14_LT
	estate->es_result_relations = resultRelInfo;
	estate->es_num_result_relations = 1;
	estate->es_result_relation_info = resultRelInfo;
	estate->es_range_table = pstate->p_rtable;

	ExecInitRangeTable(estate, estate->es_range_table);
#endif

	if (!dispatch->hypertable_result_rel_info)
		dispatch->hypertable_result_rel_info = resultRelInfo;

	singleslot = table_slot_create(resultRelInfo->ri_RelationDesc, &estate->es_tupleTable);

	/* Prepare to catch AFTER triggers. */
	AfterTriggerBeginQuery();

	if (ccstate->where_clause)
		qualexpr = ExecInitQual(castNode(List, ccstate->where_clause), NULL);

	/*
	 * Check BEFORE STATEMENT insertion triggers. It's debatable whether we
	 * should do this for COPY, since it's not really an "INSERT" statement as
	 * such. However, executing these triggers maintains consistency with the
	 * EACH ROW triggers that we already fire on COPY.
	 */
	ExecBSInsertTriggers(estate, resultRelInfo);

	bistate = GetBulkInsertState();
	econtext = GetPerTupleExprContext(estate);

	/* Set up callback to identify error line number.
	 *
	 * It is not necessary to add an entry to the error context stack if we do
	 * not have a CopyFromState or callback. In that case, we just use the existing
	 * error already on the context stack. */
	if (ccstate->cstate && callback)
	{
		errcallback.previous = error_context_stack;
		error_context_stack = &errcallback;
	}

	/*
	 * Multi-insert buffers (CIM_MULTI_CONDITIONAL) can only be used if no triggers are
	 * defined on the target table. Otherwise, the tuples may be inserted in an out-of-order
	 * manner, which might violate the semantics of the triggers. So, they are inserted
	 * tuple-per-tuple (CIM_SINGLE). However, the ts_block trigger on the hypertable can
	 * be ignored.
	 */

	/* Before INSERT Triggers */
	has_before_insert_row_trig = has_other_before_insert_row_trigger_than_ts(resultRelInfo);

	/* Instead of INSERT Triggers */
	has_instead_insert_row_trig =
		(resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_insert_instead_row);

	/* Depending on the configured trigger, enable or disable the multi-insert buffers */
	if (has_before_insert_row_trig || has_instead_insert_row_trig)
	{
		insertMethod = CIM_SINGLE;
		ereport(DEBUG1,
				(errmsg("Using normal unbuffered copy operation (CIM_SINGLE) "
						"because triggers are defined on the destination table.")));
	}
	else
	{
		insertMethod = CIM_MULTI_CONDITIONAL;
		ereport(DEBUG1,
				(errmsg("Using optimized multi-buffer copy operation (CIM_MULTI_CONDITIONAL).")));
		TSCopyMultiInsertInfoInit(&multiInsertInfo,
								  resultRelInfo,
								  ccstate,
								  estate,
								  mycid,
								  ti_options,
								  ht);
	}

	for (;;)
	{
		TupleTableSlot *myslot = NULL;
		bool skip_tuple;
		Point *point = NULL;
		ChunkInsertState *cis = NULL;
		TSCopyMultiInsertBuffer *buffer = NULL;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Reset the per-tuple exprcontext. We do this after every tuple, to
		 * clean-up after expression evaluations etc.
		 */
		ResetPerTupleExprContext(estate);

		myslot = singleslot;
		Assert(myslot != NULL);

		/* Switch into its memory context */
		MemoryContextSwitchTo(GetPerTupleMemoryContext(estate));

		ExecClearTuple(myslot);

		if (!ccstate->next_copy_from(ccstate, econtext, myslot->tts_values, myslot->tts_isnull))
			break;

		ExecStoreVirtualTuple(myslot);

		/* Calculate the tuple's point in the N-dimensional hyperspace */
		point = ts_hyperspace_calculate_point(ht->space, myslot);

		/* Find or create the insert state matching the point */
		cis = ts_chunk_dispatch_get_chunk_insert_state(dispatch,
													   point,
													   myslot,
													   on_chunk_insert_state_changed,
													   bistate);

		Assert(cis != NULL);

		/* Triggers and stuff need to be invoked in query context. */
		MemoryContextSwitchTo(oldcontext);

		currentTupleInsertMethod = insertMethod;

		/* Determine which triggers exist on this chunk */
		has_before_insert_row_trig =
			(cis->result_relation_info->ri_TrigDesc &&
			 cis->result_relation_info->ri_TrigDesc->trig_insert_before_row);

		has_instead_insert_row_trig =
			(cis->result_relation_info->ri_TrigDesc &&
			 cis->result_relation_info->ri_TrigDesc->trig_insert_instead_row);

		if (has_before_insert_row_trig || has_instead_insert_row_trig)
		{
			/*
			 * Flush pending inserts if this partition can't use
			 * batching, so rows are visible to triggers etc.
			 */
			if (insertMethod == CIM_MULTI_CONDITIONAL)
				TSCopyMultiInsertInfoFlush(&multiInsertInfo, cis);

			currentTupleInsertMethod = CIM_SINGLE;
		}

		/* Convert the tuple to match the chunk's rowtype */
		if (currentTupleInsertMethod == CIM_SINGLE)
		{
			if (NULL != cis->hyper_to_chunk_map)
				myslot = execute_attr_map_slot(cis->hyper_to_chunk_map->attrMap, myslot, cis->slot);
		}
		else
		{
			/*
			 * Get the multi-insert buffer for the chunk.
			 */
			buffer = TSCopyMultiInsertInfoGetOrSetupBuffer(&multiInsertInfo, cis, point);

			/*
			 * Prepare to queue up tuple for later batch insert into
			 * current chunk.
			 */
			TupleTableSlot *batchslot;

			batchslot = TSCopyMultiInsertInfoNextFreeSlot(&multiInsertInfo,
														  cis->result_relation_info,
														  buffer);

			if (NULL != cis->hyper_to_chunk_map)
				myslot = execute_attr_map_slot(cis->hyper_to_chunk_map->attrMap, myslot, batchslot);
			else
			{
				/*
				 * This looks more expensive than it is (Believe me, I
				 * optimized it away. Twice.). The input is in virtual
				 * form, and we'll materialize the slot below - for most
				 * slot types the copy performs the work materialization
				 * would later require anyway.
				 */
				ExecCopySlot(batchslot, myslot);
				myslot = batchslot;
			}
		}

		if (qualexpr != NULL)
		{
			econtext->ecxt_scantuple = myslot;
			if (!ExecQual(qualexpr, econtext))
				continue;
		}

		/*
		 * Set the result relation in the executor state to the target chunk.
		 * This makes sure that the tuple gets inserted into the correct
		 * chunk.
		 */
		saved_resultRelInfo = resultRelInfo;
		resultRelInfo = cis->result_relation_info;
#if PG14_LT
		estate->es_result_relation_info = resultRelInfo;
#endif

		/* Set the right relation for triggers */
		ts_tuptableslot_set_table_oid(myslot, RelationGetRelid(resultRelInfo->ri_RelationDesc));

		skip_tuple = false;

		/* BEFORE ROW INSERT Triggers */
		if (resultRelInfo->ri_TrigDesc && resultRelInfo->ri_TrigDesc->trig_insert_before_row)
			skip_tuple = !ExecBRInsertTriggers(estate, resultRelInfo, myslot);

		if (!skip_tuple)
		{
			/* Note that PostgreSQL's copy path would check INSTEAD OF
			 * INSERT/UPDATE/DELETE triggers here, but such triggers can only
			 * exist on views and chunks cannot be views.
			 */
			List *recheckIndexes = NIL;

			/* Compute stored generated columns */
			if (resultRelInfo->ri_RelationDesc->rd_att->constr &&
				resultRelInfo->ri_RelationDesc->rd_att->constr->has_generated_stored)
				ExecComputeStoredGeneratedCompat(resultRelInfo, estate, myslot, CMD_INSERT);

			/*
			 * If the target is a plain table, check the constraints of
			 * the tuple.
			 */
			if (resultRelInfo->ri_FdwRoutine == NULL &&
				resultRelInfo->ri_RelationDesc->rd_att->constr)
			{
				Assert(resultRelInfo->ri_RangeTableIndex > 0 && estate->es_range_table);
				ExecConstraints(resultRelInfo, myslot, estate);
			}

			if (currentTupleInsertMethod == CIM_SINGLE)
			{
				/* OK, store the tuple and create index entries for it */
				table_tuple_insert(resultRelInfo->ri_RelationDesc,
								   myslot,
								   mycid,
								   ti_options,
								   bistate);

				if (resultRelInfo->ri_NumIndices > 0)
					recheckIndexes = ExecInsertIndexTuplesCompat(resultRelInfo,
																 myslot,
																 estate,
																 false,
																 false,
																 NULL,
																 NIL,
																 false);
				/* AFTER ROW INSERT Triggers */
				ExecARInsertTriggers(estate,
									 resultRelInfo,
									 myslot,
									 recheckIndexes,
									 NULL /* transition capture */);
			}
			else
			{
				/*
				 * The slot previously might point into the per-tuple
				 * context. For batching it needs to be longer lived.
				 */
				ExecMaterializeSlot(myslot);

				/* Add this tuple to the tuple buffer */
				TSCopyMultiInsertInfoStore(&multiInsertInfo,
										   resultRelInfo,
										   buffer,
										   myslot,
										   ccstate->cstate);

				/*
				 * If enough inserts have queued up, then flush all
				 * buffers out to their tables.
				 */
				if (TSCopyMultiInsertInfoIsFull(&multiInsertInfo))
				{
					ereport(DEBUG2,
							(errmsg("flush called with %d bytes and %d buffered tuples",
									multiInsertInfo.bufferedBytes,
									multiInsertInfo.bufferedTuples)));

					TSCopyMultiInsertInfoFlush(&multiInsertInfo, cis);
				}
			}

			list_free(recheckIndexes);

			/*
			 * We count only tuples not suppressed by a BEFORE INSERT trigger;
			 * this is the same definition used by execMain.c for counting
			 * tuples inserted by an INSERT command.
			 */
			processed++;
		}

		resultRelInfo = saved_resultRelInfo;
#if PG14_LT
		estate->es_result_relation_info = resultRelInfo;
#endif
	}

#if PG14_LT
	estate->es_result_relation_info = ccstate->dispatch->hypertable_result_rel_info;
#endif

	/* Flush any remaining buffered tuples */
	if (insertMethod != CIM_SINGLE)
		TSCopyMultiInsertInfoFlushAndCleanup(&multiInsertInfo);

	/* Done, clean up */
	if (ccstate->cstate && callback)
		error_context_stack = errcallback.previous;

	FreeBulkInsertState(bistate);

	MemoryContextSwitchTo(oldcontext);

	/* Execute AFTER STATEMENT insertion triggers */
	ExecASInsertTriggers(estate, resultRelInfo, NULL);

	/* Handle queued AFTER triggers */
	AfterTriggerEndQuery(estate);

	ExecResetTupleTable(estate->es_tupleTable, false);

#if PG14_LT
	ExecCloseIndices(resultRelInfo);
	/* Close any trigger target relations */
	ExecCleanUpTriggerState(estate);
#else
	ExecCloseResultRelations(estate);
	ExecCloseRangeTableRelations(estate);
#endif

	/*
	 * If we skipped writing WAL, then we need to sync the heap (but not
	 * indexes since those use WAL anyway)
	 */
	if (!RelationNeedsWAL(ccstate->rel))
		smgrimmedsync(RelationGetSmgr(ccstate->rel), MAIN_FORKNUM);

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
copy_constraints_and_check(ParseState *pstate, Relation rel, List *attnums)
{
	ListCell *cur;
	char *xactReadOnly;
	ParseNamespaceItem *nsitem =
		addRangeTableEntryForRelation(pstate, rel, RowExclusiveLock, NULL, false, false);
	RangeTblEntry *rte = nsitem->p_rte;
	addNSItemToQuery(pstate, nsitem, true, true, true);

#if PG16_LT
	rte->requiredPerms = ACL_INSERT;

	foreach (cur, attnums)
	{
		int attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;
		rte->insertedCols = bms_add_member(rte->insertedCols, attno);
	}

	ExecCheckRTPerms(pstate->p_rtable, true);
#else
	RTEPermissionInfo *perminfo = nsitem->p_perminfo;
	perminfo->requiredPerms = ACL_INSERT;

	foreach (cur, attnums)
	{
		int attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;
		perminfo->insertedCols = bms_add_member(perminfo->insertedCols, attno);
	}

	ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), true);
#endif

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
	CopyFromState cstate;
	bool pipe = (stmt->filename == NULL);
	Relation rel;
	List *attnums = NIL;
	Node *where_clause = NULL;
	ParseState *pstate;
	MemoryContext copycontext = NULL;

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
	 * to ensure no one else is. Because of the check above, we know that
	 * `stmt->relation` is defined, so we are guaranteed to have a relation
	 * available.
	 */
	rel = table_openrv(stmt->relation, RowExclusiveLock);

	attnums = timescaledb_CopyGetAttnums(RelationGetDescr(rel), rel, stmt->attlist);

	pstate = make_parsestate(NULL);
	pstate->p_sourcetext = queryString;
	copy_constraints_and_check(pstate, rel, attnums);

	cstate = BeginCopyFrom(pstate,
						   rel,
#if PG14_GE
						   NULL,
#endif
						   stmt->filename,
						   stmt->is_program,
						   NULL,
						   stmt->attlist,
						   stmt->options);

	if (stmt->whereClause)
	{
		if (hypertable_is_distributed(ht))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY WHERE clauses are not supported on distributed hypertables")));

		where_clause = transformExpr(pstate, stmt->whereClause, EXPR_KIND_COPY_WHERE);

		where_clause = coerce_to_boolean(pstate, where_clause, "WHERE");
		assign_expr_collations(pstate, where_clause);

		where_clause = eval_const_expressions(NULL, where_clause);

		where_clause = (Node *) canonicalize_qual((Expr *) where_clause, false);
		where_clause = (Node *) make_ands_implicit((Expr *) where_clause);
	}

	ccstate = copy_chunk_state_create(ht, rel, next_copy_from, cstate, NULL);
	ccstate->where_clause = where_clause;

	if (hypertable_is_distributed(ht))
		*processed = ts_cm_functions->distributed_copy(stmt, ccstate, attnums);
	else
	{
#if PG14_GE
		/* Take the copy memory context from cstate, if we can access the struct (PG>=14) */
		copycontext = cstate->copycontext;
#else
		/* Or create a new memory context. */
		copycontext = AllocSetContextCreate(CurrentMemoryContext, "COPY", ALLOCSET_DEFAULT_SIZES);
#endif
		*processed = copyfrom(ccstate, pstate, ht, copycontext, CopyFromErrorCallback, cstate);
	}

	copy_chunk_state_destroy(ccstate);
	EndCopyFrom(cstate);
	free_parsestate(pstate);
	table_close(rel, NoLock);

#if PG14_LT
	if (MemoryContextIsValid(copycontext))
		MemoryContextDelete(copycontext);
#endif
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
	ParseState *pstate = make_parsestate(NULL);
	Snapshot snapshot;
	List *attnums = NIL;
	MemoryContext copycontext;

	RangeVar rv = {
		.schemaname = NameStr(ht->fd.schema_name),
		.relname = NameStr(ht->fd.table_name),
		.inh = false, /* Don't recurse */
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

	copycontext = AllocSetContextCreate(CurrentMemoryContext, "COPY", ALLOCSET_DEFAULT_SIZES);

	copy_constraints_and_check(pstate, rel, attnums);
	snapshot = RegisterSnapshot(GetLatestSnapshot());
	scandesc = table_beginscan(rel, snapshot, 0, NULL);
	ccstate = copy_chunk_state_create(ht, rel, next_copy_from_table_to_chunks, NULL, scandesc);
	copyfrom(ccstate, pstate, ht, copycontext, copy_table_to_chunk_error_callback, scandesc);
	copy_chunk_state_destroy(ccstate);
	table_endscan(scandesc);
	UnregisterSnapshot(snapshot);
	table_close(rel, lockmode);

	if (MemoryContextIsValid(copycontext))
		MemoryContextDelete(copycontext);

	ExecuteTruncate(&stmt);
}
