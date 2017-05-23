#include <postgres.h>
#include <funcapi.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <catalog/pg_opfamily.h>
#include <utils/rel.h>
#include <utils/tuplesort.h>
#include <utils/tqual.h>
#include <utils/rls.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/guc.h>
#include <commands/tablecmds.h>
#include <commands/trigger.h>

#include <access/xact.h>
#include <access/htup_details.h>
#include <access/heapam.h>

#include <miscadmin.h>
#include <fmgr.h>

#include "cache.h"
#include "hypertable_cache.h"
#include "chunk_cache.h"
#include "errors.h"
#include "utils.h"
#include "metadata_queries.h"
#include "partitioning.h"
#include "scanner.h"
#include "catalog.h"
#include "chunk.h"
#include "insert_chunk_state.h"

/*
 * State and helper functions for inserting tuples into chunk tables
 *
 */
typedef struct InsertChunkStateRel
{
	Relation	rel;
	TupleTableSlot *slot;
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	BulkInsertState bistate;
} InsertChunkStateRel;

static InsertChunkStateRel *
insert_chunk_state_rel_new(Relation rel, ResultRelInfo *resultRelInfo, List *range_table)
{
	TupleDesc	tupDesc;
	InsertChunkStateRel *rel_state = palloc(sizeof(InsertChunkStateRel));

	rel_state->estate = CreateExecutorState();
	tupDesc = RelationGetDescr(rel);

	rel_state->estate->es_result_relations = resultRelInfo;
	rel_state->estate->es_num_result_relations = 1;
	rel_state->estate->es_result_relation_info = resultRelInfo;
	rel_state->estate->es_range_table = range_table;

	rel_state->slot = ExecInitExtraTupleSlot(rel_state->estate);
	ExecSetSlotDescriptor(rel_state->slot, tupDesc);

	rel_state->rel = rel;
	rel_state->resultRelInfo = resultRelInfo;
	rel_state->bistate = GetBulkInsertState();
	return rel_state;
}

static void
insert_chunk_state_rel_destroy(InsertChunkStateRel *rel_state)
{
	FreeBulkInsertState(rel_state->bistate);
	ExecCloseIndices(rel_state->resultRelInfo);
	ExecResetTupleTable(rel_state->estate->es_tupleTable, false);
	FreeExecutorState(rel_state->estate);
	heap_close(rel_state->rel, NoLock);
}

static void
insert_chunk_state_rel_insert_tuple(InsertChunkStateRel *rel_state, HeapTuple tuple)
{
	int			hi_options = 0; /* no optimization */
	CommandId	mycid = GetCurrentCommandId(true);

	/*
	 * Constraints might reference the tableoid column, so initialize
	 * t_tableOid before evaluating them.
	 */
	tuple->t_tableOid = RelationGetRelid(rel_state->rel);

	ExecStoreTuple(tuple, rel_state->slot, InvalidBuffer, false);

	if (rel_state->rel->rd_att->constr)
		ExecConstraints(rel_state->resultRelInfo, rel_state->slot, rel_state->estate);

	/* OK, store the tuple and create index entries for it */
	heap_insert(rel_state->rel, tuple, mycid, hi_options, rel_state->bistate);

	if (rel_state->resultRelInfo->ri_NumIndices > 0)
		ExecInsertIndexTuples(rel_state->slot, &(tuple->t_self),
							  rel_state->estate, false, NULL,
							  NIL);
}

extern InsertChunkState *
insert_chunk_state_new(Chunk *chunk)
{
	List	   *rel_state_list = NIL;
	InsertChunkState *state;
	Relation	rel;
	RangeTblEntry *rte;
	List	   *range_table;
	ResultRelInfo *resultRelInfo;
	InsertChunkStateRel *rel_state;;

	state = palloc(sizeof(InsertChunkState));

	rel = heap_open(chunk->table_id, RowExclusiveLock);

	/* permission check */
	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->requiredPerms = ACL_INSERT;
	range_table = list_make1(rte);
	ExecCheckRTPerms(range_table, true);

	if (check_enable_rls(rte->relid, InvalidOid, false) == RLS_ENABLED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Hypertables don't support Row level security")));

	}

	if (XactReadOnly && !rel->rd_islocaltemp)
		PreventCommandIfReadOnly("COPY FROM");

	PreventCommandIfParallelMode("COPY FROM");

	if (rel->rd_rel->relkind != RELKIND_RELATION)
	{
		elog(ERROR, "inserting not to table");
	}

	/*
	 * We need a ResultRelInfo so we can use the regular executor's
	 * index-entry-making machinery.  (There used to be a huge amount of code
	 * here that basically duplicated execUtils.c ...)
	 */

	resultRelInfo = makeNode(ResultRelInfo);
	InitResultRelInfo(resultRelInfo,
					  rel,
					  1,		/* dummy rangetable index */
					  0);

	ExecOpenIndices(resultRelInfo, false);

	if (resultRelInfo->ri_TrigDesc != NULL)
	{
		elog(ERROR, "triggers on chunk tables not supported");
	}

	rel_state = insert_chunk_state_rel_new(rel, resultRelInfo, range_table);
	rel_state_list = lappend(rel_state_list, rel_state);

	state->replica_states = rel_state_list;
	state->chunk = chunk;
	return state;
}

extern void
insert_chunk_state_destroy(InsertChunkState *state)
{
	ListCell   *lc;

	if (state == NULL)
	{
		return;
	}

	foreach(lc, state->replica_states)
	{
		InsertChunkStateRel *rel_state = lfirst(lc);

		insert_chunk_state_rel_destroy(rel_state);
	}
}

extern void
insert_chunk_state_insert_tuple(InsertChunkState *state, HeapTuple tup)
{
	ListCell   *lc;

	foreach(lc, state->replica_states)
	{
		InsertChunkStateRel *rel_state = lfirst(lc);

		insert_chunk_state_rel_insert_tuple(rel_state, tup);
	}
}
