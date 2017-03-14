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

#include "insert.h"
#include "cache.h"
#include "hypertable_cache.h"
#include "chunk_cache.h"
#include "errors.h"
#include "utils.h"
#include "metadata_queries.h"
#include "partitioning.h"
#include "scanner.h"
#include "catalog.h"
#include "pgmurmur3.h"
#include "chunk.h"
#include "timescaledb.h"

static FmgrInfo *
get_close_if_needed_fn()
{
	static FmgrInfo *single = NULL;

	if (single == NULL)
	{
		MemoryContext old;

		old = MemoryContextSwitchTo(TopMemoryContext);
		single = create_fmgr("_timescaledb_internal", "close_chunk_if_needed", 1);
		MemoryContextSwitchTo(old);
	}
	return single;
}

static void
close_if_needed(Hypertable * hci, Chunk * chunk)
{
	ChunkReplica *cr;
	Catalog    *catalog = catalog_get();

	cr = chunk_get_replica(chunk, catalog->database_name);

	if (cr != NULL && hci->chunk_size_bytes > chunk_replica_size_bytes(cr))
	{
		return;
	}
	FunctionCall1(get_close_if_needed_fn(), Int32GetDatum(chunk->id));
}

/*
 * Helper functions for inserting tuples into chunk tables
 *
 * We insert one chunk at a time and hold a context while we insert
 * a particular chunk;
 */
typedef struct ChunkInsertCtxRel
{
	Relation	rel;
	TupleTableSlot *slot;
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	BulkInsertState bistate;
}	ChunkInsertCtxRel;

static ChunkInsertCtxRel *
chunk_insert_ctx_rel_new(Relation rel, ResultRelInfo *resultRelInfo, List *range_table)
{
	TupleDesc	tupDesc;
	ChunkInsertCtxRel *rel_ctx = palloc(sizeof(ChunkInsertCtxRel));

	rel_ctx->estate = CreateExecutorState();
	tupDesc = RelationGetDescr(rel);

	rel_ctx->estate->es_result_relations = resultRelInfo;
	rel_ctx->estate->es_num_result_relations = 1;
	rel_ctx->estate->es_result_relation_info = resultRelInfo;
	rel_ctx->estate->es_range_table = range_table;

	rel_ctx->slot = ExecInitExtraTupleSlot(rel_ctx->estate);
	ExecSetSlotDescriptor(rel_ctx->slot, tupDesc);

	rel_ctx->rel = rel;
	rel_ctx->resultRelInfo = resultRelInfo;
	rel_ctx->bistate = GetBulkInsertState();
	return rel_ctx;
}

static void
chunk_insert_ctx_rel_destroy(ChunkInsertCtxRel * rel_ctx)
{
	FreeBulkInsertState(rel_ctx->bistate);
	ExecCloseIndices(rel_ctx->resultRelInfo);
	ExecResetTupleTable(rel_ctx->estate->es_tupleTable, false);
	FreeExecutorState(rel_ctx->estate);
	heap_close(rel_ctx->rel, NoLock);
}


static void
chunk_insert_ctx_rel_insert_tuple(ChunkInsertCtxRel * rel_ctx, HeapTuple tuple)
{
	int			hi_options = 0; /* no optimization */
	CommandId	mycid = GetCurrentCommandId(true);

	/*
	 * Constraints might reference the tableoid column, so initialize
	 * t_tableOid before evaluating them.
	 */
	tuple->t_tableOid = RelationGetRelid(rel_ctx->rel);

	ExecStoreTuple(tuple, rel_ctx->slot, InvalidBuffer, false);

	if (rel_ctx->rel->rd_att->constr)
		ExecConstraints(rel_ctx->resultRelInfo, rel_ctx->slot, rel_ctx->estate);

	/* OK, store the tuple and create index entries for it */
	heap_insert(rel_ctx->rel, tuple, mycid, hi_options, rel_ctx->bistate);

	if (rel_ctx->resultRelInfo->ri_NumIndices > 0)
		ExecInsertIndexTuples(rel_ctx->slot, &(tuple->t_self),
							  rel_ctx->estate, false, NULL,
							  NIL);
}

typedef struct ChunkInsertCtx
{
	Chunk	   *chunk;
	Cache	   *pinned;
	List	   *ctxs;
}	ChunkInsertCtx;

static ChunkInsertCtx *
chunk_insert_ctx_new(Chunk * chunk, Cache * pinned)
{
	List	   *rel_ctx_list = NIL;
	ChunkInsertCtx *ctx;
	int			i;

	ctx = palloc(sizeof(ChunkInsertCtx));
	ctx->pinned = pinned;

	for (i = 0; i < chunk->num_replicas; i++)
	{
		ChunkReplica *cr = &chunk->replicas[i];
		Relation	rel;
		RangeTblEntry *rte;
		List	   *range_table;
		ResultRelInfo *resultRelInfo;
		ChunkInsertCtxRel *rel_ctx;;

		rel = heap_open(cr->table_id, RowExclusiveLock);

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
		 * index-entry-making machinery.  (There used to be a huge amount of
		 * code here that basically duplicated execUtils.c ...)
		 */

		resultRelInfo = makeNode(ResultRelInfo);
		InitResultRelInfo(resultRelInfo,
						  rel,
						  1,	/* dummy rangetable index */
						  0);

		ExecOpenIndices(resultRelInfo, false);

		if (resultRelInfo->ri_TrigDesc != NULL)
		{
			elog(ERROR, "triggers on chunk tables not supported");
		}

		rel_ctx = chunk_insert_ctx_rel_new(rel, resultRelInfo, range_table);
		rel_ctx_list = lappend(rel_ctx_list, rel_ctx);
	}

	ctx->ctxs = rel_ctx_list;
	ctx->chunk = chunk;
	return ctx;
}

static void
chunk_insert_ctx_destroy(ChunkInsertCtx * ctx)
{
	ListCell   *lc;

	if (ctx == NULL)
	{
		return;
	}

	cache_release(ctx->pinned);

	foreach(lc, ctx->ctxs)
	{
		ChunkInsertCtxRel *rel_ctx = lfirst(lc);

		chunk_insert_ctx_rel_destroy(rel_ctx);
	}
}

static void
chunk_insert_ctx_insert_tuple(ChunkInsertCtx * ctx, HeapTuple tup)
{
	ListCell   *lc;

	foreach(lc, ctx->ctxs)
	{
		ChunkInsertCtxRel *rel_ctx = lfirst(lc);

		chunk_insert_ctx_rel_insert_tuple(rel_ctx, tup);
	}
}

Datum		insert_root_table_trigger(PG_FUNCTION_ARGS);
Datum		insert_root_table_trigger_after(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(insert_root_table_trigger);
PG_FUNCTION_INFO_V1(insert_root_table_trigger_after);

typedef struct InsertTriggerCtx
{
	Cache	   *hypertable_cache;
	Hypertable *hypertable;
	PartitionEpoch *epoch;
	Oid			relid;
	AttrNumber	time_attno;
	Partition  *part;
	ChunkInsertCtx *chunk_ctx;
	Tuplesortstate *sort;
	TupleDesc	expanded_tupdesc;
	HeapTuple	first_tuple;
	MemoryContext mctx;
}	InsertTriggerCtx;


static InsertTriggerCtx *insert_trigger_ctx;

static TupleDesc
tuple_desc_expand(TupleDesc old_tupdesc, AttrNumber *time_attno, const char *time_column_name)
{
	TupleDesc	new_tupdesc = CreateTemplateTupleDesc(old_tupdesc->natts + 1, false);
	int			i;

	for (i = 1; i <= old_tupdesc->natts; i++)
	{
		if (strncmp(old_tupdesc->attrs[i - 1]->attname.data, time_column_name, NAMEDATALEN) == 0)
		{
			*time_attno = i;
		}
		TupleDescCopyEntry(new_tupdesc, (AttrNumber) i, old_tupdesc, (AttrNumber) i);
	}

	TupleDescInitEntry(new_tupdesc, (AttrNumber) new_tupdesc->natts,
					   CATALOG_SCHEMA_NAME "_partition_id", INT4OID, -1, 0);

	return BlessTupleDesc(new_tupdesc);
}

static Oid
time_type_to_sort_type(Oid time_type)
{
	switch (time_type)
	{
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return INT8OID;
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return time_type;
		default:
			elog(ERROR, "Unsupported time type %u", time_type);
	}
}

static Tuplesortstate *
tuple_sort_state_init(TupleDesc tupdesc, AttrNumber time_attno, Oid time_type)
{
	Oid			sort_time_type = time_type_to_sort_type(time_type);
	AttrNumber	columns[2] = {
		tupdesc->natts,
		time_attno,
	};
	Oid			ops[2] = {
		get_opfamily_member(INTEGER_BTREE_FAM_OID, INT4OID, INT4OID, BTLessStrategyNumber),
		get_opfamily_member(INTEGER_BTREE_FAM_OID, sort_time_type, sort_time_type, BTLessStrategyNumber),
	};
	Oid			collations[2] = {InvalidOid, InvalidOid};
	bool		nullsFirstFlags[2] = {false, false};

	return tuplesort_begin_heap(tupdesc, 2, columns, ops, collations, nullsFirstFlags, work_mem, false);
}

static InsertTriggerCtx *
insert_trigger_ctx_create(HeapTuple tuple, Oid main_table_relid, Oid relid)
{
	MemoryContext mctx = AllocSetContextCreate(CacheMemoryContext,
											   "Insert context",
											   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldctx;
	InsertTriggerCtx *tctx;

	oldctx = MemoryContextSwitchTo(mctx);

	tctx = palloc0(sizeof(InsertTriggerCtx));
	tctx->mctx = mctx;
	tctx->relid = relid;
	tctx->hypertable_cache = hypertable_cache_pin();
	tctx->hypertable = hypertable_cache_get_entry(tctx->hypertable_cache, main_table_relid);
	tctx->first_tuple = heap_copytuple(tuple);

	MemoryContextSwitchTo(oldctx);

	return tctx;
}

static void
insert_trigger_ctx_sort_init(InsertTriggerCtx * tctx, TupleDesc tupdesc)
{
	tctx->expanded_tupdesc = tuple_desc_expand(tupdesc, &tctx->time_attno, tctx->hypertable->time_column_name);
	tctx->sort = tuple_sort_state_init(tctx->expanded_tupdesc, tctx->time_attno, tctx->hypertable->time_column_type);
}

static void
insert_trigger_ctx_free(InsertTriggerCtx * tctx)
{
	if (tctx->chunk_ctx != NULL)
		chunk_insert_ctx_destroy(tctx->chunk_ctx);

	cache_release(tctx->hypertable_cache);
	MemoryContextDelete(tctx->mctx);
}

static HeapTuple
heap_tuple_add_partition_index(HeapTuple tuple, TupleDesc tupdesc, TupleDesc new_tupdesc, int32 partition_index)
{
	Datum		values[new_tupdesc->natts];
	bool		nulls[new_tupdesc->natts];
	HeapTuple	new_tuple;

	heap_deform_tuple(tuple, tupdesc, values, nulls);

	values[new_tupdesc->natts - 1] = Int32GetDatum(partition_index);
	nulls[new_tupdesc->natts - 1] = false;

	new_tuple = heap_form_tuple(new_tupdesc, values, nulls);

	new_tuple->t_data->t_ctid = tuple->t_data->t_ctid;
	new_tuple->t_self = tuple->t_self;
	new_tuple->t_tableOid = tuple->t_tableOid;

	if (tupdesc->tdhasoid)
		HeapTupleSetOid(new_tuple, HeapTupleGetOid(tuple));

	return new_tuple;
}

static void
insert_tuple(InsertTriggerCtx * tctx, TupleInfo * ti, int partition_index, int64 time_pt)
{
	if (tctx->chunk_ctx != NULL && !chunk_timepoint_is_member(tctx->chunk_ctx->chunk, time_pt))
	{
		/* moving on to next chunk; */
		chunk_insert_ctx_destroy(tctx->chunk_ctx);
		tctx->chunk_ctx = NULL;
	}

	if (tctx->part != NULL && tctx->part->index != partition_index)
	{
		/* moving on to next partition. */
		chunk_insert_ctx_destroy(tctx->chunk_ctx);
		tctx->chunk_ctx = NULL;
	}

	tctx->part = &tctx->epoch->partitions[partition_index];

	if (tctx->chunk_ctx == NULL)
	{
		Chunk	   *chunk;
		Cache	   *pinned = chunk_cache_pin();

		/*
		 * TODO: this first call should be non-locking and use a cache(for
		 * performance)
		 */
		chunk = chunk_cache_get(pinned, tctx->part, tctx->hypertable->num_replicas, time_pt, false);
		close_if_needed(tctx->hypertable, chunk);

		/*
		 * chunk may have been closed and thus changed /or/ need to get share
		 * lock
		 */
		chunk = chunk_cache_get(pinned, tctx->part, tctx->hypertable->num_replicas, time_pt, true);

		tctx->chunk_ctx = chunk_insert_ctx_new(chunk, pinned);
	}

	/* insert here: */
	chunk_insert_ctx_insert_tuple(tctx->chunk_ctx, ti->tuple);
}

static Partition *
insert_trigger_ctx_lookup_partition(InsertTriggerCtx * tctx, HeapTuple tuple,
									TupleDesc tupdesc, int64 *timepoint_out)
{
	Datum		datum;
	bool		isnull;
	int64		timepoint,
				spacepoint;
	PartitionEpoch *epoch;

	/*
	 * Get the timepoint from the tuple, converting to our internal time
	 * representation
	 */
	datum = heap_getattr(tuple, tctx->time_attno, tupdesc, &isnull);

	if (isnull)
	{
		elog(ERROR, "No time attribute in tuple");
	}

	timepoint = time_value_to_internal(datum, tctx->hypertable->time_column_type);

	epoch = hypertable_cache_get_partition_epoch(tctx->hypertable_cache, tctx->hypertable,
												 timepoint, tctx->relid);

	/* Save the epoch in the insert state */
	tctx->epoch = epoch;

	if (epoch->num_partitions > 1)
	{
		spacepoint = partitioning_func_apply_tuple(epoch->partitioning, tuple, tupdesc);
	}
	else
	{
		spacepoint = KEYSPACE_PT_NO_PARTITIONING;
	}

	if (timepoint_out != NULL)
		*timepoint_out = timepoint;

	return partition_epoch_get_partition(epoch, spacepoint);
}


static void
insert_trigger_ctx_tuplesort_put(InsertTriggerCtx * tctx, HeapTuple tuple, TupleDesc tupdesc)
{
	Partition  *part;
	TupleTableSlot *slot;

	/*
	 * Get the epoch (time) and partition (space) based on the information in
	 * the tuple
	 */
	part = insert_trigger_ctx_lookup_partition(tctx, tuple, tupdesc, NULL);

	/*
	 * Create a new (expanded) tuple from the old one that has the partition
	 * index as the last attribute
	 */
	tuple = heap_tuple_add_partition_index(tuple, tupdesc, tctx->expanded_tupdesc, part->index);

	/* Put the new tuple into the tuple sort set */
	slot = MakeSingleTupleTableSlot(tctx->expanded_tupdesc);
	slot = ExecStoreTuple(tuple, slot, InvalidBuffer, false);
	tuplesort_puttupleslot(tctx->sort, slot);
	ExecClearTuple(slot);
}

/*
 * This row-level trigger is called for every row INSERTed into a hypertable. We
 * use it to redirect inserted tuples to the correct hypertable chunk in space
 * and time.
 *
 * To avoid deadlocks, we need to insert tuples in partition and chunk/time
 * order. Therefore, we collect tuples for every insert batch, sort them at the
 * end of the batch, and finally insert in chunk order.
 *
 * Tuples are collected into a context that keeps the state across single
 * invocations of the row trigger. The insert context is allocated on the first
 * row of a batch, and reset in an 'after' trigger when the batch completes. The
 * insert context is tracked via a static/global pointer.
 *
 * The insert trigger supports two processing paths, depending on single-row or
 * multi-row batch inserts:
 *
 * Single-row (fast) path:
 * =====================
 *
 * For single-row batches, no sorting is needed. Therefore, the trigger
 * allocates only a partial insert state for the first tuple, eschewing the
 * sorting state. If the after trigger is called without the sorting state,
 * there is only one tuple and no sorting occurs in the 'after' trigger.
 *
 *
 * Multi-row (slow) path:
 * ======================
 *
 * For multi-row batches, sorting is required. On the second tuple encountered a
 * sorting state is initialized and both the first tuple and all following
 * tuples are inserted into the sorting state. All tuples are sorted in the
 * 'after' trigger before insertion into chunks.
 */
Datum
insert_root_table_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	InsertTriggerCtx *tctx = insert_trigger_ctx;
	char	*main_table_schema = trigdata->tg_trigger->tgargs[0];
	char    *main_table_name = trigdata->tg_trigger->tgargs[1];
	Oid		main_table_oid = get_relname_relid(main_table_name, get_namespace_oid(main_table_schema, false));
	HeapTuple	tuple;
	TupleDesc	tupdesc = trigdata->tg_relation->rd_att;
	MemoryContext oldctx;

	/* Check that this is called the way it should be */
	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "Trigger not called by trigger manager");

	if (!TRIGGER_FIRED_BEFORE(trigdata->tg_event))
		elog(ERROR, "Trigger should only fire before insert");

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		tuple = trigdata->tg_newtuple;
	else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		tuple = trigdata->tg_trigtuple;
	else
		elog(ERROR, "Unsupported event for trigger");

	if (insert_trigger_ctx == NULL)
	{
		/* This is the first row. Allocate a new insert context */
		insert_trigger_ctx = insert_trigger_ctx_create(tuple, main_table_oid, trigdata->tg_relation->rd_id);
		return PointerGetDatum(NULL);
	}

	/*
	 * Use the insert context's memory context so that the state we use
	 * survives across trigger invocations until the after trigger.
	 */
	oldctx = MemoryContextSwitchTo(tctx->mctx);

	if (tctx->sort == NULL)
	{
		/*
		 * Multi-tuple case, i.e., we must sort the tuples. Initialize the
		 * sort state and put the first tuple that we saved from the last row
		 * trigger in the same batch.
		 */
		insert_trigger_ctx_sort_init(insert_trigger_ctx, tupdesc);
		insert_trigger_ctx_tuplesort_put(insert_trigger_ctx, tctx->first_tuple, tupdesc);
	}

	/* The rest of the tuples in the batch are put into the sort state here */
	insert_trigger_ctx_tuplesort_put(insert_trigger_ctx, tuple, tupdesc);

	MemoryContextSwitchTo(oldctx);

	/* Return NULL since we do not want the tuple in the trigger's table */
	return PointerGetDatum(NULL);
}

Datum
insert_root_table_trigger_after(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	InsertTriggerCtx *tctx = insert_trigger_ctx;
	char	   *insert_guard;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "not called by trigger manager");

	if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event) &&
		!TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		elog(ERROR, "Unsupported event for trigger");

	/*
	 * --This guard protects against calling insert_data() twice in the same
	 * transaction, --which might otherwise cause a deadlock in case the
	 * second insert_data() involves a chunk --that was inserted into in the
	 * first call to insert_data(). --This is a temporary safe guard that
	 * should ideally be removed once chunk management --has been refactored
	 * and improved to avoid such deadlocks. --NOTE: In its current form, this
	 * safe guard unfortunately prohibits transactions --involving INSERTs on
	 * two different hypertables.
	 */
	insert_guard = GetConfigOptionByName("io.insert_data_guard", NULL, true);

	if (insert_guard != NULL && strcmp(insert_guard, "on") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_IO_OPERATION_NOT_SUPPORTED),
		   errmsg("insert_data() can only be called once per transaction")));
	}
	/* set the guard locally (for this transaction) */
	set_config_option("io.insert_data_guard", "on", PGC_USERSET, PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);


	if (tctx->sort == NULL)
	{
		/* Single-tuple batch fast path */
		Partition  *part;
		int64		timepoint;
		TupleInfo	ti = {
			.desc = trigdata->tg_relation->rd_att,
			.tuple = tctx->first_tuple,
		};

		/* We must set the time attno because it was not set on the fast path */
		tctx->time_attno = get_attnum(trigdata->tg_relation->rd_id,
									  tctx->hypertable->time_column_name);
		part = insert_trigger_ctx_lookup_partition(insert_trigger_ctx,
												   ti.tuple,
												   ti.desc,
												   &timepoint);

		insert_tuple(insert_trigger_ctx, &ti, part->index, timepoint);
	}
	else
	{
		/* Multi-tuple batch slow path: sort the tuples */
		TupleDesc	tupdesc = tctx->expanded_tupdesc;
		TupleTableSlot *slot = MakeSingleTupleTableSlot(tupdesc);
		bool		doreplace[trigdata->tg_relation->rd_att->natts];
		bool		validslot;
		Datum		datum;

		memset(doreplace, 0, sizeof(doreplace));

		tuplesort_performsort(tctx->sort);

		/* Loop over the sorted tuples and insert one by one */
		validslot = tuplesort_gettupleslot(tctx->sort, true, slot, NULL);

		while (validslot)
		{
			bool		isnull;
			int			partition_index;
			int64		timepoint;
			HeapTuple	tuple = ExecFetchSlotTuple(slot);
			TupleInfo	ti = {
				.desc = trigdata->tg_relation->rd_att,

				/*
				 * Strip off the partition attribute from the tuple so that we
				 * do not add it to the chunk when we insert.
				 */
				.tuple = heap_modify_tuple(tuple, trigdata->tg_relation->rd_att, NULL, NULL, doreplace),
			};

			datum = heap_getattr(tuple, tupdesc->natts, tupdesc, &isnull);
			partition_index = DatumGetInt32(datum);
			datum = heap_getattr(tuple, tctx->time_attno, tupdesc, &isnull);
			timepoint = time_value_to_internal(datum, tctx->hypertable->time_column_type);

			insert_tuple(insert_trigger_ctx, &ti, partition_index, timepoint);

			ExecClearTuple(slot);
			validslot = tuplesort_gettupleslot(tctx->sort, true, slot, NULL);
		}

		tuplesort_end(tctx->sort);
	}

	insert_trigger_ctx_free(tctx);
	insert_trigger_ctx = NULL;

	return PointerGetDatum(NULL);
}
