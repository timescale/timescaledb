#include "postgres.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "access/relscan.h"
#include "commands/defrem.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_class.h"
#include "optimizer/planner.h"
#include "optimizer/clauses.h"
#include "nodes/nodes.h"
#include "nodes/print.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/int8.h"
#include "executor/spi.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "deps/dblink.h"
#include "parser/parse_utilcmd.h"
#include "parser/parser.h"

#include "access/xact.h"
#include "access/htup_details.h"
#include "parser/parse_oper.h"
#include "parser/parse_func.h"

#include "fmgr.h"

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

#include <utils/tqual.h>
#include <utils/rls.h>
#include <miscadmin.h>
#include <access/heapam.h>

#define INSERT_TRIGGER_COPY_TABLE_FN	"insert_trigger_on_copy_table_c"
#define INSERT_TRIGGER_COPY_TABLE_NAME	"insert_trigger"

/* private funcs */

static ObjectAddress create_insert_index(int32 hypertable_id, char * time_field, PartitioningInfo *part_info,epoch_and_partitions_set *epoch);
static Node *get_keyspace_fn_call(PartitioningInfo *part_info);

/*
 * Inserts rows from the temporary copy table into correct hypertable child tables.
 * hypertable_id - ID of the hypertable the data belongs to
 */

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

/*
 *
 * Helper functions for inserting tuples into chunk tables
 *
 * We insert one chunk at a time and hold a context while we insert
 * a particular chunk;
 *
 * */


typedef struct ChunkInsertCtxRel
{
	Relation	rel;
	TupleTableSlot *slot;
	EState	   *estate;
	ResultRelInfo *resultRelInfo;
	BulkInsertState bistate;
} ChunkInsertCtxRel;

static ChunkInsertCtxRel*
chunk_insert_ctx_rel_new(Relation	rel, ResultRelInfo *resultRelInfo, List	   *range_table) {
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
chunk_insert_ctx_rel_destroy(ChunkInsertCtxRel *rel_ctx)
{
	FreeBulkInsertState(rel_ctx->bistate);
	ExecCloseIndices(rel_ctx->resultRelInfo);
	ExecResetTupleTable(rel_ctx->estate->es_tupleTable, false);
	FreeExecutorState(rel_ctx->estate);
	heap_close(rel_ctx->rel, NoLock);
}


static void
chunk_insert_ctx_rel_insert_tuple(ChunkInsertCtxRel *rel_ctx, HeapTuple tuple)
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
	chunk_cache_entry *chunk;
	Cache *pinned;
	List	   *ctxs;
} ChunkInsertCtx;

static ChunkInsertCtx *
chunk_insert_ctx_new(chunk_cache_entry *chunk, Cache *pinned)
{
	ListCell   *lc;
	List	   *rel_ctx_list = NIL;
	ChunkInsertCtx *ctx;

	ctx = palloc(sizeof(ChunkInsertCtx));
	ctx->pinned = pinned;

	foreach(lc, chunk->crns->tables)
	{
		crn_row    *cr = lfirst(lc);
		RangeVar   *rv = makeRangeVarFromNameList(list_make2(makeString(cr->schema_name.data), makeString(cr->table_name.data)));
		Relation	rel;
		RangeTblEntry *rte;
		List	   *range_table;
		ResultRelInfo *resultRelInfo;
		ChunkInsertCtxRel *rel_ctx;;

		rel = heap_openrv(rv, RowExclusiveLock);

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
chunk_insert_ctx_destroy(ChunkInsertCtx *ctx)
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
chunk_insert_ctx_insert_tuple(ChunkInsertCtx *ctx, HeapTuple tup)
{
	ListCell   *lc;

	foreach(lc, ctx->ctxs)
	{
		ChunkInsertCtxRel *rel_ctx = lfirst(lc);
		chunk_insert_ctx_rel_insert_tuple(rel_ctx, tup);
	}
}

typedef struct CopyTableQueryCtx {
	Partition *part;
	ChunkInsertCtx *chunk_ctx;
	epoch_and_partitions_set *pe;
	hypertable_cache_entry *hci;
} CopyTableQueryCtx;

static bool
copy_table_tuple_found(TupleInfo *ti, void *data)
{
	bool is_null;
	CopyTableQueryCtx *ctx = data;
	int16		keyspace_pt;
	int64		time_pt;

	if (ctx->pe->num_partitions > 1)
	{
		/* first element is partition index (used for sorting but not necessary here) */
		Datum		time_datum = index_getattr(ti->ituple, 2, ti->ituple_desc, &is_null);
		Datum		keyspace_datum = index_getattr(ti->ituple, 3, ti->ituple_desc, &is_null);

		time_pt = time_value_to_internal(time_datum, ctx->hci->time_column_type);
		keyspace_pt = DatumGetInt16(keyspace_datum);
	}
	else
	{
		Datum		time_datum = index_getattr(ti->ituple, 1, ti->ituple_desc, &is_null);
		time_pt = time_value_to_internal(time_datum, ctx->hci->time_column_type);
		keyspace_pt = KEYSPACE_PT_NO_PARTITIONING;
	}


	if (ctx->chunk_ctx != NULL && !chunk_row_timepoint_is_member(ctx->chunk_ctx->chunk->chunk, time_pt))
	{
		/* moving on to next chunk; */
		chunk_insert_ctx_destroy(ctx->chunk_ctx);
		ctx->chunk_ctx = NULL;

	}
	if (ctx->part != NULL && !partition_keyspace_pt_is_member(ctx->part, keyspace_pt))
	{
		/* moving on to next ctx->partition. */
		chunk_insert_ctx_destroy(ctx->chunk_ctx);
		ctx->chunk_ctx = NULL;
		ctx->part = NULL;
	}

	if (ctx->part == NULL)
	{
		ctx->part = partition_epoch_get_partition(ctx->pe, keyspace_pt);
	}

	if (ctx->chunk_ctx == NULL)
	{
		Datum		was_closed_datum;
		chunk_cache_entry *chunk;
		Cache *pinned = chunk_crn_set_cache_pin();
		/*
		 * TODO: this first call should be non-locking and use a cache(for
		 * performance)
		 */
		chunk = get_chunk_cache_entry(pinned, ctx->part, time_pt, false);
		was_closed_datum = FunctionCall1(get_close_if_needed_fn(), Int32GetDatum(chunk->id));
		/* chunk may have been closed and thus changed /or/ need to get share lock */
		chunk = get_chunk_cache_entry(pinned, ctx->part, time_pt, true);

		ctx->chunk_ctx = chunk_insert_ctx_new(chunk, pinned);
	}

	/* insert here: */
	/* has to be a copy(not sure why) */
	chunk_insert_ctx_insert_tuple(ctx->chunk_ctx,heap_copytuple(ti->tuple));
	return true;
}

static void scan_copy_table_and_insert_post(int num_tuples, void *data)
{
	CopyTableQueryCtx *ctx = data;
	if (ctx->chunk_ctx != NULL)
		chunk_insert_ctx_destroy(ctx->chunk_ctx);
}

static void scan_copy_table_and_insert( hypertable_cache_entry *hci,
							epoch_and_partitions_set *pe,
							Oid table, Oid index)
{
	CopyTableQueryCtx query_ctx = {
		.pe = pe,
		.hci = hci,
	};

	ScannerCtx scanCtx = {
		.table = table,
		.index = index,
		.scantype = ScannerTypeIndex,
		.want_itup = true,
		.nkeys = 0,
		.scankey = NULL,
		.data = &query_ctx,
		.tuple_found = copy_table_tuple_found,
		.postscan = scan_copy_table_and_insert_post,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on primary key. */
	scanner_scan(&scanCtx);
}


PG_FUNCTION_INFO_V1(insert_trigger_on_copy_table_c);
Datum
insert_trigger_on_copy_table_c(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;

	/* arg 0 = hypertable id */
	char	   *hypertable_id_arg = trigdata->tg_trigger->tgargs[0];
	int32		hypertable_id = atoi(hypertable_id_arg);

	hypertable_cache_entry *hci;

	epoch_and_partitions_set *pe;
	Cache *hypertable_cache;
	ObjectAddress idx;

	DropStmt   *drop = makeNode(DropStmt);

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
	char	   *insert_guard = GetConfigOptionByName("io.insert_data_guard", NULL, true);

	if (insert_guard != NULL && strcmp(insert_guard, "on") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_IO_OPERATION_NOT_SUPPORTED),
		   errmsg("insert_data() can only be called once per transaction")));
	}
	/* set the guard locally (for this transaction) */
	set_config_option("io.insert_data_guard", "on", PGC_USERSET, PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	/*
	 * get the hypertable cache; use the time column name to figure out the
	 * column fnum for time field
	 */
	hypertable_cache = hypertable_cache_pin();

	hci = hypertable_cache_get_entry(hypertable_cache, hypertable_id);

	/* TODO: hack assumes single pe. */
	pe = hypertable_cache_get_partition_epoch(hypertable_cache, hci, 0, trigdata->tg_relation->rd_id);

	/*
	 * create an index that colocates row from the same chunk together and
	 * guarantees an order on chunk access as well
	 */
	idx = create_insert_index(hypertable_id, hci->time_column_name, pe->partitioning, pe);


	scan_copy_table_and_insert(hci, pe, trigdata->tg_relation->rd_id, idx.objectId);

	cache_release(hypertable_cache);

	drop->removeType = OBJECT_INDEX;
	drop->missing_ok = FALSE;
	drop->objects = list_make1(list_make1(makeString("copy_insert")));
	drop->arguments = NIL;
	drop->behavior = DROP_RESTRICT;
	drop->concurrent = false;

	RemoveRelations(drop);
	return PointerGetDatum(NULL);
}


/* Creates a temp table for INSERT and COPY commands. This table
 * stores the data until it is distributed to the appropriate chunks.
 * The copy table uses ON COMMIT DELETE ROWS and inherits from the root table.
 * */
Oid
create_copy_table(int32 hypertable_id, Oid root_oid)
{
	/*
	 * Inserting into a hypertable transformed into inserting into a "copy"
	 * temporary table that has a trigger which calls insert_data afterwords
	 */
	Oid			copy_table_relid;
	ObjectAddress copyTableRelationAddr;
	StringInfo	temp_table_name = makeStringInfo();
	StringInfo	hypertable_id_arg = makeStringInfo();
	RangeVar   *parent,
			   *rel;
	CreateStmt *create;
	CreateTrigStmt *createTrig;

	appendStringInfo(temp_table_name, "_copy_temp_%d", hypertable_id);
	appendStringInfo(hypertable_id_arg, "%d", hypertable_id);

	parent = makeRangeVarFromRelid(root_oid);

	rel = makeRangeVar("pg_temp", copy_table_name(hypertable_id), -1);
	rel->relpersistence = RELPERSISTENCE_TEMP;

	RangeVarGetAndCheckCreationNamespace(rel, NoLock, &copy_table_relid);

	if (OidIsValid(copy_table_relid))
	{
		return copy_table_relid;
	}

	create = makeNode(CreateStmt);

	/*
	 * Create the target relation by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 */
	create->relation = rel;
	create->tableElts = NIL;
	create->inhRelations = list_make1(parent);
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = NIL;
	create->oncommit = ONCOMMIT_DELETE_ROWS;
	create->tablespacename = NULL;
	create->if_not_exists = false;

	copyTableRelationAddr = DefineRelation(create, RELKIND_RELATION, InvalidOid, NULL);

	createTrig = makeNode(CreateTrigStmt);
	createTrig->trigname = INSERT_TRIGGER_COPY_TABLE_NAME;
	createTrig->relation = rel;
	createTrig->funcname = list_make2(makeString(TIMESCALEDB_INTERNAL_SCHEMA), makeString(INSERT_TRIGGER_COPY_TABLE_FN));
	createTrig->args = list_make1(makeString(hypertable_id_arg->data));
	createTrig->row = false;
	createTrig->timing = TRIGGER_TYPE_AFTER;
	createTrig->events = TRIGGER_TYPE_INSERT;
	createTrig->columns = NIL;
	createTrig->whenClause = NULL;
	createTrig->isconstraint = FALSE;
	createTrig->deferrable = FALSE;
	createTrig->initdeferred = FALSE;
	createTrig->constrrel = NULL;

	CreateTrigger(createTrig, NULL, copyTableRelationAddr.objectId, 0, 0, 0, false);

	/* make trigger visible */
	CommandCounterIncrement();

	return copyTableRelationAddr.objectId;
}

static IndexElem *
makeIndexElem(char *name, Node *expr){
	Assert((name ==NULL || expr == NULL) && (name !=NULL || expr !=NULL));

	IndexElem *time_elem = makeNode(IndexElem);
	time_elem->name = name;
	time_elem->expr = expr;
	time_elem->indexcolname = NULL;
	time_elem->collation = NIL;
	time_elem->opclass = NIL;
	time_elem->ordering = SORTBY_DEFAULT;
	time_elem->nulls_ordering = SORTBY_NULLS_DEFAULT;
	return time_elem;
}

/* creates index for inserting in set chunk order.
 *
 * The index is the following:
 *	If there is a partitioning_func:
 *		partition_no, time, keyspace_value
 *	If there is no partitioning_func:
 *		time
 *
 *	Partition_num is simply a unique number identifying the partition for the epoch the row belongs to.
 *	It is obtained by getting the maximal index in the end_time_partitions array such that the keyspace value
 *	is less than or equal to the value in the array.
 *
 *	Keyspace_value without partition_num is not sufficient because:
 *		consider the partitions with keyspaces 0-5,6-10, and time partitions 100-200,201-300
 *		Then consider the following input:
 *			row 1: keyspace=0, time=100
 *			row 2: keyspace=2, time=250
 *			row 3: keyspace=4, time=100
 *		row 1 and 3 should be in the same chunk but they are now not together in the order (row 2 is between them).
 *
 *	keyspace_value should probably be moved out of the index.
 *
 * */
static ObjectAddress
create_insert_index(int32 hypertable_id, char *time_field, PartitioningInfo *part_info, epoch_and_partitions_set *epoch)
{
	IndexStmt  *index_stmt = makeNode(IndexStmt);
	IndexElem  *time_elem;
	Oid			relid;
	List	   *indexElem = NIL;
	int			i;

	time_elem = makeIndexElem(time_field, NULL);

	if (part_info != NULL)
	{
		IndexElem  *partition_elem;
		IndexElem  *keyspace_elem;
		List	   *array_pos_func_name = list_make2(makeString(CATALOG_SCHEMA_NAME), makeString(ARRAY_POSITION_LEAST_FN_NAME));
		List	   *array_pos_args;
		List	   *array_list = NIL;
		A_ArrayExpr *array_expr;
		FuncCall   *array_pos_fc;

		for (i = 0; i < epoch->num_partitions; i++)
		{
			A_Const    *end_time_const = makeNode(A_Const);
			TypeCast   *cast = makeNode(TypeCast);

			end_time_const->val = *makeInteger((int) epoch->partitions[i].keyspace_end);
			end_time_const->location = -1;


			cast->arg = (Node *) end_time_const;
			cast->typeName = SystemTypeName("int2");
			cast->location = -1;

			array_list = lappend(array_list, cast);
		}

		array_expr = makeNode(A_ArrayExpr);
		array_expr->elements = array_list;
		array_expr->location = -1;

		array_pos_args = list_make2(array_expr, get_keyspace_fn_call(part_info));
		array_pos_fc = makeFuncCall(array_pos_func_name, array_pos_args, -1);

		partition_elem = makeIndexElem(NULL, (Node *) array_pos_fc);
		keyspace_elem = makeIndexElem(NULL, (Node *) get_keyspace_fn_call(part_info));

		/* partition_number, time, keyspace */
		/* can probably get rid of keyspace but later */
		indexElem = list_make3(partition_elem, time_elem, keyspace_elem);
	}
	else
	{
		indexElem = list_make1(time_elem);
	}

	index_stmt->idxname = "copy_insert";
	index_stmt->relation = makeRangeVar("pg_temp", copy_table_name(hypertable_id), -1);
	index_stmt->accessMethod = "btree";
	index_stmt->indexParams = indexElem;

	relid =
		RangeVarGetRelidExtended(index_stmt->relation, ShareLock,
								 false, false,
								 RangeVarCallbackOwnsRelation,
								 NULL);

	index_stmt = transformIndexStmt(relid, index_stmt, "");
	return DefineIndex(relid,	/* OID of heap relation */
					   index_stmt,
					   InvalidOid,		/* no predefined OID */
					   false,	/* is_alter_table */
					   true,	/* check_rights */
					   false,	/* skip_build */
					   false);	/* quiet */

}

/* Helper function to create the FuncCall for calculating the keyspace_value. Used for
 * creating the copy_insert index
 *
 */
static Node *
get_keyspace_fn_call(PartitioningInfo *part_info)
{
	ColumnRef  *col_ref = makeNode(ColumnRef);
	A_Const    *mod_const;
	List	   *part_func_name = list_make2(makeString(part_info->partfunc.schema), makeString(part_info->partfunc.name));
	List	   *part_func_args;

	col_ref->fields = list_make1(makeString(part_info->column));
	col_ref->location = -1;

	mod_const = makeNode(A_Const);
	mod_const->val = *makeInteger(part_info->partfunc.modulos);
	mod_const->location = -1;

	part_func_args = list_make2(col_ref, mod_const);
	return (Node *) makeFuncCall(part_func_name, part_func_args, -1);
}
