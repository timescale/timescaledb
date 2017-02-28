#include "postgres.h"
#include "funcapi.h"
#include "access/htup_details.h"
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
#include "utils/tqual.h"

#include "access/xact.h"
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

#define INSERT_TRIGGER_COPY_TABLE_FN	"insert_trigger_on_copy_table_c"
#define INSERT_TRIGGER_COPY_TABLE_NAME	"insert_trigger"

/* private funcs */
static int	tuple_fnumber(TupleDesc tupdesc, const char *fname);

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
		single = create_fmgr("_iobeamdb_internal", "close_chunk_if_needed", 1);
		MemoryContextSwitchTo(old);
	}
	return single;
}

PG_FUNCTION_INFO_V1(insert_trigger_on_copy_table_c);

Datum
insert_trigger_on_copy_table_c(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;

	/* arg 0 = hypertable id */
	char	   *hypertable_id_arg = trigdata->tg_trigger->tgargs[0];

	HeapTuple	firstrow;
	hypertable_cache_entry *hci;
	int			time_fnum,
				i,
				num_chunks;
	bool		isnull;

	List	   *chunk_id_list = NIL;
	ListCell   *chunk_id_cell;
	int		   *chunk_id_array;

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
	HeapScanDesc scan;
	ScanKeyData scankey[1];
	int nkeys = 0;
	
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
	hci = hypertable_cache_get(atoi(hypertable_id_arg));
	time_fnum = tuple_fnumber(trigdata->tg_relation->rd_att, hci->time_column_name);

	scan = heap_beginscan(trigdata->tg_relation, SnapshotSelf, nkeys, scankey);
 
	/* get one row in a loop until the copy table is empty. */
	while ((firstrow = heap_getnext(scan, ForwardScanDirection)))
	{
		Datum		time_datum;
		int64		time_internal;
		epoch_and_partitions_set *pe_entry;
		Partition   *part = NULL;
		chunk_cache_entry *chunk;
		int			ret;
		
		time_datum = heap_getattr(firstrow, time_fnum, trigdata->tg_relation->rd_att, &isnull);

		if (isnull)
		{
			elog(ERROR, "Time column is null");
		}

		time_internal = time_value_to_internal(time_datum, hci->time_column_type);
		
		pe_entry = hypertable_cache_get_partition_epoch(hci, time_internal,
														trigdata->tg_relation->rd_id);

		if (pe_entry->partitioning != NULL && pe_entry->num_partitions > 1)
		{
			PartitioningInfo *pi = pe_entry->partitioning;
			Datum part_value = heap_getattr(firstrow, pi->column_attnum,
											trigdata->tg_relation->rd_att, &isnull);
			int16 keyspace_pt = partitioning_func_apply(&pi->partfunc, part_value);

			/* get the partition using the keyspace value of the row. */
			part = partition_epoch_get_partition(pe_entry, keyspace_pt);
		}
		else
		{
			/* Not Partitioning: get the first and only partition */
			part = partition_epoch_get_partition(pe_entry, -1);
		}

		chunk = get_chunk_cache_entry(hci, pe_entry, part, time_internal, true);
		
		if (chunk->chunk->end_time == OPEN_END_TIME)
		{
			chunk_id_list = lappend_int(chunk_id_list, chunk->id);
		}
		if (SPI_connect() < 0)
		{
			elog(ERROR, "Got an SPI connect error");
		}
		ret = SPI_execute_plan(chunk->move_from_copyt_plan, NULL, NULL, false, 1);
		if (ret <= 0)
		{
			elog(ERROR, "Got an SPI error %d", ret);
		}
		SPI_finish();

	}

	heap_endscan(scan);
	
	/* build chunk id array */
	num_chunks = list_length(chunk_id_list);
	chunk_id_array = palloc(sizeof(int) * num_chunks);
	i = 0;
	foreach(chunk_id_cell, chunk_id_list)
	{
		int			chunk_id = lfirst_int(chunk_id_cell);

		chunk_id_array[i++] = chunk_id;
	}
	/* sort by chunk_id to avoid deadlocks */
	qsort(chunk_id_array, num_chunks, sizeof(int), int_cmp);

	/* close chunks */
	for (i = 0; i < num_chunks; i++)
	{
		/* TODO: running this on every insert is really expensive */
		/* Keep some kind of cache of result an run this heuristically. */
		int32		chunk_id = chunk_id_array[i];

		FunctionCall1(get_close_if_needed_fn(), Int32GetDatum(chunk_id));
	}
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
	createTrig->funcname = list_make2(makeString(IOBEAMDB_INTERNAL_SCHEMA), makeString(INSERT_TRIGGER_COPY_TABLE_FN));
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


static int
tuple_fnumber(TupleDesc tupdesc, const char *fname)
{
	int			res;

	for (res = 0; res < tupdesc->natts; res++)
	{
		if (namestrcmp(&tupdesc->attrs[res]->attname, fname) == 0)
			return res + 1;
	}

	elog(ERROR, "field not found: %s", fname);
}

