#include "metadata_queries.h"
#include "utils.h"
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/catcache.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "access/xact.h"

#define DEFINE_PLAN(fnname, query, num, args) \
  static SPIPlanPtr fnname() {\
	static SPIPlanPtr plan = NULL; \
	if(plan != NULL) \
	  return plan; \
	plan = prepare_plan(query, num, args); \
	return plan; \
  }

/* Utility function to prepare an SPI plan */
SPIPlanPtr
prepare_plan(const char *src, int nargs, Oid *argtypes)
{
	SPIPlanPtr	plan;

	if (SPI_connect() < 0)
	{
		elog(ERROR, "Could not connect for prepare");
	}
	plan = SPI_prepare(src, nargs, argtypes);
	if (NULL == plan)
	{
		elog(ERROR, "Could not prepare plan");
	}
	if (SPI_keepplan(plan) != 0)
	{
		elog(ERROR, "Could not keep plan");
	}
	if (SPI_finish() < 0)
	{
		elog(ERROR, "Could not finish for prepare");
	}

	return plan;
}

/*
 *
 *	Functions for fetching info from the db.
 *
 */

#define HYPERTABLE_QUERY_ARGS (Oid[]) { INT4OID }
#define HYPERTABLE_QUERY "SELECT id, time_column_name, time_column_type FROM _iobeamdb_catalog.hypertable h WHERE h.id = $1"
DEFINE_PLAN(get_hypertable_plan, HYPERTABLE_QUERY, 1, HYPERTABLE_QUERY_ARGS)

hypertable_basic_info *
fetch_hypertable_info(hypertable_basic_info *entry, int32 hypertable_id)
{
	SPIPlanPtr	plan = get_hypertable_plan();
	Datum		args[1] = {Int32GetDatum(hypertable_id)};
	int			ret;
	bool		is_null;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Name		time_column_name;
	int			sql_len = NAMEDATALEN * 2 + 100;
	char		get_one_tuple_copyt_sql[sql_len];

	if (entry == NULL)
	{
		entry = palloc(sizeof(hypertable_basic_info));
	}
	CACHE2_elog(WARNING, "Looking up hypertable info: %d", hypertable_id);

	if (SPI_connect() < 0)
	{
		elog(ERROR, "Got an SPI connect error");
	}
	ret = SPI_execute_plan(plan, args, NULL, true, 2);
	if (ret <= 0)
	{
		elog(ERROR, "Got an SPI error %d", ret);
	}
	if (SPI_processed != 1)
	{
		elog(ERROR, "Got not 1 row but %lu", SPI_processed);
	}

	tupdesc = SPI_tuptable->tupdesc;
	tuple = SPI_tuptable->vals[0];

	entry->id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &is_null));

	time_column_name = DatumGetName(SPI_getbinval(tuple, tupdesc, 2, &is_null));
	memcpy(entry->time_column_name.data, time_column_name, NAMEDATALEN);

	entry->time_column_type = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 3, &is_null));

	SPI_finish();
	snprintf(get_one_tuple_copyt_sql, sql_len, "SELECT * FROM %s LIMIT 1", copy_table_name(entry->id));
	entry->get_one_tuple_copyt_plan = prepare_plan(get_one_tuple_copyt_sql, 0, NULL);
	return entry;
}


#define EPOCH_AND_PARTITION_ARGS (Oid[]) { INT4OID, INT8OID }
#define EPOCH_AND_PARTITION_QUERY "SELECT  pe.id as epoch_id, hypertable_id, start_time, end_time, \
                partitioning_func_schema, partitioning_func, partitioning_mod, partitioning_column, \
                p.id as partition_id, keyspace_start, keyspace_end \
                FROM _iobeamdb_catalog.partition_epoch pe\
                INNER JOIN _iobeamdb_catalog.partition p ON (p.epoch_id = pe.id) \
                WHERE pe.hypertable_id = $1 AND \
                (pe.start_time <= $2 OR pe.start_time IS NULL) AND \
                (pe.end_time   >= $2 OR pe.end_time IS NULL) \
                ORDER BY p.keyspace_start ASC"

DEFINE_PLAN(get_epoch_and_partition_plan, EPOCH_AND_PARTITION_QUERY, 2, EPOCH_AND_PARTITION_ARGS)

epoch_and_partitions_set *
fetch_epoch_and_partitions_set(epoch_and_partitions_set *entry, int32 hypertable_id, int64 time_pt, Oid relid)
{
	MemoryContext orig_ctx = CurrentMemoryContext;
	SPIPlanPtr	plan = get_epoch_and_partition_plan();
	Datum		args[2] = {Int32GetDatum(hypertable_id), Int64GetDatum(time_pt)};
	int			ret,
				j;
	int64		time_ret;
	int32		mod_ret;
	Name		name_ret;
	bool		is_null;
	TupleDesc	tupdesc;
	HeapTuple	tuple;

	if (entry == NULL)
	{
		entry = palloc(sizeof(epoch_and_partitions_set));
	}
	CACHE3_elog(WARNING, "Looking up cache partition_epoch %d %lu", hypertable_id, time_pt);

	if (SPI_connect() < 0)
	{
		elog(ERROR, "Got an SPI connect error");
	}
	ret = SPI_execute_plan(plan, args, NULL, true, 0);
	if (ret <= 0)
	{
		elog(ERROR, "Got an SPI error %d", ret);
	}
	if (SPI_processed < 1)
	{
		elog(ERROR, "Could not find partition epoch");
	}

	tupdesc = SPI_tuptable->tupdesc;
	tuple = SPI_tuptable->vals[0];

	entry->id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &is_null));
	entry->hypertable_id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &is_null));

	time_ret = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 3, &is_null));
	if (is_null)
	{
		entry->start_time = OPEN_START_TIME;
	}
	else
	{
		entry->start_time = time_ret;
	}

	time_ret = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 4, &is_null));
	if (is_null)
	{
		entry->end_time = OPEN_END_TIME;
	}
	else
	{
		entry->end_time = time_ret;
	}

	name_ret = DatumGetName(SPI_getbinval(tuple, tupdesc, 5, &is_null));
	if (is_null)
	{
		entry->partitioning_func_schema = NULL;
	}
	else
	{
		entry->partitioning_func_schema = SPI_palloc(sizeof(NameData));
		memcpy(entry->partitioning_func_schema, name_ret, sizeof(NameData));
	}

	name_ret = DatumGetName(SPI_getbinval(tuple, tupdesc, 6, &is_null));
	if (is_null)
	{
		entry->partitioning_func = NULL;
	}
	else
	{
		entry->partitioning_func = SPI_palloc(sizeof(NameData));
		memcpy(entry->partitioning_func, name_ret, sizeof(NameData));
	}

	mod_ret = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 7, &is_null));
	if (is_null)
	{
		entry->partitioning_mod = -1;
	}
	else
	{
		entry->partitioning_mod = mod_ret;
	}


	name_ret = DatumGetName(SPI_getbinval(tuple, tupdesc, 8, &is_null));
	if (is_null)
	{
		entry->partitioning_column = NULL;
		entry->partitioning_column_attrnumber = InvalidAttrNumber;
		entry->partitioning_column_text_func = InvalidOid;
	}
	else
	{
		Oid			typeoid;
		Oid			textfn;
		bool		isVarlena;
		FmgrInfo   *textfn_finfo = SPI_palloc(sizeof(FmgrInfo));

		entry->partitioning_column = SPI_palloc(sizeof(NameData));
		memcpy(entry->partitioning_column, name_ret, sizeof(NameData));
		entry->partitioning_column_attrnumber = get_attnum(relid, entry->partitioning_column->data);
		typeoid = get_atttype(relid, entry->partitioning_column_attrnumber);
		getTypeOutputInfo(typeoid, &textfn, &isVarlena);
		entry->partitioning_column_text_func = textfn;
		fmgr_info_cxt(textfn, textfn_finfo, orig_ctx);
		entry->partitioning_column_text_func_fmgr = textfn_finfo;
	}

	if (entry->partitioning_func != NULL)
	{
		FmgrInfo   *finfo = SPI_palloc(sizeof(FmgrInfo));
		FuncCandidateList part_func = FuncnameGetCandidates(list_make2(makeString(entry->partitioning_func_schema->data),
								 makeString(entry->partitioning_func->data)),
											   2, NULL, false, false, false);

		if (part_func == NULL)
		{
			elog(ERROR, "couldn't find the partitioning function");
		}
		if (part_func->next != NULL)
		{
			elog(ERROR, "multiple partitioning functions found");
		}

		fmgr_info_cxt(part_func->oid, finfo, orig_ctx);
		entry->partition_func_fmgr = finfo;
	}
	else
	{

		entry->partition_func_fmgr = NULL;
	}

	entry->num_partitions = SPI_processed;
	entry->partitions = SPI_palloc(sizeof(partition_info *) * entry->num_partitions);
	for (j = 0; j < entry->num_partitions; j++)
	{
		HeapTuple	tuple = SPI_tuptable->vals[j];

		entry->partitions[j] = SPI_palloc(sizeof(partition_info));

		entry->partitions[j]->id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 9, &is_null));
		entry->partitions[j]->keyspace_start = DatumGetInt16(SPI_getbinval(tuple, tupdesc, 10, &is_null));
		entry->partitions[j]->keyspace_end = DatumGetInt16(SPI_getbinval(tuple, tupdesc, 11, &is_null));
	}

	SPI_finish();
	return entry;
}

void
free_epoch(epoch_and_partitions_set *epoch)
{
	int			j;

	for (j = 0; j < epoch->num_partitions; j++)
	{
		partition_info *part = epoch->partitions[j];

		pfree(part);
	}
	pfree(epoch);
}

#define CRN_QUERY_ARGS (Oid[]) { INT4OID }
#define CRN_QUERY "SELECT schema_name, table_name \
                FROM _iobeamdb_catalog.chunk_replica_node AS crn \
                WHERE crn.chunk_id = $1"

DEFINE_PLAN(get_crn_plan, CRN_QUERY, 1, CRN_QUERY_ARGS)

crn_set *
fetch_crn_set(crn_set *entry, int32 chunk_id)
{
	SPIPlanPtr	plan = get_crn_plan();
	Datum		args[1] = {Int32GetDatum(chunk_id)};
	int			ret,
				total_rows,
				j;
	bool		is_null;
	TupleDesc	tupdesc;
	crn_row   **tab_array;

	if (entry == NULL)
		entry = palloc(sizeof(crn_set));
	CACHE2_elog(WARNING, "Looking up crn %d", chunk_id);

	entry->chunk_id = chunk_id;

	if (SPI_connect() < 0)
	{
		elog(ERROR, "Got an SPI connect error");
	}
	ret = SPI_execute_plan(plan, args, NULL, false, 0);
	if (ret <= 0)
	{
		elog(ERROR, "Got an SPI error %d", ret);
	}
	if (SPI_processed < 1)
	{
		elog(ERROR, "Could not find crn");
	}


	tupdesc = SPI_tuptable->tupdesc;

	total_rows = SPI_processed;
	tab_array = SPI_palloc(total_rows * sizeof(crn_row *));

	for (j = 0; j < total_rows; j++)
	{
		HeapTuple	tuple = SPI_tuptable->vals[j];
		crn_row    *tab = SPI_palloc(sizeof(crn_row));

		Name		name = DatumGetName(SPI_getbinval(tuple, tupdesc, 1, &is_null));

		memcpy(tab->schema_name.data, name, NAMEDATALEN);

		name = DatumGetName(SPI_getbinval(tuple, tupdesc, 2, &is_null));
		memcpy(tab->table_name.data, name, NAMEDATALEN);

		tab_array[j] = tab;
	}
	SPI_finish();

	entry->tables = NIL;
	for (j = 0; j < total_rows; j++)
	{
		entry->tables = lappend(entry->tables, tab_array[j]);
	}
	pfree(tab_array);
	return entry;
}


/*
 * Retrieving chunks:
 *
 * Locked chunk retrieval has to occur on every row. So we have a fast and slowpath.
 * The fastpath retrieves and locks the chunk only if it already exists locally. The
 * fastpath is faster since it does not call a plpgsql function but calls sql directly.
 * This was found to make a performance difference in tests.
 *
 * The slowpath calls  get_or_create_chunk(), and is called only if the fastpath returned no rows.
 *
 */
#define CHUNK_QUERY_ARGS (Oid[]) {INT4OID, INT8OID, BOOLOID}
#define CHUNK_QUERY "SELECT id, partition_id, start_time, end_time \
                FROM get_or_create_chunk($1, $2, $3)"

#define CHUNK_QUERY_LOCKED_FASTPATH_ARGS (Oid[]) {INT4OID, INT8OID}
#define CHUNK_QUERY_LOCKED_FASTPATH "SELECT id, partition_id, start_time, end_time \
                FROM _iobeamdb_catalog.chunk c \
                WHERE c.partition_id = $1 AND \
                    (c.start_time <= $2 OR c.start_time IS NULL) AND \
                    (c.end_time >= $2 OR c.end_time IS NULL) \
                FOR SHARE;"


/* plan for getting a chunk via get_or_create_chunk(). */
DEFINE_PLAN(get_chunk_plan, CHUNK_QUERY, 3, CHUNK_QUERY_ARGS)
/*
 * Plan for getting a locked chunk. This uses a faster query but will only succeed if there is already a local chunk to lock.
 * Thus, this may return 0 rows, in which case you should fall back to chunk_plan.
 */
DEFINE_PLAN(get_chunk_plan_locked_fastpath, CHUNK_QUERY_LOCKED_FASTPATH, 2, CHUNK_QUERY_LOCKED_FASTPATH_ARGS)


chunk_row *
fetch_chunk_row(chunk_row *entry, int32 partition_id, int64 time_pt, bool lock)
{
	HeapTuple	tuple = NULL;
	TupleDesc	tupdesc = NULL;
	SPIPlanPtr	plan_locked_fastpath = get_chunk_plan_locked_fastpath();
	SPIPlanPtr	plan_slowpath = get_chunk_plan();
	int64		time_ret;
	bool		is_null;


	if (entry == NULL)
		entry = palloc(sizeof(chunk_row));

	if (SPI_connect() < 0)
	{
		elog(ERROR, "Got an SPI connect error");
	}

	if (lock)
	{
		/* try the fastpath */
		int			ret;

		Datum		args[2] = {Int32GetDatum(partition_id), Int64GetDatum(time_pt)};

		ret = SPI_execute_plan(plan_locked_fastpath, args, NULL, false, 2);
		if (ret <= 0)
		{
			elog(ERROR, "Got an SPI error %d", ret);
		}

		if (SPI_processed > 1)
		{
			elog(ERROR, "Got more than 1 row but %lu", SPI_processed);
		}

		if (SPI_processed == 1)
		{
			tupdesc = SPI_tuptable->tupdesc;
			tuple = SPI_tuptable->vals[0];
		}
	}

	if (tuple == NULL)
	{
		/* the fastpath was n/a or returned 0 rows. */
		int			ret;
		Datum		args[3] = {Int32GetDatum(partition_id), Int64GetDatum(time_pt), BoolGetDatum(lock)};

		ret = SPI_execute_plan(plan_slowpath, args, NULL, false, 2);
		if (ret <= 0)
		{
			elog(ERROR, "Got an SPI error %d", ret);
		}
		if (SPI_processed != 1)
		{
			elog(ERROR, "Got not 1 row but %lu", SPI_processed);
		}

		tupdesc = SPI_tuptable->tupdesc;
		tuple = SPI_tuptable->vals[0];
	}
	entry->id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &is_null));
	entry->partition_id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &is_null));

	time_ret = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 3, &is_null));
	if (is_null)
	{
		entry->start_time = OPEN_START_TIME;
	}
	else
	{
		entry->start_time = time_ret;
	}

	time_ret = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 4, &is_null));
	if (is_null)
	{
		entry->end_time = OPEN_END_TIME;
	}
	else
	{
		entry->end_time = time_ret;
	}

	SPI_finish();
	return entry;
}
