#include <postgres.h>
#include "catalog/pg_type.h"
#include "commands/trigger.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/catcache.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "access/xact.h"

#include "metadata_queries.h"
#include "utils.h"
#include "partitioning.h"

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

void
free_epoch(epoch_and_partitions_set *epoch)
{
	if (epoch->partitioning != NULL)
		pfree(epoch->partitioning);
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

/* plan for getting a chunk via get_or_create_chunk(). */
DEFINE_PLAN(get_chunk_plan, CHUNK_QUERY, 3, CHUNK_QUERY_ARGS)

static HeapTuple
chunk_tuple_create_spi_connected(int32 partition_id, int64 timepoint, bool lock, TupleDesc *desc, SPIPlanPtr plan)
{
	/* the fastpath was n/a or returned 0 rows. */
	int			ret;
	Datum		args[3] = {Int32GetDatum(partition_id), Int64GetDatum(timepoint), BoolGetDatum(lock)};
	HeapTuple	tuple;

	ret = SPI_execute_plan(plan, args, NULL, false, 2);

	if (ret <= 0)
	{
		elog(ERROR, "Got an SPI error %d", ret);
	}

	if (SPI_processed != 1)
	{
		elog(ERROR, "Got not 1 row but %lu", SPI_processed);
	}

	if (desc != NULL)
		*desc = SPI_tuptable->tupdesc;

	tuple = SPI_tuptable->vals[0];

	return tuple;
}

static chunk_row *
chunk_row_fill_in(chunk_row *chunk, HeapTuple tuple, TupleDesc tupdesc)
{
	int64		time_ret;
	bool		is_null;

	chunk->id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 1, &is_null));
	chunk->partition_id = DatumGetInt32(SPI_getbinval(tuple, tupdesc, 2, &is_null));

	time_ret = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 3, &is_null));
	if (is_null)
	{
		chunk->start_time = OPEN_START_TIME;
	}
	else
	{
		chunk->start_time = time_ret;
	}

	time_ret = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 4, &is_null));
	if (is_null)
	{
		chunk->end_time = OPEN_END_TIME;
	}
	else
	{
		chunk->end_time = time_ret;
	}

	return chunk;
}

chunk_row *
chunk_row_insert_new(int32 partition_id, int64 timepoint, bool lock)
{
	HeapTuple tuple;
	TupleDesc desc;
	chunk_row *chunk = palloc(sizeof(chunk_row));
	SPIPlanPtr plan = get_chunk_plan();

	if (SPI_connect() < 0)
		elog(ERROR, "Got an SPI connect error");

	tuple = chunk_tuple_create_spi_connected(partition_id, timepoint, lock, &desc, plan);
	chunk = chunk_row_fill_in(chunk, tuple, desc);

	SPI_finish();

	return chunk;
}


bool chunk_row_timepoint_is_member(const chunk_row *row, const int64 time_pt){
  return row->start_time <= time_pt && row->end_time >= time_pt;
}
