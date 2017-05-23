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
#include "chunk.h"

/* Utility function to prepare an SPI plan */
static SPIPlanPtr
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

#define DEFINE_PLAN(fnname, query, num, args)	\
	static SPIPlanPtr fnname() {				\
		static SPIPlanPtr plan = NULL;			\
		if(plan != NULL)						\
			return plan;						\
		plan = prepare_plan(query, num, args);	\
		return plan;							\
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
#define CHUNK_QUERY_ARGS (Oid[]) {INT4OID, INT8OID}
#define CHUNK_QUERY "SELECT * \
				FROM _timescaledb_internal.get_or_create_chunk($1, $2)"

/* plan for getting a chunk via get_or_create_chunk(). */
DEFINE_PLAN(get_chunk_plan, CHUNK_QUERY, 2, CHUNK_QUERY_ARGS)

static HeapTuple
chunk_tuple_create_spi_connected(int32 partition_id, int64 timepoint, TupleDesc *desc, SPIPlanPtr plan)
{
	/* the fastpath was n/a or returned 0 rows. */
	int			ret;
	Datum		args[3] = {Int32GetDatum(partition_id), Int64GetDatum(timepoint)};
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

Chunk *
chunk_insert_new(int32 partition_id, int64 timepoint)
{
	HeapTuple	tuple;
	TupleDesc	desc;
	Chunk	   *chunk;
	MemoryContext top = CurrentMemoryContext;
	SPIPlanPtr	plan = get_chunk_plan();

	if (SPI_connect() < 0)
		elog(ERROR, "Got an SPI connect error");

	tuple = chunk_tuple_create_spi_connected(partition_id, timepoint, &desc, plan);
	chunk = chunk_create(tuple, desc, top);

	SPI_finish();

	return chunk;
}
