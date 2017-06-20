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
#define INT8ARRAYOID 1016

#define CHUNK_QUERY_ARGS (Oid[]) {INT4ARRAYOID, INT8ARRAYOID}
#define CHUNK_QUERY "SELECT * \
				FROM _timescaledb_internal.chunk_get_or_create($1, $2)"

/* plan for getting a chunk via get_or_create_chunk(). */
DEFINE_PLAN(get_chunk_plan, CHUNK_QUERY, 4, CHUNK_QUERY_ARGS)

#define CHUNK_CREATE_ARGS (Oid[]) {INT4ARRAYOID, INT8ARRAYOID}
#define CHUNK_CREATE "SELECT * \
				FROM _timescaledb_internal.chunk_create($1, $2)"

#define MAX_DIMENSIONS 10

/* plan for creating a chunk via create_chunk(). */
DEFINE_PLAN(create_chunk_plan, CHUNK_CREATE, 2, CHUNK_CREATE_ARGS)

static HeapTuple
chunk_tuple_create_spi_connected(Hyperspace *hs, Point *p,
								TupleDesc *desc, SPIPlanPtr plan)
{
	int			ret;
	Datum		dimension_ids[MAX_DIMENSIONS];
	Datum		dimension_values[MAX_DIMENSIONS];

	for (int dim = 0; dim < HYPERSPACE_NUM_DIMENSIONS(hs); dim++)
	{
		dimension_ids[dim] = Int32GetDatum(hs->dimensions[dim].fd.id);
		dimension_values[dim] = Int64GetDatum(p->coordinates[dim]);
	}

	Datum		args[2] = {
         PointerGetDatum(construct_array(dimension_ids, HYPERSPACE_NUM_DIMENSIONS(hs), INT4OID, 4, true, 'i')), 
         PointerGetDatum(construct_array(dimension_values, HYPERSPACE_NUM_DIMENSIONS(hs), INT8OID, 8, true, 'd')), 
	};

	HeapTuple	tuple;

	ret = SPI_execute_plan(plan, args, NULL, false, 4);

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
spi_chunk_get_or_create(Hyperspace *hs, Point *p)
{
	HeapTuple	tuple;
	TupleDesc	desc;
	Chunk	   *chunk;
	MemoryContext old, top = CurrentMemoryContext;
	SPIPlanPtr	plan = get_chunk_plan();

	if (SPI_connect() < 0)
		elog(ERROR, "Got an SPI connect error");

	tuple = chunk_tuple_create_spi_connected(hs, p,
											 &desc, plan);

	old = MemoryContextSwitchTo(top);
	chunk = chunk_create_from_tuple(tuple, HYPERSPACE_NUM_DIMENSIONS(hs));
	MemoryContextSwitchTo(old);

	SPI_finish();

	return chunk;
}


Chunk *
spi_chunk_create(Hyperspace *hs, Point *p)
{
	HeapTuple	tuple;
	TupleDesc	desc;
	Chunk	   *chunk;
	MemoryContext old, top = CurrentMemoryContext;
	SPIPlanPtr	plan = create_chunk_plan();

	if (SPI_connect() < 0)
		elog(ERROR, "Got an SPI connect error");

	tuple = chunk_tuple_create_spi_connected(hs, p,
											 &desc, plan);

	old = MemoryContextSwitchTo(top);
	chunk = chunk_create_from_tuple(tuple,  HYPERSPACE_NUM_DIMENSIONS(hs));
	MemoryContextSwitchTo(old);

	SPI_finish();

	return chunk;
}
