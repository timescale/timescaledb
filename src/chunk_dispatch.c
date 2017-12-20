#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <utils/rel.h>
#include <catalog/pg_type.h>

#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "subspace_store.h"
#include "dimension.h"

ChunkDispatch *
chunk_dispatch_create(Hypertable *ht, EState *estate, Query *parse)
{
	ChunkDispatch *cd = palloc0(sizeof(ChunkDispatch));

	cd->hypertable = ht;
	cd->estate = estate;
	cd->hypertable_result_rel_info = NULL;
	cd->parse = parse;
	cd->cache = subspace_store_init(ht->space->num_dimensions, estate->es_query_cxt, 0);

	return cd;
}

void
chunk_dispatch_destroy(ChunkDispatch *cd)
{
	subspace_store_free(cd->cache);
}

static void
destroy_chunk_insert_state(void *cis)
{
	chunk_insert_state_destroy((ChunkInsertState *) cis);
}

/*
 * Get the chunk insert state for the chunk that matches the given point in the
 * partitioned hyperspace.
 */
extern ChunkInsertState *
chunk_dispatch_get_chunk_insert_state(ChunkDispatch *dispatch, Point *point, CmdType operation)
{
	ChunkInsertState *cis;

	cis = subspace_store_get(dispatch->cache, point);

	if (NULL == cis)
	{
		Chunk	   *new_chunk;

		new_chunk = hypertable_get_chunk(dispatch->hypertable, point);

		if (NULL == new_chunk)
			elog(ERROR, "No chunk found or created");

		cis = chunk_insert_state_create(new_chunk, dispatch, operation);
		subspace_store_add(dispatch->cache, new_chunk->cube, cis, destroy_chunk_insert_state);
	}

	Assert(cis != NULL);
	return cis;
}
