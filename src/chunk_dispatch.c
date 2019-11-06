/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <utils/rel.h>
#include <catalog/pg_type.h>

#include "compat.h"

#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "subspace_store.h"
#include "dimension.h"
#include "guc.h"

ChunkDispatch *
ts_chunk_dispatch_create(Hypertable *ht, EState *estate)
{
	ChunkDispatch *cd = palloc0(sizeof(ChunkDispatch));

	cd->hypertable = ht;
	cd->estate = estate;
	cd->hypertable_result_rel_info = NULL;
	cd->on_conflict = ONCONFLICT_NONE;
	cd->arbiter_indexes = NIL;
	cd->cmd_type = CMD_INSERT;
	cd->cache =
		ts_subspace_store_init(ht->space, estate->es_query_cxt, ts_guc_max_open_chunks_per_insert);
	cd->prev_cis = NULL;
	cd->prev_cis_oid = InvalidOid;

	return cd;
}

void
ts_chunk_dispatch_destroy(ChunkDispatch *cd)
{
	ts_subspace_store_free(cd->cache);
}

static void
destroy_chunk_insert_state(void *cis)
{
	ts_chunk_insert_state_destroy((ChunkInsertState *) cis);
}

/*
 * Get the chunk insert state for the chunk that matches the given point in the
 * partitioned hyperspace.
 */
extern ChunkInsertState *
ts_chunk_dispatch_get_chunk_insert_state(ChunkDispatch *dispatch, Point *point,
										 bool *cis_changed_out, const TupleTableSlotOps *const ops)
{
	ChunkInsertState *cis;

	Assert(cis_changed_out != NULL);
	cis = ts_subspace_store_get(dispatch->cache, point);
	*cis_changed_out = true;

	if (NULL == cis)
	{
		Chunk *new_chunk;

		new_chunk = ts_hypertable_get_or_create_chunk(dispatch->hypertable, point);

		if (NULL == new_chunk)
			elog(ERROR, "no chunk found or created");

		cis = ts_chunk_insert_state_create(new_chunk, dispatch, ops);
		ts_subspace_store_add(dispatch->cache, new_chunk->cube, cis, destroy_chunk_insert_state);
	}
	else if (cis->rel->rd_id == dispatch->prev_cis_oid && cis == dispatch->prev_cis)
	{
		/* got the same item from cache as before */
		*cis_changed_out = false;
	}

	if (*cis_changed_out)
		ts_chunk_insert_state_switch(cis);

	Assert(cis != NULL);
	dispatch->prev_cis = cis;
	dispatch->prev_cis_oid = cis->rel->rd_id;
	return cis;
}
