/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/nodes.h>
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
	cd->cache =
		ts_subspace_store_init(ht->space, estate->es_query_cxt, ts_guc_max_open_chunks_per_insert);
	cd->prev_cis = NULL;
	cd->prev_cis_oid = InvalidOid;

	return cd;
}

static inline ModifyTableState *
get_modifytable_state(ChunkDispatch *dispatch)
{
	return dispatch->dispatch_state->mtstate;
}

static inline ModifyTable *
get_modifytable(ChunkDispatch *dispatch)
{
	return castNode(ModifyTable, get_modifytable_state(dispatch)->ps.plan);
}

bool
ts_chunk_dispatch_has_returning(ChunkDispatch *dispatch)
{
	if (NULL == dispatch->dispatch_state)
		return false;
	return get_modifytable(dispatch)->returningLists != NIL;
}

List *
ts_chunk_dispatch_get_returning_clauses(ChunkDispatch *dispatch)
{
	ModifyTableState *mtstate = dispatch->dispatch_state->mtstate;

	return list_nth(get_modifytable(dispatch)->returningLists, mtstate->mt_whichplan);
}

List *
ts_chunk_dispatch_get_arbiter_indexes(ChunkDispatch *dispatch)
{
	return dispatch->dispatch_state->arbiter_indexes;
}

OnConflictAction
ts_chunk_dispatch_get_on_conflict_action(ChunkDispatch *dispatch)
{
	if (NULL == dispatch->dispatch_state)
		return ONCONFLICT_NONE;
	return get_modifytable(dispatch)->onConflictAction;
}

List *
ts_chunk_dispatch_get_on_conflict_set(ChunkDispatch *dispatch)
{
	return get_modifytable(dispatch)->onConflictSet;
}

Node *
ts_chunk_dispatch_get_on_conflict_where(ChunkDispatch *dispatch)
{
	return get_modifytable(dispatch)->onConflictWhere;
}

CmdType
ts_chunk_dispatch_get_cmd_type(ChunkDispatch *dispatch)
{
	return dispatch->dispatch_state == NULL ? CMD_INSERT :
											  dispatch->dispatch_state->mtstate->operation;
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
										 on_chunk_changed_func on_chunk_changed, void *data)
{
	ChunkInsertState *cis;
	bool cis_changed = true;

	cis = ts_subspace_store_get(dispatch->cache, point);

	if (NULL == cis)
	{
		Chunk *new_chunk;

		new_chunk = ts_hypertable_get_or_create_chunk(dispatch->hypertable, point);

		if (NULL == new_chunk)
			elog(ERROR, "no chunk found or created");

		cis = ts_chunk_insert_state_create(new_chunk, dispatch);
		ts_subspace_store_add(dispatch->cache, new_chunk->cube, cis, destroy_chunk_insert_state);
	}
	else if (cis->rel->rd_id == dispatch->prev_cis_oid && cis == dispatch->prev_cis)
	{
		/* got the same item from cache as before */
		cis_changed = false;
	}

	if (cis_changed && on_chunk_changed)
		on_chunk_changed(cis, data);

	Assert(cis != NULL);
	dispatch->prev_cis = cis;
	dispatch->prev_cis_oid = cis->rel->rd_id;
	return cis;
}
