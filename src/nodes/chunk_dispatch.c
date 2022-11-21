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

#include "compat/compat.h"
#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "errors.h"
#include "subspace_store.h"
#include "dimension.h"
#include "guc.h"
#include "ts_catalog/chunk_data_node.h"

ChunkDispatch *
ts_chunk_dispatch_create(Hypertable *ht, EState *estate, int eflags)
{
	ChunkDispatch *cd = palloc0(sizeof(ChunkDispatch));

	cd->hypertable = ht;
	cd->estate = estate;
	cd->eflags = eflags;
	cd->hypertable_result_rel_info = NULL;
	cd->cache =
		ts_subspace_store_init(ht->space, estate->es_query_cxt, ts_guc_max_open_chunks_per_insert);
	cd->prev_cis = NULL;
	cd->prev_cis_oid = InvalidOid;

	return cd;
}

static inline ModifyTableState *
get_modifytable_state(const ChunkDispatch *dispatch)
{
	return dispatch->dispatch_state->mtstate;
}

static inline ModifyTable *
get_modifytable(const ChunkDispatch *dispatch)
{
	return castNode(ModifyTable, get_modifytable_state(dispatch)->ps.plan);
}

bool
ts_chunk_dispatch_has_returning(const ChunkDispatch *dispatch)
{
	if (!dispatch->dispatch_state)
		return false;
	return get_modifytable(dispatch)->returningLists != NIL;
}

List *
ts_chunk_dispatch_get_returning_clauses(const ChunkDispatch *dispatch)
{
#if PG14_LT
	ModifyTableState *mtstate = get_modifytable_state(dispatch);
	return list_nth(get_modifytable(dispatch)->returningLists, mtstate->mt_whichplan);
#else
	Assert(list_length(get_modifytable(dispatch)->returningLists) == 1);
	return linitial(get_modifytable(dispatch)->returningLists);
#endif
}

List *
ts_chunk_dispatch_get_arbiter_indexes(const ChunkDispatch *dispatch)
{
	return dispatch->dispatch_state->arbiter_indexes;
}

OnConflictAction
ts_chunk_dispatch_get_on_conflict_action(const ChunkDispatch *dispatch)
{
	if (!dispatch->dispatch_state)
		return ONCONFLICT_NONE;
	return get_modifytable(dispatch)->onConflictAction;
}

List *
ts_chunk_dispatch_get_on_conflict_set(const ChunkDispatch *dispatch)
{
	return get_modifytable(dispatch)->onConflictSet;
}

CmdType
ts_chunk_dispatch_get_cmd_type(const ChunkDispatch *dispatch)
{
	return dispatch->dispatch_state == NULL ? CMD_INSERT :
											  dispatch->dispatch_state->mtstate->operation;
}

void
ts_chunk_dispatch_destroy(ChunkDispatch *chunk_dispatch)
{
	ts_subspace_store_free(chunk_dispatch->cache);
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
										 const on_chunk_changed_func on_chunk_changed, void *data)
{
	ChunkInsertState *cis;
	bool cis_changed = true;

	/* Direct inserts into internal compressed hypertable is not supported.
	 * For compression chunks are created explicitly by compress_chunk and
	 * inserted into directly so we should never end up in this code path
	 * for a compressed hypertable.
	 */
	if (dispatch->hypertable->fd.compression_state == HypertableInternalCompressionTable)
		elog(ERROR, "direct insert into internal compressed hypertable is not supported");

	cis = ts_subspace_store_get(dispatch->cache, point);

	if (NULL == cis)
	{
		/*
		 * The chunk search functions may leak memory, so switch to a temporary
		 * memory context.
		 */
		MemoryContext old_context =
			MemoryContextSwitchTo(GetPerTupleMemoryContext(dispatch->estate));

		/*
		 * Normally, for every row of the chunk except the first one, we expect
		 * the chunk to exist already. The "create" function would take a lock
		 * on the hypertable to serialize the concurrent chunk creation. Here we
		 * first use the "find" function to try to find the chunk without
		 * locking the hypertable. This serves as a fast path for the usual case
		 * where the chunk already exists.
		 */
		bool found;
		Chunk *new_chunk = ts_hypertable_find_chunk_for_point(dispatch->hypertable, point);

#if PG14_GE
		/*
		 * Frozen chunks require at least PG14.
		 */
		if (new_chunk && ts_chunk_is_frozen(new_chunk))
			elog(ERROR,
				 "cannot INSERT into frozen chunk \"%s\"",
				 get_rel_name(new_chunk->table_id));
#endif

		if (new_chunk == NULL)
		{
			new_chunk = ts_hypertable_create_chunk_for_point(dispatch->hypertable, point, &found);
		}
		else
			found = true;

		/* get the filtered list of "available" DNs for this chunk but only if it's replicated */
		if (found && dispatch->hypertable->fd.replication_factor > 1)
		{
			List *chunk_data_nodes =
				ts_chunk_data_node_scan_by_chunk_id_filter(new_chunk->fd.id, CurrentMemoryContext);

			/*
			 * If the chunk was not created as part of this insert, we need to check whether any
			 * of the chunk's data nodes are currently unavailable and in that case consider the
			 * chunk stale on those data nodes. Do that by removing the AN's chunk-datanode
			 * mapping for the unavailable data nodes.
			 */
			if (dispatch->hypertable->fd.replication_factor > list_length(chunk_data_nodes))
				ts_cm_functions->dist_update_stale_chunk_metadata(new_chunk, chunk_data_nodes);

			list_free(chunk_data_nodes);
		}

		if (NULL == new_chunk)
			elog(ERROR, "no chunk found or created");

		cis = ts_chunk_insert_state_create(new_chunk, dispatch);
		ts_subspace_store_add(dispatch->cache, new_chunk->cube, cis, destroy_chunk_insert_state);

		MemoryContextSwitchTo(old_context);
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
