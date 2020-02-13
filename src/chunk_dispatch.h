/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_DISPATCH_H
#define TIMESCALEDB_CHUNK_DISPATCH_H

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/execnodes.h>
#include <executor/tuptable.h>

#include "hypertable_cache.h"
#include "cache.h"
#include "subspace_store.h"
#include "chunk_dispatch_state.h"
#include "chunk_insert_state.h"

/*
 * ChunkDispatch keeps cached state needed to dispatch tuples to chunks. It is
 * separate from any plan and executor nodes, since it is used both for INSERT
 * and COPY.
 */
typedef struct ChunkDispatch
{
	/* Link to the executor state for INSERTs. This is not set for COPY path. */
	const struct ChunkDispatchState *dispatch_state;
	Hypertable *hypertable;
	SubspaceStore *cache;
	EState *estate;
	/*
	 * Keep a pointer to the original (hypertable's) ResultRelInfo since we
	 * will reset the pointer in EState as we lookup new chunks.
	 */
	ResultRelInfo *hypertable_result_rel_info;
	ChunkInsertState *prev_cis;
	Oid prev_cis_oid;
} ChunkDispatch;

typedef struct Point Point;

typedef void (*on_chunk_changed_func)(ChunkInsertState *state, void *data);

extern ChunkDispatch *ts_chunk_dispatch_create(Hypertable *ht, EState *estate);
extern void ts_chunk_dispatch_destroy(ChunkDispatch *dispatch);
extern ChunkInsertState *
ts_chunk_dispatch_get_chunk_insert_state(ChunkDispatch *dispatch, Point *p,
										 const on_chunk_changed_func on_chunk_changed, void *data);
extern bool ts_chunk_dispatch_has_returning(const ChunkDispatch *dispatch);
extern List *ts_chunk_dispatch_get_returning_clauses(const ChunkDispatch *dispatch);
extern List *ts_chunk_dispatch_get_arbiter_indexes(const ChunkDispatch *dispatch);
extern OnConflictAction ts_chunk_dispatch_get_on_conflict_action(const ChunkDispatch *dispatch);
extern List *ts_chunk_dispatch_get_on_conflict_set(const ChunkDispatch *dispatch);
extern Node *ts_chunk_dispatch_get_on_conflict_where(const ChunkDispatch *dispatch);
extern CmdType ts_chunk_dispatch_get_cmd_type(const ChunkDispatch *dispatch);

#endif /* TIMESCALEDB_CHUNK_DISPATCH_H */
