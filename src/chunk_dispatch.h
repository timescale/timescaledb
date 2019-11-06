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
	Hypertable *hypertable;
	SubspaceStore *cache;
	EState *estate;

	/*
	 * Keep a pointer to the original (hypertable's) ResultRelInfo since we
	 * will reset the pointer in EState as we lookup new chunks.
	 */
	ResultRelInfo *hypertable_result_rel_info;
	OnConflictAction on_conflict;
	List *arbiter_indexes;
	int returning_index;
	List *returning_lists;
	List *on_conflict_set;
	List *on_conflict_where;
	CmdType cmd_type;
	ChunkInsertState *prev_cis;
	Oid prev_cis_oid;
} ChunkDispatch;

typedef struct Point Point;

extern ChunkDispatch *ts_chunk_dispatch_create(Hypertable *ht, EState *estate);
void ts_chunk_dispatch_destroy(ChunkDispatch *dispatch);
extern ChunkInsertState *
ts_chunk_dispatch_get_chunk_insert_state(ChunkDispatch *dispatch, Point *p, bool *cis_changed_out,
										 const TupleTableSlotOps *const ops);

#endif /* TIMESCALEDB_CHUNK_DISPATCH_H */
