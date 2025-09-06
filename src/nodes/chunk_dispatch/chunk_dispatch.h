/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>

#include "cache.h"
#include "chunk_insert_state.h"
#include "export.h"
#include "hypertable_cache.h"
#include "subspace_store.h"

typedef struct ChunkTupleRouting ChunkTupleRouting;

/*
 * ChunkDispatch keeps cached state needed to dispatch tuples to chunks. It is
 * separate from any plan and executor nodes, since it is used both for INSERT
 * and COPY.
 */
typedef struct ChunkDispatch
{
	/* Link to the executor state for INSERTs. This is an mostly empty dummy state in the COPY path.
	 */
	struct ChunkDispatchState *dispatch_state;
	Hypertable *hypertable;
	SubspaceStore *cache;
	EState *estate;
	ChunkTupleRouting *ctr;

} ChunkDispatch;

typedef struct ChunkDispatchPath
{
	CustomPath cpath;
	ModifyTablePath *mtpath;
	Oid hypertable_relid;
} ChunkDispatchPath;

typedef struct Cache Cache;

/* State used for every tuple in an insert statement */
typedef struct ChunkDispatchState
{
	CustomScanState cscan_state;
	Plan *subplan;
	Cache *hypertable_cache;
	Oid hypertable_relid;

	/*
	 * Keep a pointer to the parent ModifyTableState executor node since we need
	 * to manipulate the current result relation on-the-fly for chunk routing
	 * during inserts.
	 */
	ModifyTableState *mtstate;
	/*
	 * The chunk dispatch state. Keeps cached chunk insert states (with result
	 * relations) for each chunk.
	 */
	ChunkDispatch *dispatch;

	/*
	 * Keep the chunk insert state available to pass it from
	 * ExecGetInsertNewTuple() to ExecInsert(), where the actual slot to
	 * use is decided.
	 */
	ChunkInsertState *cis;

	/* flag to represent dropped attributes */
	bool is_dropped_attr_exists;
} ChunkDispatchState;

extern TSDLLEXPORT bool ts_is_chunk_dispatch_state(PlanState *state);
extern void ts_chunk_dispatch_state_set_parent(ChunkDispatchState *state,
											   ModifyTableState *mtstate);
typedef struct Point Point;

extern ChunkDispatch *ts_chunk_dispatch_create(Hypertable *ht, EState *estate);
extern void ts_chunk_dispatch_destroy(ChunkDispatch *chunk_dispatch);
extern ChunkInsertState *ts_chunk_dispatch_get_chunk_insert_state(ChunkDispatch *dispatch,
																  Point *p);
extern void ts_chunk_dispatch_decompress_batches_for_insert(ChunkInsertState *cis,
															TupleTableSlot *slot, EState *estate,
															bool update_counter);

extern TSDLLEXPORT Path *ts_chunk_dispatch_path_create(PlannerInfo *root, ModifyTablePath *mtpath);
