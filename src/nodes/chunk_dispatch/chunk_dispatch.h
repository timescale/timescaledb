/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>
#include <nodes/execnodes.h>
#include <executor/tuptable.h>

#include "hypertable_cache.h"
#include "cache.h"
#include "export.h"
#include "subspace_store.h"
#include "chunk_insert_state.h"

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
	int eflags;

	/*
	 * Keep a pointer to the original (hypertable's) ResultRelInfo since we
	 * will reset the pointer in EState as we lookup new chunks.
	 */
	ResultRelInfo *hypertable_result_rel_info;
	ChunkInsertState *prev_cis;
	Oid prev_cis_oid;
} ChunkDispatch;

typedef struct ChunkDispatchPath
{
	CustomPath cpath;
	ModifyTablePath *mtpath;
	Index hypertable_rti;
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
	List *arbiter_indexes;
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
	ResultRelInfo *rri;
	/* flag to represent dropped attributes */
	bool is_dropped_attr_exists;
	int64 batches_decompressed;
	int64 tuples_decompressed;
} ChunkDispatchState;

extern TSDLLEXPORT bool ts_is_chunk_dispatch_state(PlanState *state);
extern void ts_chunk_dispatch_state_set_parent(ChunkDispatchState *state,
											   ModifyTableState *mtstate);
typedef struct Point Point;

typedef void (*on_chunk_changed_func)(ChunkInsertState *state, void *data);

extern ChunkDispatch *ts_chunk_dispatch_create(Hypertable *ht, EState *estate, int eflags);
extern void ts_chunk_dispatch_destroy(ChunkDispatch *chunk_dispatch);
extern ChunkInsertState *
ts_chunk_dispatch_get_chunk_insert_state(ChunkDispatch *dispatch, Point *p, TupleTableSlot *slot,
										 const on_chunk_changed_func on_chunk_changed, void *data);

extern TSDLLEXPORT Path *ts_chunk_dispatch_path_create(PlannerInfo *root, ModifyTablePath *mtpath,
													   Index hypertable_rti, int subpath_index);
