/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_DISPATCH_STATE_H
#define TIMESCALEDB_CHUNK_DISPATCH_STATE_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <nodes/parsenodes.h>

typedef struct ChunkDispatch ChunkDispatch;
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
} ChunkDispatchState;

extern bool ts_is_chunk_dispatch_state(PlanState *state);
extern ChunkDispatchState *ts_chunk_dispatch_state_create(Oid hypertable_oid, Plan *plan);
extern void ts_chunk_dispatch_state_set_parent(ChunkDispatchState *state, ModifyTableState *parent);

#endif /* TIMESCALEDB_CHUNK_DISPATCH_STATE_H */
