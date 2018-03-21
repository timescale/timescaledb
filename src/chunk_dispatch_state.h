#ifndef TIMESCALEDB_CHUNK_DISPATCH_STATE_H
#define TIMESCALEDB_CHUNK_DISPATCH_STATE_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <nodes/parsenodes.h>

typedef struct ChunkDispatch ChunkDispatch;
typedef struct ChunkDispatchInfo ChunkDispatchInfo;
typedef struct Cache Cache;

/* State used for every tuple in an insert statement */
typedef struct ChunkDispatchState
{
	CustomScanState cscan_state;
	Plan	   *subplan;
	Cache	   *hypertable_cache;
	Oid			hypertable_relid;

	/*
	 * Keep pointers to the original parsed Query and the ModifyTableState
	 * plan node. We need these to compute and update the arbiter indexes for
	 * each chunk we INSERT into.
	 */
	ModifyTableState *parent;

	/*
	 * The chunk dispatch state. Keeps cached insert states (result relations)
	 * for each chunk.
	 */
	ChunkDispatch *dispatch;
} ChunkDispatchState;

#define CHUNK_DISPATCH_STATE_NAME "ChunkDispatchState"

ChunkDispatchState *chunk_dispatch_state_create(ChunkDispatchInfo *, Plan *);
void
			chunk_dispatch_state_set_parent(ChunkDispatchState *state, ModifyTableState *parent);


#endif							/* TIMESCALEDB_CHUNK_DISPATCH_STATE_H */
