#ifndef TIMESCALEDB_CHUNK_DISPATCH_STATE_H
#define TIMESCALEDB_CHUNK_DISPATCH_STATE_H

#include <postgres.h>
#include <nodes/execnodes.h>
#include <nodes/parsenodes.h>

typedef struct ChunkDispatchState ChunkDispatchState;

#include "chunk_dispatch.h"
#include "chunk_dispatch_info.h"

/* State used for every tuple in an insert statement */
struct ChunkDispatchState
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
};

#define CHUNK_DISPATCH_STATE_NAME "ChunkDispatchState"

ChunkDispatchState *chunk_dispatch_state_create(ChunkDispatchInfo *, Plan *);
void
			chunk_dispatch_state_set_parent(ChunkDispatchState *state, ModifyTableState *parent);


#endif							/* TIMESCALEDB_CHUNK_DISPATCH_STATE_H */
