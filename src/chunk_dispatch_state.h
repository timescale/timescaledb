#ifndef TIMESCALEDB_CHUNK_DISPATCH_STATE_H
#define TIMESCALEDB_CHUNK_DISPATCH_STATE_H

#include <postgres.h>
#include <nodes/execnodes.h>

typedef struct ChunkDispatch ChunkDispatch;
typedef struct Cache Cache;

/* State used for every tuple in an insert statement */
typedef struct ChunkDispatchState
{
	CustomScanState cscan_state;
	Plan	   *subplan;
	PlanState  *subplan_state;
	Cache	   *hypertable_cache;
	Oid			hypertable_relid;
	ChunkDispatch *dispatch;
} ChunkDispatchState;

ChunkDispatchState *chunk_dispatch_state_create(Oid, Plan *);

#endif   /* TIMESCALEDB_CHUNK_DISPATCH_STATE_H */
