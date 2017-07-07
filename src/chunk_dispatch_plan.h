#ifndef TIMESCALEDB_CHUNK_DISPATCH_PLAN_H
#define TIMESCALEDB_CHUNK_DISPATCH_PLAN_H

#include <postgres.h>
#include <nodes/plannodes.h>

typedef struct ChunkDispatchInfo
{
	Oid			hypertable_relid;
} ChunkDispatchInfo;

extern CustomScan *chunk_dispatch_plan_create(Plan *subplan, Oid hypertable_relid);

#endif   /* TIMESCALEDB_CHUNK_DISPATCH_PLAN_H */
