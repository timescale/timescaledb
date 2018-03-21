#ifndef TIMESCALEDB_CHUNK_DISPATCH_INFO_H
#define TIMESCALEDB_CHUNK_DISPATCH_INFO_H

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/extensible.h>

/*
 * ChunkDispatchInfo holds plan info that needs to be passed on to the
 * execution stage. Since it is part of the plan tree, it needs to be able
 * support copyObject(), and therefore extends ExtensibleNode.
 */
typedef struct ChunkDispatchInfo
{
	ExtensibleNode enode;
	/* Copied fields */
	Oid			hypertable_relid;
} ChunkDispatchInfo;

extern ChunkDispatchInfo *chunk_dispatch_info_create(Oid hypertable_relid, Query *parse);

extern void _chunk_dispatch_info_init(void);
extern void _chunk_dispatch_info_fini(void);

#endif							/* TIMESCALEDB_CHUNK_DISPATCH_INFO_H */
