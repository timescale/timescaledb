#ifndef TIMESCALEDB_CHUNK_DISPATCH_H
#define TIMESCALEDB_CHUNK_DISPATCH_H

#include <postgres.h>
#include <nodes/execnodes.h>

#include "hypertable_cache.h"
#include "cache.h"
#include "subspace_store.h"

/*
 * ChunkDispatch keeps cached state needed to dispatch tuples to chunks. It is
 * separate from any plan and executor nodes, since it is used both for INSERT
 * and COPY.
*/
typedef struct ChunkDispatch
{
	Hypertable *hypertable;
	SubspaceStore *cache;
	EState	   *estate;

	/*
	 * Keep a pointer to the original (hypertable's) ResultRelInfo since we
	 * will reset the pointer in EState as we lookup new chunks.
	 */
	ResultRelInfo *hypertable_result_rel_info;
	Query	   *parse;
} ChunkDispatch;

typedef struct Point Point;
typedef struct ChunkInsertState ChunkInsertState;

ChunkDispatch *chunk_dispatch_create(Hypertable *, EState *, Query *);
void		chunk_dispatch_destroy(ChunkDispatch *);
ChunkInsertState *chunk_dispatch_get_chunk_insert_state(ChunkDispatch *, Point *);

#endif   /* TIMESCALEDB_CHUNK_DISPATCH_H */
