#ifndef TIMESCALEDB_CHUNK_DISPATCH_H
#define TIMESCALEDB_CHUNK_DISPATCH_H

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/execnodes.h>

typedef struct ChunkDispatch ChunkDispatch;

#include "cache.h"
#include "chunk_dispatch_state.h"
#include "chunk_insert_state.h"
#include "hypertable_cache.h"
#include "subspace_store.h"

/*
 * ChunkDispatch keeps cached state needed to dispatch tuples to chunks. It is
 * separate from any plan and executor nodes, since it is used both for INSERT
 * and COPY.
*/
struct ChunkDispatch
{
	Hypertable *hypertable;
	SubspaceStore *cache;
	EState	   *estate;

	/*
	 * Keep a pointer to the original (hypertable's) ResultRelInfo since we
	 * will reset the pointer in EState as we lookup new chunks.
	 */
	ResultRelInfo *hypertable_result_rel_info;
	OnConflictAction on_conflict;
	List	   *arbiter_indexes;
	int			returning_index;
	List	   *returning_lists;
	List	   *on_conflict_set;
	List	   *on_conflict_where;
	CmdType		cmd_type;

};

ChunkDispatch *chunk_dispatch_create(Hypertable *ht, EState *estate);
void		chunk_dispatch_destroy(ChunkDispatch *dispatch);
ChunkInsertState *chunk_dispatch_get_chunk_insert_state(ChunkDispatch *dispatch, Point *p);

#endif							/* TIMESCALEDB_CHUNK_DISPATCH_H */
