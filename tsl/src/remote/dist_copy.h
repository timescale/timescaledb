/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_DIST_COPY_H
#define TIMESCALEDB_TSL_REMOTE_DIST_COPY_H

#include <postgres.h>
#include <commands/copy.h>

typedef struct Hypertable Hypertable;
typedef struct CopyChunkState CopyChunkState;
typedef struct RemoteCopyContext RemoteCopyContext;
typedef struct ChunkInsertState ChunkInsertState;

extern uint64 remote_distributed_copy(const CopyStmt *stmt, CopyChunkState *ccstate, List *attnums);
extern RemoteCopyContext *remote_copy_begin(const CopyStmt *stmt, Hypertable *ht,
											ExprContext *per_tuple_ctx, List *attnums,
											bool binary_copy);
extern void remote_copy_end_on_success(RemoteCopyContext *context);
extern bool remote_copy_send_slot(RemoteCopyContext *context, TupleTableSlot *slot,
								  const ChunkInsertState *cis);
extern const char *remote_copy_get_copycmd(RemoteCopyContext *context);

#endif /* TIMESCALEDB_TSL_REMOTE_DIST_COPY_H */
