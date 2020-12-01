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

extern uint64 remote_distributed_copy(const CopyStmt *stmt, CopyChunkState *ccstate, List *attnums);

#endif /* TIMESCALEDB_TSL_REMOTE_DIST_COPY_H */
