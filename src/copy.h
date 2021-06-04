/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COPY_H
#define TIMESCALEDB_COPY_H

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <access/xact.h>
#include <access/relscan.h>
#include <executor/executor.h>
#include <commands/copy.h>
#include <storage/lockdefs.h>

typedef struct ChunkDispatch ChunkDispatch;
typedef struct CopyChunkState CopyChunkState;
typedef struct Hypertable Hypertable;

typedef bool (*CopyFromFunc)(CopyChunkState *ccstate, ExprContext *econtext, Datum *values,
							 bool *nulls);

typedef struct CopyChunkState
{
	Relation rel;
	EState *estate;
	ChunkDispatch *dispatch;
	CopyFromFunc next_copy_from;
	CopyFromState cstate;
	TableScanDesc scandesc;
	Node *where_clause;
} CopyChunkState;

extern void timescaledb_DoCopy(const CopyStmt *stmt, const char *queryString, uint64 *processed,
							   Hypertable *ht);
extern void timescaledb_move_from_table_to_chunks(Hypertable *ht, LOCKMODE lockmode);

#endif /* TIMESCALEDB_COPY_H */
