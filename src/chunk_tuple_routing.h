/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <executor/nodeModifyTable.h>

#include "hypertable.h"
#include "nodes/chunk_dispatch/chunk_insert_state.h"

typedef struct ChunkTupleRouting
{
	Relation partition_root;
	Hypertable *hypertable;
	ResultRelInfo *hypertable_rri;
	Cache *hypertable_cache;
	MemoryContext memcxt;

	SubspaceStore *subspace;
	EState *estate;
	bool create_compressed_chunk;

	ModifyHypertableState *mht_state;  /* state for the ModifyHypertable custom scan node */
	OnConflictAction onConflictAction; /* ON CONFLICT action for the current statement */

	SharedCounters *counters; /* shared counters for the current statement */
} ChunkTupleRouting;

ChunkTupleRouting *ts_chunk_tuple_routing_create(EState *estate, ResultRelInfo *rri);
void ts_chunk_tuple_routing_destroy(ChunkTupleRouting *ctr);
ChunkInsertState *ts_chunk_tuple_routing_find_chunk(ChunkTupleRouting *ctr, Point *point);
extern void ts_chunk_tuple_routing_decompress_for_insert(ChunkInsertState *cis,
														 TupleTableSlot *slot, EState *estate,
														 bool update_counter);
