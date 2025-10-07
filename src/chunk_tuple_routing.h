/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <executor/nodeModifyTable.h>

#include "chunk_insert_state.h"
#include "hypertable.h"

typedef struct ModifyHypertableState ModifyHypertableState;

typedef struct ChunkTupleRouting
{
	Hypertable *hypertable;
	/*
	 * When single_chunk_insert is true, root_rel and root_rri point to the
	 * chunk being inserted into. Otherwise, they point to the hypertable.
	 */
	Relation root_rel;
	ResultRelInfo *root_rri;
	bool single_chunk_insert;
	Cache *hypertable_cache;

	SubspaceStore *subspace;
	EState *estate;
	bool create_compressed_chunk;
	bool has_dropped_attrs;

	ModifyHypertableState *mht_state; /* state for the ModifyHypertable custom scan node */
	ChunkInsertState *cis;

	SharedCounters *counters; /* shared counters for the current statement */
} ChunkTupleRouting;

ChunkTupleRouting *ts_chunk_tuple_routing_create(EState *estate, ResultRelInfo *rri);
void ts_chunk_tuple_routing_destroy(ChunkTupleRouting *ctr);
ChunkInsertState *ts_chunk_tuple_routing_find_chunk(ChunkTupleRouting *ctr, Point *point);
extern void ts_chunk_tuple_routing_decompress_for_insert(ChunkInsertState *cis,
														 TupleTableSlot *slot, EState *estate,
														 bool update_counter);
