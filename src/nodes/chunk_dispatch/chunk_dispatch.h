/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <executor/tuptable.h>
#include <nodes/execnodes.h>
#include <nodes/extensible.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>

#include "cache.h"
#include "chunk_insert_state.h"
#include "export.h"
#include "hypertable_cache.h"
#include "subspace_store.h"

typedef struct ChunkTupleRouting ChunkTupleRouting;

typedef struct ChunkDispatchPath
{
	CustomPath cpath;
	ModifyTablePath *mtpath;
} ChunkDispatchPath;

typedef struct Cache Cache;

/* State used for every tuple in an insert statement */
typedef struct ChunkDispatchState
{
	CustomScanState cscan_state;
	Plan *subplan;

	ChunkTupleRouting *ctr;
} ChunkDispatchState;

extern TSDLLEXPORT bool ts_is_chunk_dispatch_state(PlanState *state);

extern TSDLLEXPORT Path *ts_chunk_dispatch_path_create(PlannerInfo *root, ModifyTablePath *mtpath);
