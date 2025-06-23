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

// extern ChunkInsertState *
// ts_chunk_dispatch_get_chunk_insert_state(ChunkDispatch *dispatch, Point *p,
// 										 const on_chunk_changed_func on_chunk_changed, void *data);
// extern void ts_chunk_dispatch_decompress_batches_for_insert(ChunkInsertState *cis,
// 															TupleTableSlot *slot, EState *estate,
// 															bool update_counter);
//
// extern TSDLLEXPORT Path *ts_chunk_dispatch_path_create(PlannerInfo *root, ModifyTablePath
// *mtpath);
