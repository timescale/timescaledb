/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <foreign/fdwapi.h>
#include <nodes/execnodes.h>

#include "hypertable.h"
#include "import/ht_hypertable_modify.h"

typedef struct HypertableModifyPath
{
	CustomPath cpath;
} HypertableModifyPath;

typedef struct HypertableModifyState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
	bool comp_chunks_processed;
	Snapshot snapshot;
	int64 tuples_decompressed;
	int64 batches_decompressed;
} HypertableModifyState;

extern void ts_hypertable_modify_fixup_tlist(Plan *plan);
extern Path *ts_hypertable_modify_path_create(PlannerInfo *root, ModifyTablePath *mtpath,
											  Hypertable *ht, RelOptInfo *input_rel);
extern List *ts_replace_rowid_vars(PlannerInfo *root, List *tlist, int varno);

extern TupleTableSlot *ExecInsert(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
								  TupleTableSlot *slot, bool canSetTag);
