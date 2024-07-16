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

/*
 * State for the hypertable_modify custom scan node.
 *
 * This struct definition is also used in ts_stat_statements, so any new fields
 * should only be added at the end of the struct.
 */
typedef struct HypertableModifyState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
	bool comp_chunks_processed;
	Snapshot snapshot;
	int64 tuples_decompressed;
	int64 batches_decompressed;
	int64 batches_filtered;
} HypertableModifyState;

extern void ts_hypertable_modify_fixup_tlist(Plan *plan);
extern Path *ts_hypertable_modify_path_create(PlannerInfo *root, ModifyTablePath *mtpath,
											  Hypertable *ht, RelOptInfo *input_rel);
extern List *ts_replace_rowid_vars(PlannerInfo *root, List *tlist, int varno);

extern TupleTableSlot *ExecInsert(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
								  TupleTableSlot *slot, bool canSetTag);
