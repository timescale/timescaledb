/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <foreign/fdwapi.h>
#include <nodes/execnodes.h>

#include "chunk_tuple_routing.h"
#include "hypertable.h"

/* Forward declarations */
typedef struct ModifyTableContext ModifyTableContext;
typedef struct RowCompressor RowCompressor;
typedef struct BulkWriter BulkWriter;

typedef struct ModifyHypertablePath
{
	CustomPath cpath;
} ModifyHypertablePath;

/*
 * State for the hypertable_modify custom scan node.
 *
 * This struct definition is also used in ts_stat_statements, so any new fields
 * should only be added at the end of the struct.
 */
typedef struct ModifyHypertableState
{
	CustomScanState cscan_state;
	ModifyTable *mt;
	ChunkTupleRouting *ctr;
	Hypertable *ht;
	Cache *ht_cache;
	bool has_continuous_aggregate;

	RowCompressor *compressor;
	BulkWriter *bulk_writer;
	Oid compressor_relid;
	bool columnstore_insert;

	bool comp_chunks_processed;
	Snapshot snapshot;
	int64 tuples_decompressed;
	int64 batches_decompressed;
	int64 batches_filtered;
	int64 batches_deleted;
	int64 tuples_deleted;

	/*
	 * When EXPLAIN VERBOSE is used, we temporarily nullify the targetlist of the
	 * lefttree of the ModifyTable to avoid printing out the full targetlist since
	 * they can't be resolved by EXPLAIN. To not corrupt cached plans we need to
	 * restore them to their original value afterwards.
	 */
	List *explain_saved_tlist;
	List *explain_saved_custom_scan_tlist;

} ModifyHypertableState;

extern TSDLLEXPORT bool ts_is_modify_hypertable_plan(Plan *plan);

extern void ts_modify_hypertable_fixup_tlist(Plan *plan);
extern Path *ts_modify_hypertable_path_create(PlannerInfo *root, ModifyTablePath *mtpath,
											  RelOptInfo *input_rel);
extern List *ts_replace_rowid_vars(PlannerInfo *root, List *tlist, int varno);

TupleTableSlot *ExecModifyTable(CustomScanState *cs_node, PlanState *pstate);
TupleTableSlot *ExecInsert(ModifyTableContext *context, ResultRelInfo *resultRelInfo,
						   ChunkTupleRouting *ctr, TupleTableSlot *slot, bool canSetTag);
