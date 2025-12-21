/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/extensible.h>

#include "nodes/columnar_scan/columnar_scan.h"

typedef struct ColumnarIndexScanPath
{
	CustomPath custom_path;
	const CompressionInfo *info;
	AttrNumber aggregate_attno; /* Chunk attno of the aggregate column */
	AttrNumber metadata_attno;	/* Compressed chunk attno for metadata */
} ColumnarIndexScanPath;

extern void _columnar_index_scan_init(void);
extern bool ts_is_columnar_index_scan_path(Path *path);
extern bool ts_is_columnar_index_scan_plan(Plan *plan);

extern ColumnarIndexScanPath *
ts_columnar_index_scan_path_create(PlannerInfo *root, const Chunk *chunk, RelOptInfo *chunk_rel,
								   const CompressionInfo *info, Path *compressed_path);
extern Plan *columnar_index_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
											 List *output_targetlist, List *clauses,
											 List *custom_plans);
extern Node *columnar_index_scan_state_create(CustomScan *cscan);
