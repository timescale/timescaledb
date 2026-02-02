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
	List *aggregate_attnos; /* Chunk attnos of aggregate columns in target list order */
	List *metadata_attnos;	/* Compressed chunk attnos for metadata in target list order */
	List *aggregate_fnoids; /* Aggregate function OIDs in target list order */
} ColumnarIndexScanPath;

/*
 * Post-process a plan tree to fix Aggref references for ColumnarIndexScan.
 * This is needed when multiple aggregates reference the same column.
 */
extern void ts_columnar_index_scan_fix_aggrefs(Plan *plan);

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
