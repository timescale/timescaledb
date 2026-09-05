/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/pathnodes.h>
#include <nodes/plannodes.h>

#define COLUMNAR_INDEX_SCAN_NAME "ColumnarIndexScan"

extern void _columnar_index_scan_init(void);
extern Path *columnar_index_scan_path_create(Path *compressed_path, RelOptInfo *chunk_rel,
											 List *pathkeys, List *metadata_output_map,
											 double limit_tuples);
extern CustomScan *columnar_index_scan_make_plan(List *custom_plans, Index scanrelid,
												 List *targetlist, List *custom_scan_tlist,
												 List *exec_output_map, int flags);
extern Node *columnar_index_scan_state_create(CustomScan *cscan);
extern Plan *try_insert_columnar_index_scan_node(Plan *plan, List *rtable);
