/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

typedef enum
{
	CSS_HypertableId = 0,
	CSS_ChunkRelid = 1,
	CSS_Reverse = 2,
	CSS_BatchSortedMerge = 3,
	CSS_EnableBulkDecompression = 4,
	CSS_HasRowMarks = 5,
	CSS_Count
} ColumnarScanSettingsIndex;

typedef enum
{
	CSP_Settings = 0,
	CSP_DecompressionMap = 1,
	CSP_IsSegmentbyColumn = 2,
	CSP_BulkDecompressionColumn = 3,
	CSP_SortInfo = 4,
	CSP_Count
} ColumnarScanPrivateIndex;

extern Plan *columnar_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
										  List *output_targetlist, List *clauses,
										  List *custom_plans);

extern void _columnar_scan_init(void);
