/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

typedef enum
{
	DCS_HypertableId = 0,
	DCS_ChunkRelid = 1,
	DCS_Reverse = 2,
	DCS_BatchSortedMerge = 3,
	DCS_EnableBulkDecompression = 4,
	DCS_Count
} DecompressChunkSettingsIndex;

typedef enum
{
	DCP_Settings = 0,
	DCP_DecompressionMap = 1,
	DCP_IsSegmentbyColumn = 2,
	DCP_BulkDecompressionColumn = 3,
	DCP_SortInfo = 4,
	DCP_Count
} DecompressChunkPrivateIndex;

extern Plan *decompress_chunk_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
										  List *tlist, List *clauses, List *custom_plans);

extern void _decompress_chunk_init(void);
