/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/plannodes.h>

typedef enum SkipKeyNullStatus
{
	SK_NOT_NULL = 0,
	SK_NULLS_FIRST,
	SK_NULLS_LAST
} SkipKeyNullStatus;

typedef enum
{
	SK_DistinctColAttno = 0,
	SK_DistinctByVal = 1,
	SK_DistinctTypeLen = 2,
	SK_NullStatus = 3,
	SK_IndexKeyAttno = 4
} SkipScanPrivateIndex;

extern void tsl_skip_scan_paths_add(PlannerInfo *root, RelOptInfo *input_rel,
									RelOptInfo *output_rel, UpperRelationKind stage);
extern Node *tsl_skip_scan_state_create(CustomScan *cscan);
extern void _skip_scan_init(void);
