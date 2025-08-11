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
	SKIPKEY_NOT_NULL = 0,
	SKIPKEY_NULLS_FIRST,
	SKIPKEY_NULLS_LAST
} SkipKeyNullStatus;

extern void tsl_skip_scan_paths_add(PlannerInfo *root, RelOptInfo *input_rel,
									RelOptInfo *output_rel, UpperRelationKind stage);
extern Node *tsl_skip_scan_state_create(CustomScan *cscan);
extern void _skip_scan_init(void);
