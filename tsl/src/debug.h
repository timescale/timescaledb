/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <lib/stringinfo.h>
#include <nodes/pathnodes.h>
#include <utils/guc.h>

#ifdef TS_DEBUG
extern void tsl_debug_log_rel_with_paths(PlannerInfo *root, RelOptInfo *rel,
										 UpperRelationKind *upper_stage);
#endif
