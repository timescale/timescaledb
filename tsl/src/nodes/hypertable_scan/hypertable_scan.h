/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/pathnodes.h>

#include "hypertable.h"

extern bool ts_should_hypertable_scan(const Query *query, const Hypertable *ht);
extern void ts_hypertable_scan_add_path(PlannerInfo *root, RelOptInfo *rel, const Hypertable *ht);
extern void _hypertable_scan_init(void);
