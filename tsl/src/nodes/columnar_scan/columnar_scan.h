/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_COLUMNAR_SCAN_H
#define TIMESCALEDB_COLUMNAR_SCAN_H

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>

#include "hypertable.h"

typedef struct ColumnarScanPath
{
	CustomPath custom_path;
} ColumnarScanPath;

extern ColumnarScanPath *columnar_scan_path_create(PlannerInfo *root, RelOptInfo *rel,
												   Relids required_outer, int parallel_workers);
extern void columnar_scan_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht);
extern void _columnar_scan_init(void);

#endif /* TIMESCALEDB_COLUMNAR_SCAN_H */
