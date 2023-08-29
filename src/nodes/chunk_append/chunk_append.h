/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_APPEND_H
#define TIMESCALEDB_CHUNK_APPEND_H

#include <postgres.h>
#include <nodes/extensible.h>

#include "hypertable.h"

typedef struct ChunkAppendPath
{
	CustomPath cpath;
	bool startup_exclusion;
	bool runtime_exclusion_parent;
	bool runtime_exclusion_children;
	bool pushdown_limit;
	int limit_tuples;
	int first_partial_path;
} ChunkAppendPath;

extern TSDLLEXPORT ChunkAppendPath *ts_chunk_append_path_copy(ChunkAppendPath *ca, List *subpaths);
extern Path *ts_chunk_append_path_create(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
										 Path *subpath, bool parallel_aware, bool ordered,
										 List *nested_oids);
extern Plan *ts_chunk_append_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
										 List *tlist, List *clauses, List *custom_plans);
extern Node *ts_chunk_append_state_create(CustomScan *cscan);

extern bool ts_ordered_append_should_optimize(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
											  List *join_conditions, int *order_attno,
											  bool *reverse);

extern TSDLLEXPORT bool ts_is_chunk_append_path(Path *path);
extern TSDLLEXPORT bool ts_is_chunk_append_plan(Plan *plan);

extern Scan *ts_chunk_append_get_scan_plan(Plan *plan);

void _chunk_append_init(void);

#endif /* TIMESCALEDB_CHUNK_APPEND_H */
