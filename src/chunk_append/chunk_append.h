/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_APPEND_H
#define TIMESCALEDB_CHUNK_APPEND_H

#include <postgres.h>

#include "hypertable.h"

typedef struct ChunkAppendPath
{
	CustomPath cpath;
	bool startup_exclusion;
	bool runtime_exclusion;
	bool pushdown_limit;
	int limit_tuples;
	int first_partial_path;
} ChunkAppendPath;

extern Path *ts_chunk_append_path_create(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
										 Path *subpath, bool parallel_aware, bool ordered,
										 List *nested_oids);

extern bool ts_ordered_append_should_optimize(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
											  List *join_conditions, int *order_attno,
											  bool *reverse);

#endif /* TIMESCALEDB_CHUNK_APPEND_H */
