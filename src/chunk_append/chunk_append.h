/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_APPEND_H
#define TIMESCALEDB_CHUNK_APPEND_H

#include <postgres.h>
#include <nodes/relation.h>
#include <nodes/extensible.h>

#include "hypertable.h"

typedef struct ChunkAppendPath
{
	CustomPath cpath;
	bool startup_exclusion;
	bool runtime_exclusion;
} ChunkAppendPath;

extern Path *ts_chunk_append_path_create(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
										 Path *subpath, bool ordered, List *nested_oids);

extern bool ts_ordered_append_should_optimize(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
											  List *join_conditions, bool *reverse);

#endif /* TIMESCALEDB_CHUNK_APPEND_H */
