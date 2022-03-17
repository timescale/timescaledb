/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H
#define TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H

#include <postgres.h>
#include <nodes/extensible.h>

typedef struct ConstraintAwareAppendPath
{
	CustomPath cpath;
} ConstraintAwareAppendPath;

typedef struct ConstraintAwareAppendState
{
	CustomScanState csstate;
	Plan *subplan;
	Size num_append_subplans;
	Size num_chunks_excluded;
} ConstraintAwareAppendState;

typedef struct Hypertable Hypertable;

extern bool ts_constraint_aware_append_possible(Path *path);
extern TSDLLEXPORT Path *ts_constraint_aware_append_path_create(PlannerInfo *root, Path *subpath);
extern TSDLLEXPORT bool ts_is_constraint_aware_append_path(Path *path);
extern void _constraint_aware_append_init(void);

#endif /* TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H */
