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
#if PG12_GE
	TupleTableSlot *slot;
#endif
} ConstraintAwareAppendState;

typedef struct Hypertable Hypertable;

Path *ts_constraint_aware_append_path_create(PlannerInfo *root, Hypertable *ht, Path *subpath);

void _constraint_aware_append_init(void);

#endif /* TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H */
