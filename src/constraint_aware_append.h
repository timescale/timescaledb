#ifndef TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H
#define TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H

#include <postgres.h>
#include <nodes/relation.h>
#include <nodes/extensible.h>

typedef struct ConstraintAwareAppendPath ConstraintAwareAppendPath;
typedef struct ConstraintAwareAppendState ConstraintAwareAppendState;

#include "hypertable.h"

struct ConstraintAwareAppendPath
{
	CustomPath	cpath;
};

struct ConstraintAwareAppendState
{
	CustomScanState csstate;
	Plan	   *subplan;
	Size		num_append_subplans;
};


Path	   *constraint_aware_append_path_create(PlannerInfo *root, Hypertable *ht, Path *subpath);


#endif							/* TIMESCALEDB_CONSTRAINT_AWARE_APPEND_H */
