/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_IMPORT_ALLPATHS_H
#define TIMESCALEDB_IMPORT_ALLPATHS_H

#include <postgres.h>

#include "compat.h"

#if PG12_GE
#include <nodes/pathnodes.h>
#else
#include <nodes/relation.h>
#endif

extern void ts_set_rel_size(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
extern void ts_set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
									   RangeTblEntry *rte);

#endif /* TIMESCALEDB_IMPORT_ALLPATHS_H */
