/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/pathnodes.h>

#include "export.h"

extern void ts_set_rel_size(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);
extern void ts_set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *parent_rel,
									   Index parent_rt_index, RangeTblEntry *parent_rte);
