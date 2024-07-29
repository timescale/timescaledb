/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <nodes/pathnodes.h>

#include "export.h"
#include "hypertable.h"

extern TSDLLEXPORT void ts_pushdown_partial_agg(PlannerInfo *root, Hypertable *ht,
												RelOptInfo *input_rel, RelOptInfo *output_rel,
												void *extra);
