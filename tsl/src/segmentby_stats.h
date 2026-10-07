/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <nodes/pathnodes.h>
#include <utils/selfuncs.h>

bool tsl_get_relation_stats(PlannerInfo *root, RangeTblEntry *rte, AttrNumber attnum,
							VariableStatData *vardata);
