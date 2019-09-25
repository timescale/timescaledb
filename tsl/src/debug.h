/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DEBUG_H
#define TIMESCALEDB_DEBUG_H

#include <postgres.h>
#include <lib/stringinfo.h>
#include <utils/guc.h>

#include <compat.h>
#if PG12_GE
#include <nodes/pathnodes.h>
#else
#include <nodes/relation.h>
#endif

#ifdef TS_DEBUG
extern const char *upperrel_stage_name[UPPERREL_FINAL + 1];
extern void tsl_debug_append_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel);
extern void tsl_debug_append_path(StringInfo buf, PlannerInfo *root, Path *path, int indent);
extern void tsl_debug_append_pathlist(StringInfo buf, PlannerInfo *root, List *pathlist,
									  int indent);
#endif

#endif /* TIMESCALEDB_DEBUG_H */
