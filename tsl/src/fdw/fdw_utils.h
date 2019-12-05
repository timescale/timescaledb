/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_FDW_UTILS_H
#define TIMESCALEDB_TSL_FDW_FDW_UTILS_H

#include <postgres.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>

#include "relinfo.h"

#if TS_DEBUG

extern void fdw_utils_add_path(RelOptInfo *rel, Path *new_path);
extern void fdw_utils_free_path(ConsideredPath *path);
#else

#define fdw_utils_add_path(rel, path) add_path(rel, path);

#endif /* TS_DEBUG */

#endif /* TIMESCALEDB_TSL_FDW_FDW_UTILS_H */
