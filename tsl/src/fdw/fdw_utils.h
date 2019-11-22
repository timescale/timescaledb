/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_UTILS_H
#define TIMESCALEDB_TSL_FDW_UTILS_H

#include <postgres.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>

void fdw_utils_add_path(RelOptInfo *rel, Path *new_path);
void fdw_utils_free_path(Path *path);

#endif
