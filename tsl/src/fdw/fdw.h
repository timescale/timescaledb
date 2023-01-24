/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_FDW_H
#define TIMESCALEDB_TSL_FDW_FDW_H

#include <postgres.h>
#include <fmgr.h>
#include <extension_constants.h>

extern void tsl_mn_get_foreign_join_paths(PlannerInfo *root, RelOptInfo *joinrel,
										  RelOptInfo *outerrel, RelOptInfo *innerrel,
										  JoinType jointype, JoinPathExtraData *extra);

extern Datum timescaledb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum timescaledb_fdw_validator(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_FDW_FDW_H */
