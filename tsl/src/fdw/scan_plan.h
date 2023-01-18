/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_FDW_SCAN_PLAN_H
#define TIMESCALEDB_TSL_FDW_SCAN_PLAN_H

#include <postgres.h>
#include <commands/explain.h>
#include <fmgr.h>
#include <foreign/foreign.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <nodes/pathnodes.h>
#include <utils/relcache.h>

#include "data_node_chunk_assignment.h"

typedef struct TsFdwRelInfo TsFdwRelInfo;

typedef struct ScanInfo
{
	Oid data_node_serverid;
	Index scan_relid;
	List *local_exprs;
	List *fdw_private;
	List *fdw_scan_tlist;
	List *fdw_recheck_quals;
	List *params_list;
	bool systemcol;
} ScanInfo;

typedef Path *(*CreatePathFunc)(PlannerInfo *root, RelOptInfo *rel, PathTarget *target, double rows,
								Cost startup_cost, Cost total_cost, List *pathkeys,
								Relids required_outer, Path *fdw_outerpath, List *fdw_private);

typedef Path *(*CreateUpperPathFunc)(PlannerInfo *root, RelOptInfo *rel, PathTarget *target,
									 double rows, Cost startup_cost, Cost total_cost,
									 List *pathkeys, Path *fdw_outerpath, List *fdw_private);

extern void fdw_scan_info_init(ScanInfo *scaninfo, PlannerInfo *root, RelOptInfo *rel,
							   Path *best_path, List *scan_clauses, Plan *outer_plan);

extern void fdw_add_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel, Path *epq_path,
												CreatePathFunc create_scan_path);
extern void fdw_add_upper_paths_with_pathkeys_for_rel(PlannerInfo *root, RelOptInfo *rel,
													  Path *epq_path,
													  CreateUpperPathFunc create_upper_path);

extern void fdw_create_upper_paths(TsFdwRelInfo *input_fpinfo, PlannerInfo *root,
								   UpperRelationKind stage, RelOptInfo *input_rel,
								   RelOptInfo *output_rel, void *extra,
								   CreateUpperPathFunc create_paths);

#endif /* TIMESCALEDB_TSL_FDW_SCAN_PLAN_H */
