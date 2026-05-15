/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/extensible.h>

#include "hypertable.h"

/*
 * Indexes into the settings list (first element of custom_private).
 */
typedef enum
{
	CAS_StartupExclusion = 0,
	CAS_RuntimeExclusionParent = 1,
	CAS_RuntimeExclusionChildren = 2,
	CAS_Limit = 3,
	CAS_FirstPartialPath = 4,
	CAS_Count
} ChunkAppendSettingsIndex;

/*
 * Indexes into custom_private for ChunkAppend.
 */
typedef enum
{
	CAP_Settings = 0,
	CAP_ChunkRIClauses = 1,
	CAP_RTIndexes = 2,
	CAP_SortOptions = 3,
	CAP_ParentClauses = 4,
	CAP_Count
} ChunkAppendPrivateIndex;

typedef struct ChunkAppendPath
{
	CustomPath cpath;
	bool startup_exclusion;
	bool runtime_exclusion_parent;
	bool runtime_exclusion_children;
	bool pushdown_limit;
	int limit_tuples;
	int first_partial_path;
	/*
	 * Estimated fraction of children that survive startup exclusion. Used to
	 * scale the row/cost estimates we hand to upstream planning. 1.0 means
	 * no discount (no exclusion expected, or estimation declined to discount).
	 * The estimate is purely advisory; the actual exclusion happens at
	 * executor startup regardless.
	 */
	double startup_exclusion_ratio;
} ChunkAppendPath;

extern TSDLLEXPORT ChunkAppendPath *ts_chunk_append_path_copy(ChunkAppendPath *ca, List *subpaths,
															  PathTarget *pathtarget);
extern Path *ts_chunk_append_path_create(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
										 Path *subpath, bool parallel_aware, bool ordered,
										 List *nested_oids);
extern Plan *ts_chunk_append_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *path,
										 List *tlist, List *clauses, List *custom_plans);
extern Node *ts_chunk_append_state_create(CustomScan *cscan);

extern bool ts_ordered_append_should_optimize(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht,
											  int *order_attno, bool *reverse);

extern TSDLLEXPORT bool ts_is_chunk_append_path(Path *path);
extern TSDLLEXPORT bool ts_is_chunk_append_plan(Plan *plan);

extern Scan *ts_chunk_append_get_scan_plan(Plan *plan);

void _chunk_append_init(void);

extern TSDLLEXPORT List *ts_constify_restrictinfos(PlannerInfo *root, List *restrictinfos);
extern TSDLLEXPORT List *ts_constify_restrictinfo_params(PlannerInfo *root, EState *state,
														 List *restrictinfos);
