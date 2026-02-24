/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/pathnodes.h>
#include <nodes/primnodes.h>

#define GAPFILL_FUNCTION "time_bucket_gapfill"
#define GAPFILL_LOCF_FUNCTION "locf"
#define GAPFILL_INTERPOLATE_FUNCTION "interpolate"

/*
 * Indices into CustomScan->custom_private for GapFill node.
 */
typedef enum GapFillPrivateIndex
{
	GFP_GapfillFunc = 0,  /* FuncExpr: time_bucket_gapfill call */
	GFP_GroupClause = 1,  /* List: parse->groupClause */
	GFP_JoinTree = 2,     /* FromExpr: parse->jointree */
	GFP_Args = 3,         /* List: gapfill function arguments */
	GFP_Count
} GapFillPrivateIndex;

void plan_add_gapfill(PlannerInfo *root, RelOptInfo *group_rel);
void gapfill_adjust_window_targetlist(PlannerInfo *root, RelOptInfo *input_rel,
									  RelOptInfo *output_rel);

typedef struct GapFillPath
{
	CustomPath cpath;
	FuncExpr *func; /* time_bucket_gapfill function call */
} GapFillPath;
