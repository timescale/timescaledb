/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/pathnodes.h>
#include <nodes/primnodes.h>
#include <utils/builtins.h>

#define GAPFILL_FUNCTION "time_bucket_gapfill"
#define GAPFILL_LOCF_FUNCTION "locf"
#define GAPFILL_INTERPOLATE_FUNCTION "interpolate"

/*
 * Check whether funcid is the gapfill function with the given name. Resolved
 * through the function cache, whose entries are keyed by our functions' fixed
 * signatures, so a user-defined function that merely shares the name is not
 * mistaken for ours.
 */
extern bool gapfill_is_extension_function(Oid funcid, const char *name);

/*
 * Indices into CustomScan->custom_private for GapFill node.
 */
typedef enum GapfillPrivateIndex
{
	GFP_GapfillFunc = 0, /* FuncExpr: time_bucket_gapfill call */
	GFP_GroupClause = 1, /* List: parse->groupClause */
	GFP_JoinTree = 2,	 /* FromExpr: parse->jointree */
	GFP_BucketWidth = 3, /* Expr: gapfill function bucket width argument */
	GFP_TimeColumn = 4,	 /* Expr: gapfill function time column argument */
	GFP_TimeZone = 5,	 /* Expr/Null: gapfill function time zone argument */
	GFP_Offset = 6,		 /* Expr/Null: gapfill function offset argument */
	GFP_Origin = 7,		 /* Expr/Null: gapfill function origin argument */
	GFP_Start = 8,		 /* Expr: gapfill function start argument */
	GFP_End = 9,		 /* Expr: gapfill function end argument */
	GFP_Count
} GapfillPrivateIndex;

void plan_add_gapfill(PlannerInfo *root, RelOptInfo *group_rel);
void gapfill_adjust_window_targetlist(PlannerInfo *root, RelOptInfo *input_rel,
									  RelOptInfo *output_rel);

typedef struct GapFillPath
{
	CustomPath cpath;
	FuncExpr *func; /* time_bucket_gapfill function call */
} GapFillPath;

typedef enum GapFillBoundary
{
	GAPFILL_START,
	GAPFILL_END,
} GapFillBoundary;

typedef struct GapFillArgEvalContext
{
	PlannerInfo *root;		/* needed for evaluation at planning time */
	CustomScanState *state; /* needed for evaluation at execution time */
	Oid typid;				/* gapfill function type */
	List *args;				/* gapfill function arguments */
	FromExpr *jt;			/* needed for inferring boundaries from WHERE */
	bool estimate_failed;	/* whether estimation during planning failed */
} GapFillArgEvalContext;

extern int64 infer_gapfill_boundary(GapFillArgEvalContext *gapfill_ctx, GapFillBoundary boundary);
extern Datum gapfill_estimate_arg(GapFillArgEvalContext *gapfill_plan_ctx, Node *arg, bool *isnull);

extern bool gapfill_is_const_null(Expr *expr);
extern bool gapfill_is_simple_expr(Expr *node);
extern Const *make_const_value_for_gapfill_internal(Oid typid, int64 value);
extern int64 gapfill_datum_get_internal(Datum, Oid);
extern int64 gapfill_bucket_width_get_internal(Oid timetype, Oid argtype, Datum arg,
											   Interval **interval);
extern bool gapfill_is_integer_type(Oid bucket_type);
extern bool gapfill_is_integer(List *args);
extern Expr *gapfill_get_bucketwidth_arg(List *args);
extern Expr *gapfill_get_timecolumn_arg(List *args);
extern Expr *gapfill_get_start_arg(List *args);
extern Expr *gapfill_get_end_arg(List *args);
extern Expr *gapfill_get_timezone_arg(List *args);
extern Expr *gapfill_get_offset_arg(List *args);
extern Expr *gapfill_get_origin_arg(List *args);
