/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_FUNC_CACHE_H
#define TIMESCALEDB_FUNC_CACHE_H

#include <postgres.h>
#include <nodes/primnodes.h>
#if PG12_LT
#include <nodes/relation.h>
#endif

#include "export.h"

#define FUNC_CACHE_MAX_FUNC_ARGS 10

typedef Expr *(*sort_transform_func)(FuncExpr *func);
typedef double (*group_estimate_func)(PlannerInfo *root, FuncExpr *expr, double path_rows);

typedef struct FuncInfo
{
	const char *funcname;
	bool is_timescaledb_func;
	bool is_bucketing_func;
	int nargs;
	Oid arg_types[FUNC_CACHE_MAX_FUNC_ARGS];
	group_estimate_func group_estimate;
	sort_transform_func sort_transform;
} FuncInfo;

extern TSDLLEXPORT FuncInfo *ts_func_cache_get(Oid funcid);
extern TSDLLEXPORT FuncInfo *ts_func_cache_get_bucketing_func(Oid funcid);

#endif /* TIMESCALEDB_FUNC_CACHE_H */
