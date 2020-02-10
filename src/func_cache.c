/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <miscadmin.h>
#include <parser/parse_oper.h>
#include <catalog/namespace.h>
#include <catalog/pg_type.h>
#include <catalog/pg_proc.h>
#include <access/htup.h>
#include <access/heapam.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/selfuncs.h>
#include <utils/builtins.h>
#include <utils/rel.h>

#include "compat.h"
#if PG12_LT
#include <nodes/relation.h>
#include <optimizer/cost.h>
#include <optimizer/clauses.h>
#else
#include <nodes/pathnodes.h>
#include <optimizer/optimizer.h>
#endif

#include "utils.h"
#include "cache.h"
#include "func_cache.h"
#include "extension.h"
#include "estimate.h"
#include "sort_transform.h"

/*
 * func_cache - a cache for quick identification of, and access to, functions
 * useful for TimescaleDB. The function info is used in various query
 * optimizations, for instance, we provide custom group estimate functions for
 * use when grouping on time buckets. We also provide functions that allow
 * sorting time buckets using an index on the non-bucketed expression/column.
 */

static Expr *
date_trunc_sort_transform(FuncExpr *func)
{
	/*
	 * date_trunc (const, var) => var
	 *
	 * proof: date_trunc(c, time1) >= date_trunc(c,time2) iff time1 > time2
	 */
	Expr *second;

	if (list_length(func->args) != 2 || !IsA(linitial(func->args), Const))
		return (Expr *) func;

	second = ts_sort_transform_expr(lsecond(func->args));

	if (!IsA(second, Var))
		return (Expr *) func;

	return (Expr *) copyObject(second);
}

/*
 * Check that time_bucket period is Const and if an offset is supplied
 * that it is Const as well
 */
#define time_bucket_has_const_period_and_offset(func)                                              \
	(IsA(linitial((func)->args), Const) &&                                                         \
	 (list_length((func)->args) == 2 || IsA(lthird((func)->args), Const)))

static Expr *
time_bucket_sort_transform(FuncExpr *func)
{
	/*
	 * time_bucket(const, var, const) => var
	 *
	 * proof: time_bucket(const1, time1) >= time_bucket(const1,time2) iff time1
	 * > time2
	 */
	Expr *second;

	Assert(list_length(func->args) >= 2);

	/*
	 * If period and offset are not constants we must not do the optimization
	 */
	if (!time_bucket_has_const_period_and_offset(func))
		return (Expr *) func;

	second = ts_sort_transform_expr(lsecond(func->args));

	if (!IsA(second, Var))
		return (Expr *) func;

	return (Expr *) copyObject(second);
}

/* For time_bucket this estimate currently works by seeing how many possible
 * buckets there will be if the data spans the entire hypertable. Note that
 * this is an overestimate.
 * */
static double
time_bucket_group_estimate(PlannerInfo *root, FuncExpr *expr, double path_rows)
{
	Node *first_arg = eval_const_expressions(root, linitial(expr->args));
	Expr *second_arg = lsecond(expr->args);
	Const *c;
	double period;

	if (!IsA(first_arg, Const))
		return INVALID_ESTIMATE;

	c = (Const *) first_arg;
	switch (c->consttype)
	{
		case INT2OID:
			period = (double) DatumGetInt16(c->constvalue);
			break;
		case INT4OID:
			period = (double) DatumGetInt32(c->constvalue);
			break;
		case INT8OID:
			period = (double) DatumGetInt64(c->constvalue);
			break;
		case INTERVALOID:
			period = (double) ts_get_interval_period_approx(DatumGetIntervalP(c->constvalue));
			break;
		default:
			return INVALID_ESTIMATE;
	}
	return ts_estimate_group_expr_interval(root, second_arg, period);
}

/* For date_trunc this estimate currently works by seeing how many possible
 * buckets there will be if the data spans the entire hypertable. Note that
 * this is an overestimate.
 * */
static double
date_trunc_group_estimate(PlannerInfo *root, FuncExpr *expr, double path_rows)
{
	Node *first_arg = eval_const_expressions(root, linitial(expr->args));
	Expr *second_arg = lsecond(expr->args);
	Const *c;
	text *interval;

	if (!IsA(first_arg, Const))
		return INVALID_ESTIMATE;

	c = (Const *) first_arg;
	interval = DatumGetTextPP(c->constvalue);

	return ts_estimate_group_expr_interval(root,
										   second_arg,
										   (double) ts_date_trunc_interval_period_approx(interval));
}

typedef struct FuncEntry
{
	Oid funcid;
	FuncInfo *funcinfo;
} FuncEntry;

/* Information about functions that we put in the cache */
static FuncInfo funcinfo[] = {
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 2,
		.arg_types = { INTERVALOID, TIMESTAMPOID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 3,
		.arg_types = { INTERVALOID, TIMESTAMPOID, TIMESTAMPOID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 2,
		.arg_types = { INTERVALOID, TIMESTAMPTZOID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 3,
		.arg_types = { INTERVALOID, TIMESTAMPTZOID, TIMESTAMPTZOID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 2,
		.arg_types = { INTERVALOID, DATEOID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 3,
		.arg_types = { INTERVALOID, DATEOID, DATEOID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 2,
		.arg_types = { INT2OID, INT2OID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 3,
		.arg_types = { INT2OID, INT2OID, INT2OID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 2,
		.arg_types = { INT4OID, INT4OID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 3,
		.arg_types = { INT4OID, INT4OID, INT4OID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 2,
		.arg_types = { INT8OID, INT8OID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = true,
		.is_bucketing_func = true,
		.funcname = "time_bucket",
		.nargs = 3,
		.arg_types = { INT8OID, INT8OID, INT8OID },
		.group_estimate = time_bucket_group_estimate,
		.sort_transform = time_bucket_sort_transform,
	},
	{
		.is_timescaledb_func = false,
		.is_bucketing_func = true,
		.funcname = "date_trunc",
		.nargs = 2,
		.arg_types = { TEXTOID, TIMESTAMPOID },
		.group_estimate = date_trunc_group_estimate,
		.sort_transform = date_trunc_sort_transform,
	},
	{
		.is_timescaledb_func = false,
		.is_bucketing_func = true,
		.funcname = "date_trunc",
		.nargs = 2,
		.arg_types = { TEXTOID, TIMESTAMPTZOID },
		.group_estimate = date_trunc_group_estimate,
		.sort_transform = date_trunc_sort_transform,
	},
};

#define _MAX_CACHE_FUNCTIONS (sizeof(funcinfo) / sizeof(funcinfo[0]))

static HTAB *func_hash = NULL;

static Oid
proc_get_oid(HeapTuple tuple)
{
#if PG12_LT
	return HeapTupleGetOid(tuple);
#else
	Form_pg_proc form = (Form_pg_proc) GETSTRUCT(tuple);
	return form->oid;
#endif
}

static void
initialize_func_info()
{
	HASHCTL hashctl = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(FuncEntry),
		.hcxt = CacheMemoryContext,
	};
	Oid extension_nsp = get_namespace_oid(ts_extension_schema_name(), false);
	Oid pg_nsp = get_namespace_oid("pg_catalog", false);
	HeapTuple tuple;
	Relation rel;
	int i;

	func_hash = hash_create("func_cache", _MAX_CACHE_FUNCTIONS, &hashctl, HASH_ELEM | HASH_BLOBS);

	rel = table_open(ProcedureRelationId, AccessShareLock);

	for (i = 0; i < _MAX_CACHE_FUNCTIONS; i++)
	{
		FuncInfo *finfo = &funcinfo[i];
		Oid namespaceoid = finfo->is_timescaledb_func ? extension_nsp : pg_nsp;
		oidvector *paramtypes = buildoidvector(finfo->arg_types, finfo->nargs);
		FuncEntry *fentry;
		bool hash_found;
		Oid funcid;

		tuple = SearchSysCache3(PROCNAMEARGSNSP,
								PointerGetDatum(finfo->funcname),
								PointerGetDatum(paramtypes),
								ObjectIdGetDatum(namespaceoid));

		if (!HeapTupleIsValid(tuple))
			elog(ERROR,
				 "cache lookup failed for function \"%s\" with %d args",
				 finfo->funcname,
				 finfo->nargs);

		funcid = proc_get_oid(tuple);

		fentry = hash_search(func_hash, &funcid, HASH_ENTER, &hash_found);
		Assert(!hash_found);
		fentry->funcid = funcid;
		fentry->funcinfo = finfo;
		ReleaseSysCache(tuple);
	}

	table_close(rel, AccessShareLock);
}

FuncInfo *
ts_func_cache_get(Oid funcid)
{
	FuncEntry *entry;

	if (NULL == func_hash)
		initialize_func_info();

	entry = hash_search(func_hash, &funcid, HASH_FIND, NULL);

	return (NULL == entry) ? NULL : entry->funcinfo;
}

FuncInfo *
ts_func_cache_get_bucketing_func(Oid funcid)
{
	FuncInfo *finfo = ts_func_cache_get(funcid);

	if (NULL == finfo)
		return NULL;

	return finfo->is_bucketing_func ? finfo : NULL;
}
