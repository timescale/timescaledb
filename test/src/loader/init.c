/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <access/xact.h>
#include <config.h>
#ifndef WIN32
#include <access/parallel.h>
#endif
#include "compat/compat.h"
#include "export.h"
#include "extension.h"
#include <commands/extension.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include <utils/guc.h>
#include <utils/inval.h>

#define STR_EXPAND(x) #x
#define STR(x) STR_EXPAND(x)

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#if PG16_LT
extern void PGDLLEXPORT _PG_init(void);
#endif

static post_parse_analyze_hook_type prev_post_parse_analyze_hook;

bool ts_license_guc_check_hook(char **newval, void **extra, GucSource source);
void ts_license_guc_assign_hook(const char *newval, void *extra);

TS_FUNCTION_INFO_V1(ts_post_load_init);

static void
cache_invalidate_callback(Datum arg, Oid relid)
{
	if (ts_extension_is_proxy_table_relid(relid))
		ts_extension_invalidate();
}

static void
post_analyze_hook(ParseState *pstate, Query *query, JumbleState *jstate)
{
	if (ts_extension_is_loaded_and_not_upgrading())
		elog(WARNING, "mock post_analyze_hook " STR(TIMESCALEDB_VERSION_MOD));

		/*
		 * a symbol needed by IsParallelWorker is not exported on windows so we do
		 * not perform this check
		 */
#ifndef WIN32
	if (prev_post_parse_analyze_hook != NULL && !IsParallelWorker())
		elog(ERROR, "the extension called with a loader should always have a NULL prev hook");
#endif
	if (BROKEN && !creating_extension)
		elog(ERROR, "mock broken " STR(TIMESCALEDB_VERSION_MOD));
}

void
_PG_init(void)
{
	/*
	 * Check extension_is loaded to catch certain errors such as calls to
	 * functions defined on the wrong extension version
	 */
	ts_extension_check_version(TIMESCALEDB_VERSION_MOD);
	elog(WARNING, "mock init " STR(TIMESCALEDB_VERSION_MOD));
	prev_post_parse_analyze_hook = post_parse_analyze_hook;

	/*
	 * a symbol needed by IsParallelWorker is not exported on windows so we do
	 * not perform this check
	 */
#ifndef WIN32
	if (prev_post_parse_analyze_hook != NULL && !IsParallelWorker())
		elog(ERROR, "the extension called with a loader should always have a NULL prev hook");
#endif
	post_parse_analyze_hook = post_analyze_hook;
	CacheRegisterRelcacheCallback(cache_invalidate_callback, PointerGetDatum(NULL));
}

/* mock for extension.c */
void ts_catalog_reset(void);
void
ts_catalog_reset()
{
}

/* mock for guc.c */
void ts_hypertable_cache_invalidate_callback(void);
void
ts_hypertable_cache_invalidate_callback(void)
{
}

TS_FUNCTION_INFO_V1(ts_mock_function);

Datum
ts_mock_function(PG_FUNCTION_ARGS)
{
	elog(WARNING, "mock function call " STR(TIMESCALEDB_VERSION_MOD));
	PG_RETURN_VOID();
}

TSDLLEXPORT Datum
ts_post_load_init(PG_FUNCTION_ARGS)
{
	PG_RETURN_CHAR(0);
}

bool
ts_license_guc_check_hook(char **newval, void **extra, GucSource source)
{
	return true;
}

void
ts_license_guc_assign_hook(const char *newval, void *extra)
{
}
