/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include <access/xact.h>
#include <catalog/namespace.h>
#include <config.h>
#ifndef WIN32
#include <access/parallel.h>
#endif
#include "compat/compat.h"
#include "export.h"
#include "extension.h"
#include "extension_constants.h"
#include "loader/loader.h"
#include <commands/extension.h>
#include <miscadmin.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <parser/analyze.h>
#include <tcop/utility.h>
#include <utils/guc.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>

#define STR_EXPAND(x) #x
#define STR(x) STR_EXPAND(x)

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

bool ts_license_guc_check_hook(char **newval, void **extra, GucSource source);
void ts_license_guc_assign_hook(const char *newval, void *extra);

TS_FUNCTION_INFO_V1(ts_post_load_init);

static ProcessUtility_hook_type prev_ProcessUtility_hook;
static TsProcessUtilityRendezvous *process_utility_rendezvous = NULL;

static void
cache_invalidate_callback(Datum arg, Oid relid)
{
	if (ts_extension_is_proxy_table_relid(relid))
	{
		ts_extension_invalidate();
	}
}

/*
 * Mock ProcessUtility published through the loader rendezvous so tests can
 * verify TimescaleDB stays last in the hook chain when another extension
 * installs itself as the head (see timescaledb_pu_probe and timescaledb_osm).
 */
static void
mock_process_utility_hook(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
						  ProcessUtilityContext context, ParamListInfo params,
						  QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc)
{
	if (nodeTag(pstmt->utilityStmt) == T_DropStmt)
	{
		DropStmt *stmt = (DropStmt *) pstmt->utilityStmt;

		if (stmt->removeType == OBJECT_TABLE)
		{
			ListCell *lc;

			foreach (lc, stmt->objects)
			{
				RangeVar *relation = makeRangeVarFromNameList(lfirst(lc));

				if (relation != NULL)
				{
					Oid relid = RangeVarGetRelid(relation, NoLock, true);

					elog(NOTICE,
						 "mock-%s got DROP TABLE '%s'",
						 TIMESCALEDB_VERSION_MOD,
						 get_rel_name(relid));
				}
			}
		}
	}

	if (prev_ProcessUtility_hook)
		prev_ProcessUtility_hook(pstmt,
								 queryString,
								 readOnlyTree,
								 context,
								 params,
								 queryEnv,
								 dest,
								 qc);
	else
		standard_ProcessUtility(pstmt,
								queryString,
								readOnlyTree,
								context,
								params,
								queryEnv,
								dest,
								qc);
}

void
_PG_init(void)
{
	TsProcessUtilityRendezvous **rendezvous =
		(TsProcessUtilityRendezvous **) find_rendezvous_variable(RENDEZVOUS_PROCESS_UTILITY_HOOK);

	/*
	 * Check extension_is loaded to catch certain errors such as calls to
	 * functions defined on the wrong extension version
	 */
	ts_extension_check_version(TIMESCALEDB_VERSION_MOD);
	elog(WARNING, "mock init " STR(TIMESCALEDB_VERSION_MOD));

	/*
	 * The loader sets post_parse_analyze_hook to NULL before calling
	 * ts_post_load_init so that a versioned extension cannot splice its own
	 * hook into the chain. A non-NULL value here would indicate the loader
	 * contract has changed.
	 *
	 * A symbol needed by IsParallelWorker is not exported on windows so we
	 * do not perform this check there.
	 */
#ifndef WIN32
	if (post_parse_analyze_hook != NULL && !IsParallelWorker())
	{
		elog(ERROR, "the extension called with a loader should always have a NULL prev hook");
	}
#endif
	CacheRegisterRelcacheCallback(cache_invalidate_callback, PointerGetDatum(NULL));

	/*
	 * Register ProcessUtility through the loader rendezvous when available so
	 * TimescaleDB remains last in the hook chain (matching the real
	 * extension). Fall back to installing as the head when the rendezvous is
	 * absent.
	 */
	if (*rendezvous != NULL)
	{
		process_utility_rendezvous = *rendezvous;
		prev_ProcessUtility_hook = process_utility_rendezvous->prev_hook;
		process_utility_rendezvous->versioned_hook = mock_process_utility_hook;
	}
	else
	{
		prev_ProcessUtility_hook = ProcessUtility_hook;
		ProcessUtility_hook = mock_process_utility_hook;
	}
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
