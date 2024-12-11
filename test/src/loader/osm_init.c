/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/namespace.h>
#include <tcop/utility.h>

#include "compat/compat.h"
#include "export.h"
#include "loader/lwlocks.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static ProcessUtility_hook_type prev_ProcessUtility_hook;

static void osm_process_utility_hook(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
									 ProcessUtilityContext context, ParamListInfo params,
									 QueryEnvironment *queryEnv, DestReceiver *dest,
									 QueryCompletion *qc);

#if PG16_LT
extern void PGDLLEXPORT _PG_init(void);
#endif

void
_PG_init(void)
{
	elog(WARNING, "OSM-%s _PG_init", OSM_VERSION_MOD);
	void **osm_lock_pointer = find_rendezvous_variable(RENDEZVOUS_OSM_PARALLEL_LWLOCK);
	if (osm_lock_pointer != NULL)
	{
		elog(WARNING, "got lwlock osm lock");
	}
	else
	{
		elog(WARNING, "NO lwlock osm lock");
	}

	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = osm_process_utility_hook;
}

TS_FUNCTION_INFO_V1(ts_mock_osm);
Datum
ts_mock_osm(PG_FUNCTION_ARGS)
{
	elog(WARNING, "OSM-%s mock function call", OSM_VERSION_MOD);
	PG_RETURN_VOID();
}

static void
osm_process_utility_hook(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
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
						 "OSM-%s got DROP TABLE '%s'",
						 OSM_VERSION_MOD,
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
}
