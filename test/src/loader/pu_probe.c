/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/* Standalone ProcessUtility probe, no TimescaleDB dependency. Loaded before the versioned
 * mock to distinguish late head replacement from the shim arrangement. */

#include <postgres.h>
#include <catalog/namespace.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <tcop/utility.h>
#include <utils/lsyscache.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static ProcessUtility_hook_type prev_ProcessUtility_hook;

static void
pu_probe_process_utility(PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
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

					if (OidIsValid(relid))
					{
						elog(NOTICE, "pu-probe got DROP TABLE '%s'", get_rel_name(relid));
					}
				}
			}
		}
	}

	ProcessUtility_hook_type hook;

	if (prev_ProcessUtility_hook != NULL)
	{
		hook = prev_ProcessUtility_hook;
	}
	else
	{
		hook = standard_ProcessUtility;
	}

	hook(pstmt, queryString, readOnlyTree, context, params, queryEnv, dest, qc);
}

void
_PG_init(void)
{
	elog(WARNING, "pu-probe _PG_init");
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pu_probe_process_utility;
}
