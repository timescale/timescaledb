#include <postgres.h>
#include <nodes/parsenodes.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <commands/copy.h>

#include "utils.h"
#include "hypertable_cache.h"
#include "extension.h"
#include "executor.h"

void		_process_utility_init(void);
void		_process_utility_fini(void);

static ProcessUtility_hook_type prev_ProcessUtility_hook;

/* Calls the default ProcessUtility */
static void
prev_ProcessUtility(Node *parsetree,
					const char *queryString,
					ProcessUtilityContext context,
					ParamListInfo params,
					DestReceiver *dest,
					char *completionTag)
{
	if (prev_ProcessUtility_hook != NULL)
	{
		/* Call any earlier hooks */
		(prev_ProcessUtility_hook) (parsetree, queryString, context, params, dest, completionTag);
	}
	else
	{
		/* Call the standard */
		standard_ProcessUtility(parsetree, queryString, context, params, dest, completionTag);
	}
}

/* Hook-intercept for ProcessUtility. Used to make COPY use a temp copy table and */
/* blocking renaming of hypertables. */
static void
timescaledb_ProcessUtility(Node *parsetree,
						   const char *queryString,
						   ProcessUtilityContext context,
						   ParamListInfo params,
						   DestReceiver *dest,
						   char *completionTag)
{
	if (!extension_is_loaded())
	{
		prev_ProcessUtility(parsetree, queryString, context, params, dest, completionTag);
		return;
	}

	/* We don't support renaming hypertables yet so we need to block it */
	if (IsA(parsetree, RenameStmt))
	{
		RenameStmt *renamestmt = (RenameStmt *) parsetree;
		Oid			relId = RangeVarGetRelid(renamestmt->relation, NoLock, true);

		if (OidIsValid(relId))
		{
			Cache	   *hcache = hypertable_cache_pin();
			Hypertable *hentry = hypertable_cache_get_entry(hcache, relId);

			if (hentry != NULL && renamestmt->renameType == OBJECT_TABLE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					   errmsg("Renaming hypertables is not yet supported")));
			cache_release(hcache);
		}
		prev_ProcessUtility((Node *) renamestmt, queryString, context, params, dest, completionTag);
		return;
	}
	if (IsA(parsetree, CopyStmt))
	{
		/*
		 * Needed to add the appropriate number of tuples to the completion
		 * tag
		 */
		uint64		processed;

		executor_level_enter();
		DoCopy((CopyStmt *) parsetree, queryString, &processed);
		executor_level_exit();
		processed += executor_get_additional_tuples_processed();
		if (completionTag)
			snprintf(completionTag, COMPLETION_TAG_BUFSIZE,
					 "COPY " UINT64_FORMAT, processed);
		return;
	}

	prev_ProcessUtility(parsetree, queryString, context, params, dest, completionTag);
}

void
_process_utility_init(void)
{
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = timescaledb_ProcessUtility;
}

void
_process_utility_fini(void)
{
	ProcessUtility_hook = prev_ProcessUtility_hook;
}
