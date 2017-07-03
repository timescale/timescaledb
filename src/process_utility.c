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
					const char *query_string,
					ProcessUtilityContext context,
					ParamListInfo params,
					DestReceiver *dest,
					char *completion_tag)
{
	if (prev_ProcessUtility_hook != NULL)
	{
		/* Call any earlier hooks */
		(prev_ProcessUtility_hook) (parsetree, query_string, context, params, dest, completion_tag);
	}
	else
	{
		/* Call the standard */
		standard_ProcessUtility(parsetree, query_string, context, params, dest, completion_tag);
	}
}

/* Hook-intercept for ProcessUtility. */
static void
timescaledb_ProcessUtility(Node *parsetree,
						   const char *query_string,
						   ProcessUtilityContext context,
						   ParamListInfo params,
						   DestReceiver *dest,
						   char *completion_tag)
{
	if (!extension_is_loaded())
	{
		prev_ProcessUtility(parsetree, query_string, context, params, dest, completion_tag);
		return;
	}

	/* We don't support renaming hypertables yet so we need to block it */
	if (IsA(parsetree, RenameStmt))
	{
		RenameStmt *renamestmt = (RenameStmt *) parsetree;
		Oid			relid = RangeVarGetRelid(renamestmt->relation, NoLock, true);

		if (OidIsValid(relid))
		{
			Cache	   *hcache = hypertable_cache_pin();
			Hypertable *hentry = hypertable_cache_get_entry(hcache, relid);

			if (hentry != NULL && renamestmt->renameType == OBJECT_TABLE)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					   errmsg("Renaming hypertables is not yet supported")));
			cache_release(hcache);
		}
		prev_ProcessUtility((Node *) renamestmt, query_string, context, params, dest, completion_tag);
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
		DoCopy((CopyStmt *) parsetree, query_string, &processed);
		executor_level_exit();
		processed += executor_get_additional_tuples_processed();

		if (completion_tag)
			snprintf(completion_tag, COMPLETION_TAG_BUFSIZE,
					 "COPY " UINT64_FORMAT, processed);
		return;
	}

	prev_ProcessUtility(parsetree, query_string, context, params, dest, completion_tag);
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
