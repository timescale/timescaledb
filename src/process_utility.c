#include <postgres.h>
#include <nodes/parsenodes.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <commands/copy.h>

#include "utils.h"
#include "hypertable_cache.h"
#include "extension.h"
#include "executor.h"
#include "metadata_queries.h"
#include "copy.h"

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

/* Truncate a hypertable */
static void
process_truncate(Node *parsetree)
{
	TruncateStmt *truncatestmt = (TruncateStmt *) parsetree;
	ListCell   *cell;

	foreach(cell, truncatestmt->relations)
	{
		Oid			relId = RangeVarGetRelid(lfirst(cell), NoLock, true);

		if (OidIsValid(relId))
		{
			Cache	   *hcache = hypertable_cache_pin();
			Hypertable *hentry = hypertable_cache_get_entry(hcache, relId);

			if (hentry != NULL)
			{
				executor_level_enter();
				spi_hypertable_truncate(hentry);
				executor_level_exit();
			}
			cache_release(hcache);
		}
	}
}

/* Change the schema of a hypertable */
static void
process_alterobjectschema(Node *parsetree)
{
	AlterObjectSchemaStmt *alterstmt = (AlterObjectSchemaStmt *) parsetree;
	Oid			relId = RangeVarGetRelid(alterstmt->relation, NoLock, true);

	if (OidIsValid(relId))
	{
		Cache	   *hcache = hypertable_cache_pin();
		Hypertable *hentry = hypertable_cache_get_entry(hcache, relId);

		if (hentry != NULL && alterstmt->objectType == OBJECT_TABLE)
		{
			executor_level_enter();
			spi_hypertable_rename(hentry,
								  alterstmt->newschema,
								  NameStr(hentry->fd.table_name));
			executor_level_exit();
		}
		cache_release(hcache);
	}
}

/* Rename hypertable */
static void
process_rename(Node *parsetree)
{
	RenameStmt *renamestmt = (RenameStmt *) parsetree;
	Oid			relid = RangeVarGetRelid(renamestmt->relation, NoLock, true);

	if (OidIsValid(relid))
	{
		Cache	   *hcache = hypertable_cache_pin();
		Hypertable *hentry = hypertable_cache_get_entry(hcache, relid);

		if (hentry != NULL && renamestmt->renameType == OBJECT_TABLE)
		{
			executor_level_enter();
			spi_hypertable_rename(hentry,
								  NameStr(hentry->fd.schema_name),
								  renamestmt->newname);
			executor_level_exit();
		}
		cache_release(hcache);
	}
}

static void
process_copy(Node *parsetree, const char *query_string, char *completion_tag)
{
	CopyStmt   *stmt = (CopyStmt *) parsetree;

	/*
	 * Needed to add the appropriate number of tuples to the completion tag
	 */
	uint64		processed;
	Hypertable *ht = NULL;
	Cache	   *hcache = NULL;

	executor_level_enter();

	if (stmt->is_from)
	{
		Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);

		if (OidIsValid(relid))
		{
			hcache = hypertable_cache_pin();
			ht = hypertable_cache_get_entry(hcache, relid);
		}
	}

	if (stmt->is_from && ht != NULL)
		timescaledb_DoCopy(stmt, query_string, &processed, ht);
	else
		DoCopy(stmt, query_string, &processed);

	executor_level_exit();
	processed += executor_get_additional_tuples_processed();

	if (completion_tag)
		snprintf(completion_tag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processed);

	if (NULL != hcache)
		cache_release(hcache);
}


/* Hook-intercept for ProcessUtility. Used to set COPY completion tag and */
/* renaming of hypertables. */
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

	switch (nodeTag(parsetree))
	{
		case T_TruncateStmt:
			process_truncate(parsetree);
			break;
		case T_AlterObjectSchemaStmt:
			process_alterobjectschema(parsetree);
			break;
		case T_RenameStmt:
			process_rename(parsetree);
			break;
		case T_CopyStmt:
			process_copy(parsetree, query_string, completion_tag);
			return;
		default:
			break;
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
