#include <postgres.h>
#include <nodes/parsenodes.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits_fn.h>
#include <catalog/index.h>
#include <commands/copy.h>
#include <commands/vacuum.h>
#include <commands/defrem.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <miscadmin.h>

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
	Cache	   *hcache = hypertable_cache_pin();
	ListCell   *cell;

	foreach(cell, truncatestmt->relations)
	{
		Oid			relId = RangeVarGetRelid(lfirst(cell), NoLock, true);

		if (OidIsValid(relId))
		{
			Hypertable *ht = hypertable_cache_get_entry(hcache, relId);

			if (ht != NULL)
			{
				executor_level_enter();
				spi_hypertable_truncate(ht);
				executor_level_exit();
			}
		}
	}
	cache_release(hcache);
}

/* Change the schema of a hypertable */
static void
process_alterobjectschema(Node *parsetree)
{
	AlterObjectSchemaStmt *alterstmt = (AlterObjectSchemaStmt *) parsetree;
	Oid			relid = RangeVarGetRelid(alterstmt->relation, NoLock, true);
	Cache	   *hcache;
	Hypertable *ht;

	if (!OidIsValid(relid) ||
		alterstmt->objectType != OBJECT_TABLE)
		return;

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, relid);

	if (ht != NULL)
	{
		executor_level_enter();
		spi_hypertable_rename(ht,
							  alterstmt->newschema,
							  NameStr(ht->fd.table_name));
		executor_level_exit();
	}

	cache_release(hcache);
}

/* Rename hypertable */
static void
process_rename(Node *parsetree)
{
	RenameStmt *renamestmt = (RenameStmt *) parsetree;
	Oid			relid = RangeVarGetRelid(renamestmt->relation, NoLock, true);
	Cache	   *hcache;
	Hypertable *ht;

	if (!OidIsValid(relid) ||
		renamestmt->renameType != OBJECT_TABLE)
		return;

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, relid);

	if (ht != NULL)
	{
		executor_level_enter();
		spi_hypertable_rename(ht,
							  NameStr(ht->fd.schema_name),
							  renamestmt->newname);
		executor_level_exit();
	}

	cache_release(hcache);
}

static bool
process_copy(Node *parsetree, const char *query_string, char *completion_tag)
{
	CopyStmt   *stmt = (CopyStmt *) parsetree;

	/*
	 * Needed to add the appropriate number of tuples to the completion tag
	 */
	uint64		processed;
	Hypertable *ht;
	Cache	   *hcache;
	Oid			relid;

	if (!stmt->is_from)
		return false;

	relid = RangeVarGetRelid(stmt->relation, NoLock, true);

	if (!OidIsValid(relid))
		return false;

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, relid);

	if (ht == NULL)
	{
		cache_release(hcache);
		return false;
	}

	executor_level_enter();
	timescaledb_DoCopy(stmt, query_string, &processed, ht);
	executor_level_exit();

	processed += executor_get_additional_tuples_processed();

	if (completion_tag)
		snprintf(completion_tag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processed);

	cache_release(hcache);

	return true;
}

typedef void (*process_chunk_t) (Oid chunk_relid, void *arg);

/*
 * Applies a function to each chunk of a hypertable.
 *
 * Returns the number of processed chunks, or -1 if the table was not a
 * hypertable.
 */
static int
foreach_chunk(RangeVar *rv, process_chunk_t process_chunk, void *arg)
{
	Oid			relid = hypertable_relid(rv);
	List	   *chunks;
	ListCell   *lc;
	int			n = 0;

	if (!OidIsValid(relid))
		/* Not a hypertable */
		return -1;

	chunks = find_inheritance_children(relid, NoLock);

	foreach(lc, chunks)
	{
		process_chunk(lfirst_oid(lc), arg);
		n++;
	}

	return n;
}

typedef struct VacuumCtx
{
	VacuumStmt *stmt;
	bool		is_toplevel;
} VacuumCtx;

/* Vacuums a single chunk */
static void
vacuum_chunk(Oid chunk_relid, void *arg)
{
	VacuumCtx  *ctx = (VacuumCtx *) arg;
	char	   *relschema = get_namespace_name(get_rel_namespace(chunk_relid));
	char	   *relname = get_rel_name(chunk_relid);

	ctx->stmt->relation->relname = relname;
	ctx->stmt->relation->schemaname = relschema;
	ExecVacuum(ctx->stmt, ctx->is_toplevel);
}

/* Vacuums each chunk of a hypertable */
static bool
process_vacuum(Node *parsetree, ProcessUtilityContext context)
{
	VacuumStmt *stmt = (VacuumStmt *) parsetree;
	VacuumCtx	ctx = {
		.stmt = stmt,
		.is_toplevel = (context == PROCESS_UTILITY_TOPLEVEL),
	};

	if (stmt->relation == NULL)
		/* Vacuum is for all tables */
		return false;

	if (!OidIsValid(hypertable_relid(stmt->relation)))
		return false;

	PreventCommandDuringRecovery((stmt->options & VACOPT_VACUUM) ?
								 "VACUUM" : "ANALYZE");

	return foreach_chunk(stmt->relation, vacuum_chunk, &ctx) >= 0;
}

static void
reindex_chunk(Oid chunk_relid, void *arg)
{
	ReindexStmt *stmt = (ReindexStmt *) arg;
	char	   *relschema = get_namespace_name(get_rel_namespace(chunk_relid));
	char	   *relname = get_rel_name(chunk_relid);

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			stmt->relation->relname = relname;
			stmt->relation->schemaname = relschema;
			ReindexTable(stmt->relation, stmt->options);
			break;
		case REINDEX_OBJECT_INDEX:
			/* Not supported, a.t.m. See note in process_reindex(). */
		default:
			break;
	}
}

/*
 * Reindex a hypertable and all its chunks. Currently works only for REINDEX
 * TABLE.
 */
static bool
process_reindex(Node *parsetree)
{
	ReindexStmt *stmt = (ReindexStmt *) parsetree;
	Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			if (!is_hypertable(relid))
				return false;

			PreventCommandDuringRecovery("REINDEX");

			if (foreach_chunk(stmt->relation, reindex_chunk, stmt) >= 0)
				return true;
			break;
		case REINDEX_OBJECT_INDEX:
			if (!is_hypertable(IndexGetRelation(relid, true)))
				return false;

			/*
			 * Recursing to chunks is currently not supported. Need to look up
			 * all chunk indexes that corresponds to the hypertable's index.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Reindexing of a specific index on a hypertable is currently unsupported."),
					 errhint("As a workaround, it is possible to run REINDEX TABLE to reindex all "
			  "indexes on a hypertable, including all indexes on chunks.")));
			break;
		default:
			break;
	}

	return false;
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
			if (process_copy(parsetree, query_string, completion_tag))
				return;
			break;
		case T_VacuumStmt:
			if (process_vacuum(parsetree, context))
				return;
			break;
		case T_ReindexStmt:
			if (process_reindex(parsetree))
				return;
			break;
		case T_ClusterStmt:

			/*
			 * CLUSTER command on hypertables do not yet recurse to chunks.
			 * This requires mapping a hypertable index to corresponding
			 * indexes on each chunk.
			 */
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
