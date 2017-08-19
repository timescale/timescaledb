#include <postgres.h>
#include <nodes/parsenodes.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits_fn.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/builtins.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/copy.h>
#include <commands/vacuum.h>
#include <parser/parser.h>
#include <optimizer/clauses.h>
#include <access/xact.h>
#include <storage/lmgr.h>
#include <miscadmin.h>

#include "utils.h"
#include "hypertable_cache.h"
#include "extension.h"
#include "executor.h"
#include "metadata_queries.h"
#include "copy.h"
#include "chunk.h"
#include "constraint.h"

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

static bool
dimension_set_column_name(TupleInfo *ti, void *data)
{
	RenameStmt *stmt = (RenameStmt *) data;
	FormData_dimension *dim = (FormData_dimension *) GETSTRUCT(ti->tuple);
	HeapTuple	tuple;

	if (strncmp(NameStr(dim->column_name), stmt->subname, NAMEDATALEN) != 0)
		return true;

	tuple = heap_copytuple(ti->tuple);
	dim = (FormData_dimension *) GETSTRUCT(tuple);
	strncpy(NameStr(dim->column_name), stmt->newname, NAMEDATALEN);
	catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);

	return false;
}

static ChunkConstraint *
chunk_constraint_get_from_constraint(Cache *hcache, Oid relid, const char *conname)
{
	Chunk	   *chunk = chunk_get_by_oid(relid);
	Hypertable *ht;

	if (NULL == chunk)
		return NULL;

	ht = hypertable_cache_get_entry(hcache, chunk->parent_id);

	if (NULL == ht)
		elog(ERROR, "No hypertable found for chunk %s.%s",
			 NameStr(chunk->fd.schema_name),
			 NameStr(chunk->fd.table_name));

	return chunk_get_dimension_constraint_by_constraint_name(chunk, conname);
}

static void
process_rename_table(Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);

	if (NULL == ht)
		return;

	executor_level_enter();
	spi_hypertable_rename(ht, NameStr(ht->fd.schema_name), stmt->newname);
	executor_level_exit();
}

static void
process_rename_column(Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);
	Dimension  *dim;

	if (NULL == ht)
		return;

	dim = hypertable_get_dimension(ht, stmt->subname);

	if (NULL == dim)
		return;

	/* Update the cache entry */
	strncpy(NameStr(dim->fd.column_name), stmt->newname, NAMEDATALEN);

	/* Update the column name in the dimension metadata table */
	dimension_scan_update(ht->fd.id, dimension_set_column_name, stmt);
}

static void
process_rename_constraint(Cache *hcache, Oid relid, const char *conname)
{
	ChunkConstraint *cc;

	cc = chunk_constraint_get_from_constraint(hcache, relid, conname);

	if (NULL == cc)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Cannot rename dimension constraints on chunks")));
}

static void
process_rename(Node *parsetree)
{
	RenameStmt *stmt = (RenameStmt *) parsetree;
	Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	Cache	   *hcache;

	if (!OidIsValid(relid))
		return;

	hcache = hypertable_cache_pin();

	switch (stmt->renameType)
	{
		case OBJECT_TABLE:
			process_rename_table(hcache, relid, stmt);
			break;
		case OBJECT_COLUMN:
			process_rename_column(hcache, relid, stmt);
			break;
		case OBJECT_TABCONSTRAINT:
			process_rename_constraint(hcache, relid, stmt->newname);
		default:
			break;
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

static inline const char *
typename_get_unqual_name(TypeName *tn)
{
	Value	   *name = llast(tn->names);

	return name->val.str;
}

static bool
dimension_set_column_type(TupleInfo *ti, void *data)
{
	ColumnDef  *coldef = (ColumnDef *) data;
	FormData_dimension *dim = (FormData_dimension *) GETSTRUCT(ti->tuple);
	HeapTuple	tuple;

	if (strncmp(NameStr(dim->column_name), coldef->colname, NAMEDATALEN) != 0)
		return true;

	tuple = heap_copytuple(ti->tuple);
	dim = (FormData_dimension *) GETSTRUCT(tuple);
	dim->column_type = TypenameGetTypid(typename_get_unqual_name(coldef->typeName));
	catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);

	return false;
}

/*
 * Updates a partitioning dimension CHECK constraint on all chunks.
 */
static void
alter_chunk_dimension_constraints(Hypertable *ht, Dimension *dim, ColumnDef *coldef)
{
	List	   *chunks = find_inheritance_children(ht->main_table_relid, NoLock);
	ListCell   *lc;

	foreach(lc, chunks)
	{
		Oid			chunk_relid = lfirst_oid(lc);
		const char *relschema = get_namespace_name(get_rel_namespace(chunk_relid));
		const char *relname = get_rel_name(chunk_relid);
		Chunk	   *chunk = chunk_get_by_name(relschema, relname, ht->fd.num_dimensions, true);
		ChunkConstraint *chunk_constraint = chunk_get_dimension_constraint(chunk, dim->fd.id);
		AlterTableCmd *drop,
				   *add;
		List	   *cmds = NIL;

		if (NULL == chunk_constraint)
			elog(ERROR, "No constraint found for dimension %s on chunk %s",
				 NameStr(dim->fd.column_name), NameStr(chunk->fd.table_name));

		/* Drop the old constraint */
		drop = makeNode(AlterTableCmd);
		drop->subtype = AT_DropConstraint;
		drop->name = NameStr(chunk_constraint->fd.constraint_name);
		cmds = lappend(cmds, drop);

		/* Add the new constraint */
		add = makeNode(AlterTableCmd);
		add->subtype = AT_AddConstraint;
		add->name = NameStr(chunk_constraint->fd.constraint_name);
		add->def = (Node *) constraint_make_dimension_check(chunk, dim, coldef);
		cmds = lappend(cmds, add);

		/*
		 * Forgo firing per-chunk event triggers to give the impression that
		 * the hypertable is a single table. Might reconsider in the future if
		 * we need event triggers on chunks here.
		 */
		AlterTableInternal(chunk_relid, cmds, false);
	}
}

static void
process_alter_column_type(Cache *hcache, AlterTableCmd *cmd, Oid relid)
{
	Hypertable *ht;
	Dimension  *dim;
	ColumnDef  *coldef = (ColumnDef *) cmd->def;

	ht = hypertable_cache_get_entry(hcache, relid);

	if (NULL == ht)
		return;

	dim = hypertable_get_dimension(ht, cmd->name);

	if (NULL == dim)
		return;

	if (NULL == coldef->colname)
		coldef->colname = cmd->name;

	/* Update the column type in the dimension metadata table */
	dimension_scan_update(ht->fd.id, dimension_set_column_type, coldef);

	/* Update the cache entry */
	dim->fd.column_type = TypenameGetTypid(typename_get_unqual_name(coldef->typeName));

	/*
	 * Only update the type of chunk constraints on open (time) dimensions,
	 * since closed (space) dimensions are always hashed by casting the type
	 * to 'text', which is the type visible in the constraint
	 */
	if (NULL == dim->partitioning)
		alter_chunk_dimension_constraints(ht, dim, coldef);
}

static void
process_alter_or_drop_constraint(Cache *hcache, Oid relid, const char *conname)
{
	ChunkConstraint *cc;

	cc = chunk_constraint_get_from_constraint(hcache, relid, conname);

	if (NULL != cc)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
		errmsg("Cannot add, alter or drop dimension constraints on chunks")));
}

static void
process_altertable(Node *parsetree)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	Oid			relid = AlterTableLookupRelation(stmt, NoLock);
	Cache	   *hcache = NULL;
	ListCell   *lc;

	if (!OidIsValid(relid))
		return;

	hcache = hypertable_cache_pin();

	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_AlterColumnType:
				process_alter_column_type(hcache, cmd, relid);
				break;
			case AT_AlterConstraint:
			case AT_DropConstraint:
				process_alter_or_drop_constraint(hcache, relid, cmd->name);
				break;
			default:
				break;
		}
	}

	cache_release(hcache);
}

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
		case T_AlterTableStmt:
			/* Process main table first to error out if not a valid alter */
			prev_ProcessUtility(parsetree, query_string, context, params, dest, completion_tag);
			process_altertable(parsetree);
			return;
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
