#include <postgres.h>
#include <nodes/parsenodes.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits_fn.h>
#include <catalog/index.h>
#include <catalog/objectaddress.h>
#include <commands/copy.h>
#include <commands/vacuum.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
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
	Oid			relid;
	Cache	   *hcache;
	Hypertable *ht;

	if (alterstmt->objectType != OBJECT_TABLE)
		return;

	relid = RangeVarGetRelid(alterstmt->relation, NoLock, true);

	if (!OidIsValid(relid))
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

static bool
process_drop(Node *parsetree)
{
	DropStmt   *stmt = (DropStmt *) parsetree;
	ListCell   *cell1;
	Cache	   *hcache = NULL;
	bool		handled = false;

	if (stmt->removeType != OBJECT_TABLE)
	{
		return false;
	}

	hcache = hypertable_cache_pin();

	foreach(cell1, stmt->objects)
	{
		List	   *object = lfirst(cell1);
		Oid			relid = RangeVarGetRelid(makeRangeVarFromNameList(object), NoLock, true);

		if (OidIsValid(relid))
		{
			Hypertable *ht;

			ht = hypertable_cache_get_entry(hcache, relid);
			if (NULL != ht)
			{
				if (list_length(stmt->objects) != 1)
				{
					elog(ERROR, "Cannot drop a hypertable along with other objects");
				}
				CatalogInternalCall2(DDL_DROP_HYPERTABLE, Int32GetDatum(ht->fd.id), BoolGetDatum(stmt->behavior == DROP_CASCADE));
				handled = true;
			}
		}
	}

	cache_release(hcache);
	return handled;
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

static void
process_altertable_change_owner(Hypertable *ht, AlterTableCmd *cmd, Oid relid)
{
	RoleSpec   *role;

	Assert(IsA(cmd->newowner, RoleSpec));
	role = (RoleSpec *) cmd->newowner;

	CatalogInternalCall2(DDL_CHANGE_OWNER, ObjectIdGetDatum(relid), CStringGetDatum(role->rolename));
}

static void
process_altertable_add_constraint(Hypertable *ht, const char *constraint_name)
{
	Assert(constraint_name != NULL);
	CatalogInternalCall2(DDL_ADD_CONSTRAINT, Int32GetDatum(ht->fd.id), CStringGetDatum(constraint_name));
}


static void
process_altertable_drop_constraint(Hypertable *ht, AlterTableCmd *cmd, Oid relid)
{
	char	   *constraint_name = NULL;

	constraint_name = cmd->name;
	Assert(constraint_name != NULL);
	CatalogInternalCall2(DDL_DROP_CONSTRAINT, Int32GetDatum(ht->fd.id), CStringGetDatum(constraint_name));
}

/* foreign-key constraints to hypertables are not allowed */
static void
verify_constraint(Constraint *stmt)
{
	if (stmt->contype == CONSTR_FOREIGN)
	{
		RangeVar   *primary_table = stmt->pktable;
		Oid			primary_oid = RangeVarGetRelid(primary_table, NoLock, true);

		if (OidIsValid(primary_oid))
		{
			Cache	   *hcache = hypertable_cache_pin();
			Hypertable *ht = hypertable_cache_get_entry(hcache, primary_oid);

			if (NULL != ht)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("Foreign keys to hypertables are not supported.")));
			}
			cache_release(hcache);
		}
	}
}

static void
verify_constraint_list(List *constraint_list)
{
	ListCell   *lc;

	foreach(lc, constraint_list)
	{
		Constraint *constraint = (Constraint *) lfirst(lc);

		verify_constraint(constraint);
	}
}

/* process all create table commands to make sure their constraints are kosher */
static void
process_create_table(Node *parsetree)
{
	CreateStmt *stmt = (CreateStmt *) parsetree;
	ListCell   *lc;

	verify_constraint_list(stmt->constraints);
	foreach(lc, stmt->tableElts)
	{
		ColumnDef  *column_def = (ColumnDef *) lfirst(lc);

		verify_constraint_list(column_def->constraints);
	}
}

/* process all regular-table alter commands to make sure they aren't adding
 * foreign-key constraints to hypertables */
static void
process_altertable_plain_table(Node *parsetree)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	ListCell   *lc;

	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_AddConstraint:
			case AT_AddConstraintRecurse:
				{
					Constraint *constraint = (Constraint *) cmd->def;

					Assert(IsA(cmd->def, Constraint));

					verify_constraint(constraint);
				}
			default:
				break;
		}
	}
}

static void
process_altertable(Node *parsetree)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	Oid			relid = AlterTableLookupRelation(stmt, NoLock);
	Cache	   *hcache = NULL;
	ListCell   *lc;
	Hypertable *ht;

	if (!OidIsValid(relid))
		return;

	hcache = hypertable_cache_pin();

	ht = hypertable_cache_get_entry(hcache, relid);
	if (NULL == ht)
	{
		cache_release(hcache);
		process_altertable_plain_table(parsetree);
		return;
	}

	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_ChangeOwner:
				process_altertable_change_owner(ht, cmd, relid);
				break;
			case AT_AddIndex:
				{
					IndexStmt  *stmt = (IndexStmt *) cmd->def;

					Assert(IsA(cmd->def, IndexStmt));

					Assert(stmt->isconstraint);
					process_altertable_add_constraint(ht, stmt->idxname);
				}

				/*
				 * AddConstraint sometimes transformed to AddIndex if Index is
				 * involved. different path than CREATE INDEX.
				 */
			case AT_AddConstraint:
			case AT_AddConstraintRecurse:
				{
					Constraint *stmt = (Constraint *) cmd->def;

					Assert(IsA(cmd->def, Constraint));

					process_altertable_add_constraint(ht, stmt->conname);
				}

				break;
			case AT_DropConstraint:
			case AT_DropConstraintRecurse:
				process_altertable_drop_constraint(ht, cmd, relid);
				break;
			default:
				break;
		}
	}

	cache_release(hcache);
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
		case T_AlterTableStmt:
			/* Process main table first to error out if not a valid alter */
			prev_ProcessUtility(parsetree, query_string, context, params, dest, completion_tag);
			process_altertable(parsetree);
			return;
		case T_AlterObjectSchemaStmt:
			process_alterobjectschema(parsetree);
			break;
		case T_RenameStmt:
			process_rename(parsetree);
			break;
		case T_CreateStmt:
			process_create_table(parsetree);
		case T_DropStmt:

			/*
			 * Drop associated metadata/chunks but also continue on to drop
			 * the main table. Because chunks are deleted before the main
			 * table is dropped, the drop respects CASCADE in the expected
			 * way.
			 */
			process_drop(parsetree);
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
